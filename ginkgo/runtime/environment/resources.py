"""Run resource monitoring for Ginkgo process trees."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event, Lock, Thread
from typing import Any, Callable


@dataclass(frozen=True, kw_only=True)
class _ProcessTreeUsage:
    """A single process-tree usage sample.

    Parameters
    ----------
    cpu_percent : float
        Total CPU usage percentage across the tracked process tree.
    rss_bytes : int
        Total resident memory in bytes across the tracked process tree.
    process_count : int
        Number of processes included in the sample.
    """

    cpu_percent: float
    rss_bytes: int
    process_count: int


class RunResourceMonitor:
    """Capture CPU and memory usage for the current Ginkgo process tree.

    Parameters
    ----------
    root_pid : int
        Root process identifier to track recursively.
    sample_interval_seconds : float
        Interval between resource samples.
    sink : Callable[[dict[str, Any]], None] | None
        Optional callback invoked whenever the summary changes.
    """

    def __init__(
        self,
        *,
        root_pid: int,
        sample_interval_seconds: float = 1.0,
        sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._root_pid = root_pid
        self._sample_interval_seconds = sample_interval_seconds
        self._sink = sink
        self._stop_event = Event()
        self._lock = Lock()
        self._thread: Thread | None = None
        self._status = "pending"
        self._reason: str | None = None
        self._updated_at: str | None = None
        self._sample_count = 0
        self._cpu_total = 0.0
        self._rss_total = 0
        self._process_total = 0
        self._current: dict[str, Any] | None = None
        self._peak: dict[str, Any] | None = None

    def start(self) -> None:
        """Start background resource sampling."""
        if shutil.which("ps") is None:
            self._mark_unavailable(reason="`ps` command not available")
            return

        if self._thread is not None:
            return

        self._status = "running"
        self._thread = Thread(target=self._run, name="ginkgo-resource-monitor", daemon=True)
        self._thread.start()

    def stop(self, *, final_status: str = "completed") -> dict[str, Any]:
        """Stop sampling and return the final resource summary.

        Parameters
        ----------
        final_status : str
            Final status label to persist in the summary.

        Returns
        -------
        dict[str, Any]
            A JSON/YAML-serializable resource summary.
        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._sample_interval_seconds * 2))
            self._thread = None

        with self._lock:
            if self._status != "unavailable":
                self._status = final_status
                self._updated_at = _timestamp()
            snapshot = self._snapshot_locked()

        self._emit(snapshot)
        return snapshot

    def current_summary(self) -> dict[str, Any]:
        """Return the latest resource summary snapshot."""
        with self._lock:
            return self._snapshot_locked()

    def _run(self) -> None:
        """Sample process-tree usage until the monitor is stopped."""
        while not self._stop_event.is_set():
            try:
                usage = _collect_process_tree_usage(root_pid=self._root_pid)
            except Exception as exc:
                self._mark_unavailable(reason=str(exc))
                return

            self._record_sample(usage=usage)
            if self._stop_event.wait(self._sample_interval_seconds):
                return

    def _record_sample(self, *, usage: _ProcessTreeUsage) -> None:
        """Update rolling aggregates from one process-tree sample."""
        sampled_at = _timestamp()

        with self._lock:
            self._sample_count += 1
            self._cpu_total += usage.cpu_percent
            self._rss_total += usage.rss_bytes
            self._process_total += usage.process_count
            self._updated_at = sampled_at
            self._current = {
                "cpu_percent": round(usage.cpu_percent, 2),
                "rss_bytes": usage.rss_bytes,
                "process_count": usage.process_count,
                "sampled_at": sampled_at,
            }

            if self._peak is None:
                self._peak = dict(self._current)
            else:
                if usage.cpu_percent >= float(self._peak["cpu_percent"]):
                    self._peak["cpu_percent"] = round(usage.cpu_percent, 2)
                    self._peak["sampled_at"] = sampled_at
                if usage.rss_bytes >= int(self._peak["rss_bytes"]):
                    self._peak["rss_bytes"] = usage.rss_bytes
                    self._peak["sampled_at"] = sampled_at
                if usage.process_count >= int(self._peak["process_count"]):
                    self._peak["process_count"] = usage.process_count
                    self._peak["sampled_at"] = sampled_at

            snapshot = self._snapshot_locked()

        self._emit(snapshot)

    def _mark_unavailable(self, *, reason: str) -> None:
        """Freeze the monitor in an unavailable state."""
        with self._lock:
            self._status = "unavailable"
            self._reason = reason
            self._updated_at = _timestamp()
            snapshot = self._snapshot_locked()

        self._emit(snapshot)

    def _snapshot_locked(self) -> dict[str, Any]:
        """Build a serializable resource summary while holding the state lock."""
        average: dict[str, Any] | None = None
        if self._sample_count > 0:
            average = {
                "cpu_percent": round(self._cpu_total / self._sample_count, 2),
                "rss_bytes": int(round(self._rss_total / self._sample_count)),
                "process_count": round(self._process_total / self._sample_count, 2),
            }

        snapshot: dict[str, Any] = {
            "status": self._status,
            "scope": "process_tree",
            "sample_interval_seconds": self._sample_interval_seconds,
            "sample_count": self._sample_count,
            "updated_at": self._updated_at,
            "current": None if self._current is None else dict(self._current),
            "peak": None if self._peak is None else dict(self._peak),
            "average": average,
        }
        if self._reason is not None:
            snapshot["reason"] = self._reason
        return snapshot

    def _emit(self, snapshot: dict[str, Any]) -> None:
        """Send a snapshot to the configured sink if one exists."""
        if self._sink is None:
            return
        self._sink(snapshot)


def _collect_process_tree_usage(*, root_pid: int) -> _ProcessTreeUsage:
    """Return a process-tree resource sample rooted at ``root_pid``."""
    completed = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,%cpu=,rss="],
        check=True,
        capture_output=True,
        text=True,
    )
    rows = _parse_ps_rows(completed.stdout)
    descendants = _descendant_process_ids(rows=rows, root_pid=root_pid)
    if not descendants:
        return _ProcessTreeUsage(cpu_percent=0.0, rss_bytes=0, process_count=0)

    cpu_percent = 0.0
    rss_bytes = 0
    for pid in descendants:
        row = rows[pid]
        cpu_percent += row["cpu_percent"]
        rss_bytes += row["rss_kb"] * 1024

    return _ProcessTreeUsage(
        cpu_percent=cpu_percent,
        rss_bytes=rss_bytes,
        process_count=len(descendants),
    )


def _parse_ps_rows(output: str) -> dict[int, dict[str, float | int]]:
    """Parse ``ps`` output into a PID-indexed mapping."""
    rows: dict[int, dict[str, float | int]] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 3)
        if len(parts) != 4:
            continue
        pid_text, ppid_text, cpu_text, rss_text = parts
        rows[int(pid_text)] = {
            "ppid": int(ppid_text),
            "cpu_percent": float(cpu_text),
            "rss_kb": int(rss_text),
        }
    return rows


def _descendant_process_ids(*, rows: dict[int, dict[str, float | int]], root_pid: int) -> set[int]:
    """Return all tracked descendant PIDs rooted at ``root_pid``."""
    if root_pid not in rows:
        return set()

    children_by_parent: dict[int, list[int]] = {}
    for pid, row in rows.items():
        ppid = int(row["ppid"])
        children_by_parent.setdefault(ppid, []).append(pid)

    descendants = {root_pid}
    frontier = [root_pid]
    while frontier:
        parent = frontier.pop()
        for child in children_by_parent.get(parent, []):
            if child in descendants:
                continue
            descendants.add(child)
            frontier.append(child)
    return descendants


def _timestamp() -> str:
    return datetime.now(UTC).isoformat()
