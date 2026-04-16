"""Async remote executor protocol for submitting tasks to remote backends.

Defines the ``RemoteExecutor`` protocol that remote backends (Kubernetes,
cloud batch services) implement to receive task submissions from the
evaluator.  The evaluator dispatches to remote backends through this
protocol instead of the local process-pool executor.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class RemoteJobState(Enum):
    """Lifecycle states for a remotely submitted job."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Return whether the state is final."""
        return self in {
            RemoteJobState.SUCCEEDED,
            RemoteJobState.FAILED,
            RemoteJobState.CANCELLED,
        }


@dataclass(frozen=True, kw_only=True)
class RemoteJobResult:
    """Result returned when a remote job reaches a terminal state.

    Parameters
    ----------
    state : RemoteJobState
        Final job state.
    payload : dict[str, Any]
        Worker result dictionary, same shape as ``run_task`` return value.
    exit_code : int | None
        Process exit code if available.
    logs : str | None
        Tail of job logs.
    """

    state: RemoteJobState
    payload: dict[str, Any]
    exit_code: int | None = None
    logs: str | None = None


@dataclass(kw_only=True)
class RemoteDispatchStats:
    """Counters collected during remote task dispatch.

    Thread-safe — multiple watcher threads may record concurrently.
    """

    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    jobs_submitted: int = 0
    jobs_succeeded: int = 0
    jobs_failed: int = 0
    total_pending_seconds: float = 0.0
    total_running_seconds: float = 0.0
    upload_bytes: int = 0
    download_bytes: int = 0

    def record_submit(self) -> None:
        with self._lock:
            self.jobs_submitted += 1

    def record_terminal(self, *, state: RemoteJobState) -> None:
        with self._lock:
            if state == RemoteJobState.SUCCEEDED:
                self.jobs_succeeded += 1
            else:
                self.jobs_failed += 1

    def record_phase_time(self, *, pending: float, running: float) -> None:
        with self._lock:
            self.total_pending_seconds += pending
            self.total_running_seconds += running

    def record_upload(self, *, nbytes: int) -> None:
        with self._lock:
            self.upload_bytes += nbytes

    def record_download(self, *, nbytes: int) -> None:
        with self._lock:
            self.download_bytes += nbytes

    def summary(self) -> str | None:
        """One-line human-readable summary, or None if no remote work."""
        if self.jobs_submitted == 0:
            return None
        parts = [f"{self.jobs_submitted} remote"]
        if self.jobs_succeeded:
            parts.append(f"{self.jobs_succeeded} succeeded")
        if self.jobs_failed:
            parts.append(f"{self.jobs_failed} failed")
        if self.total_pending_seconds > 0:
            parts.append(f"pending {self.total_pending_seconds:.0f}s")
        if self.total_running_seconds > 0:
            parts.append(f"running {self.total_running_seconds:.0f}s")
        if self.upload_bytes > 0:
            parts.append(f"{_fmt_bytes(self.upload_bytes)} uploaded")
        if self.download_bytes > 0:
            parts.append(f"{_fmt_bytes(self.download_bytes)} downloaded")
        return ", ".join(parts)


def _fmt_bytes(n: int) -> str:
    """Format a byte count as a human-readable string."""
    if n >= 1 << 30:
        return f"{n / (1 << 30):.1f} GiB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.0f} MiB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.0f} KiB"
    return f"{n} B"


@runtime_checkable
class RemoteJobHandle(Protocol):
    """Handle to a running remote job.

    Provides polling, blocking wait, cancellation, and log access.
    """

    @property
    def job_id(self) -> str:
        """Stable identifier for the remote job."""
        ...

    def state(self) -> RemoteJobState:
        """Poll the current job state."""
        ...

    def result(self) -> RemoteJobResult:
        """Block until the job reaches a terminal state and return the result."""
        ...

    def cancel(self) -> None:
        """Request cancellation of the remote job."""
        ...

    def logs_tail(self, *, lines: int = 100) -> str:
        """Return the last *lines* lines of job output."""
        ...


@runtime_checkable
class RemoteExecutor(Protocol):
    """Protocol for submitting tasks to remote execution backends.

    Implementations receive a worker payload dictionary (same shape as
    the local ``run_task`` input) augmented with a ``resources`` key
    containing scheduler resource declarations.
    """

    def submit(self, *, attempt: dict[str, Any]) -> RemoteJobHandle:
        """Submit a task attempt and return a handle for polling.

        Parameters
        ----------
        attempt : dict[str, Any]
            Worker payload dictionary with an additional ``resources``
            key mapping to ``{"threads": int, "memory_gb": int}``.

        Returns
        -------
        RemoteJobHandle
        """
        ...
