"""Internal CLI render-state models."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from rich.text import Text

from ginkgo.cli.common import RunMode
from ginkgo.runtime.run_summary import DONE_STATUSES, TERMINAL_STATUSES


@dataclass(kw_only=True)
class CliRunSummary:
    """Static metadata used across the CLI renderers."""

    run_id: str
    mode: RunMode
    run_dir: Path
    cores: int
    memory: int | None = None
    # Display label for the run's default executor ("local", "Kubernetes",
    # "gpu-k8s (Kubernetes)", ...). Tasks pinned to another executor still
    # dispatch there; this line describes the run default.
    executor_label: str = "local"
    # Executors tasks pin themselves to, beyond the run default.
    pinned_executors: tuple[str, ...] = ()


@dataclass
class _TaskRow:
    """Mutable render state for a single task row."""

    node_id: int
    task_name: str
    label: str
    env_label: str
    status: str = "waiting"
    started_at: float | None = None
    finished_at: float | None = None


@dataclass(kw_only=True)
class FailureDetails:
    """Renderable diagnostics for a failed task."""

    task_label: str
    exit_code: int | None
    log_path: Path | None
    log_tail: list[str]
    error: str | None = None
    failure_kind: str | None = None
    inputs: dict[str, object] | None = None
    task_kind: str | None = None
    """The task's declared kind (``"python"``, ``"shell"``, ...), when known."""
    env_label: str = "local"
    """The environment the task ran in; ``"local"`` is the CLI's own."""
    ignored: bool = False
    """Whether the run's failure policy let this failure pass."""

    @property
    def ran_in_cli_interpreter(self) -> bool:
        """Whether this task's body executed in the interpreter the CLI runs from.

        Only a ``python`` task in the local environment does. Any other kind's
        body runs in a subprocess of its own, so
        advice about *this* interpreter would not touch what failed there.
        """
        return self.task_kind == "python" and self.env_label == "local"

    @property
    def reason_headline(self) -> str | None:
        """Return the one-line root cause to show in the failure panel.

        ``error`` is often the same multi-line traceback the log tail
        already renders in full — repeating it in the Reason row buries the
        one line that actually names the failure and doubles up on a long
        traceback. This picks a concise headline instead: the error's own
        final line whenever the rest of it duplicates what the log tail
        already shows. A single-line error, or one the log tail does not
        repeat, is returned unchanged. A tool's colour escapes are dropped.
        """
        if self.error is None:
            return None
        error = Text.from_ansi(self.error).plain
        lines = [line.strip() for line in error.splitlines() if line.strip()]
        if len(lines) <= 1:
            return error
        last_line = lines[-1]
        if any(last_line in Text.from_ansi(tail_line).plain for tail_line in self.log_tail):
            return last_line
        return error


@dataclass(frozen=True, kw_only=True)
class SkipDetails:
    """One task a failure left without a result, and why.

    ``cause`` names the failure the same way ``task_label`` names the task —
    the run's own label for a task, or its base name — so that two skips are
    counted together only when they really have the same cause.
    """

    task_label: str
    cause: str
    """Why the task has no result, such as ``blocked by load``."""


@dataclass
class _TaskGroup:
    """Render state for a collapsed group of same-task invocations.

    Parameters
    ----------
    task_name
        Fully-qualified task definition name shared by all invocations.
    label
        Display label shown in the task table (e.g. ``align (×200)``).
    env_label
        Common environment label, or ``"mixed"`` if invocations differ.
    rows
        Individual task rows belonging to this group.
    """

    task_name: str
    label: str
    env_label: str
    rows: list[_TaskRow] = field(default_factory=list)

    def status_counts(self) -> Counter[str]:
        """Return a counter of task statuses across all invocations."""
        return Counter(row.status for row in self.rows)

    def is_terminal(self) -> bool:
        """Return True if every invocation has reached a terminal state."""
        return all(row.status in TERMINAL_STATUSES for row in self.rows)

    def done_count(self) -> int:
        """Return the number of invocations that produced a result: succeeded or cached."""
        return sum(1 for row in self.rows if row.status in DONE_STATUSES)

    def elapsed(self, *, now: float) -> float | None:
        """Return wall-clock seconds from earliest start to latest finish or *now*."""
        starts = [row.started_at for row in self.rows if row.started_at is not None]
        if not starts:
            return None
        earliest = min(starts)
        if self.is_terminal():
            finishes = [row.finished_at for row in self.rows if row.finished_at is not None]
            return max(finishes) - earliest if finishes else None
        return now - earliest


@dataclass(frozen=True)
class ResourceRenderState:
    """Live resource summary provider for CLI rendering."""

    provider: Callable[[], dict[str, object]]


@dataclass(frozen=True, kw_only=True)
class CliRenderedPage:
    """A page a task rendered in this run, such as a notebook's HTML."""

    task_label: str
    html_path: Path
    replayed_from_run_id: str | None = None
    """Set when a cache hit points at a page an earlier run rendered."""
