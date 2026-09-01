"""Internal CLI render-state models."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from ginkgo.cli.common import RunMode
from ginkgo.runtime.run_summary import TERMINAL_STATUSES


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

    @property
    def ran_in_cli_interpreter(self) -> bool:
        """Whether this task's body executed in the interpreter the CLI runs from.

        Only a ``python`` task in the local environment does. A shell, script
        or notebook body runs in a subprocess or a kernel of its own, so
        advice about *this* interpreter would not touch what failed there.
        """
        return self.task_kind == "python" and self.env_label == "local"


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

    def terminal_count(self) -> int:
        """Return the number of invocations in a terminal state."""
        return sum(1 for row in self.rows if row.status in TERMINAL_STATUSES)

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
class CliNotebookSummary:
    """Rendered notebook artifact produced in a run."""

    task_label: str
    html_path: Path
    render_status: str | None = None
    render_error: str | None = None
    replayed_from_run_id: str | None = None
    """Set when a cache hit reused an artifact an earlier run produced."""

    @property
    def render_failed(self) -> bool:
        """Return True when the HTML export step failed for this notebook."""
        return self.render_status == "failed"

    @property
    def replayed(self) -> bool:
        """Return True when this run reused an earlier run's artifact."""
        return self.replayed_from_run_id is not None


@dataclass(frozen=True, kw_only=True)
class CliAssetSummary:
    """Asset materialised in a run."""

    name: str
