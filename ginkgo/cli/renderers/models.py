"""Internal CLI render-state models."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from ginkgo.cli.common import RunMode


@dataclass(kw_only=True)
class _RunSummary:
    """Static metadata used across the CLI renderers."""

    run_id: str
    mode: RunMode
    run_dir: Path
    cores: int
    memory: int | None = None
    executor: str = "local"


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
class _FailureDetails:
    """Renderable diagnostics for a failed task."""

    task_label: str
    exit_code: int | None
    log_path: Path | None
    log_tail: list[str]
    error: str | None = None
    inputs: dict[str, object] | None = None


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
        return all(row.status in {"cached", "succeeded", "failed"} for row in self.rows)

    def terminal_count(self) -> int:
        """Return the number of invocations in a terminal state."""
        return sum(1 for row in self.rows if row.status in {"cached", "succeeded", "failed"})

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
class _ResourceRenderState:
    """Live resource summary provider for CLI rendering."""

    provider: Callable[[], dict[str, object]]


@dataclass(frozen=True, kw_only=True)
class _NotebookSummary:
    """Rendered notebook artifact produced in a run."""

    task_label: str
    html_path: Path


@dataclass(frozen=True, kw_only=True)
class _AssetSummary:
    """Asset materialised in a run."""

    name: str
