"""Internal CLI render-state models."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ginkgo.cli.common import RunMode


@dataclass(kw_only=True)
class _RunSummary:
    """Static metadata used across the CLI renderers."""

    run_id: str
    mode: RunMode
    run_dir: Path
    memory: int | None = None


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


@dataclass(frozen=True)
class _ResourceRenderState:
    """Live resource summary provider for CLI rendering."""

    provider: Callable[[], dict[str, object]]
