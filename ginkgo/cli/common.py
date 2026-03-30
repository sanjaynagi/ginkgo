"""Shared CLI constants and utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from rich.console import Console

from ginkgo.runtime.provenance import latest_run_dir

RUNS_ROOT = Path(".ginkgo") / "runs"
CACHE_ROOT = Path(".ginkgo") / "cache"
ASSETS_ROOT = Path(".ginkgo") / "assets"
RunMode = Literal["default", "verbose", "agent", "agent_verbose"]


def console(output_stream, *, width: int | None = None) -> Console:
    """Build the Rich console used by the CLI."""
    return Console(
        file=output_stream,
        highlight=False,
        soft_wrap=False,
        force_terminal=getattr(output_stream, "isatty", lambda: False)(),
        width=width,
    )


def resolve_run_dir(run_id: str | None) -> Path:
    """Resolve a run id or return the latest run directory."""
    if run_id is not None:
        run_dir = RUNS_ROOT / run_id
        if not run_dir.is_dir():
            raise FileNotFoundError(f"Run not found: {run_id}")
        return run_dir

    latest = latest_run_dir(RUNS_ROOT)
    if latest is None:
        raise FileNotFoundError(f"No runs found in {RUNS_ROOT}")
    return latest
