"""``ginkgo models`` — list model assets produced by the latest run."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any

from rich import box
from rich.table import Table

from ginkgo.cli.common import console, resolve_run_dir
from ginkgo.runtime.run_summary import RunSummary


@dataclass(frozen=True, kw_only=True)
class ModelRow:
    """One display row for ``ginkgo models``."""

    task: str
    name: str
    framework: str
    metrics: str
    version_id: str


def command_models(args) -> int:
    """Handle ``ginkgo models`` — list models produced by a run."""
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    rich_console = console(sys.stdout, width=None if is_tty else 160)

    run_id = getattr(args, "run_id", None)
    try:
        run_dir = resolve_run_dir(run_id)
    except FileNotFoundError as exc:
        rich_console.print(f"[red]{exc}[/]")
        return 1

    summary = RunSummary.load(run_dir)
    rows = collect_model_rows(run_summary=summary)

    rich_console.print(f"[bold green]🌿 ginkgo models[/] [dim]run={summary.run_id}[/]\n")
    if not rows:
        rich_console.print("[dim]No model assets in this run.[/]")
        return 0

    table = Table(
        box=box.SQUARE,
        border_style="#0f766e",
        header_style="bold #134e4a",
        expand=False,
    )
    table.add_column("Task", style="bold", overflow="fold")
    table.add_column("Name", overflow="fold")
    table.add_column("Framework")
    table.add_column("Metrics", overflow="fold")
    table.add_column("Version", overflow="fold")
    for row in rows:
        table.add_row(row.task, row.name, row.framework, row.metrics, row.version_id)
    rich_console.print(table)
    return 0


def collect_model_rows(*, run_summary: RunSummary) -> list[ModelRow]:
    """Return one row per model asset materialised in the run.

    Parameters
    ----------
    run_summary : RunSummary
        Parsed run summary whose task entries carry rendered asset refs.

    Returns
    -------
    list[ModelRow]
        Rows ordered by task node id, matching the run's task ordering.
    """
    rows: list[ModelRow] = []
    for task in run_summary.tasks:
        for asset in task.assets:
            if asset.get("namespace") != "model":
                continue
            metadata = asset.get("metadata") or {}
            rows.append(
                ModelRow(
                    task=task.base_name,
                    name=str(asset.get("name", "-")),
                    framework=str(metadata.get("framework", "-")),
                    metrics=_format_metrics(metadata.get("metrics") or {}),
                    version_id=str(asset.get("version_id", "-")),
                )
            )
    return rows


def _format_metrics(metrics: dict[str, Any]) -> str:
    """Format a metrics dict as a compact ``name=value`` list."""
    if not metrics:
        return "-"
    parts: list[str] = []
    for name in sorted(metrics):
        value = metrics[name]
        if isinstance(value, float):
            parts.append(f"{name}={value:.4g}")
        else:
            parts.append(f"{name}={value}")
    return ", ".join(parts)


__all__ = ["ModelRow", "collect_model_rows", "command_models"]
