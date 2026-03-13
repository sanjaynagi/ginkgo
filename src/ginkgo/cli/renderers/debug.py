"""Rich renderers for ``ginkgo debug`` output."""

from __future__ import annotations

from pathlib import Path

import yaml
from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ginkgo.cli.renderers.models import _FailureDetails


def render_debug_header(*, run_dir: Path, manifest: dict[str, object], failures: int) -> Panel:
    """Render the top-level ``ginkgo debug`` report header."""
    summary = Table.grid(padding=(0, 1))
    summary.add_column(style="bold #134e4a", no_wrap=True)
    summary.add_column()
    summary.add_row("Run ID", str(manifest.get("run_id", run_dir.name)))
    summary.add_row("Workflow", str(manifest.get("workflow", "unknown")))
    summary.add_row("Status", str(manifest.get("status", "unknown")))
    summary.add_row("Failures", str(failures))
    summary.add_row("Run directory", str(run_dir))
    return Panel(
        summary,
        title="[bold #0f766e]Debug Report[/]",
        border_style="#0f766e",
        box=box.SQUARE,
        expand=False,
    )


def render_debug_failure_panel(details: _FailureDetails) -> Panel:
    """Render a failed task report for ``ginkgo debug``."""
    summary = Table.grid(padding=(0, 1))
    summary.add_column(style="bold #7f1d1d", no_wrap=True)
    summary.add_column()
    summary.add_row("Task", details.task_label)
    summary.add_row("Exit code", str(details.exit_code) if details.exit_code is not None else "?")
    if details.error:
        summary.add_row("Error", details.error)
    if details.log_path is not None:
        summary.add_row("Log", str(details.log_path))

    sections: list[object] = [summary]
    if details.inputs:
        sections.append(Text(""))
        sections.append(Text("Inputs", style="bold #7f1d1d"))
        sections.append(
            Text(yaml.safe_dump(details.inputs, sort_keys=False).rstrip(), style="#7f1d1d")
        )
    if details.log_tail:
        sections.append(Text(""))
        sections.append(Text("Log tail", style="bold #7f1d1d"))
        sections.append(Text("\n".join(details.log_tail), style="#7f1d1d"))

    return Panel(
        Group(*sections),
        title=f"[bold red]Failed Task: {details.task_label}[/]",
        border_style="red",
        box=box.SQUARE,
        expand=False,
    )
