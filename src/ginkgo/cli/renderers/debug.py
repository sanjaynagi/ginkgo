"""Rich renderers for ``ginkgo debug`` output."""

from __future__ import annotations

import yaml
from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ginkgo.cli.renderers.models import FailureDetails
from ginkgo.runtime.run_summary import RunSummary


def render_debug_header(*, summary: RunSummary, failures: int) -> Panel:
    """Render the top-level ``ginkgo debug`` report header."""
    grid = Table.grid(padding=(0, 1))
    grid.add_column(style="bold #134e4a", no_wrap=True)
    grid.add_column(overflow="fold")
    grid.add_row("Run ID", summary.run_id)
    # Paths, labels and messages are user data: as Text, brackets stay literal.
    grid.add_row("Workflow", Text(summary.workflow or "unknown"))
    grid.add_row("Status", summary.status)
    grid.add_row("Failures", str(failures))
    grid.add_row("Run directory", Text(str(summary.run_dir)))
    return Panel(
        grid,
        title="[bold #0f766e]Debug Report[/]",
        border_style="#0f766e",
        box=box.SQUARE,
        expand=False,
    )


def render_debug_failure_panel(details: FailureDetails) -> Panel:
    """Render a failed task report for ``ginkgo debug``."""
    summary = Table.grid(padding=(0, 1))
    summary.add_column(style="bold #7f1d1d", no_wrap=True)
    # Folded rather than cut with an ellipsis, so a long path stays copyable.
    summary.add_column(overflow="fold")
    summary.add_row("Task", Text(details.task_label))
    if details.ignored:
        # Both kinds of failure are worth debugging, but only one of them
        # ended the run, and the reader is owed which one this was.
        summary.add_row("Policy", "ignored - the run continued past this failure")
    if details.exit_code is not None:
        summary.add_row("Exit code", str(details.exit_code))
    if details.error:
        summary.add_row("Error", Text(details.reason_headline))
    if details.log_path is not None:
        summary.add_row("Log", Text(str(details.log_path)))

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
        title=Text(f"Failed Task: {details.task_label}", style="bold red"),
        border_style="red",
        box=box.SQUARE,
        expand=False,
    )


def render_run_failure_panel(run_error: object) -> Panel:
    """Render the run-level failure recorded in the manifest."""
    message = str(run_error) if run_error is not None else "No error recorded in the manifest."
    return Panel(
        Text(message, style="#7f1d1d"),
        title="[bold red]Run Failure[/]",
        border_style="red",
        box=box.SQUARE,
        expand=False,
    )
