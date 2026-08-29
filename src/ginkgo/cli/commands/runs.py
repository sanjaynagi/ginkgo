"""``ginkgo runs`` — what has been run in this workspace, and what one run did."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from rich import box
from rich.table import Table

from ginkgo import query
from ginkgo.cli.common import console, open_run
from ginkgo.formatting import format_duration, format_timestamp, parse_timestamp
from ginkgo.query import RunRow
from ginkgo.runtime.run_summary import RunSummary

__all__ = ["command_runs"]


def command_runs(args) -> int:
    """Handle ``ginkgo runs`` — ``ls`` for the index, ``show`` for one run."""
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    rich_console = console(sys.stdout, width=None if is_tty else 160)
    as_json = bool(getattr(args, "json", False))

    if args.runs_command == "ls":
        with query.open(missing_ok=True) as reader:
            rows = reader.runs(
                workflow=getattr(args, "workflow", None),
                status=getattr(args, "status", None),
                since=getattr(args, "since", None),
                limit=getattr(args, "limit", 20),
            )
        return _render_ls(rich_console, rows=rows, as_json=as_json)

    with open_run(args.run_id) as (reader, run_id):
        summary = reader.run(run_id)
    return _render_show(rich_console, summary=summary, as_json=as_json)


def _render_ls(rich_console, *, rows: list[RunRow], as_json: bool) -> int:
    """Print the run index as JSON or as a table."""
    if as_json:
        print(json.dumps([row.to_payload() for row in rows], indent=2, sort_keys=True))
        return 0

    rich_console.print("[bold green]🌿 ginkgo runs[/] [bold]ls[/]\n")
    if not rows:
        rich_console.print("[dim]No runs recorded in this workspace.[/]")
        return 0

    table = Table(
        box=box.SQUARE,
        border_style="#0f766e",
        header_style="bold #134e4a",
        expand=False,
    )
    table.add_column("Run ID", style="bold", no_wrap=True)
    table.add_column("Workflow", overflow="fold")
    table.add_column("Status")
    table.add_column("Started", no_wrap=True)
    table.add_column("Duration", justify="right")
    for row in rows:
        table.add_row(
            row.run_id,
            Path(row.workflow).name if row.workflow else "-",
            row.status,
            format_timestamp(parse_timestamp(row.started_at)),
            format_duration(row.duration_s),
        )
    rich_console.print(table)
    return 0


def _render_show(rich_console, *, summary: RunSummary, as_json: bool) -> int:
    """Print one run as JSON or as a header and a task table."""
    if as_json:
        print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
        return 0

    rich_console.print(f"[bold green]🌿 ginkgo runs[/] [bold]show[/] [dim]{summary.run_id}[/]\n")
    rich_console.print(f"Workflow: [bold]{summary.workflow_label}[/]")
    rich_console.print(f"Status: [bold]{summary.status}[/]")
    rich_console.print(f"Started: {format_timestamp(summary.started_at)}")
    rich_console.print(f"Duration: {format_duration(summary.duration_s)}")
    rich_console.print(f"Run directory: {summary.run_dir}")
    if summary.error:
        rich_console.print(f"Error: [red]{summary.error}[/]")

    if not summary.tasks:
        rich_console.print("\n[dim]This run recorded no tasks.[/]")
        return 0

    table = Table(
        box=box.SQUARE,
        border_style="#0f766e",
        header_style="bold #134e4a",
        expand=False,
    )
    table.add_column("Task", style="bold", overflow="fold")
    table.add_column("Status")
    table.add_column("Cached")
    table.add_column("Duration", justify="right")
    table.add_column("Attempts", justify="right")
    for task in summary.tasks:
        table.add_row(
            task.display_label or task.base_name,
            task.status,
            "yes" if task.cached else "no",
            format_duration(task.duration_s),
            str(task.attempts),
        )
    rich_console.print("")
    rich_console.print(table)
    return 0
