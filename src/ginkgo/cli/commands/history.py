"""``ginkgo history`` — every run of one task, across runs."""

from __future__ import annotations

import json
import sys

from rich import box
from rich.table import Table

from ginkgo import query
from ginkgo.cli.common import console
from ginkgo.cli.renderers.common import task_base_name
from ginkgo.formatting import format_duration, format_timestamp, parse_timestamp
from ginkgo.query import TaskRow

__all__ = ["command_history"]

_KEY_PREFIX = 12
"""Characters of a cache key shown in the table. The full key is in ``--json``."""


def command_history(args) -> int:
    """Handle ``ginkgo history`` — how one task has fared over its runs."""
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    rich_console = console(sys.stdout, width=None if is_tty else 160)

    with query.open(missing_ok=True) as reader:
        rows = reader.task_history(args.task, limit=getattr(args, "limit", 20))

    if getattr(args, "json", False):
        print(json.dumps([row.to_payload() for row in rows], indent=2, sort_keys=True))
        return 0

    rich_console.print(f"[bold green]🌿 ginkgo history[/] [dim]{args.task}[/]\n")
    if not rows:
        rich_console.print(f"[dim]No run has a task named {args.task}.[/]")
        return 0
    rich_console.print(_table(rows))
    return 0


def _table(rows: list[TaskRow]) -> Table:
    """Build the per-run table for one task.

    The Task column carries the display label where a run had one: a fan-out
    matches every sibling branch, and without the label the rows that differ
    read identically.
    """
    table = Table(
        box=box.SQUARE,
        border_style="#0f766e",
        header_style="bold #134e4a",
        expand=False,
    )
    table.add_column("Task", style="bold", overflow="fold")
    table.add_column("Run ID", no_wrap=True)
    table.add_column("Started", no_wrap=True)
    table.add_column("Duration", justify="right")
    table.add_column("Status")
    table.add_column("Cached")
    table.add_column("Cache Key", no_wrap=True)
    table.add_column("Attempts", justify="right")
    for row in rows:
        table.add_row(
            row.display_label or task_base_name(row.name),
            row.run_id,
            format_timestamp(parse_timestamp(row.started_at)),
            format_duration(row.duration_s),
            row.status,
            "yes" if row.cached else "no",
            row.cache_key[:_KEY_PREFIX] if row.cache_key else "-",
            str(row.attempts),
        )
    return table
