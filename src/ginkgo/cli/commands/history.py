"""``ginkgo history`` — every run of one task, across runs."""

from __future__ import annotations

import json

from rich.console import Group
from rich.table import Table
from rich.text import Text

from ginkgo import query
from ginkgo.cli.common import stdout_console, new_table
from ginkgo.cli.renderers.common import task_base_name
from ginkgo.formatting import format_bytes, format_duration, format_timestamp, parse_timestamp
from ginkgo.query import TaskRow
from ginkgo.resource_history import (
    ResourceHistory,
    group_by_label,
    samples_from_rows,
    summarize,
)

__all__ = ["command_history"]

_KEY_PREFIX = 12
"""Characters of a cache key shown in the table. The full key is in ``--json``."""


def command_history(args) -> int:
    """Handle ``ginkgo history`` — how one task has fared over its runs."""
    rich_console = stdout_console()
    limit = getattr(args, "limit", 20)
    resources = getattr(args, "resources", False)
    by_label = getattr(args, "by_label", False)

    with query.open(missing_ok=True) as reader:
        rows = reader.task_history(args.task, limit=limit)
        # The distribution covers all of the task's history; --limit bounds the
        # table under it, and a percentile that moved with a display setting
        # would be a percentile of nothing in particular.
        history = reader.task_resource_history(args.task) if resources else []

    summaries = _summaries(history, by_label=by_label) if resources else []

    if getattr(args, "json", False):
        if not resources:
            print(json.dumps([row.to_payload() for row in rows], indent=2, sort_keys=True))
            return 0
        payload = {
            "task": args.task,
            "resources": [item.to_payload() for item in summaries],
            "runs": [row.to_payload() for row in rows],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    rich_console.print(f"[bold green]🌿 ginkgo history[/] [dim]{args.task}[/]\n")
    if not rows:
        rich_console.print(f"[dim]No run has a task named {args.task}.[/]")
        return 0
    if resources:
        rich_console.print(_resource_summary(summaries))
        rich_console.print()
    rich_console.print(_table(rows, resources=resources))
    return 0


def _summaries(rows: list[TaskRow], *, by_label: bool) -> list[ResourceHistory]:
    """Aggregate task rows, pooled or split by fan-out branch."""
    if by_label:
        return group_by_label(rows)
    samples, cached = samples_from_rows(rows)
    return [summarize(samples, cached=cached, runs=len(rows))]


def _resource_summary(summaries: list[ResourceHistory]) -> Group:
    """Render the distribution block printed above the per-run table."""
    return Group(*(_summary_block(item) for item in summaries))


def _summary_block(history: ResourceHistory) -> Text:
    """Render one task, or one fan-out branch, as a distribution."""
    text = Text()
    if history.label is not None:
        text.append(f"{history.label}\n", style="bold")

    if history.peak_rss_bytes is None and history.cpu_seconds is None:
        text.append(_no_measurement_line(history), style="dim")
        return text

    text.append(f"Peak RSS over {history.n} ", style="bold")
    text.append("execution" if history.n == 1 else "executions", style="bold")
    runs = "run" if history.runs == 1 else "runs"
    text.append(f" ({history.runs} {runs}, {history.cached} cached)\n", style="dim")

    peak = history.peak_rss_bytes
    if peak is not None:
        text.append("  p50 ")
        text.append(format_bytes(int(peak.p50)), style="bold")
        text.append("   p95 ")
        text.append(format_bytes(int(peak.p95)), style="bold")
        text.append("   max ")
        text.append(format_bytes(int(peak.max)), style="bold")
        text.append("\n")

    declared = history.declared_memory_gb
    if declared:
        text.append(f"  declared {_gib(declared)}")
        if history.effective_memory_gb and history.effective_memory_gb != declared:
            text.append(f"  ·  ran against {_gib(history.effective_memory_gb)} after escalation")
        headroom = history.headroom
        if headroom is not None:
            text.append(f"  ·  p95 is {headroom:.0%} of declared")
        if history.declaration_varied:
            text.append("  ·  declaration varied across runs", style="yellow")
        text.append("\n")

    cpu = history.cpu_seconds
    if cpu is not None:
        text.append("CPU time\n", style="bold")
        text.append(
            f"  p50 {format_duration(cpu.p50)}   p95 {format_duration(cpu.p95)}"
            f"   total {format_duration(cpu.total)}\n"
        )
        if history.declared_threads:
            threads = history.declared_threads
            text.append(f"  declared {threads} thread{'' if threads == 1 else 's'}\n", style="dim")

    if history.failed:
        text.append(_censored_line(history), style="yellow")
    return text


def _no_measurement_line(history: ResourceHistory) -> str:
    """Explain an empty distribution in terms of what the runs were doing."""
    if history.runs and history.cached == history.runs:
        return f"No measurements: all {history.runs} runs were served from the cache.\n"
    return "No measurements recorded for this task yet.\n"


def _censored_line(history: ResourceHistory) -> str:
    """Report failed executions as a count, never as part of the distribution.

    A task killed at its ceiling used *more than* that ceiling, so its peak is a
    lower bound. Averaging it in would pull the distribution towards the value
    that already failed.
    """
    count = history.failed
    noun = "attempt" if count == 1 else "attempts"
    floor = history.failed_peak_floor_bytes
    if floor is None:
        return f"{count} failed {noun} excluded\n"
    return f"{count} failed {noun} excluded (peak ≥ {format_bytes(floor)})\n"


def _gib(value: float) -> str:
    """Render a declared memory figure, which is whole GiB in practice."""
    return f"{value:g} GiB"


def _table(rows: list[TaskRow], *, resources: bool = False) -> Table:
    """Build the per-run table for one task.

    The Task column carries the display label where a run had one: a fan-out
    matches every sibling branch, and without the label the rows that differ
    read identically.
    """
    table = new_table()
    table.add_column("Task", style="bold", overflow="fold")
    table.add_column("Run ID", no_wrap=True)
    table.add_column("Started", no_wrap=True)
    table.add_column("Duration", justify="right")
    if resources:
        table.add_column("Peak RSS", justify="right")
        table.add_column("CPU", justify="right")
    table.add_column("Status")
    table.add_column("Cached")
    table.add_column("Cache Key", no_wrap=True)
    table.add_column("Attempts", justify="right")
    for row in rows:
        measured = (
            [
                format_bytes(row.peak_rss_bytes),
                format_duration(row.cpu_seconds),
            ]
            if resources
            else []
        )
        table.add_row(
            Text(row.display_label or task_base_name(row.name)),
            row.run_id,
            format_timestamp(parse_timestamp(row.started_at)),
            format_duration(row.duration_s),
            *measured,
            row.status,
            "yes" if row.cached else "no",
            row.cache_key[:_KEY_PREFIX] if row.cache_key else "-",
            str(row.attempts),
        )
    return table
