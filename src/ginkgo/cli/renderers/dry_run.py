"""Rich rendering for the ``ginkgo run --dry-run`` execution-plan preview."""

from __future__ import annotations

from collections import Counter

from rich.console import Console
from rich.text import Text

from ginkgo.cli.renderers.common import environment_label
from ginkgo.runtime.dry_run import CacheStatus, DryRunPlan, PlannedTask, PlanWave

# Fan-out groups larger than this collapse to a single line unless --verbose.
_COLLAPSE_THRESHOLD = 8

# Longest task-label column before truncation.
_MAX_LABEL_WIDTH = 46

_GLYPH: dict[CacheStatus, str] = {"cached": "✓", "will_run": "•", "unknown": "?"}
_STYLE: dict[CacheStatus, str] = {"cached": "green", "will_run": "cyan", "unknown": "yellow"}
_TAG: dict[CacheStatus, str] = {
    "cached": "[cached]",
    "will_run": "[will run]",
    "unknown": "[unknown]",
}


def render_dry_run_plan(*, plan: DryRunPlan, console: Console, verbose: bool) -> None:
    """Print a wave-grouped static execution plan for a dry run.

    Parameters
    ----------
    plan : DryRunPlan
        The plan to render.
    console : Console
        Destination Rich console.
    verbose : bool
        When ``True``, expand every fan-out branch instead of collapsing
        large groups.
    """
    console.print(_header(plan))

    if plan.task_count == 0:
        console.print(Text("  no tasks in workflow", style="dim"))
        return

    label_width = min(
        _MAX_LABEL_WIDTH,
        max(len(task.label) for wave in plan.waves for task in wave.tasks),
    )
    for wave in plan.waves:
        console.print()
        _render_wave(wave=wave, console=console, label_width=label_width, verbose=verbose)

    console.print()
    console.print(_summary(plan))


def _header(plan: DryRunPlan) -> Text:
    """Return the one-line plan header."""
    header = Text("Dry run", style="bold")
    waves = "wave" if plan.wave_count == 1 else "waves"
    tasks = "task" if plan.task_count == 1 else "tasks"
    header.append(f"  ·  {plan.task_count} {tasks}  ·  {plan.wave_count} {waves}", style="dim")
    if plan.task_count > 0 and plan.cached_count == plan.task_count:
        header.append("  ·  all cached, run would be a no-op", style="green")
    return header


def _render_wave(*, wave: PlanWave, console: Console, label_width: int, verbose: bool) -> None:
    """Print one wave: a header line followed by its tasks."""
    header = Text(f"Wave {wave.index}", style="bold")
    if len(wave.tasks) > 1:
        header.append(f"  ·  {len(wave.tasks)} tasks", style="dim")
    console.print(header)

    # Fan-out branches of one mapped task collapse to a single line.
    groups: dict[str, list[PlannedTask]] = {}
    for task in wave.tasks:
        if task.mapped:
            groups.setdefault(task.base_name, []).append(task)

    collapsed: set[str] = set()
    for task in wave.tasks:
        group = groups.get(task.base_name) if task.mapped else None
        if group is not None and len(group) > _COLLAPSE_THRESHOLD and not verbose:
            if task.base_name not in collapsed:
                collapsed.add(task.base_name)
                _render_collapsed_group(base_name=task.base_name, group=group, console=console)
            continue
        console.print(_render_task_line(task=task, label_width=label_width))


def _render_task_line(*, task: PlannedTask, label_width: int) -> Text:
    """Return the rendered line for a single task."""
    style = _STYLE[task.cache_status]
    label = task.label
    if len(label) > label_width:
        label = label[: label_width - 1] + "…"

    line = Text("  ")
    line.append(f"{_GLYPH[task.cache_status]} ", style=style)
    line.append(label.ljust(label_width))
    line.append("  ")
    line.append(_TAG[task.cache_status], style=style)
    if task.kind != "python":
        line.append(f"  · {task.kind}", style="dim")
    if task.env is not None:
        line.append(f"  · {environment_label(task.env)}", style="dim")
    return line


def _render_collapsed_group(*, base_name: str, group: list[PlannedTask], console: Console) -> None:
    """Print a single collapsed line for a large fan-out group."""
    counts = Counter(task.cache_status for task in group)
    breakdown = ", ".join(
        f"{counts[status]} {_TAG[status].strip('[]')}"
        for status in ("cached", "will_run", "unknown")
        if counts[status]
    )
    line = Text("  ")
    line.append(f"{base_name} × {len(group)}", style="bold")
    line.append(f"   {breakdown}", style="dim")
    console.print(line)

    shown = 3
    sample = " · ".join(task.label for task in group[:shown])
    detail = Text(f"    {sample}", style="dim")
    remaining = len(group) - shown
    if remaining > 0:
        detail.append(f"  (+{remaining} more — --verbose for all)", style="dim")
    console.print(detail)


def _summary(plan: DryRunPlan) -> Text:
    """Return the trailing resource / no-execution summary line."""
    resources = plan.resources
    parts: list[str] = []
    if resources.peak_wave_threads:
        parts.append(
            f"{resources.peak_wave_threads} cores peak (wave {resources.peak_wave_index})"
        )
    if resources.peak_wave_memory_gb:
        parts.append(f"{resources.peak_wave_memory_gb} GiB peak")
    if resources.gpu_task_count:
        suffix = "" if resources.gpu_task_count == 1 else "s"
        parts.append(f"{resources.gpu_task_count} GPU task{suffix}")
    parts.append("no tasks executed")
    return Text("  ·  ".join(parts), style="dim")
