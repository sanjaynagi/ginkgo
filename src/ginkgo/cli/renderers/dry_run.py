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

# Legend for the one status that is not self-explanatory.
_UNKNOWN_MEANING = (
    "cache status cannot be known without running something: an input is an "
    "upstream output the run restores from the cache first, or a script, notebook "
    "or subworkflow task's body is more than a single call a dry run can work out"
)

# Legend for an unknown task that only waits on its upstream tasks.
_UPSTREAM_PENDING = "after upstream"
_UPSTREAM_PENDING_MEANING = (
    "an upstream task runs first; this task is served from cache if that task's "
    "output comes out unchanged and nothing of its own changed"
)

# Why a task will run, by ``ginkgo cache explain`` reason code.
_RUN_REASON: dict[str, str] = {
    "no_prior_entry": "no prior entry",
    "source_hash_changed": "source changed",
    "version_bump": "version changed",
    "env_changed": "environment changed",
    "input_changed": "input changed",
    "cache_key_changed": "cache key changed",
    "rerun_requested": "--rerun",
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
        large groups, and report the errors the cache probe hit.
    """
    console.print(_header(plan))

    if plan.task_count == 0:
        console.print(Text("  no tasks in workflow", style="dim"))
        _render_dropped(plan=plan, console=console)
        return

    label_width = min(
        _MAX_LABEL_WIDTH,
        max(len(task.label) for wave in plan.waves for task in wave.tasks),
    )
    for wave in plan.waves:
        console.print()
        _render_wave(wave=wave, console=console, label_width=label_width, verbose=verbose)

    unknown = [
        task for wave in plan.waves for task in wave.tasks if task.cache_status == "unknown"
    ]
    if unknown:
        console.print()
    if any(task.run_reasons for task in unknown):
        console.print(
            Text(
                f"  {_GLYPH['unknown']} [unknown: {_UPSTREAM_PENDING}]  {_UPSTREAM_PENDING_MEANING}",
                style="dim",
            )
        )
    if any(not task.run_reasons for task in unknown):
        console.print(
            Text(f"  {_GLYPH['unknown']} {_TAG['unknown']}  {_UNKNOWN_MEANING}", style="dim")
        )

    _render_fan_outs(plan=plan, console=console)
    _render_dropped(plan=plan, console=console)
    _render_problems(plan=plan, console=console)
    _render_probe_failures(plan=plan, console=console, verbose=verbose)

    console.print()
    console.print(_summary(plan))


def _render_fan_outs(*, plan: DryRunPlan, console: Console) -> None:
    """Say which tasks add tasks only when they run, and so are missing from the plan."""
    if not plan.fan_out_labels:
        return
    console.print()
    names = ", ".join(plan.fan_out_labels)
    one = len(plan.fan_out_labels) == 1
    console.print(
        Text(
            f"  ↳ {names} {'calls' if one else 'call'} tasks in {'its' if one else 'their'} "
            f"body: the tasks {'it returns are' if one else 'they return are'} added at run "
            "time, so they are not in this plan or its counts",
            style="dim",
        )
    )


def _render_dropped(*, plan: DryRunPlan, console: Console) -> None:
    """Print the calls the flow constructed but never returned."""
    if not plan.dropped_labels:
        return

    console.print()
    console.print(Text("Dropped (not reachable from the flow return value)", style="yellow"))
    for label in plan.dropped_labels:
        console.print(Text(f"  ! {label}", style="yellow"))


def _render_problems(*, plan: DryRunPlan, console: Console) -> None:
    """Print the input-contract violations the probe could already prove."""
    if not plan.diagnostics:
        return

    console.print()
    console.print(Text("Problems — this run would fail", style="bold red"))
    for diagnostic in plan.diagnostics:
        console.print(Text(f"  ✗ {diagnostic.label}", style="red"))
        console.print(Text(f"    {diagnostic.message}", style="dim"))


def _render_probe_failures(*, plan: DryRunPlan, console: Console, verbose: bool) -> None:
    """Print the errors the cache probe hit, under ``--verbose``.

    These say nothing about the workflow — only that Ginkgo could not work out
    a task's cache status, which is why they read separately from
    **Problems** and never fail the command. The count still shows without
    ``--verbose``, so a silent probe failure cannot hide behind ``[unknown]``
    (#294).
    """
    if not plan.probe_failures:
        return

    console.print()
    count = len(plan.probe_failures)
    tasks = "task" if count == 1 else "tasks"
    console.print(
        Text(f"Cache status could not be determined for {count} {tasks}", style="yellow")
    )
    if not verbose:
        console.print(Text("  (--verbose for details)", style="dim"))
        return

    for failure in plan.probe_failures:
        console.print(Text(f"  ? {failure.label}  ·  {failure.stage}", style="yellow"))
        console.print(Text(f"    {failure.exception_type}: {failure.message}", style="dim"))


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
    line.append(_tag(task), style=style)
    if task.kind != "python":
        line.append(f"  · {task.kind}", style="dim")
    if task.env is not None:
        line.append(f"  · {environment_label(task.env)}", style="dim")
    return line


def _tag(task: PlannedTask) -> str:
    """Return a task's status tag, naming why it will run when known."""
    if not task.run_reasons:
        return _TAG[task.cache_status]
    if task.cache_status == "unknown":
        return f"[unknown: {_UPSTREAM_PENDING}]"
    return f"[will run: {_run_reason(task)}]"


def _run_reason(task: PlannedTask) -> str:
    """Return the readable reasons a ``will_run`` task misses the cache."""
    return ", ".join(_RUN_REASON.get(code, code) for code in task.run_reasons)


def _render_collapsed_group(*, base_name: str, group: list[PlannedTask], console: Console) -> None:
    """Print a single collapsed line for a large fan-out group.

    Branches of one task usually miss for one or two reasons, so the
    ``will run`` count carries those reasons, counted, rather than a list.
    """
    counts = Counter(task.cache_status for task in group)
    reasons = Counter(
        _run_reason(task) for task in group if task.run_reasons and task.cache_status == "will_run"
    )
    parts = []
    for status in ("cached", "will_run", "unknown"):
        if not counts[status]:
            continue
        part = f"{counts[status]} {_TAG[status].strip('[]')}"
        if status == "will_run" and reasons:
            part += " (" + ", ".join(f"{n} {reason}" for reason, n in reasons.most_common()) + ")"
        parts.append(part)
    breakdown = ", ".join(parts)
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
    # Declared demand, not occupancy: the scheduler holds a wave to the core
    # budget, so this figure may exceed it.
    # Counted over the tasks that will run; a cache hit holds no thread. When
    # none asked for more than the default one, say so rather than calling
    # that a request.
    if resources.peak_wave_threads and (
        resources.peak_wave_threads == resources.peak_wave_task_count
    ):
        count = resources.peak_wave_task_count
        parts.append(
            f"up to {count} task{'' if count == 1 else 's'} at once, one thread each "
            f"(wave {resources.peak_wave_index})"
        )
    elif resources.peak_wave_threads:
        parts.append(
            f"{resources.peak_wave_threads} threads requested at peak "
            f"(wave {resources.peak_wave_index})"
        )
    if resources.peak_wave_memory_gb:
        parts.append(f"{resources.peak_wave_memory_gb} GiB peak")
    if resources.gpu_task_count:
        suffix = "" if resources.gpu_task_count == 1 else "s"
        parts.append(f"{resources.gpu_task_count} GPU task{suffix}")
    for dimension, total in resources.custom_totals.items():
        parts.append(f"{total} {dimension} total")
    parts.append("no tasks executed")
    return Text("  ·  ".join(parts), style="dim")
