"""Debug command handlers."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from ginkgo.cli.common import console, open_run
from ginkgo.cli.renderers.debug import (
    render_debug_failure_panel,
    render_debug_header,
    render_log_tail,
    render_run_failure_panel,
)
from ginkgo.cli.renderers.models import FailureDetails
from ginkgo.runtime.rundir import combined_log_tail
from ginkgo.runtime.run_summary import RunSummary, TaskSummary


def command_debug(args) -> int:
    """Handle ``ginkgo debug``."""
    rich_console = console(sys.stdout)
    with open_run(args.run_id) as (reader, run_id):
        summary = reader.run(run_id)

    failed_tasks = list(summary.failed_tasks)
    run_failed = summary.status == "failed"
    run_error = _run_level_error(summary=summary, failed_tasks=failed_tasks)
    if args.json:
        payload = {
            "run_id": summary.run_id,
            "workflow": summary.workflow,
            "status": summary.status,
            "error": run_error,
            "failures": _debug_failure_payload(summary=summary, failed_tasks=failed_tasks),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if not failed_tasks and not run_failed:
        rich_console.print(f"[bold green]🌿 ginkgo debug[/] [bold]{summary.run_id}[/]\n")
        rich_console.print(f"[green]✓[/] No failed tasks found in [bold]{summary.run_id}[/]")
        return 0

    # A failed run with no failed task failed at run level, and that is one
    # failure. Otherwise the run failed because its tasks did, so counting
    # the run as well would count the same failure twice.
    failures = len(failed_tasks) or 1
    rich_console.print(render_debug_header(summary=summary, failures=failures))
    for item in _debug_failure_details(summary=summary, failed_tasks=failed_tasks):
        rich_console.print(render_debug_failure_panel(item))
        if item.log_tail:
            rich_console.print(render_log_tail(item.log_tail), soft_wrap=True)

    # A run can fail without any task failing (for example an env that cannot be
    # resolved for a dynamically expanded node), so surface the recorded error too.
    if run_failed and (run_error is not None or not failed_tasks):
        rich_console.print(render_run_failure_panel(run_error))
    return 0


def _run_level_error(*, summary: RunSummary, failed_tasks: list[TaskSummary]) -> str | None:
    """Return the run's recorded error, unless it only restates a task's.

    A run a task failure ended records that failure's message as its own
    error; the failure is already reported once, with its task.
    """
    if summary.error in {task.error for task in failed_tasks}:
        return None
    return summary.error


def _log_tail(*, run_dir: Path, task: TaskSummary) -> list[str]:
    """Return the combined stdout/stderr tail for one failed task."""
    return combined_log_tail(
        run_dir=run_dir,
        stdout_log=task.stdout_log,
        stderr_log=task.stderr_log,
        lines=50,
    )


def _debug_failure_details(
    *,
    summary: RunSummary,
    failed_tasks: list[TaskSummary],
) -> list[FailureDetails]:
    """Return failure details for the rich ``ginkgo debug`` report."""
    return [
        FailureDetails(
            task_label=task.label,
            exit_code=task.exit_code,
            log_path=(summary.run_dir / task.stderr_log if task.stderr_log is not None else None),
            log_tail=_log_tail(run_dir=summary.run_dir, task=task),
            error=task.error,
            failure_kind=task.failure_kind,
            inputs=task.inputs,
            ignored=task.ignored,
        )
        for task in failed_tasks
    ]


def _debug_failure_payload(
    *,
    summary: RunSummary,
    failed_tasks: list[TaskSummary],
) -> list[dict[str, object]]:
    """Return JSON-serializable failure details.

    The task's error message is ``failure.message``; it is not repeated
    beside it.
    """
    return [
        {
            "task_id": task.task_key,
            "task_name": task.base_name,
            "display_label": task.display_label,
            "status": task.status,
            "exit_code": task.exit_code,
            "failure": task.failure,
            "ignored": task.ignored,
            "inputs": task.inputs,
            "stderr_log": task.stderr_log,
            "log_tail": _log_tail(run_dir=summary.run_dir, task=task),
        }
        for task in failed_tasks
    ]
