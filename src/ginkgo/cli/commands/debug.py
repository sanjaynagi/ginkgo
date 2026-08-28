"""Debug command handlers."""

from __future__ import annotations

import sys
import json
from pathlib import Path

from ginkgo.cli.common import console, open_run
from ginkgo.cli.renderers.common import task_base_name
from ginkgo.cli.renderers.debug import (
    render_debug_failure_panel,
    render_debug_header,
    render_run_failure_panel,
)
from ginkgo.cli.renderers.models import FailureDetails
from ginkgo.runtime.caching.provenance import combined_log_tail
from ginkgo.runtime.run_summary import RunSummary, TaskSummary


def command_debug(args) -> int:
    """Handle ``ginkgo debug``."""
    rich_console = console(sys.stdout)
    with open_run(args.run_id) as (store, run_id):
        summary = store.run(run_id)

    failed_tasks = list(summary.failed_tasks)
    run_failed = summary.status == "failed"
    if args.json:
        payload = {
            "run_id": summary.run_id,
            "workflow": summary.workflow,
            "status": summary.status,
            "error": summary.error,
            "failures": _debug_failure_payload(summary=summary, failed_tasks=failed_tasks),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if not failed_tasks and not run_failed:
        rich_console.print(f"[bold green]🌿 ginkgo debug[/] [bold]{summary.run_id}[/]\n")
        rich_console.print(f"[green]✓[/] No failed tasks found in [bold]{summary.run_id}[/]")
        return 0

    rich_console.print(render_debug_header(summary=summary, failures=len(failed_tasks)))
    for item in _debug_failure_details(summary=summary, failed_tasks=failed_tasks):
        rich_console.print(render_debug_failure_panel(item))

    # A run can fail without any task failing (for example an env that cannot be
    # resolved for a dynamically expanded node), so surface the recorded error too.
    if run_failed:
        rich_console.print(render_run_failure_panel(summary.error))
    return 0


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
            task_label=task_base_name(task.name),
            exit_code=task.exit_code,
            log_path=(summary.run_dir / task.stderr_log if task.stderr_log is not None else None),
            log_tail=_log_tail(run_dir=summary.run_dir, task=task),
            error=task.error,
            failure_kind=task.failure_kind,
            inputs=task.inputs,
        )
        for task in failed_tasks
    ]


def _debug_failure_payload(
    *,
    summary: RunSummary,
    failed_tasks: list[TaskSummary],
) -> list[dict[str, object]]:
    """Return JSON-serializable failure details."""
    return [
        {
            "task_id": task.task_key,
            "task_name": task_base_name(task.name),
            "exit_code": task.exit_code,
            "error": task.error,
            "failure": task.failure,
            "inputs": task.inputs,
            "stderr_log": task.stderr_log,
            "log_tail": _log_tail(run_dir=summary.run_dir, task=task),
        }
        for task in failed_tasks
    ]
