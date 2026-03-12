"""Debug command handlers."""

from __future__ import annotations

import sys
from pathlib import Path

from ginkgo.cli.common import console, resolve_run_dir
from ginkgo.cli.renderers.common import _task_base_name
from ginkgo.cli.renderers.debug import render_debug_failure_panel, render_debug_header
from ginkgo.cli.renderers.models import _FailureDetails
from ginkgo.runtime.provenance import load_manifest, tail_text


def command_debug(args) -> int:
    """Handle ``ginkgo debug``."""
    rich_console = console(sys.stdout)
    run_dir = resolve_run_dir(args.run_id)
    manifest = load_manifest(run_dir)
    failed_tasks = [
        task
        for task in manifest.get("tasks", {}).values()
        if task.get("status") == "failed"
    ]
    if not failed_tasks:
        rich_console.print(f"No failed tasks found in {run_dir.name}")
        return 0

    rich_console.print(render_debug_header(run_dir=run_dir, manifest=manifest, failures=len(failed_tasks)))
    details = _debug_failure_details(run_dir=run_dir, failed_tasks=failed_tasks)
    for item in details:
        rich_console.print(render_debug_failure_panel(item))
    return 0


def _debug_failure_details(
    *,
    run_dir: Path,
    failed_tasks: list[dict[str, object]],
) -> list[_FailureDetails]:
    """Return failure details for the rich ``ginkgo debug`` report."""
    details: list[_FailureDetails] = []
    for task in sorted(failed_tasks, key=lambda item: int(item.get("node_id", -1))):
        log_rel = task.get("log")
        log_path = run_dir / log_rel if isinstance(log_rel, str) else None
        task_name = str(task.get("task", "unknown"))
        details.append(
            _FailureDetails(
                task_label=_task_base_name(task_name),
                exit_code=task.get("exit_code"),
                log_path=log_path,
                log_tail=tail_text(log_path, lines=50) if log_path is not None else [],
                error=str(task.get("error")) if task.get("error") is not None else None,
                inputs=task.get("inputs") if isinstance(task.get("inputs"), dict) else None,
            )
        )
    return details
