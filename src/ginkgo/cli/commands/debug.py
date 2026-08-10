"""Debug command handlers."""

from __future__ import annotations

import sys
import json
from pathlib import Path

from rich import box
from rich.panel import Panel
from rich.text import Text

from ginkgo.cli.common import console, resolve_run_dir
from ginkgo.cli.renderers.common import task_base_name
from ginkgo.cli.renderers.debug import render_debug_failure_panel, render_debug_header
from ginkgo.cli.renderers.models import FailureDetails
from ginkgo.runtime.caching.provenance import combined_log_tail, load_manifest


def command_debug(args) -> int:
    """Handle ``ginkgo debug``."""
    rich_console = console(sys.stdout)
    run_dir = resolve_run_dir(args.run_id)
    manifest = load_manifest(run_dir)
    failed_tasks = [
        task for task in manifest.get("tasks", {}).values() if task.get("status") == "failed"
    ]
    run_error = manifest.get("error")
    run_failed = manifest.get("status") == "failed"
    if args.json:
        payload = {
            "run_id": manifest.get("run_id", run_dir.name),
            "workflow": manifest.get("workflow"),
            "status": manifest.get("status"),
            "error": str(run_error) if run_error is not None else None,
            "failures": _debug_failure_payload(run_dir=run_dir, failed_tasks=failed_tasks),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if not failed_tasks and not run_failed:
        rich_console.print(f"[bold green]🌿 ginkgo debug[/] [bold]{run_dir.name}[/]\n")
        rich_console.print(f"[green]✓[/] No failed tasks found in [bold]{run_dir.name}[/]")
        return 0

    rich_console.print(
        render_debug_header(run_dir=run_dir, manifest=manifest, failures=len(failed_tasks))
    )
    details = _debug_failure_details(run_dir=run_dir, failed_tasks=failed_tasks)
    for item in details:
        rich_console.print(render_debug_failure_panel(item))

    # A run can fail without any task failing (for example an env that cannot be
    # resolved for a dynamically expanded node), so surface the recorded error too.
    if run_failed:
        rich_console.print(_run_error_panel(run_error))
    return 0 if failed_tasks else 1


def _run_error_panel(run_error: object) -> Panel:
    """Render the run-level failure recorded in the manifest."""
    message = str(run_error) if run_error is not None else "No error recorded in the manifest."
    return Panel(
        Text(message, style="#7f1d1d"),
        title="[bold red]Run Failure[/]",
        border_style="red",
        box=box.SQUARE,
        expand=False,
    )


def _debug_failure_details(
    *,
    run_dir: Path,
    failed_tasks: list[dict[str, object]],
) -> list[FailureDetails]:
    """Return failure details for the rich ``ginkgo debug`` report."""
    details: list[FailureDetails] = []
    for task in sorted(failed_tasks, key=lambda item: int(item.get("node_id", -1))):
        log_tail = combined_log_tail(
            run_dir=run_dir,
            stdout_log=task.get("stdout_log"),
            stderr_log=task.get("stderr_log"),
            lines=50,
        )
        stderr_rel = task.get("stderr_log")
        stderr_path = run_dir / stderr_rel if isinstance(stderr_rel, str) else None
        task_name = str(task.get("task", "unknown"))
        failure = task.get("failure")
        failure_kind = (
            failure.get("kind")
            if isinstance(failure, dict) and isinstance(failure.get("kind"), str)
            else None
        )
        details.append(
            FailureDetails(
                task_label=task_base_name(task_name),
                exit_code=task.get("exit_code"),
                log_path=stderr_path,
                log_tail=log_tail,
                error=str(task.get("error")) if task.get("error") is not None else None,
                failure_kind=failure_kind,
                inputs=task.get("inputs") if isinstance(task.get("inputs"), dict) else None,
            )
        )
    return details


def _debug_failure_payload(
    *,
    run_dir: Path,
    failed_tasks: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Return JSON-serializable failure details."""
    payload: list[dict[str, object]] = []
    for task in sorted(failed_tasks, key=lambda item: int(item.get("node_id", -1))):
        stderr_rel = task.get("stderr_log")
        payload.append(
            {
                "task_id": task.get("task_id"),
                "task_name": task_base_name(str(task.get("task", "unknown"))),
                "exit_code": task.get("exit_code"),
                "error": task.get("error"),
                "failure": task.get("failure"),
                "inputs": task.get("inputs") if isinstance(task.get("inputs"), dict) else None,
                "stderr_log": stderr_rel,
                "log_tail": combined_log_tail(
                    run_dir=run_dir,
                    stdout_log=task.get("stdout_log"),
                    stderr_log=task.get("stderr_log"),
                    lines=50,
                ),
            }
        )
    return payload
