"""Debug command handlers."""

from __future__ import annotations

import sys
import json
from pathlib import Path

from ginkgo.cli.common import console, resolve_run_dir
from ginkgo.cli.renderers.common import _task_base_name
from ginkgo.cli.renderers.debug import render_debug_failure_panel, render_debug_header
from ginkgo.cli.renderers.models import _FailureDetails
from ginkgo.runtime.caching.provenance import load_manifest, tail_text


def command_debug(args) -> int:
    """Handle ``ginkgo debug``."""
    rich_console = console(sys.stdout)
    run_dir = resolve_run_dir(args.run_id)
    manifest = load_manifest(run_dir)
    failed_tasks = [
        task for task in manifest.get("tasks", {}).values() if task.get("status") == "failed"
    ]
    if args.json:
        payload = {
            "run_id": manifest.get("run_id", run_dir.name),
            "workflow": manifest.get("workflow"),
            "status": manifest.get("status"),
            "failures": _debug_failure_payload(run_dir=run_dir, failed_tasks=failed_tasks),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if not failed_tasks:
        rich_console.print(f"[bold green]🌿 ginkgo debug[/] [bold]{run_dir.name}[/]\n")
        rich_console.print(f"[green]✓[/] No failed tasks found in [bold]{run_dir.name}[/]")
        return 0

    rich_console.print(
        render_debug_header(run_dir=run_dir, manifest=manifest, failures=len(failed_tasks))
    )
    details = _debug_failure_details(run_dir=run_dir, failed_tasks=failed_tasks)
    for item in details:
        rich_console.print(render_debug_failure_panel(item))
    return 0


def _combined_log_tail(run_dir: Path, task: dict[str, object], *, lines: int) -> list[str]:
    """Combine stdout and stderr tails for failure display."""
    combined: list[str] = []
    stdout_rel = task.get("stdout_log")
    stderr_rel = task.get("stderr_log")
    if isinstance(stdout_rel, str):
        combined.extend(tail_text(run_dir / stdout_rel, lines=lines))
    if isinstance(stderr_rel, str):
        combined.extend(tail_text(run_dir / stderr_rel, lines=lines))
    return combined[-lines:]


def _debug_failure_details(
    *,
    run_dir: Path,
    failed_tasks: list[dict[str, object]],
) -> list[_FailureDetails]:
    """Return failure details for the rich ``ginkgo debug`` report."""
    details: list[_FailureDetails] = []
    for task in sorted(failed_tasks, key=lambda item: int(item.get("node_id", -1))):
        log_tail = _combined_log_tail(run_dir, task, lines=50)
        stderr_rel = task.get("stderr_log")
        stderr_path = run_dir / stderr_rel if isinstance(stderr_rel, str) else None
        task_name = str(task.get("task", "unknown"))
        details.append(
            _FailureDetails(
                task_label=_task_base_name(task_name),
                exit_code=task.get("exit_code"),
                log_path=stderr_path,
                log_tail=log_tail,
                error=str(task.get("error")) if task.get("error") is not None else None,
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
                "task_name": _task_base_name(str(task.get("task", "unknown"))),
                "exit_code": task.get("exit_code"),
                "error": task.get("error"),
                "failure": task.get("failure"),
                "inputs": task.get("inputs") if isinstance(task.get("inputs"), dict) else None,
                "stderr_log": stderr_rel,
                "log_tail": _combined_log_tail(run_dir, task, lines=50),
            }
        )
    return payload
