"""``ginkgo runs`` — what has been run in this workspace, and what one run did."""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from collections.abc import Sequence
from typing import Any

from rich.text import Text

from ginkgo import query
from ginkgo.cli.common import console, new_table, open_run
from ginkgo.cli.errors import IGNORED_FAILURES_EXIT_CODE, INTERRUPT_EXIT_CODE, UsageError
from ginkgo.formatting import format_duration, format_timestamp, parse_timestamp
from ginkgo.query import RunRow
from ginkgo.runtime.module_loader import display_path
from ginkgo.runtime.run_summary import RunSummary, TaskSummary
from ginkgo.runtime.rundir import runs_without_ledger_warning, tail_text
from ginkgo.workspace_layout import WorkspaceLayout

__all__ = ["command_runs"]

_TASK_ROW_LIMIT = 50
"""Most task rows ``runs show`` lists without ``--all``; failures always show."""

_UNREMARKABLE = frozenset({"succeeded", "cached"})
"""Task statuses a long listing may leave out."""

WAIT_TIMEOUT_EXIT_CODE = 124
"""What ``runs wait`` exits with when the run is still going at its timeout."""

_WAIT_EXIT_CODES = {"succeeded": 0, "failed": 1, "cancelled": INTERRUPT_EXIT_CODE}
"""What ``runs wait`` exits with for each way a run ends."""


def command_runs(args) -> int:
    """Handle ``ginkgo runs``: ``ls`` lists runs; ``show``, ``status`` and ``wait`` take one."""
    rich_console = console(sys.stdout)
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

    if args.runs_command == "show":
        with open_run(args.run_id) as (reader, run_id):
            summary = reader.run(run_id)
        return _render_show(
            rich_console,
            summary=summary,
            as_json=as_json,
            all_tasks=bool(getattr(args, "all_tasks", False)),
        )

    summary, payload = _outcome(args.run_id)
    if args.runs_command == "wait":
        # The ledger is the only sign of life, so a run killed outright stays
        # ``running`` there and only the timeout ends the wait.
        deadline = None if args.timeout is None else time.monotonic() + args.timeout
        while payload["status"] in {"running", "starting"} and (
            deadline is None or time.monotonic() < deadline
        ):
            time.sleep(1.0)
            summary, payload = _outcome(payload["run_id"])

    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    elif summary is not None:
        _render_show(rich_console, summary=summary, as_json=False, all_tasks=False)
    elif payload["status"] == "starting":
        print(f"Run {payload['run_id']} has not started yet (still loading the workflow).")
        print(f"Log: {payload['log']}")
    else:
        print(f"Run {payload['run_id']} was rejected before it started. Log: {payload['log']}")
        print("\n".join(payload["log_tail"]))

    if payload["status"] == "rejected":
        return 1
    if args.runs_command == "status":
        return 0
    if payload["status"] in {"running", "starting"}:
        return WAIT_TIMEOUT_EXIT_CODE
    if (
        payload["status"] == "failed"
        and 0 < payload["counts"]["ignored"] == payload["counts"]["failed"]
    ):
        return IGNORED_FAILURES_EXIT_CODE
    return _WAIT_EXIT_CODES.get(payload["status"], 1)


def _outcome(run_id: str | None) -> tuple[RunSummary | None, dict[str, Any]]:
    """Return a run and its outcome payload.

    A run ``run --detach`` started but the ledger has not recorded has no
    summary: its payload's status is ``starting`` while the process is alive,
    and ``rejected`` once it has exited, with the tail of its log.
    """
    detached = WorkspaceLayout.relative().detached.resolve()
    pid_path, log_path = detached / f"{run_id}.pid", detached / f"{run_id}.log"
    try:
        with open_run(run_id) as (reader, resolved):
            summary = reader.run(resolved)
        return summary, summary.outcome_payload()
    except FileNotFoundError as exc:
        if run_id is None or not pid_path.is_file():
            raise UsageError(str(exc)) from exc
    return None, {
        "run_id": run_id,
        "status": "starting" if _alive(int(pid_path.read_text())) else "rejected",
        "log": str(log_path),
        "log_tail": tail_text(log_path, lines=10),
    }


def _alive(pid: int) -> bool:
    """Whether a process with this pid exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _shown_tasks(tasks: Sequence[TaskSummary], *, all_tasks: bool) -> Sequence[TaskSummary]:
    """Return the tasks ``runs show`` lists, in run order.

    Every task that did not succeed, then the first of the rest up to
    :data:`_TASK_ROW_LIMIT` rows.
    """
    if all_tasks or len(tasks) <= _TASK_ROW_LIMIT:
        return tasks
    notable = [task for task in tasks if task.status not in _UNREMARKABLE]
    room = max(_TASK_ROW_LIMIT - len(notable), 0)
    rest = [task for task in tasks if task.status in _UNREMARKABLE][:room]
    kept = {id(task) for task in (*notable, *rest)}
    return [task for task in tasks if id(task) in kept]


def _render_ls(rich_console, *, rows: list[RunRow], as_json: bool) -> int:
    """Print the run index as JSON or as a table.

    An empty index is two different workspaces: one nothing has run in, and
    one whose ledger is gone from under the runs it recorded. The second gets
    the warning that says so, so that the listing never affirms an empty
    workspace over one holding runs; so does a listing whose database was
    recreated after older runs.
    """
    if as_json:
        print(json.dumps([row.to_payload() for row in rows], indent=2, sort_keys=True))
        return 0

    rich_console.print("[bold green]🌿 ginkgo runs[/] [bold]ls[/]\n")
    # Run directories the ledger has no row for are missing from the listing
    # however many rows it has: the database may have been replaced since.
    warning = runs_without_ledger_warning(layout=WorkspaceLayout.relative())
    if not rows:
        rich_console.print("[dim]No runs recorded in this workspace.[/]")
        if warning is not None:
            rich_console.print(f"[yellow]⚠[/] {warning}")
        return 0

    table = new_table()
    table.add_column("Run ID", style="bold", no_wrap=True)
    table.add_column("Workflow", overflow="fold")
    table.add_column("Status")
    table.add_column("Started", no_wrap=True)
    table.add_column("Duration", justify="right")
    for row in rows:
        table.add_row(
            row.run_id,
            Text(display_path(row.workflow)) if row.workflow else "-",
            row.status,
            format_timestamp(parse_timestamp(row.started_at)),
            format_duration(row.duration_s),
        )
    rich_console.print(table)
    if warning is not None:
        rich_console.print(f"[yellow]⚠[/] {warning}")
    return 0


def _render_show(rich_console, *, summary: RunSummary, as_json: bool, all_tasks: bool) -> int:
    """Print one run as JSON or as a header and a task table.

    A run of more than :data:`_TASK_ROW_LIMIT` tasks lists its first ones and
    every task that did not succeed, under a count of each status, unless
    *all_tasks* asks for the lot; ``--json`` always holds every task.
    """
    if as_json:
        print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
        return 0

    rich_console.print(f"[bold green]🌿 ginkgo runs[/] [bold]show[/] [dim]{summary.run_id}[/]\n")
    workflow = display_path(summary.workflow) if summary.workflow else summary.workflow_label
    rich_console.print("Workflow: ", Text(workflow, style="bold"), sep="")
    rich_console.print(f"Status: [bold]{summary.status}[/]")
    rich_console.print(f"Started: {format_timestamp(summary.started_at)}")
    rich_console.print(f"Duration: {format_duration(summary.duration_s)}")
    rich_console.print(f"Run directory: {summary.run_dir}")
    if summary.error:
        # A cancelled run has no error to report — the recorded line says which
        # signal stopped it, which is a reason, not a fault.
        # Printed as text, not markup: a label such as ``analyze[omega]`` would
        # otherwise lose its bracketed part as an unknown style tag.
        if summary.status == "cancelled":
            rich_console.print("Ended by: ", Text(summary.error, style="yellow"), sep="")
        else:
            rich_console.print("Error: ", Text(summary.error, style="red"), sep="")
    for notice in summary.notices:
        rich_console.print(Text(f"Notice: {notice}", style="yellow"))

    if not summary.tasks:
        rich_console.print("\n[dim]This run recorded no tasks.[/]")
        return 0

    shown = _shown_tasks(summary.tasks, all_tasks=all_tasks)
    if len(shown) < len(summary.tasks):
        counts = Counter(task.status for task in summary.tasks)
        breakdown = ", ".join(f"{count} {status}" for status, count in counts.most_common())
        rich_console.print(f"\nTasks: {len(summary.tasks)} ({breakdown})")

    table = new_table()
    table.add_column("Task", style="bold", overflow="fold")
    table.add_column("Status")
    table.add_column("Cached")
    table.add_column("Duration", justify="right")
    table.add_column("Attempts", justify="right")
    for task in shown:
        table.add_row(
            Text(task.label),
            task.status,
            "yes" if task.cached else "no",
            format_duration(task.duration_s),
            str(task.attempts),
        )
    rich_console.print("")
    rich_console.print(table)
    hidden = len(summary.tasks) - len(shown)
    if hidden:
        rich_console.print(
            f"[dim]… {hidden} more {'task' if hidden == 1 else 'tasks'}, all succeeded or "
            "cached — `--all` lists every one.[/]"
        )

    retried = [task for task in summary.tasks if len(task.attempt_history) > 1]
    if retried:
        rich_console.print("\n[bold]Attempts[/]")
        for task in retried:
            rich_console.print(Text(f"  {task.label}", style="bold"))
            for attempt in task.attempt_history:
                line = Text(f"    {attempt['attempt']}: {attempt['status']}")
                failure = attempt["failure"]
                if isinstance(failure, dict) and failure.get("message"):
                    line.append(f"  {failure['message']}", style="dim")
                rich_console.print(line)

    pages = [
        (task, path)
        for task in summary.tasks
        if (path := task.rendered_html_absolute(run_dir=summary.run_dir)) is not None
    ]
    if pages:
        rich_console.print("\n[bold]Rendered pages[/]")
        for task, path in pages:
            line = Text(f"  {task.label}  ", style="bold")
            line.append(str(path), style=f"link {path.as_uri()}")
            if task.rendered_html_run_id not in (None, summary.run_id):
                line.append(f"  from run {task.rendered_html_run_id}", style="dim")
            rich_console.print(line)
    return 0
