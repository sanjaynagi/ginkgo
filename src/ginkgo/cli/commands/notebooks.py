"""Notebook artifact listing command handler."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from ginkgo.cli.common import RUNS_ROOT, console
from ginkgo.runtime.caching.provenance import load_manifest


@dataclass(frozen=True, kw_only=True)
class NotebookArtifactPair:
    """Display metadata for one executed notebook artifact pair."""

    run_id: str
    run_dir: Path
    task_key: str
    task_name: str
    started_at: str
    html_path: Path
    notebook_path: Path


def command_notebooks(args) -> int:
    """Handle ``ginkgo notebooks``."""
    del args
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    rich_console = console(sys.stdout, width=None if is_tty else 240)
    rich_console.print("[bold green]🌿 ginkgo[/] [bold]notebooks[/]\n")

    entries = list_notebook_artifact_pairs(runs_root=RUNS_ROOT)
    if not entries:
        rich_console.print("[dim]No executed notebooks found.[/]")
        return 0

    for index, entry in enumerate(entries):
        if index > 0:
            rich_console.print()

        rich_console.print(
            f"[bold]{entry.task_name}[/]  [dim]run={entry.run_id} task={entry.task_key}[/]"
        )
        rich_console.print(f"HTML: {entry.html_path}")
        rich_console.print(f"Notebook: {entry.notebook_path}")
    return 0


def list_notebook_artifact_pairs(*, runs_root: Path) -> list[NotebookArtifactPair]:
    """Return executed notebook artifact pairs ordered by most recent run first."""
    if not runs_root.is_dir():
        return []

    entries: list[NotebookArtifactPair] = []
    run_dirs = sorted((path for path in runs_root.iterdir() if path.is_dir()), reverse=True)
    for run_dir in run_dirs:
        manifest = load_manifest(run_dir)
        tasks = manifest.get("tasks", {})
        if not isinstance(tasks, dict):
            continue

        started_at = str(manifest.get("started_at") or "")
        for task_key, task in tasks.items():
            if not isinstance(task_key, str) or not isinstance(task, dict):
                continue

            executed_notebook = task.get("executed_notebook")
            rendered_html = task.get("rendered_html")
            if not isinstance(executed_notebook, str) or not isinstance(rendered_html, str):
                continue

            notebook_path = (run_dir / executed_notebook).resolve()
            html_path = (run_dir / rendered_html).resolve()
            entries.append(
                NotebookArtifactPair(
                    run_id=run_dir.name,
                    run_dir=run_dir.resolve(),
                    task_key=task_key,
                    task_name=_task_base_name(task.get("task")),
                    started_at=started_at,
                    html_path=html_path,
                    notebook_path=notebook_path,
                )
            )

    entries.sort(
        key=lambda entry: (
            entry.started_at,
            entry.run_id,
            entry.task_key,
        ),
        reverse=True,
    )
    return entries


def _task_base_name(task_name: object) -> str:
    """Return the final dotted segment of a task identifier."""
    text = str(task_name or "unknown")
    return text.rsplit(".", maxsplit=1)[-1]
