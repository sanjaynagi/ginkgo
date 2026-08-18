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
    """Run whose manifest listed this pair, which replayed it on a cache hit."""

    run_dir: Path
    task_key: str
    task_name: str
    started_at: str
    html_path: Path
    notebook_path: Path
    render_status: str | None
    artifact_run_id: str | None = None
    """Run that produced the artifacts, when the manifest records it."""

    @property
    def render_failed(self) -> bool:
        """Return True when the HTML export step failed for this notebook."""
        return self.render_status == "failed"

    @property
    def producing_run_id(self) -> str:
        """Return the run that rendered these artifacts."""
        return self.artifact_run_id or self.run_id

    @property
    def replayed(self) -> bool:
        """Return True when ``run_id`` reused an earlier run's artifacts."""
        return self.artifact_run_id is not None and self.artifact_run_id != self.run_id


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

        label = (
            f"[bold]{entry.task_name}[/]  "
            f"[dim]run={entry.producing_run_id} task={entry.task_key}[/]"
        )
        if entry.replayed:
            label += f"  [#0f766e]↺ replayed in {entry.run_id}[/]"
        if entry.render_failed:
            label += "  [bold yellow]⚠ HTML export failed[/]"
        rich_console.print(label)
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
            render_status = task.get("render_status")
            artifact_run_id = task.get("notebook_artifact_run_id")
            entries.append(
                NotebookArtifactPair(
                    run_id=run_dir.name,
                    run_dir=run_dir.resolve(),
                    task_key=task_key,
                    task_name=_task_base_name(task.get("task")),
                    started_at=started_at,
                    html_path=html_path,
                    notebook_path=notebook_path,
                    render_status=render_status if isinstance(render_status, str) else None,
                    artifact_run_id=artifact_run_id if isinstance(artifact_run_id, str) else None,
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
