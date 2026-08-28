"""Notebook artifact listing command handler."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from ginkgo import query
from ginkgo.cli.common import console
from ginkgo.query import Query


_RUN_LIMIT = 200
"""How far back ``ginkgo notebooks`` looks; older runs are rarely wanted."""


@dataclass(frozen=True, kw_only=True)
class NotebookArtifactPair:
    """Display metadata for one executed notebook artifact pair."""

    run_id: str
    """Run that listed this pair, which replayed it on a cache hit."""

    run_dir: Path
    task_key: str
    task_name: str
    started_at: str
    html_path: Path
    notebook_path: Path
    render_status: str | None
    artifact_run_id: str | None = None
    """Run that produced the artifacts, when the ledger records it."""

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

    try:
        with query.open() as reader:
            entries = list_notebook_artifact_pairs(reader=reader)
    except FileNotFoundError:
        # A workspace with no ledger has no notebooks, which is an empty
        # listing rather than a failure.
        entries = []
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


def list_notebook_artifact_pairs(*, reader: Query) -> list[NotebookArtifactPair]:
    """Return executed notebook artifact pairs ordered by most recent run first."""
    entries: list[NotebookArtifactPair] = []
    for run in reader.runs(limit=_RUN_LIMIT):
        summary = reader.run(run.run_id)
        for task in summary.tasks:
            if task.executed_notebook is None or task.rendered_html is None:
                continue
            entries.append(
                NotebookArtifactPair(
                    run_id=summary.run_id,
                    run_dir=summary.run_dir.resolve(),
                    task_key=task.task_key,
                    task_name=task.base_name,
                    started_at=run.started_at or "",
                    html_path=(summary.run_dir / task.rendered_html).resolve(),
                    notebook_path=(summary.run_dir / task.executed_notebook).resolve(),
                    render_status=task.render_status,
                    artifact_run_id=task.notebook_artifact_run_id,
                )
            )

    entries.sort(
        key=lambda entry: (entry.started_at, entry.run_id, entry.task_key),
        reverse=True,
    )
    return entries
