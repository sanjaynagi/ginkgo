"""Running a ``notebook()`` task: one ``marimo export html`` per notebook.

``marimo export html <notebook> -o <html> -- <args>`` runs the notebook with the
task's arguments and renders it in the same pass, exiting non-zero when a cell
fails while still writing the page that shows the failing cell. So a notebook
task is one subprocess, and either succeeds with its rendered page or fails
pointing at it.

The page's path is recorded against the task as ``rendered_html``, the
annotation core lists in run summaries and the report, and saved with the cache
entry so a cache hit can point at the page the producing run rendered.
"""

from __future__ import annotations

import os
import shlex
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, ClassVar

from ginkgo.errors import FailureKind, GinkgoError
from ginkgo.plugins.api import RunnerContext
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.task_runners.driver import DriverTaskRunner
from ginkgo.runtime.task_runners.shell import (
    declared_output_mounts,
    iter_output_values,
    remove_declared_output,
    render_repeated_cli_tokens,
    resolve_declared_output,
)
from ginkgo.workspace_layout import WorkspaceLayout
from ginkgo_notebook.directive import NotebookDirective


class NotebookTaskError(GinkgoError, RuntimeError):
    """A notebook cell failed, or marimo could not run the notebook."""

    failure_kind: ClassVar[FailureKind] = "shell_command_error"

    def __init__(
        self, *, task_name: str, cmd: str, exit_code: int, output: str, hint: str | None
    ) -> None:
        self.exit_code = exit_code
        details = f"Notebook task {task_name} failed with exit code {exit_code}: {cmd}"
        if output:
            details = f"{details}\n{output.strip()}"
        if hint is not None:
            details = f"{details}\n{hint}"
        super().__init__(details)


@dataclass(kw_only=True)
class NotebookRunner(DriverTaskRunner):
    """Run marimo notebook tasks.

    Parameters
    ----------
    cache_store : CacheStore
        The run's cache, read on a cache hit for the rendered page's pointer.
    run_dir : RunDir | None
        The run directory, which holds rendered pages under ``notebooks/``.
        ``None`` for dry runs and validation, which record nothing.
    annotate : Callable
        Records ``rendered_html`` against the task.
    """

    cache_store: CacheStore
    run_dir: RunDir | None
    annotate: Callable[..., None]

    def run(self, *, node: Any, directive: NotebookDirective) -> Any:
        """Run the notebook and return its declared output, or the rendered page."""
        assert node.execution_args is not None
        assert node.resolved_args is not None
        output = resolve_declared_output(
            task_def=node.task_def, resolved_args=node.resolved_args, given=directive.output
        )
        if output is not None:
            for output_path in iter_output_values(output):
                remove_declared_output(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)

        html_path = self._html_path(node=node)
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.unlink(missing_ok=True)

        # marimo rewrites the notebook it exports (it stamps
        # ``__generated_with``), which would read as the task writing its own
        # input. Export a copy beside the original instead, so paths relative to
        # the notebook still resolve, and leave the user's file untouched.
        with _sibling_copy(directive.path) as notebook_copy:
            cmd = self._export_command(node=node, notebook_path=notebook_copy, html_path=html_path)
            completed = self.shell_runner.run_logged_command(
                node=node,
                cmd=cmd,
                user_log_path=Path(directive.log) if directive.log is not None else None,
                mounts=declared_output_mounts(output=output),
            )
        if html_path.is_file():
            self._record_rendered_html(node=node, html_path=html_path)

        if completed.returncode != 0:
            combined = (completed.stdout or "") + (completed.stderr or "")
            hint = self.shell_runner.failure_hint(
                node=node, exit_code=completed.returncode, output=combined
            )
            if hint is None and html_path.is_file():
                hint = f"The rendered notebook at {html_path} shows the failing cell."
            raise NotebookTaskError(
                task_name=node.task_def.name,
                cmd=cmd,
                exit_code=completed.returncode,
                output=combined,
                hint=hint,
            )

        if output is None:
            return self.validator.coerce_return_value(task_def=node.task_def, value=str(html_path))
        return self._validate_and_return_output(
            task_name=node.task_def.name, task_def=node.task_def, output=output
        )

    def replay_cached(self, *, node: Any, cache_key: str) -> None:
        """Point a cache hit at the page the producing run rendered, if it still exists."""
        if self.run_dir is None:
            return
        cached = self.cache_store.load_extra_meta(cache_key=cache_key) or {}
        extras = cached.get("runner_extras")
        if not isinstance(extras, dict):
            return
        rendered = extras.get("rendered_html")
        if isinstance(rendered, str) and Path(rendered).is_file():
            self.annotate(node=node, fields=extras)

    def _html_path(self, *, node: Any) -> Path:
        """Return where this task's rendered page goes."""
        root = (
            self.run_dir.path / "notebooks"
            if self.run_dir is not None
            else WorkspaceLayout.for_cwd().notebooks
        )
        return root / f"task_{node.node_id:04d}.html"

    @staticmethod
    def _export_command(*, node: Any, notebook_path: Path, html_path: Path) -> str:
        """Build ``marimo export html`` for the notebook, with the task's arguments.

        With no ``env=`` the notebook runs under ginkgo's own interpreter;
        with one, ``python`` resolves inside that environment.
        """
        python = "python" if node.task_def.env is not None else shlex.quote(sys.executable)
        parts = [
            python,
            "-m",
            "marimo",
            "export",
            "html",
            shlex.quote(str(notebook_path)),
            "-o",
            shlex.quote(str(html_path)),
            "--force",
            "--",
        ]
        for name, value in node.execution_args.items():
            parts.extend(
                render_repeated_cli_tokens(
                    option=f"--{name.replace('_', '-')}",
                    value=value,
                    label=f"{node.task_def.name}.{name}",
                    task_kind="notebook",
                )
            )
        return " ".join(parts)

    def _record_rendered_html(self, *, node: Any, html_path: Path) -> None:
        """Record the rendered page against the task, and stash it for the cache entry.

        The run's own record holds the page relative to its run directory. The
        cache entry holds it absolute, with the producing run's id, so a later
        cache hit can link the page this run rendered.
        """
        if self.run_dir is None:
            return
        try:
            relative = str(html_path.relative_to(self.run_dir.path))
        except ValueError:
            relative = str(html_path)
        self.annotate(
            node=node,
            fields={"rendered_html": relative, "rendered_html_run_id": self.run_dir.run_id},
        )
        node.runner_extras = {
            "rendered_html": str(html_path.resolve()),
            "rendered_html_run_id": self.run_dir.run_id,
        }


@contextmanager
def _sibling_copy(path: Path) -> Iterator[Path]:
    """Yield a hidden copy of *path* in the same directory, removed afterwards."""
    fd, name = tempfile.mkstemp(
        prefix=f".{path.stem}-ginkgo-", suffix=path.suffix, dir=path.parent
    )
    copy = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(path.read_bytes())
        yield copy
    finally:
        copy.unlink(missing_ok=True)


def build_notebook_runner(context: RunnerContext) -> NotebookRunner:
    """Build the notebook runner for a run."""
    return NotebookRunner(
        shell_runner=context.shell_runner,
        validator=context.validator,
        cache_store=context.cache_store,
        run_dir=context.run_dir,
        annotate=context.annotate,
    )
