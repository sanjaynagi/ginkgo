"""Running a ``notebook()`` task: one ``marimo export html`` per notebook.

``marimo export html <notebook> -o <html> -- <args>`` runs the notebook with the
task's arguments and renders it in the same pass, exiting non-zero when a cell
fails while still writing the page that shows the failing cell. So a notebook
task is one subprocess, and either succeeds with its rendered page or fails
pointing at it.

The page's path is recorded against the task as ``rendered_html``, the
annotation core lists in run summaries and the report. It is also left in
``node.runner_extras``, which the evaluator saves with the cache entry and
records again on a cache hit, so a hit points at the page the producing run
rendered.
"""

from __future__ import annotations

import shlex
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, ClassVar

from ginkgo.errors import FailureKind, GinkgoError
from ginkgo.plugins.api import RunnerContext
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

if TYPE_CHECKING:
    from ginkgo.plugins.api import TaskInvocation


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
    run_dir : RunDir | None
        The run directory, which holds rendered pages under ``notebooks/``.
        ``None`` when evaluating without one; pages then go to
        ``.ginkgo/notebooks/`` and nothing is recorded.
    annotate : Callable
        Records ``rendered_html`` against the task.
    """

    run_dir: RunDir | None
    annotate: Callable[..., None]

    def run(self, *, node: TaskInvocation, directive: NotebookDirective) -> Any:
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

        cmd = self._export_command(node=node, notebook_path=directive.path, html_path=html_path)
        with _unchanged(directive.path):
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
            raise NotebookTaskError(
                task_name=node.task_def.name,
                cmd=cmd,
                exit_code=completed.returncode,
                output=combined,
                hint=self._failure_hint(
                    node=node, html_path=html_path, exit_code=completed.returncode, output=combined
                ),
            )

        if output is None:
            return self.validator.coerce_return_value(task_def=node.task_def, value=str(html_path))
        return self._validate_and_return_output(
            task_name=node.task_def.name, task_def=node.task_def, output=output
        )

    def _html_path(self, *, node: TaskInvocation) -> Path:
        """Return where this task's rendered page goes."""
        root = (
            self.run_dir.path / "notebooks"
            if self.run_dir is not None
            else WorkspaceLayout.for_cwd().notebooks
        )
        return root / f"task_{node.node_id:04d}.html"

    @staticmethod
    def _export_command(*, node: TaskInvocation, notebook_path: Path, html_path: Path) -> str:
        """Build ``marimo export html`` for the notebook, with the task's arguments.

        With no ``env=`` the notebook runs under ginkgo's own interpreter;
        with one, ``python`` resolves inside that environment.
        """
        assert node.execution_args is not None
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

    def _failure_hint(
        self, *, node: TaskInvocation, html_path: Path, exit_code: int, output: str
    ) -> str | None:
        """Name the likeliest fix for a failed export, if there is one to name."""
        if "No module named marimo" in output and node.task_def.env is not None:
            return f"The environment {node.task_def.env!r} must install marimo to run notebooks."
        hint = self.shell_runner.failure_hint(node=node, exit_code=exit_code, output=output)
        if hint is None and html_path.is_file():
            hint = f"The rendered notebook at {html_path} shows the failing cell."
        return hint

    def _record_rendered_html(self, *, node: TaskInvocation, html_path: Path) -> None:
        """Record the rendered page against the task, and leave it for the cache entry.

        The run's own record holds the page relative to its run directory. The
        cache entry holds it absolute, with the producing run's id, so a later
        cache hit links the page this run rendered.
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
def _unchanged(path: Path) -> Iterator[None]:
    """Put *path*'s bytes back afterwards if they changed meanwhile.

    marimo rewrites the notebook it exports, stamping ``__generated_with``,
    which would read as the task writing its own input and re-run it every
    time. Restoring the original leaves the user's file as it was.
    """
    original = path.read_bytes()
    try:
        yield
    finally:
        if path.read_bytes() != original:
            path.write_bytes(original)


def build_notebook_runner(context: RunnerContext) -> NotebookRunner:
    """Build the notebook runner for a run."""
    return NotebookRunner(
        shell_runner=context.shell_runner,
        validator=context.validator,
        run_dir=context.run_dir,
        annotate=context.annotate,
    )
