"""ginkgo-notebook: marimo notebooks as tasks.

``@task("notebook")`` bodies return ``notebook(path)``; the task's arguments
reach the notebook as ``mo.cli_args()``, and its rendered page is recorded as
the task's ``rendered_html``.
"""

from __future__ import annotations

from ginkgo.plugins.api import Plugin, TaskKind
from ginkgo_notebook.directive import NotebookDirective, notebook
from ginkgo_notebook.runner import build_notebook_runner

__all__ = ["PLUGIN", "NotebookDirective", "notebook"]

PLUGIN = Plugin(
    name="ginkgo-notebook",
    contributions=(
        TaskKind(
            name="notebook",
            directive=NotebookDirective,
            call_form="notebook(...)",
            execution_mode="driver",
            allows_env=True,
            remote_capable=False,
            eager_source_hash=True,
            runner=build_notebook_runner,
        ),
    ),
)
