"""Warn about a notebook task's mistakes before anything runs.

``doctor`` and ``--dry-run`` call :func:`notebook_static_notices` for each
notebook task whose directive they can work out. It reads the notebook's
source for two things the run would get wrong silently: a ``mo.cli_args()``
key no argument supplies, and a project file the notebook names in a literal
(``mo.image("results/a.png")``), which it reads behind its task's cache key.
The key covers the notebook's source and arguments only, so a figure another
task redraws would be served stale from this task's cache.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from ginkgo.core.directive import ExecutionDirective
from ginkgo.core.optional import OptionalOutput
from ginkgo.runtime.literal_paths import literal_project_paths, path_strings
from ginkgo_notebook.cli_args import cli_arg_fallbacks, unpassed_cli_arg_notices
from ginkgo_notebook.directive import NotebookDirective


def notebook_static_notices(
    directive: ExecutionDirective, task_name: str, passed: Mapping[str, Any]
) -> list[str]:
    """Return the notebook's notices before anything runs.

    The ``static_notices`` hook of the notebook task kind.

    Parameters
    ----------
    directive : ExecutionDirective
        The task's ``notebook(...)`` directive.
    task_name : str
        The task, for the notice.
    passed : Mapping[str, Any]
        The task's arguments, by name.

    Returns
    -------
    list[str]
        :func:`~ginkgo_notebook.cli_args.unpassed_cli_arg_notices`, then
        :func:`literal_path_notices`; empty when the notebook cannot be read.
    """
    assert isinstance(directive, NotebookDirective)
    try:
        source = directive.path.read_text(encoding="utf-8")
    except OSError:
        return []
    return [
        *unpassed_cli_arg_notices(source=source, task_name=task_name, passed=passed),
        *literal_path_notices(
            source=source,
            task_name=task_name,
            root=Path.cwd(),
            declared=[*_declared_paths(directive), *path_strings(passed.values())],
        ),
    ]


def literal_path_notices(
    *, source: str, task_name: str, root: Path, declared: Iterable[str]
) -> list[str]:
    """Return one notice per project path the notebook names in a literal.

    A literal default for a ``mo.cli_args()`` value
    (``args.get("cases", "results/north.csv")``) is not counted.

    Parameters
    ----------
    source : str
        The notebook's source.
    task_name : str
        The task, for the notice.
    root : Path
        The project root, against which relative literals resolve.
    declared : Iterable[str]
        Paths the task declares: the directive's ``output`` and ``log``, and
        the paths it passes as arguments.

    Returns
    -------
    list[str]
        Notices, in source order.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    # A default for a missing argument is there for interactive editing; the
    # task passes the value, or the cli_args check reports the key.
    for fallback in cli_arg_fallbacks(tree):
        fallback.value = None
    ignore = [root / path for path in declared]
    return [
        f"The notebook names the project path {path!r}, which {task_name} does not pass "
        f"to it, so {task_name}'s cache key cannot see that file: a cached {task_name} is "
        f"not re-run when it changes. Pass it to {task_name} as a `file` argument and read "
        "it from mo.cli_args(); a file another task writes should be that task's "
        "`Out[file]` output, passed on from its result."
        for path in literal_project_paths(tree=tree, root=root, ignore=ignore)
    ]


def _declared_paths(directive: NotebookDirective) -> list[str]:
    """Return the output and log paths *directive* declares."""
    output = directive.output
    items = output if isinstance(output, list) else [output]
    paths = [item.payload if isinstance(item, OptionalOutput) else item for item in items]
    return [path for path in [*paths, directive.log] if isinstance(path, str)]
