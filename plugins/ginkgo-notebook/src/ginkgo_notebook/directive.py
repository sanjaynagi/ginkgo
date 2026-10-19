"""``notebook()``: run a marimo notebook as a task.

Called from inside a ``@task("notebook")`` body, ``notebook()`` returns a
``NotebookDirective``, which ginkgo-notebook's runner executes.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import final

from ginkgo.core.directive import ExecutionDirective
from ginkgo.core.hashing import hash_file
from ginkgo.core.optional import OptionalOutput

_NotebookOutputItem = str | OptionalOutput
_NotebookOutput = _NotebookOutputItem | list[_NotebookOutputItem] | None


@final
@dataclass(frozen=True)
class NotebookDirective(ExecutionDirective):
    """A request to run one marimo notebook.

    Parameters
    ----------
    path : Path
        The resolved marimo notebook, a ``.py`` file.
    output : str | OptionalOutput | list[those] | None
        Declared output path or paths, each validated for existence after the
        run. When omitted, the rendered HTML is the task's result.
    log : str | None
        Optional path to capture stdout/stderr.
    source_hash : str
        BLAKE3 hash of the notebook source, folded into the cache key.
    """

    path: Path
    output: _NotebookOutput
    log: str | None
    source_hash: str


def notebook(
    path: str | Path,
    *,
    output: _NotebookOutput = None,
    log: str | None = None,
) -> NotebookDirective:
    """Run a marimo notebook, passing the task's arguments as its CLI arguments.

    The notebook reads them with ``mo.cli_args()``. A relative path resolves
    against the working directory, which ``ginkgo`` sets to the project root
    before running a workflow; build it from ``Path(__file__)`` to anchor it to
    the workflow file instead.

    Parameters
    ----------
    path : str | Path
        The marimo notebook (``.py``).
    output : str | OptionalOutput | list[those] | None
        Declared output path or paths, validated for existence after the run.
        When omitted, the rendered HTML is returned instead.
    log : str | None
        Optional path to capture stdout/stderr.

    Returns
    -------
    NotebookDirective

    Raises
    ------
    FileNotFoundError
        If the notebook does not exist.
    ValueError
        If it is not a ``.py`` file.
    """
    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Notebook source not found: {str(resolved)!r}")
    if resolved.suffix.lower() != ".py":
        raise ValueError(
            f"notebook() runs marimo notebooks, which are .py files; got {str(resolved)!r}."
        )
    return NotebookDirective(
        path=resolved,
        output=output,
        log=log,
        source_hash=hash_file(resolved),
    )
