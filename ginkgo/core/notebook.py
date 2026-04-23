"""Notebook task execution primitive.

``notebook()`` is called from inside a ``@task("notebook")`` body and returns
a ``NotebookExpr`` sentinel. The evaluator detects this and dispatches the
execution to the configured notebook runner.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ginkgo.core.asset import AssetResult

_NOTEBOOK_EXTENSIONS = frozenset({".ipynb", ".py"})

_NotebookOutputItem = str | AssetResult
_NotebookOutput = _NotebookOutputItem | list[_NotebookOutputItem] | None


@dataclass(frozen=True)
class NotebookExpr:
    """Sentinel representing a notebook execution request.

    Parameters
    ----------
    path : Path
        Resolved source notebook path (.ipynb or .py for marimo).
    output : str | AssetResult | list[those] | None
        Declared output path or paths. When provided, every path is
        validated for existence after execution. When omitted, the
        managed HTML artifact path is returned as the task result.
    log : str | None
        Optional path to capture stdout/stderr.
    source_hash : str
        BLAKE3 hash of the notebook source file, used for cache invalidation.
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
) -> NotebookExpr:
    """Create a notebook execution expression.

    Called from inside a ``@task("notebook")`` body with fully resolved
    argument values. Relative paths resolve from the current working directory
    at the time of the call.

    Parameters
    ----------
    path : str | Path
        Source notebook file (.ipynb for Jupyter/Papermill or .py for marimo).
    output : str | AssetResult | list[those] | None
        Declared output path or paths, validated for existence after
        execution. When omitted, the managed rendered HTML artifact path
        is returned instead.
    log : str | None
        Optional path to capture stdout/stderr during execution.

    Returns
    -------
    NotebookExpr
    """
    from ginkgo.runtime.caching.hashing import hash_file

    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Notebook source not found: {str(resolved)!r}")

    suffix = resolved.suffix.lower()
    if suffix not in _NOTEBOOK_EXTENSIONS:
        supported = ", ".join(sorted(_NOTEBOOK_EXTENSIONS))
        raise ValueError(
            f"notebook path must point to a .ipynb or .py notebook, "
            f"got {str(resolved)!r}. Supported extensions: {supported}"
        )

    return NotebookExpr(
        path=resolved,
        output=output,
        log=log,
        source_hash=hash_file(resolved),
    )
