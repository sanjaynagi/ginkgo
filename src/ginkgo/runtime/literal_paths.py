"""Find project files that code names in string literals.

A task that opens ``"data/genes.tsv"`` itself, or a shell command that runs
``bash scripts/trim.sh``, depends on a file its cache key never sees: only
arguments are hashed. The scan here reads source and reports each string
literal token that names an existing file under the project root, so doctor,
dry runs and runs can say which file is invisible and how to declare it.

It only warns. Folding such a file into the key would need to know whether the
code reads or writes it, and would hide the missing graph edge that orders it
after the task that produces it; a declared ``file`` argument settles both.
"""

from __future__ import annotations

import ast
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

__all__ = ["literal_project_paths", "path_strings"]

_TOKEN_EDGES = "'\"`,;:()[]{}<>|&"


def literal_project_paths(*, tree: ast.AST, root: Path, ignore: Iterable[Path] = ()) -> list[str]:
    """Return the project paths the string literals in *tree* name.

    A literal is split on whitespace and ``=``, so a path inside a shell
    command (``"bash scripts/trim.sh {out}"``) or a flag (``--ref=ref.fa``)
    counts when it names an existing file. An f-string that builds a path
    under an existing project directory (``f"results/figures/{region}.png"``)
    counts too, reported as ``results/figures/{...}``. Docstrings do not.

    Parameters
    ----------
    tree : ast.AST
        Parsed source: a function body, or a whole module.
    root : Path
        The project root; relative tokens resolve against it, and an absolute
        token counts only inside it.
    ignore : Iterable[Path]
        Paths not to report, such as those the code is passed as arguments.

    Returns
    -------
    list[str]
        Each named path once, as written in the source, in source order.
    """
    root = root.resolve()
    ignored = {path.resolve() for path in ignore}
    docstrings = _docstring_ids(tree)
    found: list[tuple[tuple[int, int], str]] = []
    seen: set[Path] = set()

    def report(node: ast.AST, *, shown: str, path: Path | None) -> None:
        if path is None or path in ignored or path in seen:
            return
        seen.add(path)
        position = (getattr(node, "lineno", 0), getattr(node, "col_offset", 0))
        found.append((position, shown))

    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            for part, following in zip(node.values, node.values[1:]):
                if (
                    isinstance(part, ast.Constant)
                    and isinstance(part.value, str)
                    and part.value.endswith("/")
                    and isinstance(following, ast.FormattedValue)
                ):
                    token = part.value.replace("=", " ").split()[-1].lstrip(_TOKEN_EDGES)
                    report(node, shown=token + "{...}", path=_project_dir(token=token, root=root))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in docstrings:
                continue
            for token in node.value.replace("=", " ").split():
                token = token.strip(_TOKEN_EDGES)
                report(node, shown=token, path=_project_file(token=token, root=root))
    return [shown for _, shown in sorted(found, key=lambda item: item[0])]


def path_strings(values: Iterable[Any]) -> list[str]:
    """Return the path strings among *values*, and inside their lists, tuples and dicts.

    For the ``ignore`` argument of :func:`literal_project_paths`: the paths a
    task call passes as arguments are declared, so naming them is no hidden
    read.
    """
    paths: list[str] = []
    pending = list(values)
    while pending:
        value = pending.pop()
        if isinstance(value, str | os.PathLike):
            paths.append(os.fspath(value))
        elif isinstance(value, list | tuple | set | frozenset):
            pending.extend(value)
        elif isinstance(value, dict):
            pending.extend(value.values())
    return [path for path in paths if isinstance(path, str)]


def _project_file(*, token: str, root: Path) -> Path | None:
    """Return the resolved file *token* names under *root*, or ``None``."""
    path = _under_root(token=token, root=root)
    return path if path is not None and path.is_file() else None


def _project_dir(*, token: str, root: Path) -> Path | None:
    """Return the resolved directory *token* names below *root*, or ``None``."""
    path = _under_root(token=token, root=root)
    return path if path is not None and path != root and path.is_dir() else None


def _under_root(*, token: str, root: Path) -> Path | None:
    """Return *token* resolved against *root*, or ``None`` when it leaves it."""
    if not token or token.startswith("-"):
        return None
    try:
        path = (root / token).resolve()
    except (OSError, ValueError):
        return None
    return path if path.is_relative_to(root) else None


def _docstring_ids(tree: ast.AST) -> set[int]:
    """Return the ids of the docstring constants in *tree*."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(
            node, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef
        ):
            continue
        first = node.body[0] if node.body else None
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
            ids.add(id(first.value))
    return ids
