"""Check the ``mo.cli_args()`` keys a notebook reads against those its task passes.

A task passes each argument as ``--<name-with-dashes>``, and marimo keys
``mo.cli_args()`` by the flag with its dashes stripped from the front only. A
notebook that reads a key no argument supplies gets ``None`` and carries on, so
a misspelt or underscored key fails silently. Reading the notebook's source for
the literal keys it asks for catches the common shapes, ``args.get("k")``,
``args.get_all("k")``, ``args["k"]`` and ``"k" in args``, without running it.
"""

from __future__ import annotations

import ast
import difflib
from collections.abc import Callable, Iterable


def unpassed_cli_arg_notices(*, source: str, task_name: str, passed: Iterable[str]) -> list[str]:
    """Return one notice per ``mo.cli_args()`` key the task does not pass.

    Parameters
    ----------
    source : str
        The notebook's source.
    task_name : str
        The task, for the notice.
    passed : Iterable[str]
        The task's argument names, as Python identifiers.

    Returns
    -------
    list[str]
        Notices, sorted by key; empty when every literal key read is passed.
    """
    flags = sorted(name.replace("_", "-") for name in passed)
    notices = []
    for key in sorted(read_cli_arg_keys(source) - set(flags)):
        dashed = key.replace("_", "-")
        if dashed in flags:
            hint = f"the task passes it as {dashed!r}"
        else:
            close = difflib.get_close_matches(dashed, flags, n=1)
            hint = f"did you mean {close[0]!r}?" if close else f"it passes {', '.join(flags)}"
        notices.append(
            f"The notebook reads mo.cli_args() key {key!r}, which {task_name} does not "
            f"pass, so it reads None; {hint}"
        )
    return notices


def read_cli_arg_keys(source: str) -> set[str]:
    """Return the literal keys a notebook reads from ``mo.cli_args()``.

    Parameters
    ----------
    source : str
        The notebook's source.

    Returns
    -------
    set[str]
        Keys read from ``mo.cli_args()`` directly or through a name it was
        assigned to. Keys built at run time are not seen.
    """
    tree = ast.parse(source)
    is_cli_args = _cli_args_matcher(tree)

    keys: set[str] = set()
    for node in ast.walk(tree):
        key: ast.expr | None = None
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"get", "get_all"}
            and is_cli_args(node.func.value)
            and node.args
        ):
            key = node.args[0]
        elif isinstance(node, ast.Subscript) and is_cli_args(node.value):
            key = node.slice
        elif (
            isinstance(node, ast.Compare)
            and len(node.ops) == 1
            and isinstance(node.ops[0], (ast.In, ast.NotIn))
            and is_cli_args(node.comparators[0])
        ):
            key = node.left
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            keys.add(key.value)
    return keys


def cli_arg_fallbacks(tree: ast.AST) -> list[ast.Constant]:
    """Return the literal defaults a notebook falls back to when an argument is missing.

    ``args.get("k", "data/x.csv")`` and ``args.get("k") or "data/x.csv"`` are
    defaults for interactive editing; the task passes the value itself.

    Parameters
    ----------
    tree : ast.AST
        The parsed notebook.

    Returns
    -------
    list[ast.Constant]
        The default literals, as nodes of *tree*.
    """
    is_cli_args = _cli_args_matcher(tree)

    def is_read(expr: ast.AST) -> bool:
        return (
            isinstance(expr, ast.Call)
            and isinstance(expr.func, ast.Attribute)
            and expr.func.attr == "get"
            and is_cli_args(expr.func.value)
        )

    fallbacks: list[ast.expr] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and is_read(node) and len(node.args) > 1:
            fallbacks.append(node.args[1])
        elif (
            isinstance(node, ast.BoolOp)
            and isinstance(node.op, ast.Or)
            and is_read(node.values[0])
        ):
            fallbacks.extend(node.values[1:])
    return [node for node in fallbacks if isinstance(node, ast.Constant)]


def _cli_args_matcher(tree: ast.AST) -> Callable[[ast.expr], bool]:
    """Return a test for ``mo.cli_args()`` or a name *tree* assigns it to."""
    names = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and _is_cli_args_call(node.value)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    def is_cli_args(expr: ast.expr) -> bool:
        return (isinstance(expr, ast.Name) and expr.id in names) or _is_cli_args_call(expr)

    return is_cli_args


def _is_cli_args_call(expr: ast.expr) -> bool:
    """Report whether *expr* is a call of ``mo.cli_args()`` or ``cli_args()``."""
    if not isinstance(expr, ast.Call):
        return False
    func = expr.func
    return (isinstance(func, ast.Attribute) and func.attr == "cli_args") or (
        isinstance(func, ast.Name) and func.id == "cli_args"
    )
