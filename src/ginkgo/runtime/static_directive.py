"""Predict a driver task's directive without running its body.

A script, notebook or sub-workflow task's cache key includes its directive's
``source_hash`` (the script file, the child workflow's sources), and the run
learns it by calling the task body. ``ginkgo run --dry-run`` must not run user
code, so it evaluates the one body shape it can prove harmless instead: an
optional docstring then a single ``return constructor(...)``, whose arguments
are built from the task's parameters, module-level names and literals using
string formatting, ``+``, ``/`` and a few pure ``pathlib`` operations. Anything
else is not evaluated, and the caller reports the task as ``unknown``.
"""

from __future__ import annotations

import ast
import builtins
import inspect
import textwrap
from collections.abc import Callable, Mapping
from pathlib import Path, PurePath
from types import ModuleType
from typing import Any

from ginkgo.core.directive import DirectiveError, ExecutionDirective

# Pure path methods an argument expression may call.
_PATH_METHODS = frozenset(
    {"absolute", "as_posix", "joinpath", "resolve", "with_name", "with_suffix"}
)
# Path attributes an argument expression may read.
_PATH_ATTRIBUTES = frozenset({"name", "parent", "stem", "suffix"})
# Callables an argument expression may call by name.
_CONSTRUCTORS: tuple[Any, ...] = (Path, str)
# Types formatting and arithmetic may touch; none of them runs user code.
_PLAIN_TYPES = (str, int, float, PurePath)


class _NotStatic(Exception):
    """An expression lies outside the subset this module evaluates."""


def static_directive(
    *,
    fn: Callable[..., Any],
    resolved_args: Mapping[str, Any],
    directive_type: type[ExecutionDirective],
) -> ExecutionDirective | None:
    """Return the directive *fn* would return, or ``None`` when unprovable.

    Parameters
    ----------
    fn : Callable[..., Any]
        The task body.
    resolved_args : Mapping[str, Any]
        The arguments the run would call it with.
    directive_type : type[ExecutionDirective]
        The directive the task's kind returns. The called constructor must be
        defined in the same module, which is how ``script`` is told from a
        user function that happens to share its name.

    Returns
    -------
    ExecutionDirective | None
        The directive, built by calling the real constructor (which only reads
        the files it hashes), or ``None`` when the body is not the supported
        shape, an argument is outside the evaluable subset, or the constructor
        raises anything but ``DirectiveError``.

    Raises
    ------
    DirectiveError
        If the constructor refuses the request whatever the run's state, such
        as a notebook that is not a ``.py`` file.
    """
    call = _returned_call(fn)
    if call is None:
        return None

    parameters = inspect.signature(fn).parameters
    scope = _Scope(
        args={
            **{
                name: parameter.default
                for name, parameter in parameters.items()
                if parameter.default is not inspect.Parameter.empty
            },
            **resolved_args,
        },
        parameters=frozenset(parameters),
        globals_=fn.__globals__,
    )
    try:
        constructor = _evaluate(call.func, scope=scope)
        if getattr(constructor, "__module__", None) != directive_type.__module__:
            return None
        args = [_data(_evaluate(arg, scope=scope)) for arg in call.args]
        kwargs = {}
        for keyword in call.keywords:
            if keyword.arg is None:
                raise _NotStatic("** unpacking")
            kwargs[keyword.arg] = _data(_evaluate(keyword.value, scope=scope))
        directive = constructor(*args, **kwargs)
    except DirectiveError:
        # The request is wrong however the run goes, so it is worth reporting.
        raise
    except Exception:
        # _NotStatic, or anything the constructor raised (a missing script, an
        # unknown extension): either way the directive cannot be predicted.
        return None
    return directive if isinstance(directive, directive_type) else None


class _Scope:
    """Name lookup for one task body: its arguments, then module globals."""

    def __init__(
        self, *, args: dict[str, Any], parameters: frozenset[str], globals_: dict[str, Any]
    ) -> None:
        self.args = args
        self.parameters = parameters
        self.globals = globals_

    def lookup(self, name: str) -> Any:
        if name in self.args:
            return self.args[name]
        # A parameter the run would pass but the probe did not resolve (a
        # scratch directory) must not fall through to a global of that name.
        if name in self.parameters:
            raise _NotStatic(f"unresolved parameter {name!r}")
        if name in self.globals:
            return self.globals[name]
        if hasattr(builtins, name):
            return getattr(builtins, name)
        raise _NotStatic(f"unknown name {name!r}")


def _returned_call(fn: Callable[..., Any]) -> ast.Call | None:
    """Return the call in a body that is only ``[docstring] return call(...)``."""
    try:
        source = textwrap.dedent(inspect.getsource(fn))
        module = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return None
    function = next(
        (node for node in module.body if isinstance(node, ast.FunctionDef)),
        None,
    )
    if function is None:
        return None
    body = function.body
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        body = body[1:]
    if len(body) != 1 or not isinstance(body[0], ast.Return):
        return None
    value = body[0].value
    return value if isinstance(value, ast.Call) else None


def _evaluate(node: ast.expr, *, scope: _Scope) -> Any:
    """Evaluate one expression within the whitelisted subset."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return scope.lookup(node.id)
    if isinstance(node, ast.Attribute):
        base = _evaluate(node.value, scope=scope)
        # A module attribute (``ginkgo.script``) or a pure path attribute.
        if isinstance(base, ModuleType) or (
            isinstance(base, PurePath) and node.attr in _PATH_ATTRIBUTES
        ):
            return getattr(base, node.attr)
    elif isinstance(node, ast.JoinedStr):
        return "".join(str(_evaluate(value, scope=scope)) for value in node.values)
    elif isinstance(node, ast.FormattedValue):
        value = _plain(_evaluate(node.value, scope=scope))
        if node.conversion == ord("r"):
            value = repr(value)
        elif node.conversion == ord("a"):
            value = ascii(value)
        elif node.conversion == ord("s"):
            value = str(value)
        spec = "" if node.format_spec is None else _evaluate(node.format_spec, scope=scope)
        return format(value, spec)
    elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div | ast.Add):
        left = _plain(_evaluate(node.left, scope=scope))
        right = _plain(_evaluate(node.right, scope=scope))
        return left / right if isinstance(node.op, ast.Div) else left + right
    elif isinstance(node, ast.List | ast.Tuple):
        items = [_evaluate(item, scope=scope) for item in node.elts]
        return items if isinstance(node, ast.List) else tuple(items)
    elif isinstance(node, ast.Dict) and None not in node.keys:
        return {
            _evaluate(key, scope=scope): _evaluate(value, scope=scope)  # type: ignore[arg-type]
            for key, value in zip(node.keys, node.values, strict=True)
        }
    elif isinstance(node, ast.Call) and not node.keywords:
        return _call(node, scope=scope)
    raise _NotStatic(ast.dump(node))


def _call(node: ast.Call, *, scope: _Scope) -> Any:
    """Evaluate ``Path(...)``, ``str(...)`` or a whitelisted path method call."""
    args = [_plain(_evaluate(arg, scope=scope)) for arg in node.args]
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr in _PATH_METHODS:
        base = _evaluate(func.value, scope=scope)
        if isinstance(base, PurePath):
            return getattr(base, func.attr)(*args)
    else:
        callee = _evaluate(func, scope=scope)
        if any(callee is allowed for allowed in _CONSTRUCTORS):
            return callee(*args)
    raise _NotStatic(ast.dump(node))


def _data(value: Any) -> Any:
    """Return *value* if it is plain data a constructor may safely receive."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, list | tuple):
        for item in value:
            _data(item)
        return value
    if isinstance(value, dict):
        for key, item in value.items():
            _data(key)
            _data(item)
        return value
    return _plain(value)


def _plain(value: Any) -> Any:
    """Return *value* if formatting or arithmetic on it cannot run user code."""
    if isinstance(value, _PLAIN_TYPES):
        return value
    raise _NotStatic(f"unsupported value of type {type(value).__name__}")
