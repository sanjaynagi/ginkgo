"""Find the environment variables a task reads, so its cache key can cover them.

A task that reads ``os.environ["NAME"]`` computes a result that depends on
``NAME`` as much as on its arguments. The scan here reads source, never runs
it, and names every variable read with a literal name:

- ``os.environ["NAME"]``, ``os.environ.get("NAME")`` (and ``setdefault`` and
  ``pop``), ``"NAME" in os.environ``, and ``os.getenv("NAME")``, with ``os``
  reached as ``os.`` or through ``from os import environ, getenv``;
- in the task's own body, in every function it calls by name, and in every
  module-level assignment those read, as long as the code lives in the task's
  module or a module that module imports locally — the closure the source hash
  already covers.

:meth:`~ginkgo.runtime.caching.cache.CacheStore.build_cache_key` folds a digest
of each named variable's current value, or its absence, into the key. A read
the scan cannot name (``os.environ[key]``, ``dict(os.environ)``) is reported
instead, since the variable it reads cannot be keyed.
"""

from __future__ import annotations

import ast
import inspect
import os
import sys
import textwrap
from dataclasses import dataclass
from functools import cache
from importlib.util import resolve_name
from pathlib import Path
from types import FunctionType, ModuleType
from typing import Any

from ginkgo.core.hashing import hash_str
from ginkgo.core.source_hash import local_import_closure

__all__ = ["EnvironReads", "environ_digests", "environ_reads"]

_ENVIRON_ATTRIBUTES = frozenset({"environ", "environb"})
_LITERAL_KEY_METHODS = frozenset({"get", "setdefault", "pop"})


@dataclass(frozen=True, kw_only=True)
class EnvironReads:
    """The environment reads a static scan of a task found.

    Attributes
    ----------
    names : frozenset[str]
        Variables read by a literal name; their values are cache-keyed.
    unresolved : frozenset[str]
        Qualified names of the functions or module globals that touch the
        environment in a way the scan cannot name a variable for. What those
        read is not part of the cache key.
    """

    names: frozenset[str]
    unresolved: frozenset[str]


def environ_digests(*, fn: Any) -> dict[str, str | None]:
    """Return a digest of each environment variable *fn* reads, by name.

    A digest rather than the value, so a secret read from the environment
    never reaches the ledger.

    Parameters
    ----------
    fn : Any
        The task function.

    Returns
    -------
    dict[str, str | None]
        Variable name to the BLAKE3 digest of its current value, or ``None``
        when it is unset. Empty when the task reads no variable by name.
    """
    return {
        name: None if (value := os.environ.get(name)) is None else hash_str(f"{name}={value}")
        for name in sorted(environ_reads(fn).names)
    }


@cache
def environ_reads(fn: Any) -> EnvironReads:
    """Scan *fn* and the project code it reaches for environment reads.

    Parameters
    ----------
    fn : Any
        The task function. Source that cannot be read scans as no reads.

    Returns
    -------
    EnvironReads
        The variables read by name, and where a read could not be named.
    """
    scanner = _Scanner(closure=_closure_paths(fn))
    scanner.scan_function(fn)
    return EnvironReads(names=frozenset(scanner.names), unresolved=frozenset(scanner.unresolved))


def _closure_paths(fn: Any) -> frozenset[Path]:
    """Return the source files of *fn*'s module and its local imports."""
    module = sys.modules.get(getattr(fn, "__module__", ""))
    if not isinstance(module, ModuleType):
        return frozenset()
    try:
        return frozenset(path.resolve() for path in local_import_closure(module).values())
    except ValueError:
        return frozenset()


class _Scanner:
    """Walk task code, following names into project functions and globals."""

    def __init__(self, *, closure: frozenset[Path]) -> None:
        self.closure = closure
        self.names: set[str] = set()
        self.unresolved: set[str] = set()
        self._seen: set[tuple[str, str]] = set()

    def scan_function(self, fn: Any) -> None:
        """Scan one function's body in the context of its module's globals."""
        key = ("function", f"{fn.__module__}:{fn.__qualname__}")
        if key in self._seen:
            return
        self._seen.add(key)
        try:
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        except (OSError, TypeError, SyntaxError):
            return
        function = tree.body[0] if tree.body else None
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            return
        module = sys.modules.get(fn.__module__)
        self._scan(
            nodes=[function],
            namespace=fn.__globals__,
            module=module,
            where=fn.__qualname__,
            local_names=_local_names(function),
        )

    def _scan_global(self, *, module: ModuleType, name: str) -> None:
        """Scan the module-level code that binds *name* in *module*."""
        key = ("global", f"{module.__name__}:{name}")
        if key in self._seen:
            return
        self._seen.add(key)
        for binding in _module_bindings(module).get(name, ()):
            if isinstance(binding, ast.ImportFrom):
                self._follow_import(module=module, node=binding, name=name)
            else:
                self._scan(
                    nodes=[binding],
                    namespace=vars(module),
                    module=module,
                    where=name,
                    local_names=frozenset(),
                )

    def _follow_import(self, *, module: ModuleType, node: ast.ImportFrom, name: str) -> None:
        """Follow ``from m import x`` into a project module's binding of ``x``."""
        original = next(
            (alias.name for alias in node.names if (alias.asname or alias.name) == name), None
        )
        if original is None:
            return
        try:
            target_name = resolve_name("." * node.level + (node.module or ""), module.__package__)
        except (ImportError, ValueError):
            return
        target = sys.modules.get(target_name)
        if isinstance(target, ModuleType) and self._in_project(target):
            self._scan_global(module=target, name=original)

    def _scan(
        self,
        *,
        nodes: list[ast.AST],
        namespace: dict[str, Any],
        module: ModuleType | None,
        where: str,
        local_names: frozenset[str],
    ) -> None:
        """Record the reads in *nodes*, then follow the globals they use."""
        consumed: set[int] = set()
        references: list[ast.AST] = []
        followed: set[str] = set()
        for root in nodes:
            for node in ast.walk(root):
                name = _literal_read(node, namespace=namespace, consumed=consumed)
                if name is not None:
                    self.names.add(name)
                if _is_environ(node, namespace=namespace) or _is_getenv(node, namespace=namespace):
                    references.append(node)
                elif (
                    isinstance(node, ast.Name)
                    and isinstance(node.ctx, ast.Load)
                    and node.id not in local_names
                ):
                    followed.add(node.id)
                elif (
                    isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id not in local_names
                    and isinstance(namespace.get(node.value.id), ModuleType)
                ):
                    target = namespace[node.value.id]
                    if target is not os and self._in_project(target):
                        self._scan_global(module=target, name=node.attr)
        if any(id(node) not in consumed for node in references):
            self.unresolved.add(where)

        for name in sorted(followed):
            value = inspect.unwrap(namespace[name]) if name in namespace else None
            if isinstance(value, FunctionType):
                if self._in_project(value):
                    self.scan_function(value)
            elif module is not None and name in namespace:
                self._scan_global(module=module, name=name)

    def _in_project(self, obj: Any) -> bool:
        """Whether *obj*'s source file is in the scanned task's import closure."""
        try:
            source = inspect.getsourcefile(obj)
        except TypeError:
            return False
        return source is not None and Path(source).resolve() in self.closure


def _literal_read(node: ast.AST, *, namespace: dict[str, Any], consumed: set[int]) -> str | None:
    """Return the variable *node* reads by a literal name, marking its ``environ``."""
    reader: ast.expr | None = None
    key: ast.expr | None = None
    if isinstance(node, ast.Subscript) and _is_environ(node.value, namespace=namespace):
        reader = node.value
        if isinstance(node.ctx, ast.Store | ast.Del):
            # An assignment or deletion writes the variable; nothing is read.
            consumed.add(id(reader))
            return None
        key = node.slice
    elif isinstance(node, ast.Call) and node.args:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr in _LITERAL_KEY_METHODS
            and _is_environ(func.value, namespace=namespace)
        ):
            reader, key = func.value, node.args[0]
        elif _is_getenv(func, namespace=namespace):
            reader, key = func, node.args[0]
    elif (
        isinstance(node, ast.Compare)
        and len(node.ops) == 1
        and isinstance(node.ops[0], ast.In | ast.NotIn)
        and _is_environ(node.comparators[0], namespace=namespace)
    ):
        reader, key = node.comparators[0], node.left
    if reader is None or not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
        return None
    consumed.add(id(reader))
    return key.value


def _is_environ(node: ast.AST, *, namespace: dict[str, Any]) -> bool:
    """Whether *node* is ``os.environ``/``os.environb`` or a global bound to one."""
    if isinstance(node, ast.Attribute):
        return (
            node.attr in _ENVIRON_ATTRIBUTES
            and isinstance(node.value, ast.Name)
            and node.value.id == "os"
        )
    environs = (os.environ, getattr(os, "environb", os.environ))
    return isinstance(node, ast.Name) and any(namespace.get(node.id) is env for env in environs)


def _is_getenv(node: ast.AST, *, namespace: dict[str, Any]) -> bool:
    """Whether *node* is ``os.getenv`` or a global bound to it."""
    if isinstance(node, ast.Attribute):
        return node.attr == "getenv" and isinstance(node.value, ast.Name) and node.value.id == "os"
    return isinstance(node, ast.Name) and namespace.get(node.id) is os.getenv


def _local_names(function: ast.FunctionDef | ast.AsyncFunctionDef) -> frozenset[str]:
    """Return the parameters and assigned names a function's globals cannot be."""
    arguments = function.args
    parameters = [*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs]
    parameters += [arg for arg in (arguments.vararg, arguments.kwarg) if arg is not None]
    stored = {
        node.id
        for node in ast.walk(function)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store | ast.Del)
    }
    return frozenset({arg.arg for arg in parameters} | stored)


def _module_bindings(module: ModuleType) -> dict[str, list[ast.AST]]:
    """Return the module-level statements binding each name, by name."""
    source = getattr(module, "__file__", None)
    return _bindings_in(Path(source).resolve()) if source else {}


@cache
def _bindings_in(path: Path) -> dict[str, list[ast.AST]]:
    """Return what binds each name at the top level of the module at *path*.

    An assignment contributes its value, ``from m import x`` its statement.
    Statements under a top-level ``if``, ``try`` or ``with`` count; function and
    class bodies do not, since a function is followed through the object a
    global holds.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, ValueError):
        return {}
    bindings: dict[str, list[ast.AST]] = {}
    pending: list[ast.stmt] = list(tree.body)
    while pending:
        statement = pending.pop(0)
        if isinstance(statement, ast.If | ast.With | ast.Try):
            for field in ("body", "orelse", "finalbody"):
                pending.extend(getattr(statement, field, []))
            for handler in getattr(statement, "handlers", []):
                pending.extend(handler.body)
        elif isinstance(statement, ast.Assign):
            for target in statement.targets:
                for name in _target_names(target):
                    bindings.setdefault(name, []).append(statement.value)
        elif isinstance(statement, ast.AnnAssign | ast.AugAssign) and statement.value is not None:
            for name in _target_names(statement.target):
                bindings.setdefault(name, []).append(statement.value)
        elif isinstance(statement, ast.ImportFrom):
            for alias in statement.names:
                bindings.setdefault(alias.asname or alias.name, []).append(statement)
    return bindings


def _target_names(target: ast.expr) -> list[str]:
    """Return the plain names an assignment target binds."""
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Tuple | ast.List):
        return [name for element in target.elts for name in _target_names(element)]
    return []
