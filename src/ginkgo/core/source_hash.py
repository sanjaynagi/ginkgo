"""Source hashing for task cache keys.

A task's cache key must change when the task body changes, and also when any
local helper module it statically imports changes. This module walks that
import closure with the ``ast`` module and folds every source file into a
single digest.

The closure covers the project's own source only. Installed distributions and
the standard library are pinned by the environment lock instead, and a virtual
environment nested inside the project root (``.pixi/envs/``, ``.venv/``) would
otherwise put the whole interpreter inside the closure. When the task module is
itself installed, its own package stands in for that project tree.

Every source text folded into the digest — the task definition and each file in
the closure — first has the resource-only keyword arguments of its ``@task(...)``
decorators deleted, so that retuning ``threads`` or ``memory`` — or changing
what a failure stops, with ``on_failure`` — mid-run does not discard completed
results. Nothing else about the text is normalised.

It lives in ``core`` rather than ``runtime.caching`` so that ``core.task`` can
import it without inverting the layer dependency, alongside the primitives in
:mod:`ginkgo.core.hashing`.
"""

from __future__ import annotations

import ast
import inspect
import sys
import sysconfig
import textwrap
import tokenize
from functools import cache
from importlib.metadata import packages_distributions
from importlib.util import resolve_name
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

from ginkgo.core.hashing import hash_file, hash_str

__all__ = ["compute_source_hash"]

#: ``task()`` keyword arguments that steer scheduling, placement, and what a
#: failure stops — never what a task produces. None of them reaches the
#: cache-key payload, so their text must not reach the source digest either.
#: Everything else the decorator accepts — ``kind``, ``env``, ``version`` and
#: any argument added later — keeps invalidating: ``kind`` in particular is
#: absent from the payload, so flipping it would otherwise hit a stale entry.
_RESOURCE_ONLY_KWARGS = frozenset(
    {
        "gpu",
        "gpu_type",
        "memory",
        "memory_retry_multiplier",
        "on_failure",
        "priority",
        "resources",
        "retries",
        "retry_backoff",
        "retry_backoff_max",
        "retry_backoff_multiplier",
        "retry_on",
        "retry_on_exit_codes",
        "threads",
    }
)


def compute_source_hash(fn: Callable[..., Any]) -> str:
    """Return the BLAKE3 digest of task source and local imports.

    The resource-only keyword arguments of ``@task(...)`` — ``threads``,
    ``memory``, ``gpu``, ``retries``, ``on_failure`` and their siblings in
    :data:`_RESOURCE_ONLY_KWARGS` — are deleted from every source text before
    it is hashed, so retuning them leaves the cache key untouched. The digest
    covers both the task definition and each file in its local import closure,
    and both are filtered the same way; without the second, the decorator text
    would re-enter the digest through the task's own module file.

    Every other byte still counts. ``kind``, ``env`` and ``version`` keep
    invalidating, as do a user's own stacked decorators, whose arguments this
    module cannot interpret.

    Parameters
    ----------
    fn : Callable
        The function to hash.

    Returns
    -------
    str
        Hex-encoded BLAKE3 digest.

    Raises
    ------
    ValueError
        If the source cannot be extracted (lambdas, dynamic functions), or
        if a module in the local import closure cannot be read or parsed.
    """
    try:
        raw_source = inspect.getsource(fn)
    except OSError as exc:
        raise ValueError(
            f"Cannot extract source for task '{fn.__qualname__}'. "
            "Tasks must be defined as named, top-level functions."
        ) from exc
    source = _without_resource_kwargs(textwrap.dedent(raw_source))
    module = sys.modules.get(fn.__module__)
    if not isinstance(module, ModuleType):
        return hash_str(source)

    modules = _local_import_closure(module)
    module_hashes = [
        f"{name}:{_hash_module_source(path)}" for name, path in sorted(modules.items())
    ]
    return hash_str("\n".join((source, *module_hashes)))


def _hash_module_source(path: Path) -> str:
    """Return the digest of one closure member, resource kwargs removed."""
    try:
        with tokenize.open(path) as source_file:
            source = source_file.read()
    except (OSError, SyntaxError, UnicodeDecodeError):
        # Unreadable or wrongly declared encoding: hash the raw bytes. The
        # closure walk raises on such a file anyway, but hashing must not be
        # the thing that raises.
        return hash_file(path)
    return hash_str(_without_resource_kwargs(source))


def _without_resource_kwargs(source: str) -> str:
    """Return ``source`` with resource-only ``@task(...)`` arguments deleted.

    Only the text between a matching call's parentheses is rewritten; every
    byte outside it — signature, body, comments, docstrings — is the original.
    Inside, the surviving arguments keep their own text and are re-joined with
    ``", "``, so a reflow of the decorator across lines stops mattering, and a
    comment sitting among its arguments is dropped along with them.

    Source that will not parse — a fragment, or a nested definition whose body
    holds a column-0 line inside a multi-line string, which ``textwrap.dedent``
    cannot flatten — is returned unchanged. Over-invalidation is the safe
    direction here.
    """
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return source

    data = source.encode("utf-8")
    line_starts = _line_start_offsets(data)
    edits = [
        edit
        for node in ast.walk(tree)
        for decorator in getattr(node, "decorator_list", ())
        if (edit := _resource_free_arguments(decorator, data=data, line_starts=line_starts))
    ]
    if not edits:
        return source

    for start, end, replacement in sorted(edits, key=lambda edit: edit[0], reverse=True):
        data = data[:start] + replacement + data[end:]
    return data.decode("utf-8")


def _resource_free_arguments(
    decorator: ast.expr, *, data: bytes, line_starts: list[int]
) -> tuple[int, int, bytes] | None:
    """Return the rewrite of one decorator's argument list, or ``None``.

    The rewrite is ``(start, end, replacement)`` over ``data``, spanning the
    bytes between the call's parentheses.
    """
    if not isinstance(decorator, ast.Call) or not _names_the_task_decorator(decorator.func):
        return None

    elements: list[ast.expr | ast.keyword] = [*decorator.args, *decorator.keywords]
    if not any(_is_resource_kwarg(element) for element in elements):
        return None

    elements.sort(key=lambda element: (element.lineno, element.col_offset))
    kept = [
        data[
            _offset(element.lineno, element.col_offset, line_starts) : _offset(
                element.end_lineno or element.lineno,
                element.end_col_offset or 0,
                line_starts,
            )
        ]
        for element in elements
        if not _is_resource_kwarg(element)
    ]

    func_end = _offset(
        decorator.func.end_lineno or decorator.func.lineno,
        decorator.func.end_col_offset or 0,
        line_starts,
    )
    call_end = _offset(
        decorator.end_lineno or decorator.lineno, decorator.end_col_offset or 0, line_starts
    )
    open_paren = data.find(b"(", func_end, call_end)
    if open_paren == -1:
        return None
    return open_paren + 1, call_end - 1, b", ".join(kept)


def _is_resource_kwarg(element: ast.expr | ast.keyword) -> bool:
    """Return whether ``element`` is a keyword argument that only steers resources."""
    return isinstance(element, ast.keyword) and element.arg in _RESOURCE_ONLY_KWARGS


def _names_the_task_decorator(func: ast.expr) -> bool:
    """Return whether ``func`` spells ``task`` or ``<something>.task``.

    An aliased import (``from ginkgo import task as run``) is not recognised,
    and its resource arguments keep invalidating: the filter must never guess
    which arguments of an unknown decorator are safe to drop.
    """
    if isinstance(func, ast.Name):
        return func.id == "task"
    return isinstance(func, ast.Attribute) and func.attr == "task"


def _line_start_offsets(data: bytes) -> list[int]:
    """Return the byte offset of each line start, for resolving AST positions.

    ``col_offset`` counts UTF-8 bytes within a line, not characters, so the
    rewrite works on the encoded source throughout.
    """
    starts = [0]
    newline = data.find(b"\n")
    while newline != -1:
        starts.append(newline + 1)
        newline = data.find(b"\n", newline + 1)
    return starts


def _offset(lineno: int, col_offset: int, line_starts: list[int]) -> int:
    """Return the absolute byte offset of an AST position."""
    return line_starts[lineno - 1] + col_offset


def _local_import_closure(module: ModuleType) -> dict[str, Path]:
    """Return loaded local Python modules statically imported by ``module``."""
    source_path = _module_source_path(module)
    if source_path is None:
        return {}

    # An installed task module's own package is its project tree: helpers
    # beside it must keep invalidating the cache, while the other distributions
    # sharing its install root must stay out. That package bound does the
    # separating, so the installed-module filter — which would empty the
    # closure — is switched off for it. When no bound can be established
    # (``None``), the root widens back to the whole install root and the filter
    # stays off: over-hashing costs a walk, while narrowing on a guess would
    # silently stop the module's own helpers invalidating it.
    import_root = _module_source_root(module=module, source_path=source_path)
    installed = _is_installed_module(source_path)
    closure_root = import_root
    if installed:
        bound = _installed_closure_root(
            module=module, source_path=source_path, import_root=import_root
        )
        closure_root = bound if bound is not None else import_root
    pending = [module]
    sources: dict[str, Path] = {}

    while pending:
        current = pending.pop()
        if current.__name__ in sources:
            continue

        current_path = _module_source_path(current)
        if current_path is None or not current_path.is_relative_to(closure_root):
            continue
        if not installed and _is_installed_module(current_path):
            continue

        sources[current.__name__] = current_path
        pending.extend(_imported_modules(module=current, source_path=current_path))

    return sources


def _module_source_path(module: ModuleType) -> Path | None:
    """Return the resolved Python source path for ``module``, when available."""
    filename = getattr(module, "__file__", None)
    if filename is None:
        return None

    path = Path(filename).resolve()
    return path if path.suffix == ".py" and path.is_file() else None


@cache
def _installed_roots() -> tuple[Path, ...]:
    """Return the directories holding the interpreter and its installed packages.

    A project that keeps its environment inside its own tree — ``.pixi/envs/``,
    ``.venv/`` — puts every installed module below the project root, so the
    root alone cannot tell project source from dependency.
    """
    candidates = [sys.prefix, sys.base_prefix, *sysconfig.get_paths().values()]
    roots = {Path(candidate).resolve() for candidate in candidates if candidate}
    return tuple(sorted(roots))


def _is_installed_module(source_path: Path) -> bool:
    """Return whether ``source_path`` belongs to the environment, not the project."""
    return any(source_path.is_relative_to(root) for root in _installed_roots())


def _module_source_root(*, module: ModuleType, source_path: Path) -> Path:
    """Return the import root containing the task module's package."""
    package_depth = len(module.__name__.split("."))
    if source_path.name != "__init__.py":
        package_depth -= 1

    source_root = source_path.parent
    for _ in range(package_depth):
        source_root = source_root.parent
    return source_root


def _installed_closure_root(
    *, module: ModuleType, source_path: Path, import_root: Path
) -> Path | None:
    """Return the closure bound for an installed task module, or ``None``.

    The bound is normally the module's own outermost regular package — the
    shallowest ancestor directory carrying an ``__init__.py``. Namespace
    portions are walked past, because unrelated distributions can contribute
    packages under one namespace directory.

    A package with no ``__init__.py`` anywhere above the module — a PEP 420
    namespace package, or one shipped as stubs — leaves that walk with nothing,
    so the module's top-level name is looked up in the installed distribution
    metadata instead. A name that belongs to a distribution bounds the closure
    at its top-level directory, keeping sibling helpers in.

    ``None`` says no bound could be established: the module's top level is in
    no distribution's metadata, as for a directory dropped on ``sys.path`` or a
    ``__main__``-ish load. The caller then over-hashes rather than guessing.
    """
    candidate = import_root
    for part in source_path.parent.relative_to(import_root).parts:
        candidate = candidate / part
        if (candidate / "__init__.py").is_file():
            return candidate

    top_level = module.__name__.split(".")[0]
    if top_level not in _packages_distributions():
        return None

    top_level_dir = import_root / top_level
    return top_level_dir if top_level_dir.is_dir() else source_path


@cache
def _packages_distributions() -> dict[str, list[str]]:
    """Return the installed top-level import names, mapped to their distributions.

    Metadata that cannot be read at all leaves the mapping empty, which reads
    as "nothing resolves" and sends every installed module down the
    over-hashing fallback.
    """
    try:
        return packages_distributions()
    except Exception:  # pragma: no cover - defensive; metadata layout varies
        return {}


def _imported_modules(*, module: ModuleType, source_path: Path) -> list[ModuleType]:
    """Return already-loaded modules named by static imports in ``module``.

    Raises
    ------
    ValueError
        If the module source cannot be read or parsed. Swallowing the error
        would silently truncate the import closure and stop deeper helper
        changes from invalidating the cache.
    """
    try:
        with tokenize.open(source_path) as source_file:
            tree = ast.parse(source_file.read(), filename=str(source_path))
    except (OSError, SyntaxError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Cannot parse imports of module '{module.__name__}' ({source_path}) "
            "while hashing the task's local import closure. Fix the module source "
            "so cache invalidation can track the full import closure."
        ) from exc

    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = _import_base_name(node=node, package=module.__package__)
            if base is None:
                continue
            names.add(base)
            names.update(f"{base}.{alias.name}" for alias in node.names)

    return [
        imported
        for name in sorted(names)
        if isinstance(imported := sys.modules.get(name), ModuleType)
    ]


def _import_base_name(*, node: ast.ImportFrom, package: str | None) -> str | None:
    """Resolve the module portion of an import statement without importing it."""
    if node.level == 0:
        return node.module
    if not package:
        return None

    relative_name = "." * node.level + (node.module or "")
    return resolve_name(relative_name, package)
