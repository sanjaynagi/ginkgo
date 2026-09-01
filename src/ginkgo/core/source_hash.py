"""Source hashing for task cache keys.

A task's cache key must change when the task body changes, and also when any
local helper module it statically imports changes. This module walks that
import closure with the ``ast`` module and folds every source file into a
single digest.

The closure covers the project's own source only. Installed distributions and
the standard library are pinned by the environment lock instead, and a virtual
environment nested inside the project root (``.pixi/envs/``, ``.venv/``) would
otherwise put the whole interpreter inside the closure.

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
from importlib.util import resolve_name
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

from ginkgo.core.hashing import hash_file, hash_str

__all__ = ["compute_source_hash"]


def compute_source_hash(fn: Callable[..., Any]) -> str:
    """Return the BLAKE3 digest of task source and local imports.

    Decorator lines are excluded from the digest, so a change to a resource
    declaration (``threads``, ``memory``, ``gpu``, ``retries``) leaves the
    cache key untouched. The decorator arguments that are cache-relevant —
    ``env`` and ``version`` — are hashed explicitly by the cache-key payload.

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
    source = _source_without_decorators(raw_source)
    module = sys.modules.get(fn.__module__)
    if not isinstance(module, ModuleType):
        return hash_str(source)

    modules = _local_import_closure(module)
    module_hashes = [f"{name}:{hash_file(path)}" for name, path in sorted(modules.items())]
    return hash_str("\n".join((source, *module_hashes)))


def _source_without_decorators(source: str) -> str:
    """Return ``source`` from its ``def`` line on, dropping decorator lines.

    The remaining text is the original bytes of the definition, not a
    regenerated form, so every edit to a signature, body, comment or docstring
    still changes the digest. A source that will not parse — a fragment, or a
    definition mid-edit — is returned dedented and unchanged rather than
    raising: over-invalidation is the safe direction here.
    """
    dedented = textwrap.dedent(source)
    try:
        tree = ast.parse(dedented)
    except SyntaxError:
        return dedented

    definition = tree.body[0] if tree.body else None
    if not isinstance(definition, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return dedented
    if not definition.decorator_list:
        return dedented

    # ``lineno`` on a decorated definition points at the ``def`` keyword line;
    # every decorator, however many lines its call spans, sits above it.
    lines = dedented.splitlines(keepends=True)
    return "".join(lines[definition.lineno - 1 :])


def _local_import_closure(module: ModuleType) -> dict[str, Path]:
    """Return loaded local Python modules statically imported by ``module``."""
    source_path = _module_source_path(module)
    if source_path is None:
        return {}

    source_root = _module_source_root(module=module, source_path=source_path)
    # A task module that is itself installed has no project tree to separate
    # from the environment, so excluding installed modules would empty its
    # closure and stop its helpers from invalidating the cache.
    skip_installed = not _is_installed_module(source_path)
    pending = [module]
    sources: dict[str, Path] = {}

    while pending:
        current = pending.pop()
        if current.__name__ in sources:
            continue

        current_path = _module_source_path(current)
        if current_path is None or not current_path.is_relative_to(source_root):
            continue
        if skip_installed and _is_installed_module(current_path):
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
