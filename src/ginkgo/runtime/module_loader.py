"""Helpers for loading workflow modules from source files."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from ginkgo.project import find_project_root


def module_name_for_path(path: str | Path) -> str:
    """Return a stable synthetic module name for a source file."""
    source_path = Path(path).resolve()
    digest = hashlib.sha1(str(source_path).encode("utf-8")).hexdigest()[:10]
    stem = "".join(ch if ch.isalnum() else "_" for ch in source_path.stem) or "workflow"
    return f"ginkgo_user_{stem}_{digest}"


def import_roots_for_path(path: str | Path) -> list[str]:
    """Return import roots needed to load a source file and its package."""
    source_dir = Path(path).resolve().parent
    roots = [str(source_dir)]

    current = source_dir
    package_root_parent: Path | None = None
    while (current / "__init__.py").is_file():
        package_root_parent = current.parent.resolve()
        current = current.parent.resolve()

    if package_root_parent is not None:
        candidate = str(package_root_parent)
        if candidate not in roots:
            roots.append(candidate)

    # A workflow file need not live inside the package it imports from (e.g.
    # a test workflow under tests/workflows/ importing the project's own
    # package). Include the enclosing project root, identified by the
    # nearest ginkgo.toml/yaml/yml, so such imports resolve regardless of
    # where the file sits relative to that package.
    project_root = find_project_root(source_dir)
    if project_root is not None:
        candidate = str(project_root)
        if candidate not in roots:
            roots.append(candidate)

    return roots


def package_qualified_name(path: str | Path) -> str | None:
    """Return the dotted module name for a source file that lives in a package.

    Parameters
    ----------
    path : str | Path
        Path to a Python source file.

    Returns
    -------
    str | None
        The dotted name (e.g. ``workflow.flow``) when an ``__init__.py``
        sits beside the file, otherwise ``None``.
    """
    source_path = Path(path).resolve()
    if not (source_path.parent / "__init__.py").is_file():
        return None

    parts = [source_path.stem]
    current = source_path.parent
    while (current / "__init__.py").is_file():
        parts.append(current.name)
        current = current.parent
    return ".".join(reversed(parts))


def _evict_shadowed_package(*, package_name: str, package_root: Path) -> None:
    """Drop a cached package that is not the one about to be loaded.

    Two packages cannot share a name in one interpreter, so a cached
    ``package_name`` living somewhere else would answer the parent import and
    resolve the entry's imports against the wrong tree. A cached package that is
    the same directory is left alone: its submodules are the very files about to
    be imported, and reusing them keeps task identity stable across the two
    loads a single ``ginkgo run`` performs.
    """
    cached = sys.modules.get(package_name)
    if cached is None:
        return

    cached_file = getattr(cached, "__file__", None)
    if cached_file is not None and Path(cached_file).resolve().parent == package_root:
        return

    prefix = f"{package_name}."
    for name in [name for name in sys.modules if name == package_name or name.startswith(prefix)]:
        del sys.modules[name]


def load_module_from_path(path: str | Path, *, module_name: str | None = None) -> ModuleType:
    """Import a Python source file, preferring its real dotted name inside a package.

    A dotted name — given explicitly, or derived because an ``__init__.py`` sits
    beside the file — is loaded with its parent package imported and
    ``__package__`` set, so relative imports resolve. A bare source file is
    loaded under a synthetic top-level name instead.
    """
    source_path = Path(path).resolve()

    for import_root in reversed(import_roots_for_path(source_path)):
        if import_root not in sys.path:
            sys.path.insert(0, import_root)

    dotted_name = module_name or package_qualified_name(source_path)
    if dotted_name is not None and "." in dotted_name:
        return _load_package_module(source_path=source_path, dotted_name=dotted_name)

    chosen_name = dotted_name or module_name_for_path(source_path)
    return _exec_module(source_path=source_path, module_name=chosen_name)


def _exec_module(
    *, source_path: Path, module_name: str, parent: ModuleType | None = None
) -> ModuleType:
    """Create, register and execute one module from a source file.

    A module that raises while executing is removed from ``sys.modules`` again:
    leaving the half-initialised object cached would make later loads
    short-circuit onto it and appear to succeed.
    """
    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from {source_path}")

    module = importlib.util.module_from_spec(spec)
    if parent is not None:
        module.__package__ = parent.__name__
    sys.modules[module_name] = module

    try:
        spec.loader.exec_module(module)
    except BaseException as error:
        del sys.modules[module_name]
        if isinstance(error, ImportError):
            raise _relative_import_error(source_path=source_path, error=error) from error
        raise

    # Bind on the parent only once the module is usable, as the import system does.
    if parent is not None:
        setattr(parent, module_name.rsplit(".", 1)[1], module)
    return module


def _relative_import_error(*, source_path: Path, error: ImportError) -> ImportError:
    """Return *error*, or a version naming the missing ``__init__.py``.

    A bare module has no parent package, so Python rejects its relative imports
    with a message that names neither the file nor the fix.
    """
    if "attempted relative import with no known parent package" not in str(error):
        return error

    directory = source_path.parent
    return ImportError(
        f"{_display_path(source_path)} uses a relative import, but "
        f"{_display_path(directory)}/ is not a Python package. Add an empty "
        f"{_display_path(directory / '__init__.py')}, or use absolute imports."
    )


def _display_path(path: Path) -> str:
    """Return *path* relative to the working directory when it sits below it."""
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


def _load_package_module(*, source_path: Path, dotted_name: str) -> ModuleType:
    """Load a source file under its real dotted name, with its parent package imported."""
    depth = dotted_name.count(".")
    top_level_root = source_path.parents[depth - 1]
    _evict_shadowed_package(
        package_name=dotted_name.split(".", 1)[0],
        package_root=top_level_root,
    )

    # The package root's parent must win over the entry file's own directory,
    # which is also on sys.path. Otherwise a module inside the package whose
    # name matches the package shadows the package itself, and the parent
    # import below resolves to that module instead — the pkg/pkg.py case,
    # which an explicit workflow path can always reach.
    package_root_parent = str(source_path.parents[depth])
    if package_root_parent in sys.path:
        sys.path.remove(package_root_parent)
    sys.path.insert(0, package_root_parent)

    parent = importlib.import_module(dotted_name.rsplit(".", 1)[0])
    return _exec_module(source_path=source_path, module_name=dotted_name, parent=parent)


def load_module(module_name: str, *, module_file: str | None = None) -> ModuleType:
    """Import a module by name, optionally falling back to a source path."""
    if module_name in sys.modules:
        return sys.modules[module_name]

    if module_file is not None:
        return load_module_from_path(module_file, module_name=module_name)

    return importlib.import_module(module_name)


def resolve_module_file(module_name: str) -> str | None:
    """Return the source file for a loaded module when available."""
    module = sys.modules.get(module_name)
    module_file = getattr(module, "__file__", None) if module is not None else None
    return str(Path(module_file).resolve()) if module_file else None
