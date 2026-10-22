"""Helpers for loading workflow modules from source files."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from ginkgo.project import find_project_root, project_root


USER_MODULE_PREFIX = "ginkgo_user_"
"""Prefix of the synthetic module name a single-file flow is loaded under.

The name is unique to this process and this absolute path, so nothing outside
the run can import it. Anything that records a class by module name — pickle,
above all — must treat such a name as unresolvable rather than durable, and
record :func:`workflow_module_reference` instead.
"""


CACHE_IDENTITY_MODULE = "<workflow>"
"""Location-independent stand-in for a ``USER_MODULE_PREFIX`` module name."""


def cache_identity_module_name(name: str) -> str:
    """Return *name* with a single-file workflow's synthetic module replaced.

    A single-file workflow's module name is unique to the *absolute path* it
    was loaded from, so anything folded into a cache key under that name
    would change whenever the file is renamed, moved or copied, even
    byte-for-byte. It is replaced with :data:`CACHE_IDENTITY_MODULE`; a real
    package module keeps its stable dotted name.
    """
    return CACHE_IDENTITY_MODULE if name.startswith(USER_MODULE_PREFIX) else name


def workflow_module_reference(module_name: str) -> str | None:
    """Return where a single-file workflow module can be found again, or ``None``.

    A class or function defined in a single-file workflow records the
    synthetic module name, which no other process can import. This is the
    portable stand-in pickle records instead: the module's file relative to
    the project root, or its absolute path when it lies outside the project.

    Parameters
    ----------
    module_name : str
        The ``__module__`` of the class or function being recorded.

    Returns
    -------
    str | None
        The reference, or ``None`` for a module with a real, importable name.
    """
    if not module_name.startswith(USER_MODULE_PREFIX):
        return None
    module_file = getattr(sys.modules.get(module_name), "__file__", None)
    if module_file is None:
        return None
    path = Path(module_file).resolve()
    try:
        return path.relative_to(project_root().resolve()).as_posix()
    except ValueError:
        return str(path)


def import_workflow_attribute(reference: str, qualname: str) -> Any:
    """Return the object *qualname* names in the single-file workflow at *reference*.

    The reconstructor pickle calls, with the arguments it recorded, for a class
    or function pickled by :func:`workflow_module_reference`; hence the
    positional parameters. The workflow already loaded in this process is
    preferred, so a value decoded in a run is an instance of the very class
    that run's tasks use: first the module loaded from *reference*, then, when
    the workflow has since been renamed or copied, the most recently loaded
    single-file workflow defining *qualname*. Only when neither is loaded (a
    fresh worker whose task lives in another module) is the file at
    *reference* loaded.

    Parameters
    ----------
    reference : str
        A :func:`workflow_module_reference`.
    qualname : str
        The object's qualified name within the module.

    Returns
    -------
    Any
        The class or function.
    """
    path = Path(reference)
    if not path.is_absolute():
        path = project_root() / path
    path = path.resolve()

    loaded = [
        module
        for name, module in list(sys.modules.items())
        if name.startswith(USER_MODULE_PREFIX) and module is not None
    ]
    module = next(
        (
            module
            for module in loaded
            if getattr(module, "__file__", None) and Path(module.__file__).resolve() == path
        ),
        None,
    )
    if module is None:
        module = next(
            (module for module in reversed(loaded) if _has_qualname(module, qualname)),
            None,
        )
    if module is None:
        module = load_module_from_path(path)
    return _lookup_qualname(module, qualname)


def _lookup_qualname(module: ModuleType, qualname: str) -> Any:
    """Return the object a dotted *qualname* names inside *module*."""
    target: Any = module
    for part in qualname.split("."):
        target = getattr(target, part)
    return target


def _has_qualname(module: ModuleType, qualname: str) -> bool:
    """Return whether *module* defines *qualname*."""
    try:
        _lookup_qualname(module, qualname)
    except AttributeError:
        return False
    return True


def task_base_name(name: str) -> str:
    """Return a recorded task name without its module: the name users see.

    Parameters
    ----------
    name : str
        A qualified task name, as events and the ledger record it
        (``<workflow>.align``, ``pipeline.steps.align``), or already a base
        name.

    Returns
    -------
    str
        The function name alone (``align``).
    """
    return name.rsplit(".", 1)[-1]


def module_name_for_path(path: str | Path) -> str:
    """Return a stable synthetic module name for a source file."""
    source_path = Path(path).resolve()
    digest = hashlib.sha1(str(source_path).encode("utf-8")).hexdigest()[:10]
    stem = "".join(ch if ch.isalnum() else "_" for ch in source_path.stem) or "workflow"
    return f"{USER_MODULE_PREFIX}{stem}_{digest}"


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
    # where the file sits relative to that package. ginkgo.toml is optional:
    # without one the project root is the working directory, as everywhere
    # else in ginkgo.
    candidate = str(find_project_root(source_dir) or Path.cwd().resolve())
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
        f"{display_path(source_path)} uses a relative import, but "
        f"{display_path(directory)}/ is not a Python package. Add an empty "
        f"{display_path(directory / '__init__.py')}, or use absolute imports."
    )


def display_path(path: str | Path) -> str:
    """Return *path* relative to the working directory when it sits below it.

    How output names a file the user ran or should look at: the path they
    would type from where they stand, or the absolute path from anywhere else.
    """
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(resolved)


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
