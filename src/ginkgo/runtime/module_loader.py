"""Helpers for loading workflow modules from source files."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType


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

    return roots


def load_module_from_path(path: str | Path, *, module_name: str | None = None) -> ModuleType:
    """Import a Python source file under a synthetic module name."""
    source_path = Path(path).resolve()
    chosen_name = module_name or module_name_for_path(source_path)
    if chosen_name in sys.modules:
        del sys.modules[chosen_name]

    for import_root in reversed(import_roots_for_path(source_path)):
        if import_root not in sys.path:
            sys.path.insert(0, import_root)

    spec = importlib.util.spec_from_file_location(chosen_name, source_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from {source_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[chosen_name] = module
    spec.loader.exec_module(module)
    return module


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
