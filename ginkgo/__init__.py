"""Ginkgo — a dynamic, reproducible workflow orchestrator for scientific and data analyses."""

from __future__ import annotations

from pathlib import Path
import sys
from importlib import import_module
from types import ModuleType
from typing import Any

_EXPORTS = {
    "AssetKey": ("ginkgo.core.asset", "AssetKey"),
    "AssetRef": ("ginkgo.core.asset", "AssetRef"),
    "AssetResult": ("ginkgo.core.asset", "AssetResult"),
    "AssetVersion": ("ginkgo.core.asset", "AssetVersion"),
    "Expr": ("ginkgo.core.expr", "Expr"),
    "ExprList": ("ginkgo.core.expr", "ExprList"),
    "FlowDef": ("ginkgo.core.flow", "FlowDef"),
    "NotebookExpr": ("ginkgo.core.notebook", "NotebookExpr"),
    "PartialCall": ("ginkgo.core.task", "PartialCall"),
    "ScriptExpr": ("ginkgo.core.script", "ScriptExpr"),
    "SecretRef": ("ginkgo.core.secret", "SecretRef"),
    "ShellExpr": ("ginkgo.core.shell", "ShellExpr"),
    "SubWorkflowExpr": ("ginkgo.core.subworkflow", "SubWorkflowExpr"),
    "SubWorkflowResult": ("ginkgo.core.subworkflow", "SubWorkflowResult"),
    "TaskDef": ("ginkgo.core.task", "TaskDef"),
    "array": ("ginkgo.core.wrappers", "array"),
    "asset": ("ginkgo.core.asset", "asset"),
    "fig": ("ginkgo.core.wrappers", "fig"),
    "model": ("ginkgo.core.wrappers", "model"),
    "table": ("ginkgo.core.wrappers", "table"),
    "text": ("ginkgo.core.wrappers", "text"),
    "evaluate": ("ginkgo.runtime.evaluator", "evaluate"),
    "expand": ("ginkgo.wildcards", "expand"),
    "flatten": ("ginkgo.wildcards", "flatten"),
    "file": ("ginkgo.core.types", "file"),
    "flow": ("ginkgo.core.flow", "flow"),
    "folder": ("ginkgo.core.types", "folder"),
    "notebook": ("ginkgo.core.notebook", "notebook"),
    "remote_file": ("ginkgo.core.remote", "remote_file"),
    "remote_folder": ("ginkgo.core.remote", "remote_folder"),
    "script": ("ginkgo.core.script", "script"),
    "secret": ("ginkgo.core.secret", "secret"),
    "shell": ("ginkgo.core.shell", "shell"),
    "slug": ("ginkgo.wildcards", "slug"),
    "subworkflow": ("ginkgo.core.subworkflow", "subworkflow"),
    "task": ("ginkgo.core.task", "task"),
    "tmp_dir": ("ginkgo.core.types", "tmp_dir"),
    "zip_expand": ("ginkgo.wildcards", "zip_expand"),
}


class _GinkgoModule(ModuleType):
    """Package module that preserves callable exports across submodule imports."""

    def __setattr__(self, name: str, value: Any) -> None:
        # Keep ``ginkgo.config(...)`` callable even after ``ginkgo.config`` is imported.
        if (
            name == "config"
            and isinstance(value, ModuleType)
            and value.__name__ == "ginkgo.config"
        ):
            ModuleType.__setattr__(self, "_config_module", value)
            return

        super().__setattr__(name, value)


def config(path: str | Path) -> dict[str, Any]:
    """Load a project configuration file via the top-level package API."""
    from ginkgo.config import config as load_config

    return load_config(path)


def __getattr__(name: str):
    """Resolve top-level package exports lazily."""
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    module = import_module(module_name)
    value = getattr(module, attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return the names exposed by this package."""
    return sorted(list(globals().keys()) + list(_EXPORTS.keys()))


_package = sys.modules[__name__]
_package.__class__ = _GinkgoModule


__all__ = sorted(["config", *_EXPORTS.keys()])
