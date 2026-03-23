"""Ginkgo — a dynamic, reproducible workflow orchestrator for scientific and data workflows."""

from __future__ import annotations

from pathlib import Path
import sys
from importlib import import_module
from types import ModuleType
from typing import Any

_EXPORTS = {
    "Expr": ("ginkgo.core.expr", "Expr"),
    "ExprList": ("ginkgo.core.expr", "ExprList"),
    "FlowDef": ("ginkgo.core.flow", "FlowDef"),
    "PartialCall": ("ginkgo.core.task", "PartialCall"),
    "SecretRef": ("ginkgo.core.secret", "SecretRef"),
    "ShellExpr": ("ginkgo.core.shell", "ShellExpr"),
    "TaskDef": ("ginkgo.core.task", "TaskDef"),
    "evaluate": ("ginkgo.runtime.evaluator", "evaluate"),
    "expand": ("ginkgo.helpers", "expand"),
    "flatten": ("ginkgo.helpers", "flatten"),
    "file": ("ginkgo.core.types", "file"),
    "flow": ("ginkgo.core.flow", "flow"),
    "folder": ("ginkgo.core.types", "folder"),
    "secret": ("ginkgo.core.secret", "secret"),
    "shell": ("ginkgo.core.shell", "shell"),
    "slug": ("ginkgo.helpers", "slug"),
    "task": ("ginkgo.core.task", "task"),
    "tmp_dir": ("ginkgo.core.types", "tmp_dir"),
    "zip_expand": ("ginkgo.helpers", "zip_expand"),
}

_LEGACY_MODULE_ALIASES = {
    "ginkgo.cache": "ginkgo.runtime.cache",
    "ginkgo.evaluator": "ginkgo.runtime.evaluator",
    "ginkgo.expr": "ginkgo.core.expr",
    "ginkgo.flow": "ginkgo.core.flow",
    "ginkgo.pixi": "ginkgo.envs.pixi",
    "ginkgo.pixi_worker": "ginkgo.envs.pixi_worker",
    "ginkgo.scheduler": "ginkgo.runtime.scheduler",
    "ginkgo.shell": "ginkgo.core.shell",
    "ginkgo.task": "ginkgo.core.task",
    "ginkgo.types": "ginkgo.core.types",
    "ginkgo.value_codec": "ginkgo.runtime.value_codec",
    "ginkgo.worker": "ginkgo.runtime.worker",
}


class _LazyAliasModule(ModuleType):
    """Module proxy that loads a legacy alias target on first attribute access."""

    def __init__(self, *, alias_name: str, target_name: str) -> None:
        super().__init__(alias_name)
        self._target_name = target_name

    def _resolve(self) -> ModuleType:
        module = import_module(self._target_name)
        sys.modules[self.__name__] = module
        return module

    def __getattr__(self, name: str):
        return getattr(self._resolve(), name)

    def __dir__(self) -> list[str]:
        return sorted(dir(self._resolve()))


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


for legacy_name, current_name in _LEGACY_MODULE_ALIASES.items():
    sys.modules.setdefault(
        legacy_name,
        _LazyAliasModule(alias_name=legacy_name, target_name=current_name),
    )


_package = sys.modules[__name__]
_package.__class__ = _GinkgoModule


__all__ = sorted(["config", *_EXPORTS.keys()])
