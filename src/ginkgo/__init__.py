"""Ginkgo — a dynamic, reproducible workflow orchestrator for genomics."""

from __future__ import annotations

import sys
from importlib import import_module

from ginkgo.config import config
from ginkgo.core.expr import Expr, ExprList
from ginkgo.core.flow import FlowDef, flow
from ginkgo.core.shell import ShellExpr, shell_task
from ginkgo.core.task import PartialCall, TaskDef, task
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.evaluator import evaluate

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

for legacy_name, current_name in _LEGACY_MODULE_ALIASES.items():
    sys.modules.setdefault(legacy_name, import_module(current_name))

__all__ = [
    "Expr",
    "ExprList",
    "FlowDef",
    "PartialCall",
    "ShellExpr",
    "TaskDef",
    "config",
    "evaluate",
    "file",
    "flow",
    "folder",
    "shell_task",
    "task",
    "tmp_dir",
]
