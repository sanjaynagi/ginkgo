"""Look up the plugin spec for a task's kind."""

from __future__ import annotations

from ginkgo.core.task import TaskDef
from ginkgo.plugins.api import TaskKind
from ginkgo.plugins.registry import plugin_registry

__all__ = ["task_kind"]


def task_kind(task_def: TaskDef) -> TaskKind:
    """Return the ``TaskKind`` an installed plugin provides for *task_def*.

    Raises
    ------
    PluginNotFoundError
        If no installed plugin provides the task's kind.
    """
    return plugin_registry().get(TaskKind, task_def.kind)
