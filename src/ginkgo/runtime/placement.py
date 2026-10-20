"""Where a task runs, and with what resources.

Both depend only on the task definition and the run's configuration, so
:class:`Placement` answers them before anything runs: the graph validates every
node's placement up front, and the evaluator reads the same answers when it
schedules.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ginkgo.core.resources import ResourceOverrides, Resources
from ginkgo.core.task import TaskDef
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.task_kinds import task_kind


@dataclass(kw_only=True)
class Placement:
    """Resolve a task's effective resources and the executor it runs on.

    Parameters
    ----------
    executor_registry : ExecutorRegistry
        The run's named executors and its default.
    resource_overrides : ResourceOverrides | None
        Site-level overrides merged over each task's declared resources.
    gpus : int
        The local GPU budget. A task needing more is placed on the default
        executor.
    """

    executor_registry: ExecutorRegistry = field(default_factory=ExecutorRegistry)
    resource_overrides: ResourceOverrides | None = None
    gpus: int = 0
    _effective_resources: dict[str, Resources] = field(default_factory=dict, repr=False)

    def effective_resources(self, *, task_def: TaskDef) -> Resources:
        """Return the task's declared resources with site overrides applied.

        Site overrides come from the ``[resources.overrides]`` runtime-config
        table and are merged over the decorator declaration. Memoized per
        task name, since the inputs are static for the lifetime of a run.
        """
        name = task_def.name
        cached = self._effective_resources.get(name)
        if cached is None:
            cached = task_def.resources
            if self.resource_overrides is not None:
                cached = self.resource_overrides.apply(task_name=name, base=cached)
            self._effective_resources[name] = cached
        return cached

    def resolve(self, *, task_def: TaskDef) -> str | None:
        """Return the executor a task is placed on, or ``None`` for local.

        Placement is derived from the declared requirement and the available
        capability. A task naming an ``executor`` routes there whatever the
        run default is; ``remote=True`` routes to the run's default executor
        (``--executor``); and a GPU requirement the local ``--gpus`` budget
        cannot satisfy falls back to that same default. Any route without a
        usable executor is an error rather than a silent local run.
        """
        registry = self.executor_registry
        if task_def.executor is not None:
            _require_remote_capable_kind(task_def=task_def, reason="executor")
            return registry.resolve(task_def.executor, task_name=task_def.name)
        if task_def.remote:
            _require_remote_capable_kind(task_def=task_def, reason="remote")
            if not registry.has_default:
                raise ValueError(
                    f"{task_def.name} declares remote=True but this run has no default "
                    f"executor (pass --executor <name>). {registry.available_hint()}"
                )
            assert registry.default_name is not None
            return registry.default_name
        gpu = self.effective_resources(task_def=task_def).gpu
        if gpu > self.gpus:
            if not task_kind(task_def).remote_capable:
                raise ValueError(
                    f"{task_def.name} requires {gpu} GPU(s) but only {self.gpus} "
                    "are available locally (--gpus), and kind="
                    f"{task_def.kind!r} cannot be dispatched remotely"
                )
            _require_remote_capable_kind(task_def=task_def, reason="a GPU requirement")
            if not registry.has_default:
                raise ValueError(
                    f"{task_def.name} requires {gpu} GPU(s) but only {self.gpus} "
                    "are available locally (--gpus) and this run has no default "
                    f"executor (--executor). {registry.available_hint()}"
                )
            return registry.default_name
        return None


def _require_remote_capable_kind(*, task_def: TaskDef, reason: str) -> None:
    """Reject remote placement for task kinds the workers cannot run.

    ``Out[...]`` parameters are supported on remote tasks: when a remote
    artifact store is configured (``[remote.artifacts] store``), declared
    output paths are rewritten to worker-local scratch paths, staged back
    through the same channel returned files use, and restored at their
    declared driver path once the job completes; see
    ``RemoteDispatchManager.dispatch`` and
    ``ginkgo.runtime.artifacts.remote_arg_transfer``. Without a configured
    store, ``Out[...]`` paths are sent to the worker unchanged, which only
    works when the worker shares the driver's filesystem.
    """
    if not task_kind(task_def).remote_capable:
        declaration = "remote=True" if reason == "remote" else f"executor={task_def.executor!r}"
        raise ValueError(
            f"{task_def.name} declares {declaration} but kind={task_def.kind!r} "
            "cannot be dispatched remotely"
        )
