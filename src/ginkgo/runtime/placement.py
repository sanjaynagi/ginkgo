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
from ginkgo.runtime.executor_registry import LOCAL, ExecutorRegistry
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
        The local GPU budget. A task placed locally may not need more.
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

        A task naming an ``executor`` routes there whatever the run default
        is, and ``executor="local"`` keeps it on the driver. Otherwise a task
        whose kind is remote-capable goes to the run's default executor
        (``--executor``) when there is one, and any other task runs locally.
        A local task whose GPU requirement the ``--gpus`` budget cannot
        satisfy is an error rather than a run that can never schedule it.
        """
        registry = self.executor_registry
        if task_def.executor not in (None, LOCAL):
            _require_remote_capable_kind(task_def=task_def)
            return registry.resolve(task_def.executor, task_name=task_def.base_name)
        remote_capable = task_kind(task_def).remote_capable
        if task_def.executor is None and remote_capable and registry.has_default:
            return registry.default_name
        gpu = self.effective_resources(task_def=task_def).gpu
        if gpu > self.gpus:
            if task_def.executor == LOCAL:
                reason = f"it declares executor={LOCAL!r}"
            elif not remote_capable:
                reason = f"kind={task_def.kind!r} cannot be dispatched remotely"
            else:
                reason = (
                    f"this run has no default executor (--executor). {registry.available_hint()}"
                )
            raise ValueError(
                f"{task_def.base_name} requires {gpu} GPU(s) but only {self.gpus} "
                f"are available locally (--gpus), and {reason}"
            )
        return None


def _require_remote_capable_kind(*, task_def: TaskDef) -> None:
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
        raise ValueError(
            f"{task_def.base_name} declares executor={task_def.executor!r} but "
            f"kind={task_def.kind!r} cannot be dispatched remotely"
        )
