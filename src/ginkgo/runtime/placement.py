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
from ginkgo.envs.container import is_container_env, parse_container_uri
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
        satisfy is an error rather than a run that can never schedule it, and
        so is a remote one whose own image could not see its script.
        """
        registry = self.executor_registry
        if task_def.executor not in (None, LOCAL):
            _require_remote_capable_kind(task_def=task_def)
            name = registry.resolve(task_def.executor, task_name=task_def.base_name)
            return self._require_visible_source(task_def=task_def, executor=name)
        remote_capable = task_kind(task_def).remote_capable
        if task_def.executor is None and remote_capable and registry.has_default:
            return self._require_visible_source(task_def=task_def, executor=registry.default_name)
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

    def _require_visible_source(self, *, task_def: TaskDef, executor: str) -> str:
        """Return *executor*, once sure the task's image there can see its source file.

        A kind whose directive names a file of the project, as a script
        task's does, runs it from the project. Remotely in its own image, the
        task sees only the job's shared scratch, which holds the project only
        when the executor syncs code: a baked project is in the worker's image.

        Raises
        ------
        ValueError
            If the task runs in its own image on an executor that bakes its code.
        """
        code = self.executor_registry.code_config(executor) or {}
        image = remote_image(task_def=task_def)
        if (
            task_kind(task_def).eager_source_hash
            and image is not None
            and code.get("mode") != "sync"
        ):
            raise ValueError(
                f"{task_def.base_name} runs its {task_def.kind} in {image!r} on executor "
                f"{executor!r}, but that executor's code is baked into the worker image, "
                f"which {image!r} cannot see. Set mode = {'sync'!r} in the executor's code "
                f"table to ship the code in a bundle, or declare executor={LOCAL!r}."
            )
        return executor

    def runs_in_remote_image(self, *, task_def: TaskDef) -> bool:
        """Return whether the task's command runs in its container env's image remotely.

        The executor pulls that image, so the driver needs neither the image
        nor a container runtime.
        """
        return (
            remote_image(task_def=task_def) is not None
            and self.resolve(task_def=task_def) is not None
        )


def remote_image(*, task_def: TaskDef) -> str | None:
    """Return the image a remote executor runs the task's command in, or ``None`` for its worker's.

    A container env's image: the worker can't start a container itself, so
    the executor runs that image as a step of the job, between the worker's
    (see :func:`ginkgo.remote.executor_support.job_steps`).
    """
    env = task_def.env
    return parse_container_uri(env).image if env is not None and is_container_env(env) else None


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
