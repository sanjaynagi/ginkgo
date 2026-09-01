"""GPU resource declaration and placement tests."""

from __future__ import annotations

import pytest

from ginkgo import evaluate, task
from ginkgo.core.resources import Resources
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.remote_executor import RemoteExecutor, RemoteJobHandle
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset


class _FakeRemoteExecutor(RemoteExecutor):
    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        raise AssertionError("placement tests never submit")


def _registry(name: str = "remote") -> ExecutorRegistry:
    """Registry with one prebuilt executor as the run default."""
    return ExecutorRegistry.for_executor(_FakeRemoteExecutor(), name=name)


@task(gpu=1)
def needs_gpu(x: int) -> int:
    return x * 2


@task(gpu=1, gpu_type="nvidia-tesla-t4")
def needs_t4(x: int) -> int:
    return x * 2


@task(remote=True)
def explicitly_remote(x: int) -> int:
    return x


@task(kind="shell", remote=True)
def remote_shell() -> str:
    raise AssertionError("never runs")


class TestResources:
    def test_defaults(self) -> None:
        resources = Resources()
        assert resources.threads == 1
        assert resources.memory is None
        assert resources.memory_gb == 0
        assert resources.gpu == 0
        assert resources.gpu_type is None

    def test_gpu_type_requires_gpu(self) -> None:
        with pytest.raises(ValueError, match="gpu_type requires gpu > 0"):
            Resources(gpu_type="nvidia-tesla-t4")

    def test_threads_validated(self) -> None:
        with pytest.raises(ValueError, match="threads must be at least 1"):
            Resources(threads=0)

    def test_gpu_validated(self) -> None:
        with pytest.raises(ValueError, match="gpu must be at least 0"):
            Resources(gpu=-1)

    def test_task_decorator_populates_resources(self) -> None:
        assert needs_t4.resources == Resources(gpu=1, gpu_type="nvidia-tesla-t4")
        assert needs_t4.gpu == 1
        assert needs_t4.gpu_type == "nvidia-tesla-t4"


class TestGpuPlacement:
    def test_gpu_task_runs_locally_within_budget(self) -> None:
        assert evaluate(needs_gpu(x=21), jobs=1, gpus=1) == 42

    def test_gpu_task_without_budget_or_executor_fails(self) -> None:
        with pytest.raises(ValueError, match="requires 1 GPU"):
            evaluate(needs_gpu(x=21), jobs=1)

    def test_remote_without_executor_fails(self) -> None:
        with pytest.raises(ValueError, match="no default executor"):
            evaluate(explicitly_remote(x=1), jobs=1)

    def test_remote_on_non_python_kind_fails(self) -> None:
        with pytest.raises(ValueError, match="only supports python tasks"):
            evaluate(remote_shell(), jobs=1)

    def test_gpu_over_budget_with_executor_places_remotely(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, executor_registry=_registry())
        assert evaluator._resolve_placement(task_def=needs_gpu) == "remote"

    def test_gpu_within_budget_with_executor_places_locally(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, gpus=1, executor_registry=_registry())
        assert evaluator._resolve_placement(task_def=needs_gpu) is None

    def test_explicit_remote_wins_over_local_capacity(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, executor_registry=_registry())
        assert evaluator._resolve_placement(task_def=explicitly_remote) == "remote"

    def test_remote_true_follows_the_run_default(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, executor_registry=_registry(name="gpu-k8s"))
        assert evaluator._resolve_placement(task_def=explicitly_remote) == "gpu-k8s"

    def test_gpu_on_non_python_kind_with_executor_names_the_kind(self) -> None:
        @task(kind="shell", gpu=1)
        def gpu_shell() -> str:
            raise AssertionError("never runs")

        evaluator = ConcurrentEvaluator(jobs=1, executor_registry=_registry())
        with pytest.raises(ValueError, match="not kind='shell'"):
            evaluator._resolve_placement(task_def=gpu_shell)

    def test_placement_misconfiguration_fails_at_build(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1)
        with pytest.raises(ValueError, match="no default executor"):
            evaluator.build_and_validate(explicitly_remote(x=1))


class TestSchedulerGpuBudget:
    def test_gpu_budget_limits_selection(self) -> None:
        selected = select_dispatch_subset(
            ready_tasks=[
                SchedulableTask(node_id=1, threads=1, memory_gb=0, gpu=1),
                SchedulableTask(node_id=2, threads=1, memory_gb=0, gpu=1),
                SchedulableTask(node_id=3, threads=1, memory_gb=0),
            ],
            jobs=3,
            cores=3,
            gpus=1,
        )
        assert 3 in selected
        assert len([node_id for node_id in selected if node_id in {1, 2}]) == 1

    def test_zero_footprint_tasks_dispatch_when_cores_exhausted(self) -> None:
        # Remote-placed tasks report a zero local footprint; an exhausted
        # core budget must not block them.
        selected = select_dispatch_subset(
            ready_tasks=[
                SchedulableTask(node_id=1, threads=0, memory_gb=0),
                SchedulableTask(node_id=2, threads=1, memory_gb=0),
            ],
            jobs=2,
            cores=0,
        )
        assert selected == [1]

    def test_zero_gpu_budget_excludes_gpu_tasks(self) -> None:
        selected = select_dispatch_subset(
            ready_tasks=[
                SchedulableTask(node_id=1, threads=1, memory_gb=0, gpu=1),
                SchedulableTask(node_id=2, threads=1, memory_gb=0),
            ],
            jobs=2,
            cores=2,
        )
        assert selected == [2]
