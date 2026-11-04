"""GPU resource declaration and placement tests."""

from __future__ import annotations

import pytest

from ginkgo import evaluate, task
from ginkgo.core.resources import Resources
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset
from tests.runtime.conftest import prebuilt_registry


@task(gpu=1)
def needs_gpu(x: int) -> int:
    return x * 2


@task(gpu=1, gpu_type="nvidia-tesla-t4")
def needs_t4(x: int) -> int:
    return x * 2


@task(gpu=1, executor="local")
def local_gpu(x: int) -> int:
    return x


@task(kind="shell", executor="remote")
def pinned_shell() -> str:
    raise AssertionError("never runs")


@task(kind="shell", gpu=1)
def gpu_shell() -> str:
    raise AssertionError("never runs")


def test_gpu_declarations() -> None:
    assert needs_t4.resources == Resources(gpu=1, gpu_type="nvidia-tesla-t4")
    assert (needs_t4.gpu, needs_t4.gpu_type) == (1, "nvidia-tesla-t4")
    with pytest.raises(ValueError, match="gpu must be at least 0"):
        Resources(gpu=-1)


class TestGpuPlacement:
    def test_gpu_task_runs_locally_within_budget(self) -> None:
        assert evaluate(needs_gpu(x=21), jobs=1, gpus=1) == 42

    @pytest.mark.parametrize(
        ("expr", "match"),
        [
            (lambda: needs_gpu(x=21), "requires 1 GPU"),
            (lambda: pinned_shell(), "cannot be dispatched remotely"),
        ],
    )
    def test_unplaceable_tasks_fail(self, expr, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            evaluate(expr(), jobs=1)

    @pytest.mark.parametrize(
        ("task_def", "gpus", "placed"),
        [
            (needs_gpu, 0, "remote"),
            (needs_gpu, 1, "remote"),
            (local_gpu, 1, None),
            (gpu_shell, 1, None),
        ],
        ids=["gpu-over-budget", "gpu-within-budget", "local-opt-out", "shell-stays-local"],
    )
    def test_placement_with_an_executor(self, task_def, gpus: int, placed) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, gpus=gpus, executor_registry=prebuilt_registry())
        assert evaluator.placement.resolve(task_def=task_def) == placed

    @pytest.mark.parametrize(
        ("task_def", "match"),
        [
            (gpu_shell, "kind='shell' cannot be dispatched remotely"),
            (local_gpu, "declares executor='local'"),
        ],
    )
    def test_a_local_task_over_the_gpu_budget_fails(self, task_def, match: str) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, executor_registry=prebuilt_registry())
        with pytest.raises(ValueError, match=match):
            evaluator.placement.resolve(task_def=task_def)

    def test_placement_misconfiguration_fails_at_build(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1)
        with pytest.raises(ValueError, match="no default executor"):
            evaluator.build_graph(needs_gpu(x=1))
            evaluator.validate_graph()


def _ready(*gpus: int) -> list[SchedulableTask]:
    return [
        SchedulableTask(node_id=index, threads=1, memory_gb=0, gpu=gpu)
        for index, gpu in enumerate(gpus, start=1)
    ]


class TestSchedulerGpuBudget:
    def test_gpu_budget_limits_selection(self) -> None:
        selected = select_dispatch_subset(ready_tasks=_ready(1, 1, 0), jobs=3, cores=3, gpus=1)
        assert 3 in selected
        assert len({1, 2} & set(selected)) == 1

    def test_zero_gpu_budget_excludes_gpu_tasks(self) -> None:
        assert select_dispatch_subset(ready_tasks=_ready(1, 0), jobs=2, cores=2) == [2]

    def test_zero_footprint_tasks_dispatch_when_cores_exhausted(self) -> None:
        # Remote-placed tasks report a zero local footprint; an exhausted
        # core budget must not block them.
        ready = [
            SchedulableTask(node_id=1, threads=0, memory_gb=0),
            SchedulableTask(node_id=2, threads=1, memory_gb=0),
        ]
        assert select_dispatch_subset(ready_tasks=ready, jobs=2, cores=0) == [1]
