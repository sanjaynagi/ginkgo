"""Dispatch selection under cores, memory, GPUs, groups, custom budgets and priority."""

from __future__ import annotations

import time

import pytest

from ginkgo import evaluate, task
from ginkgo.core.task import TaskDef
from ginkgo.runtime.scheduler import ResourceLedger, SchedulableTask, select_dispatch_subset
from tests._vw_support import peaks


def _task(node_id: int, **fields) -> SchedulableTask:
    return SchedulableTask(
        node_id=node_id, threads=fields.pop("threads", 1), memory_gb=0, **fields
    )


class TestSelectDispatchSubset:
    def test_priority_breaks_ties_but_never_beats_task_count(self) -> None:
        ready = [
            _task(1, threads=2),
            _task(2, threads=2, priority=10),
            _task(3, threads=2, priority=5),
        ]
        assert sorted(select_dispatch_subset(ready_tasks=ready, jobs=4, cores=4)) == [2, 3]

        # {2, 3} runs two tasks; the high-priority 4-core task alone runs one.
        ready = [_task(1, threads=4, priority=100), _task(2), _task(3)]
        assert sorted(select_dispatch_subset(ready_tasks=ready, jobs=4, cores=4)) == [2, 3]

    @pytest.mark.parametrize(
        ("grouped", "options", "expected_grouped"),
        [
            ({"concurrency_group": "g"}, {"available_group_slots": {"g": 1}}, 1),
            ({"concurrency_group": "g"}, {"available_group_slots": {"g": 0}}, 0),
            ({"custom": {"api_calls": 2}}, {"custom_budgets": {"api_calls": 4}}, 2),
            ({"custom": {"api_calls": 1}}, {"custom_budgets": {"api_calls": 0}}, 0),
            ({"custom": {"api_calls": 2}}, {}, 3),
        ],
        ids=["group-cap", "group-exhausted", "budget-cap", "budget-exhausted", "unbudgeted"],
    )
    def test_a_limit_caps_only_the_tasks_it_names(
        self, grouped, options, expected_grouped
    ) -> None:
        ready = [_task(i, **grouped) for i in range(3)] + [_task(10), _task(11)]

        selected = select_dispatch_subset(ready_tasks=ready, jobs=10, cores=10, **options)

        assert len([n for n in selected if n < 10]) == expected_grouped
        assert {10, 11} <= set(selected)

    def test_independent_custom_dimensions_constrain_separately(self) -> None:
        ready = [
            _task(1, custom={"api_calls": 1, "db": 1}),
            _task(2, custom={"api_calls": 1}),
            _task(3, custom={"db": 1}),
        ]
        selected = select_dispatch_subset(
            ready_tasks=ready, jobs=10, cores=10, custom_budgets={"api_calls": 1, "db": 1}
        )
        # Task 1 consumes both budgets; the count-maximizing choice is 2 and 3.
        assert sorted(selected) == [2, 3]


class TestResourceLedger:
    def test_held_cores_and_jobs_are_not_offered_until_released(self) -> None:
        ledger = ResourceLedger(jobs=2, cores=4)
        ledger.reserve(_task(1, threads=2))

        assert ledger.select([_task(2, threads=3)]) == []
        assert ledger.select([_task(3), _task(4)]) == [3]  # two cores free, one job slot
        ledger.release(1)
        assert ledger.select([_task(2, threads=3)]) == [2]

    def test_held_groups_budgets_memory_and_gpus_count(self) -> None:
        ledger = ResourceLedger(jobs=8, cores=8, memory=4, gpus=1, custom_budgets={"api": 3})
        ledger.reserve(_task(1, concurrency_group="db", concurrency_group_limit=2))
        ledger.reserve(
            SchedulableTask(node_id=2, threads=1, memory_gb=3, gpu=1, custom={"api": 2})
        )

        ready = [
            _task(3, concurrency_group="db", concurrency_group_limit=2),
            _task(4, concurrency_group="db", concurrency_group_limit=2),
            _task(5, custom={"api": 2}),
            SchedulableTask(node_id=6, threads=1, memory_gb=2),
            SchedulableTask(node_id=7, threads=1, memory_gb=0, gpu=1),
            SchedulableTask(node_id=8, threads=1, memory_gb=1),
        ]
        assert sorted(ledger.select(ready)) in ([3, 8], [4, 8])

    def test_a_node_cannot_hold_twice(self) -> None:
        ledger = ResourceLedger(jobs=8, cores=8)
        ledger.reserve(_task(1))

        with pytest.raises(RuntimeError, match="already holds"):
            ledger.reserve(_task(1))


@pytest.mark.parametrize(
    ("priority", "error", "match"),
    [(10_000, ValueError, "priority must be in range"), (1.5, TypeError, "must be an integer")],
)
def test_task_priority_is_validated(priority, error, match) -> None:
    with pytest.raises(error, match=match):
        TaskDef(fn=lambda: 0, priority=priority)


# ----- End to end: the evaluator honours the limits --------------------------


@task()
def _interval(item: str) -> dict[str, float]:
    started = time.perf_counter()
    time.sleep(0.1)
    return {"start": started, "end": time.perf_counter()}


@task(resources={"api_calls": 1})
def _api_interval(item: str) -> dict[str, float]:
    started = time.perf_counter()
    time.sleep(0.1)
    return {"start": started, "end": time.perf_counter()}


def test_max_concurrent_serializes_a_mapped_fan_out() -> None:
    records = evaluate(_interval().map(item=list("abcd"), max_concurrent=1), jobs=4, cores=4)

    assert len(records) == 4
    assert peaks(records)["tasks"] == 1


def test_a_custom_budget_serializes_contending_tasks() -> None:
    records = evaluate(
        _api_interval().map(item=list("abcd")), jobs=4, cores=4, resource_budgets={"api_calls": 1}
    )

    assert len(records) == 4
    assert peaks(records)["tasks"] == 1


def test_max_concurrent_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
        _interval().map(item=["a"], max_concurrent=0)


@task(resources={"api_calls": 4})
def _hungry() -> int:
    return 1


@task(threads=4)
def _wide() -> int:
    return 1


@pytest.mark.parametrize(
    ("expr", "options", "match"),
    [
        (_hungry, {"resource_budgets": {"api_calls": 2}}, "requires 4 api_calls but only 2"),
        (_wide, {"cores": 1}, "requires 4 cores but only 1 are available"),
    ],
)
def test_a_task_over_a_run_budget_fails_fast(expr, options, match) -> None:
    with pytest.raises(ValueError, match=match):
        evaluate(expr(), jobs=1, **options)
