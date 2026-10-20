"""Tests for the resource ledger, without an evaluator."""

from __future__ import annotations

import pytest

from ginkgo.runtime.scheduler import ResourceLedger, SchedulableTask


def _task(node_id: int, **fields) -> SchedulableTask:
    return SchedulableTask(
        node_id=node_id, threads=fields.pop("threads", 1), memory_gb=0, **fields
    )


class TestResourceLedger:
    def test_held_cores_are_not_offered_until_released(self) -> None:
        ledger = ResourceLedger(jobs=4, cores=4)
        ledger.reserve(_task(1, threads=3))

        assert ledger.select([_task(2, threads=2)]) == []

        ledger.release(1)
        assert ledger.select([_task(2, threads=2)]) == [2]

    def test_held_tasks_take_jobs_slots(self) -> None:
        ledger = ResourceLedger(jobs=2, cores=8)
        ledger.reserve(_task(1))

        assert ledger.select([_task(2), _task(3)]) == [2]

    def test_a_group_limit_counts_its_tasks_in_flight(self) -> None:
        ledger = ResourceLedger(jobs=8, cores=8)
        ledger.reserve(_task(1, concurrency_group="db", concurrency_group_limit=2))
        ready = [
            _task(node_id, concurrency_group="db", concurrency_group_limit=2) for node_id in (2, 3)
        ]

        assert ledger.select(ready) == [2]

    def test_custom_budgets_count_what_is_held(self) -> None:
        ledger = ResourceLedger(jobs=8, cores=8, custom_budgets={"api_calls": 3})
        ledger.reserve(_task(1, custom={"api_calls": 2}))

        assert ledger.select([_task(2, custom={"api_calls": 2}), _task(3)]) == [3]

    def test_memory_and_gpus_are_held_too(self) -> None:
        ledger = ResourceLedger(jobs=8, cores=8, memory=4, gpus=1)
        ledger.reserve(
            SchedulableTask(node_id=1, threads=1, memory_gb=3, gpu=1),
        )

        ready = [
            SchedulableTask(node_id=2, threads=1, memory_gb=2),
            SchedulableTask(node_id=3, threads=1, memory_gb=0, gpu=1),
            SchedulableTask(node_id=4, threads=1, memory_gb=1),
        ]
        assert ledger.select(ready) == [4]

    def test_a_node_cannot_hold_twice(self) -> None:
        ledger = ResourceLedger(jobs=8, cores=8)
        ledger.reserve(_task(1))

        with pytest.raises(RuntimeError, match="already holds"):
            ledger.reserve(_task(1))
