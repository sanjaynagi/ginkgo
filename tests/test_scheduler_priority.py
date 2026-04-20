"""Priority tiebreak tests for the CP-SAT dispatch selection."""

from __future__ import annotations

import pytest

from ginkgo.core.task import TaskDef
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset


class TestSchedulerPriority:
    def test_priority_tiebreak_selects_higher_first(self) -> None:
        ready = [
            SchedulableTask(node_id=1, threads=2, memory_gb=0, priority=0),
            SchedulableTask(node_id=2, threads=2, memory_gb=0, priority=10),
            SchedulableTask(node_id=3, threads=2, memory_gb=0, priority=5),
        ]
        # Only 4 cores: 2 tasks can run concurrently.
        selected = select_dispatch_subset(ready_tasks=ready, jobs=4, cores=4)
        assert sorted(selected) == [2, 3]

    def test_priority_does_not_overrule_task_count_maximization(self) -> None:
        # Task A (priority 100) uses 4 cores. Tasks B, C (priority 0) use 1 core each.
        # With 4 cores total we can select {B, C} (count=2) or {A} (count=1).
        # total_selected dominates, so B and C must win despite lower priority.
        ready = [
            SchedulableTask(node_id=1, threads=4, memory_gb=0, priority=100),
            SchedulableTask(node_id=2, threads=1, memory_gb=0, priority=0),
            SchedulableTask(node_id=3, threads=1, memory_gb=0, priority=0),
        ]
        selected = select_dispatch_subset(ready_tasks=ready, jobs=4, cores=4)
        assert sorted(selected) == [2, 3]

    def test_priority_default_zero_does_not_change_selection(self) -> None:
        ready = [
            SchedulableTask(node_id=1, threads=1, memory_gb=0),
            SchedulableTask(node_id=2, threads=1, memory_gb=0),
            SchedulableTask(node_id=3, threads=1, memory_gb=0),
        ]
        selected = select_dispatch_subset(ready_tasks=ready, jobs=3, cores=3)
        assert sorted(selected) == [1, 2, 3]


class TestTaskDefPriorityValidation:
    def test_priority_out_of_range_rejected(self) -> None:
        def _f() -> int:
            return 0

        with pytest.raises(ValueError, match="priority must be in range"):
            TaskDef(fn=_f, priority=10_000)

    def test_priority_non_int_rejected(self) -> None:
        def _f() -> int:
            return 0

        with pytest.raises(TypeError, match="priority must be an integer"):
            TaskDef(fn=_f, priority=1.5)  # type: ignore[arg-type]
