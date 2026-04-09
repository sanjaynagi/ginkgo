"""Phase 20 — ``.map(max_concurrent=N)`` scheduler tests."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from ginkgo import evaluate, flow, task
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset


class TestSchedulerGroupConstraint:
    def test_group_limit_caps_selected_count(self) -> None:
        ready = [
            SchedulableTask(node_id=i, threads=1, memory_gb=0, concurrency_group="g")
            for i in range(5)
        ]

        selected = select_dispatch_subset(
            ready_tasks=ready,
            jobs=10,
            cores=10,
            available_group_slots={"g": 2},
        )

        assert len(selected) == 2

    def test_zero_remaining_slot_blocks_dispatch(self) -> None:
        ready = [
            SchedulableTask(node_id=1, threads=1, memory_gb=0, concurrency_group="g"),
            SchedulableTask(node_id=2, threads=1, memory_gb=0, concurrency_group="g"),
        ]
        selected = select_dispatch_subset(
            ready_tasks=ready,
            jobs=4,
            cores=4,
            available_group_slots={"g": 0},
        )
        assert selected == []

    def test_unrelated_tasks_unaffected_by_group_limit(self) -> None:
        ready = [
            SchedulableTask(node_id=1, threads=1, memory_gb=0, concurrency_group="g"),
            SchedulableTask(node_id=2, threads=1, memory_gb=0, concurrency_group="g"),
            SchedulableTask(node_id=3, threads=1, memory_gb=0),
            SchedulableTask(node_id=4, threads=1, memory_gb=0),
        ]
        selected = select_dispatch_subset(
            ready_tasks=ready,
            jobs=10,
            cores=10,
            available_group_slots={"g": 1},
        )
        # 1 from group g, plus both ungrouped tasks
        in_group = [n for n in selected if n in {1, 2}]
        ungrouped = [n for n in selected if n in {3, 4}]
        assert len(in_group) == 1
        assert sorted(ungrouped) == [3, 4]


# ----- End-to-end fan-out test -----------------------------------------------


_intervals_dir: dict[str, Path] = {}


@task()
def _record_interval(item: str) -> dict[str, float]:
    started = time.perf_counter()
    time.sleep(0.1)
    ended = time.perf_counter()
    return {"item": float(hash(item) % 1000), "start": started, "end": ended}


@flow
def _capped_flow(items: list[str]):
    return _record_interval().map(item=items, max_concurrent=1)


@flow
def _uncapped_flow(items: list[str]):
    return _record_interval().map(item=items)


def _peak_overlap(records: list[dict[str, float]]) -> int:
    points: list[tuple[float, int]] = []
    for record in records:
        points.append((record["start"], 1))
        points.append((record["end"], -1))
    points.sort(key=lambda item: (item[0], item[1]))
    active = 0
    peak = 0
    for _, delta in points:
        active += delta
        peak = max(peak, active)
    return peak


class TestMapMaxConcurrent:
    def test_max_concurrent_one_serializes_branches(self) -> None:
        items = [f"item_{i}" for i in range(4)]
        records = evaluate(_capped_flow(items=items), jobs=4, cores=4)
        assert len(records) == 4
        assert _peak_overlap(records) == 1

    def test_uncapped_runs_concurrently(self) -> None:
        items = [f"item_{i}" for i in range(4)]
        records = evaluate(_uncapped_flow(items=items), jobs=4, cores=4)
        assert _peak_overlap(records) >= 2

    def test_max_concurrent_must_be_positive(self) -> None:
        @task()
        def step(x: int) -> int:
            return x

        with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
            step().map(x=[1, 2], max_concurrent=0)
