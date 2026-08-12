"""VW-8 — Memory contention tests."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from ginkgo import evaluate, flow, task
from tests._vw_support import compute_peaks, load_intervals, write_interval


@task(threads=2)
def high_mem(item: str, events_dir: str, threads: int = 2, memory_gb: int = 16) -> str:
    started_at = time.time()
    time.sleep(0.15)
    ended_at = time.time()
    write_interval(
        events_dir,
        f"high-{item}",
        started_at=started_at,
        ended_at=ended_at,
        threads=threads,
        memory_gb=memory_gb,
        high_memory=True,
    )
    return f"high:{item}"


@task()
def low_mem(item: str, events_dir: str, threads: int = 1, memory_gb: int = 4) -> str:
    started_at = time.time()
    time.sleep(0.15)
    ended_at = time.time()
    write_interval(
        events_dir,
        f"low-{item}",
        started_at=started_at,
        ended_at=ended_at,
        threads=threads,
        memory_gb=memory_gb,
        high_memory=False,
    )
    return f"low:{item}"


@task()
def too_large(marker_path: str, memory_gb: int = 64) -> str:
    Path(marker_path).write_text("ran", encoding="utf-8")
    return "ran"


@flow
def memory_pipeline(items: list[str], events_dir: str):
    high_results = high_mem(events_dir=events_dir).map(item=items[:3])
    low_results = low_mem(events_dir=events_dir).map(item=items[3:])
    return high_results, low_results


class TestVW8MemoryContention:
    def test_memory_limit_is_never_exceeded(self) -> None:
        items = [f"item_{index}" for index in range(6)]
        events_dir = "memory-events"

        high_results, low_results = evaluate(
            memory_pipeline(items=items, events_dir=events_dir),
            jobs=6,
            cores=8,
            memory=32,
        )

        peaks = compute_peaks(
            load_intervals(events_dir),
            dimensions=("threads", "memory_gb", "high_memory"),
        )
        assert high_results == [f"high:{item}" for item in items[:3]]
        assert low_results == [f"low:{item}" for item in items[3:]]
        assert peaks["high_memory"] <= 2
        assert peaks["tasks"] <= 6
        assert peaks["threads"] <= 8
        assert peaks["memory_gb"] <= 32

    def test_task_larger_than_budget_fails_before_dispatch(self) -> None:
        marker_path = "too-large.marker"

        with pytest.raises(ValueError, match="requires 64 GiB but only 32 GiB are available"):
            evaluate(too_large(marker_path=marker_path), jobs=1, cores=1, memory=32)

        assert not Path(marker_path).exists()
