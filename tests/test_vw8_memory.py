"""VW-8 — Memory contention tests."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from ginkgo import evaluate, flow, task


def _write_interval(
    events_dir: str,
    name: str,
    *,
    started_at: float,
    ended_at: float,
    threads: int,
    memory_gb: int,
    high_memory: bool,
) -> None:
    Path(events_dir).mkdir(parents=True, exist_ok=True)
    Path(events_dir, f"{name}.json").write_text(
        json.dumps(
            {
                "end": ended_at,
                "high_memory": high_memory,
                "memory_gb": memory_gb,
                "start": started_at,
                "threads": threads,
            }
        ),
        encoding="utf-8",
    )


def _load_intervals(events_dir: str) -> list[dict[str, object]]:
    return [
        json.loads(path.read_text(encoding="utf-8")) for path in Path(events_dir).glob("*.json")
    ]


def _compute_peaks(intervals: list[dict[str, object]]) -> tuple[int, int, int, int]:
    points: list[tuple[float, int, int, int, int]] = []
    for interval in intervals:
        threads = int(interval["threads"])
        memory_gb = int(interval["memory_gb"])
        high_memory = 1 if interval["high_memory"] else 0
        points.append((float(interval["start"]), 1, threads, memory_gb, high_memory))
        points.append((float(interval["end"]), -1, -threads, -memory_gb, -high_memory))

    points.sort(key=lambda item: (item[0], item[1]))

    active_tasks = 0
    active_cores = 0
    active_memory = 0
    active_high_memory = 0
    peak_tasks = 0
    peak_cores = 0
    peak_memory = 0
    peak_high_memory = 0
    for _, task_delta, core_delta, memory_delta, high_memory_delta in points:
        active_tasks += task_delta
        active_cores += core_delta
        active_memory += memory_delta
        active_high_memory += high_memory_delta
        peak_tasks = max(peak_tasks, active_tasks)
        peak_cores = max(peak_cores, active_cores)
        peak_memory = max(peak_memory, active_memory)
        peak_high_memory = max(peak_high_memory, active_high_memory)

    return peak_tasks, peak_cores, peak_memory, peak_high_memory


@task(threads=2)
def high_mem(item: str, events_dir: str, threads: int = 2, memory_gb: int = 16) -> str:
    started_at = time.time()
    time.sleep(0.15)
    ended_at = time.time()
    _write_interval(
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
    _write_interval(
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

        peak_tasks, peak_cores, peak_memory, peak_high_memory = _compute_peaks(
            _load_intervals(events_dir)
        )
        assert high_results == [f"high:{item}" for item in items[:3]]
        assert low_results == [f"low:{item}" for item in items[3:]]
        assert peak_high_memory <= 2
        assert peak_tasks <= 6
        assert peak_cores <= 8
        assert peak_memory <= 32

    def test_task_larger_than_budget_fails_before_dispatch(self) -> None:
        marker_path = "too-large.marker"

        with pytest.raises(ValueError, match="requires 64 GiB but only 32 GiB are available"):
            evaluate(too_large(marker_path=marker_path), jobs=1, cores=1, memory=32)

        assert not Path(marker_path).exists()
