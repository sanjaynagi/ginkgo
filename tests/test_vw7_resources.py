"""VW-7 — Resource contention tests."""

import json
import time
from pathlib import Path

from ginkgo import evaluate, flow, task


def _write_interval(
    events_dir: str,
    name: str,
    *,
    started_at: float,
    ended_at: float,
    threads: int,
    heavy: bool,
) -> None:
    Path(events_dir).mkdir(parents=True, exist_ok=True)
    Path(events_dir, f"{name}.json").write_text(
        json.dumps(
            {
                "end": ended_at,
                "heavy": heavy,
                "start": started_at,
                "threads": threads,
            }
        ),
        encoding="utf-8",
    )


def _load_intervals(events_dir: str) -> list[dict[str, object]]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in Path(events_dir).glob("*.json")
    ]


def _compute_peaks(intervals: list[dict[str, object]]) -> tuple[int, int, int]:
    points: list[tuple[float, int, int, int]] = []
    for interval in intervals:
        threads = int(interval["threads"])
        heavy = 1 if interval["heavy"] else 0
        points.append((float(interval["start"]), 1, threads, heavy))
        points.append((float(interval["end"]), -1, -threads, -heavy))

    points.sort(key=lambda item: (item[0], item[1]))

    active_tasks = 0
    active_cores = 0
    active_heavy = 0
    peak_tasks = 0
    peak_cores = 0
    peak_heavy = 0
    for _, task_delta, core_delta, heavy_delta in points:
        active_tasks += task_delta
        active_cores += core_delta
        active_heavy += heavy_delta
        peak_tasks = max(peak_tasks, active_tasks)
        peak_cores = max(peak_cores, active_cores)
        peak_heavy = max(peak_heavy, active_heavy)

    return peak_tasks, peak_cores, peak_heavy


@task()
def heavy(item: str, events_dir: str, threads: int = 8) -> str:
    started_at = time.time()
    time.sleep(0.15)
    ended_at = time.time()
    _write_interval(
        events_dir,
        f"heavy-{item}",
        started_at=started_at,
        ended_at=ended_at,
        threads=threads,
        heavy=True,
    )
    return f"heavy:{item}"


@task()
def light(item: str, events_dir: str, threads: int = 1) -> str:
    started_at = time.time()
    time.sleep(0.15)
    ended_at = time.time()
    _write_interval(
        events_dir,
        f"light-{item}",
        started_at=started_at,
        ended_at=ended_at,
        threads=threads,
        heavy=False,
    )
    return f"light:{item}"


@flow
def resource_pipeline(items: list[str], events_dir: str):
    heavy_results = heavy(events_dir=events_dir).map(item=items[:5])
    light_results = light(events_dir=events_dir).map(item=items[5:])
    return heavy_results, light_results


class TestVW7ResourceContention:
    def test_jobs_and_core_limits_are_never_exceeded(self):
        items = [f"item_{index}" for index in range(10)]
        events_dir = "events"

        heavy_results, light_results = evaluate(
            resource_pipeline(items=items, events_dir=events_dir),
            jobs=10,
            cores=16,
        )

        peak_tasks, peak_cores, peak_heavy = _compute_peaks(_load_intervals(events_dir))
        assert heavy_results == [f"heavy:{item}" for item in items[:5]]
        assert light_results == [f"light:{item}" for item in items[5:]]
        assert peak_heavy <= 2
        assert peak_tasks <= 10
        assert peak_cores <= 16
