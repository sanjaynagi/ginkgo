"""VW-7 — Resource contention tests."""

import time

from ginkgo import evaluate, flow, task
from tests._vw_support import compute_peaks, load_intervals, write_interval


@task(threads=8)
def heavy(item: str, events_dir: str, threads: int = 8) -> str:
    started_at = time.time()
    time.sleep(0.15)
    ended_at = time.time()
    write_interval(
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
    write_interval(
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

        peaks = compute_peaks(load_intervals(events_dir), dimensions=("threads", "heavy"))
        assert heavy_results == [f"heavy:{item}" for item in items[:5]]
        assert light_results == [f"light:{item}" for item in items[5:]]
        assert peaks["heavy"] <= 2
        assert peaks["tasks"] <= 10
        assert peaks["threads"] <= 16
