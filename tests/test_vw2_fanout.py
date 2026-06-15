"""VW-2 — Fan-out / Fan-in: concurrent evaluation tests."""

import time

from ginkgo import evaluate, flow, task
from tests._vw_support import load_intervals, peak_concurrency, write_interval


@task()
def process(item: str, multiplier: int, events_dir: str) -> str:
    started_at = time.time()
    time.sleep(0.3)
    ended_at = time.time()
    write_interval(events_dir, f"process-{item}", started_at=started_at, ended_at=ended_at)
    return item * multiplier


@task()
def merge(results: list[str], events_dir: str) -> str:
    started_at = time.time()
    output = ",".join(sorted(results))
    ended_at = time.time()
    write_interval(events_dir, "merge-run", started_at=started_at, ended_at=ended_at)
    return output


@flow
def fan_pipeline(items: list[str], multiplier: int, events_dir: str):
    processed = process(multiplier=multiplier, events_dir=events_dir).map(item=items)
    return merge(results=processed, events_dir=events_dir)


class TestVW2ConcurrentEvaluation:
    def test_all_process_tasks_execute_concurrently(self):
        items = [f"item_{i}" for i in range(10)]
        events_dir = "events"

        result = evaluate(
            fan_pipeline(items=items, multiplier=2, events_dir=events_dir), jobs=10, cores=10
        )

        intervals = load_intervals(events_dir, prefix="process")
        process_makespan = max(iv["end"] for iv in intervals) - min(
            iv["start"] for iv in intervals
        )
        assert result == ",".join(sorted(item * 2 for item in items))
        assert len(intervals) == 10
        assert peak_concurrency(intervals) >= 5
        assert process_makespan < 1.5

    def test_merge_receives_resolved_results(self):
        items = ["a", "b", "c"]
        result = evaluate(
            fan_pipeline(items=items, multiplier=3, events_dir="events"), jobs=3, cores=3
        )
        assert result == "aaa,bbb,ccc"

    def test_merge_runs_after_all_fanout_tasks_complete(self):
        items = ["x", "y", "z"]
        events_dir = "events"
        evaluate(fan_pipeline(items=items, multiplier=2, events_dir=events_dir), jobs=3, cores=3)

        process_intervals = load_intervals(events_dir, prefix="process")
        merge_interval = load_intervals(events_dir, prefix="merge")[0]
        latest_process_end = max(iv["end"] for iv in process_intervals)
        assert merge_interval["start"] >= latest_process_end
