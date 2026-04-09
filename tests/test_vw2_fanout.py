"""VW-2 — Fan-out / Fan-in: concurrent evaluation tests."""

import json
import time
from pathlib import Path

from ginkgo import evaluate, flow, task


def _write_interval(events_dir: str, name: str, started_at: float, ended_at: float) -> None:
    Path(events_dir).mkdir(parents=True, exist_ok=True)
    Path(events_dir, f"{name}.json").write_text(
        json.dumps({"end": ended_at, "start": started_at}),
        encoding="utf-8",
    )


def _read_intervals(events_dir: str, prefix: str) -> dict[str, tuple[float, float]]:
    intervals: dict[str, tuple[float, float]] = {}
    for path in Path(events_dir).glob(f"{prefix}-*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        intervals[path.stem] = (payload["start"], payload["end"])
    return intervals


def _peak_overlap(intervals: list[tuple[float, float]]) -> int:
    points: list[tuple[float, int]] = []
    for start, end in intervals:
        points.append((start, 1))
        points.append((end, -1))
    points.sort(key=lambda item: (item[0], item[1]))

    active = 0
    peak = 0
    for _, delta in points:
        active += delta
        peak = max(peak, active)
    return peak


@task()
def process(item: str, multiplier: int, events_dir: str) -> str:
    started_at = time.time()
    time.sleep(0.3)
    ended_at = time.time()
    _write_interval(events_dir, f"process-{item}", started_at, ended_at)
    return item * multiplier


@task()
def merge(results: list[str], events_dir: str) -> str:
    started_at = time.time()
    output = ",".join(sorted(results))
    ended_at = time.time()
    _write_interval(events_dir, "merge-run", started_at, ended_at)
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

        intervals = list(_read_intervals(events_dir, "process").values())
        process_makespan = max(end for _, end in intervals) - min(start for start, _ in intervals)
        assert result == ",".join(sorted(item * 2 for item in items))
        assert len(intervals) == 10
        assert _peak_overlap(intervals) >= 5
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

        process_intervals = _read_intervals(events_dir, "process")
        merge_interval = _read_intervals(events_dir, "merge")["merge-run"]
        latest_process_end = max(end for _, end in process_intervals.values())
        assert merge_interval[0] >= latest_process_end
