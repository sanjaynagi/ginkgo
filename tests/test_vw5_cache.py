"""VW-5 — Selective cache invalidation tests."""

from collections import Counter
from pathlib import Path

from ginkgo import evaluate, flow, task
from tests._vw_support import append_line


@task()
def process(item: str, multiplier: int, log_path: str) -> str:
    append_line(log_path, f"process:{item}")
    return item * multiplier


@task()
def merge(results: list[str], log_path: str) -> str:
    append_line(log_path, "merge")
    return ",".join(sorted(results))


@flow
def fan_pipeline(items: list[str], multiplier: int, log_path: str):
    processed = process(multiplier=multiplier, log_path=log_path).map(item=items)
    return merge(results=processed, log_path=log_path)


class TestVW5SelectiveInvalidation:
    def test_identical_rerun_is_fully_cached(self):
        log_path = "events.log"
        items = ["a", "b", "c", "d", "e"]

        result1 = evaluate(fan_pipeline(items=items, multiplier=2, log_path=log_path))
        assert result1 == "aa,bb,cc,dd,ee"
        assert Path(".ginkgo/cache").exists()

        result2 = evaluate(fan_pipeline(items=items, multiplier=2, log_path=log_path))
        assert result2 == result1

        # The identical second run is served entirely from cache, so no task
        # body re-executes: each event appears exactly once.
        counts = Counter(Path(log_path).read_text(encoding="utf-8").splitlines())
        assert counts == Counter(
            {
                "process:a": 1,
                "process:b": 1,
                "process:c": 1,
                "process:d": 1,
                "process:e": 1,
                "merge": 1,
            }
        )

    def test_changed_branch_triggers_selective_recompute(self):
        log_path = "events.log"
        items = ["a", "b", "c", "d", "e"]

        result1 = evaluate(fan_pipeline(items=items, multiplier=2, log_path=log_path))
        assert result1 == "aa,bb,cc,dd,ee"

        updated_items = ["a", "b", "changed", "d", "e"]
        result2 = evaluate(fan_pipeline(items=updated_items, multiplier=2, log_path=log_path))
        assert result2 == "aa,bb,changedchanged,dd,ee"

        # Only the changed branch re-runs; the merge re-runs because its inputs
        # changed. The unchanged branches stay cached (one event each).
        counts = Counter(Path(log_path).read_text(encoding="utf-8").splitlines())
        assert counts == Counter(
            {
                "process:a": 1,
                "process:b": 1,
                "process:c": 1,
                "process:d": 1,
                "process:e": 1,
                "process:changed": 1,
                "merge": 2,
            }
        )
