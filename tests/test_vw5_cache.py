"""VW-5 — Selective cache invalidation tests."""

from collections import Counter
from pathlib import Path

from ginkgo import evaluate, flow, task


def _append_line(path: str, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


@task()
def process(item: str, multiplier: int, log_path: str) -> str:
    _append_line(log_path, f"process:{item}")
    return item * multiplier


@task()
def merge(results: list[str], log_path: str) -> str:
    _append_line(log_path, "merge")
    return ",".join(sorted(results))


@flow
def fan_pipeline(items: list[str], multiplier: int, log_path: str):
    processed = process(multiplier=multiplier, log_path=log_path).map(item=items)
    return merge(results=processed, log_path=log_path)


class TestVW5SelectiveInvalidation:
    def test_only_changed_branch_and_merge_are_recomputed(self):
        log_path = "events.log"
        items = ["a", "b", "c", "d", "e"]

        result1 = evaluate(fan_pipeline(items=items, multiplier=2, log_path=log_path))
        assert result1 == "aa,bb,cc,dd,ee"
        assert Path(".ginkgo/cache").exists()

        result2 = evaluate(fan_pipeline(items=items, multiplier=2, log_path=log_path))
        assert result2 == result1

        updated_items = ["a", "b", "changed", "d", "e"]
        result3 = evaluate(fan_pipeline(items=updated_items, multiplier=2, log_path=log_path))
        assert result3 == "aa,bb,changedchanged,dd,ee"

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
