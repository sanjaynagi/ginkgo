"""VW-4 — Fan-out with conditional branches: evaluation tests."""

from pathlib import Path

from ginkgo import evaluate, flow, task


def _append_line(path: str, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


@task()
def process_high(value: int, log_path: str) -> str:
    _append_line(log_path, f"process_high:{value}")
    return f"high:{value}"


@task()
def filter_or_process(item: str, log_path: str) -> str | None:
    _append_line(log_path, f"filter:{item}")
    if item.startswith("skip_"):
        return None
    return process_high(value=len(item), log_path=log_path)


@flow
def mixed_pipeline(items: list[str], log_path: str):
    return filter_or_process(log_path=log_path).map(item=items)


class TestVW4MixedEvaluation:
    def test_mixed_pipeline_preserves_order(self):
        items = ["abc", "skip_x", "defgh", "skip_y", "z"]
        result = evaluate(mixed_pipeline(items=items, log_path="events.log"))
        assert result == ["high:3", None, "high:5", None, "high:1"]

    def test_only_non_skipped_branches_spawn_process_task(self):
        log_path = "events.log"
        items = ["abc", "skip_x", "defgh", "skip_y", "z"]
        evaluate(mixed_pipeline(items=items, log_path=log_path))
        events = Path(log_path).read_text(encoding="utf-8").splitlines()
        process_calls = sorted(event for event in events if event.startswith("process_high:"))
        assert process_calls == ["process_high:1", "process_high:3", "process_high:5"]
