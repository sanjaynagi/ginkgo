"""VW-6 — Partial failure and concurrent fail-fast tests."""

from pathlib import Path
import time

import pytest

from ginkgo import evaluate, flow, task


def _append_line(path: str, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


@task()
def may_fail_transient(item: str, log_path: str) -> str:
    _append_line(log_path, f"start:{item}")
    marker = Path(f".transient-{item}")
    if item == "item_3" and not marker.exists():
        marker.write_text("failed once", encoding="utf-8")
        raise RuntimeError(f"transient failure on {item}")

    result = f"ok:{item}"
    _append_line(log_path, result)
    return result


@flow
def failure_pipeline_transient(items: list[str], log_path: str):
    return may_fail_transient(log_path=log_path).map(item=items)


@task()
def may_fail(item: str, log_path: str) -> str:
    _append_line(log_path, f"start:{item}")

    if item == "item_0":
        time.sleep(0.05)
        raise RuntimeError("deliberate failure on item_0")

    time.sleep(0.15)
    _append_line(log_path, f"finish:{item}")
    return f"ok:{item}"


@flow
def failure_pipeline(items: list[str], log_path: str):
    return may_fail(log_path=log_path).map(item=items)


class TestVW6Failure:
    def test_retry_after_transient_failure_uses_cache_for_completed_items(self):
        log_path = "events.log"
        items = [f"item_{i}" for i in range(5)]

        with pytest.raises(RuntimeError, match="transient failure on item_3"):
            evaluate(failure_pipeline_transient(items=items, log_path=log_path), jobs=1, cores=1)

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "start:item_0",
            "ok:item_0",
            "start:item_1",
            "ok:item_1",
            "start:item_2",
            "ok:item_2",
            "start:item_3",
        ]

        result = evaluate(
            failure_pipeline_transient(items=items, log_path=log_path), jobs=1, cores=1
        )

        assert result == [f"ok:item_{i}" for i in range(5)]
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "start:item_0",
            "ok:item_0",
            "start:item_1",
            "ok:item_1",
            "start:item_2",
            "ok:item_2",
            "start:item_3",
            "start:item_3",
            "ok:item_3",
            "start:item_4",
            "ok:item_4",
        ]

    def test_in_flight_tasks_are_allowed_to_finish_after_failure(self):
        log_path = "events.log"
        items = [f"item_{i}" for i in range(8)]

        with pytest.raises(RuntimeError, match="deliberate failure on item_0"):
            evaluate(failure_pipeline(items=items, log_path=log_path), jobs=4, cores=4)

        events = Path(log_path).read_text(encoding="utf-8").splitlines()
        assert sorted(events[:4]) == [
            "start:item_0",
            "start:item_1",
            "start:item_2",
            "start:item_3",
        ]
        assert "finish:item_1" in events
        assert "finish:item_2" in events
        assert "finish:item_3" in events
        assert all(f"start:item_{index}" not in events for index in range(4, 8))
