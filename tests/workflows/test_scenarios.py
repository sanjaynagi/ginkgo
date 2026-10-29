"""Validation-walkthrough scenarios, run end to end through ``evaluate``.

Tasks record what they did (a log line, or a timed interval) and each test
reads those records back to prove ordering, caching, failure handling and
resource limits.
"""

from __future__ import annotations

import time
from collections import Counter
from pathlib import Path

import pytest

from ginkgo import evaluate, flow, task, untracked
from tests._vw_support import append_line, load_intervals, peaks, timed

LOG = "events.log"


def _log() -> list[str]:
    return Path(LOG).read_text(encoding="utf-8").splitlines()


# VW-1: a linear chain ---------------------------------------------------------


@task()
def step_a(x: int, log_path: untracked) -> int:
    append_line(log_path, "step_a")
    return x + 1


@task()
def step_b(x: int, log_path: untracked) -> int:
    append_line(log_path, "step_b")
    return x * 2


@task()
def step_c(x: int, log_path: untracked) -> int:
    append_line(log_path, "step_c")
    return x - 3


@flow
def linear(start: int, log_path: str):
    a = step_a(x=start, log_path=log_path)
    return step_c(x=step_b(x=a, log_path=log_path), log_path=log_path)


def test_a_linear_chain_runs_each_step_once_in_order() -> None:
    assert evaluate(linear(start=5, log_path=LOG), jobs=1, cores=1) == 9
    assert _log() == ["step_a", "step_b", "step_c"]


# VW-2: fan-out and fan-in -----------------------------------------------------


@task()
def process(item: str, multiplier: int, events_dir: str) -> str:
    timed(events_dir, f"process-{item}", seconds=0.3)
    return item * multiplier


@task()
def merge(results: list[str], events_dir: str) -> str:
    timed(events_dir, "merge", seconds=0)
    return ",".join(sorted(results))


@flow
def fan_pipeline(items: list[str], multiplier: int, events_dir: str):
    processed = process(multiplier=multiplier, events_dir=events_dir).map(item=items)
    return merge(results=processed, events_dir=events_dir)


def test_a_fan_out_runs_concurrently_and_merges_after_every_branch() -> None:
    items = [f"item_{i}" for i in range(10)]

    result = evaluate(fan_pipeline(items=items, multiplier=2, events_dir="ev"), jobs=10, cores=10)

    branches = load_intervals("ev", prefix="process")
    (merged,) = load_intervals("ev", prefix="merge")
    assert result == ",".join(sorted(item * 2 for item in items))
    assert len(branches) == 10
    # Overlap, not a wall-clock bound: worker start-up on a small CI runner
    # varies too much to time.
    assert peaks(branches)["tasks"] >= 2
    assert merged["start"] >= max(iv["end"] for iv in branches)


# VW-3: conditional branching --------------------------------------------------


@task()
def categorise(value: int, log_path: untracked) -> str:
    append_line(log_path, "categorise")
    return "high" if value > 50 else "low"


@task()
def process_high(value: int, log_path: untracked) -> str:
    append_line(log_path, f"process_high:{value}")
    return f"high:{value}"


@task()
def process_low(value: int, log_path: untracked) -> str:
    append_line(log_path, f"process_low:{value}")
    return f"low:{value}"


@task()
def route(value: int, category: str, log_path: untracked):
    append_line(log_path, f"route:{category}")
    if category == "high":
        return process_high(value=value, log_path=log_path)
    return process_low(value=value, log_path=log_path)


@task()
def unrelated(seed: int, log_path: untracked) -> str:
    append_line(log_path, "unrelated")
    return f"unrelated:{seed}"


@flow
def conditional_pipeline(value: int, log_path: str):
    side_effect = unrelated(seed=1, log_path=log_path)
    category = categorise(value=value, log_path=log_path)
    return side_effect, route(value=value, category=category, log_path=log_path)


def test_a_branch_runs_only_its_path_and_switching_reruns_only_what_changed() -> None:
    assert evaluate(conditional_pipeline(value=80, log_path=LOG), jobs=1, cores=1) == (
        "unrelated:1",
        "high:80",
    )
    assert _log() == ["unrelated", "categorise", "route:high", "process_high:80"]

    assert evaluate(conditional_pipeline(value=20, log_path=LOG), jobs=1, cores=1) == (
        "unrelated:1",
        "low:20",
    )
    # ``unrelated`` stayed cached; everything downstream of the value reran.
    assert _log()[4:] == ["categorise", "route:low", "process_low:20"]


# VW-4: fan-out with branches that return early --------------------------------


@task()
def filter_or_process(item: str, log_path: untracked) -> str | None:
    append_line(log_path, f"filter:{item}")
    if item.startswith("skip_"):
        return None
    return process_high(value=len(item), log_path=log_path)


@flow
def mixed_pipeline(items: list[str], log_path: str):
    return filter_or_process(log_path=log_path).map(item=items)


def test_a_fan_out_keeps_order_and_expands_only_the_branches_that_ask() -> None:
    items = ["abc", "skip_x", "defgh", "skip_y", "z"]

    assert evaluate(mixed_pipeline(items=items, log_path=LOG)) == [
        "high:3",
        None,
        "high:5",
        None,
        "high:1",
    ]
    assert sorted(line for line in _log() if line.startswith("process_high:")) == [
        "process_high:1",
        "process_high:3",
        "process_high:5",
    ]


# VW-5: selective cache invalidation ------------------------------------------


@task()
def logged_process(item: str, multiplier: int, log_path: untracked) -> str:
    append_line(log_path, f"process:{item}")
    return item * multiplier


@task()
def logged_merge(results: list[str], log_path: untracked) -> str:
    append_line(log_path, "merge")
    return ",".join(sorted(results))


@flow
def logged_fan(items: list[str], log_path: str):
    processed = logged_process(multiplier=2, log_path=log_path).map(item=items)
    return logged_merge(results=processed, log_path=log_path)


def test_a_rerun_recomputes_only_the_changed_branch_and_its_dependants() -> None:
    items = ["a", "b", "c", "d", "e"]
    assert evaluate(logged_fan(items=items, log_path=LOG)) == "aa,bb,cc,dd,ee"
    assert evaluate(logged_fan(items=items, log_path=LOG)) == "aa,bb,cc,dd,ee"
    assert len(_log()) == 6  # the identical rerun executed nothing

    changed = ["a", "b", "changed", "d", "e"]
    assert evaluate(logged_fan(items=changed, log_path=LOG)) == "aa,bb,changedchanged,dd,ee"
    assert Counter(_log()[6:]) == Counter(["process:changed", "merge"])


# VW-6: partial failure --------------------------------------------------------


@task()
def may_fail_transient(item: str, log_path: untracked) -> str:
    append_line(log_path, f"start:{item}")
    marker = Path(f".transient-{item}")
    if item == "item_3" and not marker.exists():
        marker.write_text("failed once", encoding="utf-8")
        raise RuntimeError(f"transient failure on {item}")
    append_line(log_path, f"ok:{item}")
    return f"ok:{item}"


@flow
def failure_pipeline_transient(items: list[str], log_path: str):
    return may_fail_transient(log_path=log_path).map(item=items)


def test_a_rerun_after_a_transient_failure_serves_completed_items_from_cache() -> None:
    items = [f"item_{i}" for i in range(5)]
    done = [line for i in range(3) for line in (f"start:item_{i}", f"ok:item_{i}")]

    with pytest.raises(RuntimeError, match="transient failure on item_3"):
        evaluate(failure_pipeline_transient(items=items, log_path=LOG), jobs=1, cores=1)
    assert _log() == [*done, "start:item_3"]

    result = evaluate(failure_pipeline_transient(items=items, log_path=LOG), jobs=1, cores=1)

    assert result == [f"ok:item_{i}" for i in range(5)]
    assert _log() == [
        *done,
        "start:item_3",
        "start:item_3",
        "ok:item_3",
        "start:item_4",
        "ok:item_4",
    ]


def _wait_for_lines(log_path: str, lines: set[str]) -> None:
    """Block until every one of *lines* is in the log, or fail after 30 seconds."""
    deadline = time.monotonic() + 30
    while not lines <= set(Path(log_path).read_text(encoding="utf-8").splitlines()):
        if time.monotonic() > deadline:
            raise TimeoutError(f"never saw {sorted(lines)} in {log_path}")
        time.sleep(0.01)


@task()
def may_fail(item: str, log_path: untracked) -> str:
    append_line(log_path, f"start:{item}")
    # Sequenced through the log rather than by sleeps, so a slow worker start
    # cannot let a task finish, and free a slot, before item_0 has failed.
    if item == "item_0":
        _wait_for_lines(log_path, {"start:item_1", "start:item_2", "start:item_3"})
        append_line(log_path, "fail:item_0")
        raise RuntimeError("deliberate failure on item_0")
    _wait_for_lines(log_path, {"fail:item_0"})
    time.sleep(0.15)
    append_line(log_path, f"finish:{item}")
    return f"ok:{item}"


@flow
def failure_pipeline(items: list[str], log_path: str):
    return may_fail(log_path=log_path).map(item=items)


def test_tasks_in_flight_finish_after_a_failure_and_nothing_new_starts() -> None:
    items = [f"item_{i}" for i in range(8)]

    with pytest.raises(RuntimeError, match="deliberate failure on item_0"):
        evaluate(failure_pipeline(items=items, log_path=LOG), jobs=4, cores=4)

    events = _log()
    started = {line.removeprefix("start:") for line in events if line.startswith("start:")}
    assert started == {"item_0", "item_1", "item_2", "item_3"}
    assert {"finish:item_1", "finish:item_2", "finish:item_3"} <= set(events)


# VW-7 / VW-8: resource contention ---------------------------------------------


@task(threads=8)
def heavy(item: str, events_dir: str) -> str:
    timed(events_dir, f"heavy-{item}", seconds=0.15, threads=8, heavy=True)
    return f"heavy:{item}"


@task()
def light(item: str, events_dir: str) -> str:
    timed(events_dir, f"light-{item}", seconds=0.15, threads=1, heavy=False)
    return f"light:{item}"


@flow
def resource_pipeline(items: list[str], events_dir: str):
    return heavy(events_dir=events_dir).map(item=items[:5]), light(events_dir=events_dir).map(
        item=items[5:]
    )


def test_the_jobs_and_core_limits_are_never_exceeded() -> None:
    items = [f"item_{index}" for index in range(10)]

    heavy_results, light_results = evaluate(
        resource_pipeline(items=items, events_dir="ev"), jobs=10, cores=16
    )

    peak = peaks(load_intervals("ev"), "threads", "heavy")
    assert heavy_results == [f"heavy:{item}" for item in items[:5]]
    assert light_results == [f"light:{item}" for item in items[5:]]
    assert peak["heavy"] <= 2
    assert peak["tasks"] <= 10
    assert peak["threads"] <= 16


@task(threads=2, memory="16Gi")
def high_mem(item: str, events_dir: str) -> str:
    timed(events_dir, f"high-{item}", seconds=0.15, threads=2, memory_gb=16, high=True)
    return f"high:{item}"


@task(memory="4Gi")
def low_mem(item: str, events_dir: str) -> str:
    timed(events_dir, f"low-{item}", seconds=0.15, threads=1, memory_gb=4, high=False)
    return f"low:{item}"


@task(memory="64Gi")
def too_large(marker_path: str) -> str:
    Path(marker_path).write_text("ran", encoding="utf-8")
    return "ran"


@flow
def memory_pipeline(items: list[str], events_dir: str):
    return high_mem(events_dir=events_dir).map(item=items[:3]), low_mem(events_dir=events_dir).map(
        item=items[3:]
    )


def test_the_memory_limit_is_never_exceeded() -> None:
    items = [f"item_{index}" for index in range(6)]

    high_results, low_results = evaluate(
        memory_pipeline(items=items, events_dir="ev"), jobs=6, cores=8, memory=32
    )

    peak = peaks(load_intervals("ev"), "threads", "memory_gb", "high")
    assert high_results == [f"high:{item}" for item in items[:3]]
    assert low_results == [f"low:{item}" for item in items[3:]]
    assert peak["high"] <= 2
    assert peak["tasks"] <= 6
    assert peak["threads"] <= 8
    assert peak["memory_gb"] <= 32


def test_a_task_larger_than_the_memory_budget_fails_before_dispatch() -> None:
    with pytest.raises(ValueError, match="requires 64 GiB but only 32 GiB are available"):
        evaluate(too_large(marker_path="too-large.marker"), jobs=1, cores=1, memory=32)

    assert not Path("too-large.marker").exists()
