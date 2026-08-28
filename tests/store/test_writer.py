"""Tests for the queued ledger writer."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from ginkgo.runtime.events import (
    GinkgoEvent,
    GraphNodeRegistered,
    RunCompleted,
    RunStarted,
    TaskCompleted,
    TaskNotice,
)
from ginkgo.runtime.store_recorder import stored_event
from ginkgo.store.errors import StoreError
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import open_store
from ginkgo.store.writer import MAX_BATCH_EVENTS, StoreWriter


def _put(writer: StoreWriter, event: GinkgoEvent) -> None:
    """Queue one event, translating it the way the recorder does."""
    writer.put(stored_event(event))


def _writer(tmp_path: Path, *, run_id: str = "run-1") -> StoreWriter:
    writer = StoreWriter(path=tmp_path / "ginkgo.db", run_id=run_id)
    writer.start()
    return writer


def _events(tmp_path: Path) -> list[tuple[int, str]]:
    with open_store(tmp_path / "ginkgo.db", readonly=True) as store:
        return [(row["seq"], row["type"]) for row in store.query("SELECT seq, type FROM events")]


def test_stored_event_lifts_the_filtered_columns_out_of_the_payload() -> None:
    row = stored_event(
        TaskCompleted(
            run_id="run-1",
            task_id="task_0003",
            task_name="demo.task",
            attempt=2,
            cache_key="abc",
        )
    )

    assert (row.run_id, row.type, row.task_id, row.attempt, row.cache_key) == (
        "run-1",
        "task_completed",
        "task_0003",
        2,
        "abc",
    )
    assert '"task_name": "demo.task"' in row.payload


def test_events_are_stored_in_the_order_they_were_emitted(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
    for index in range(20):
        _put(
            writer,
            TaskNotice(
                run_id="run-1",
                task_id=f"task_{index:04d}",
                task_name="demo",
                message=str(index),
            ),
        )
    writer.close()

    rows = _events(tmp_path)
    assert [seq for seq, _ in rows] == sorted(seq for seq, _ in rows)
    assert rows[0][1] == "run_started"
    assert len(rows) == 21


def test_a_terminal_event_is_readable_before_the_writer_closes(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
    _put(writer, GraphNodeRegistered(run_id="run-1", task_id="task_0000", task_name="demo"))
    _put(writer, TaskCompleted(run_id="run-1", task_id="task_0000", task_name="demo", attempt=1))

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with open_store(tmp_path / "ginkgo.db", readonly=True) as store:
            rows = store.query("SELECT status FROM tasks WHERE task_id = 'task_0000'")
        if rows and rows[0]["status"] == "succeeded":
            break
        time.sleep(0.01)
    writer.close()

    assert rows and rows[0]["status"] == "succeeded"


def test_flush_makes_everything_queued_so_far_visible(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
    writer.flush()

    with open_store(tmp_path / "ginkgo.db", readonly=True) as store:
        assert store.query("SELECT run_id FROM runs")[0]["run_id"] == "run-1"
    writer.close()


def test_a_batch_larger_than_the_cap_still_lands_whole(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
    total = MAX_BATCH_EVENTS * 2 + 7
    for index in range(total):
        _put(
            writer,
            TaskNotice(run_id="run-1", task_id="task_0000", task_name="demo", message=str(index)),
        )
    writer.close()

    assert len(_events(tmp_path)) == total + 1


def test_ten_thousand_events_land_quickly(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    started = time.perf_counter()
    for index in range(10_000):
        _put(
            writer,
            TaskNotice(run_id="run-1", task_id="task_0000", task_name="demo", message=str(index)),
        )
    writer.close()
    elapsed = time.perf_counter() - started

    assert len(_events(tmp_path)) == 10_000
    assert elapsed < 5.0, f"10k events took {elapsed:.2f}s"


def test_the_run_records_what_writing_its_ledger_cost(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
    _put(writer, RunCompleted(run_id="run-1", status="success"))
    writer.close()

    with open_store(tmp_path / "ginkgo.db", readonly=True) as store:
        timings = store.query(
            "SELECT json_extract(timings, '$.provenance_write_seconds') AS s FROM runs"
        )
    assert timings[0]["s"] > 0


def test_a_write_failure_fails_the_run_and_names_the_database(tmp_path: Path) -> None:
    path = tmp_path / "ginkgo.db"
    writer = _writer(tmp_path)
    _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
    writer.flush()
    # Take the projection table out from under the writer: the next batch's
    # insert then has nowhere to land.
    with open_store(path) as store, store.transaction():
        store.apply([ProjectionOp(sql="DROP TABLE runs", params=())])

    with pytest.raises(StoreError, match=str(path)):
        _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
        writer.flush()
    # The failure is kept, not consumed: events reach put() from the resource
    # sampler's own thread, where a raise would die with the thread, so the
    # main thread's close() has to fail the run too.
    with pytest.raises(StoreError, match=str(path)):
        writer.close()


def test_close_is_idempotent(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
    writer.close()
    writer.close()

    assert len(_events(tmp_path)) == 1


def test_putting_after_close_is_refused(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.close()

    with pytest.raises(StoreError, match="already closed"):
        _put(writer, RunStarted(run_id="run-1", workflow="flow.py"))
