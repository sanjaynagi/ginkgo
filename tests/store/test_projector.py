"""Table-driven tests for the event-to-projection mapping.

Each case feeds a golden event fixture from ``tests/runtime/fixtures/events``
through the writer's own path — ``stored_event`` then ``projection_ops`` — and
asserts on the rows that come out, so the fixtures that pin the wire format
also pin what the ledger makes of it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ginkgo.store.projector import projection_ops
from ginkgo.store.protocol import StoredEvent
from ginkgo.store.sqlite import SqliteStore, open_store

FIXTURES = Path(__file__).parents[1] / "runtime" / "fixtures" / "events"
RUN_ID = "run_20260828_090000"
TASK_ID = "task_0007"


def _fixture(name: str, **overrides: Any) -> StoredEvent:
    """Return the stored row for one golden event payload."""
    payload = {**json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8")), **overrides}
    return StoredEvent(
        run_id=payload["run_id"],
        ts=payload["ts"],
        type=payload["event"],
        v=int(payload.get("v", 1)),
        task_id=payload.get("task_id"),
        attempt=payload.get("attempt"),
        cache_key=payload.get("cache_key"),
        asset_key=payload.get("asset_key"),
        payload=json.dumps(payload, sort_keys=True),
    )


def _apply(store: SqliteStore, *events: StoredEvent) -> None:
    with store.transaction():
        store.append(events)
        for event in events:
            store.apply(projection_ops(event))


@pytest.fixture
def store(tmp_path: Path):
    with open_store(tmp_path / "ginkgo.db") as opened:
        yield opened


@pytest.fixture
def started(store: SqliteStore) -> SqliteStore:
    """A store holding one started run with one registered task."""
    _apply(store, _fixture("run_started"), _fixture("graph_node_registered"))
    return store


def _row(store: SqliteStore, sql: str) -> dict[str, Any]:
    rows = store.query(sql)
    assert rows, sql
    return dict(rows[0])


def test_run_started_inserts_the_run(store: SqliteStore) -> None:
    _apply(store, _fixture("run_started"))

    run = _row(store, "SELECT * FROM runs")
    assert run["run_id"] == RUN_ID
    assert run["status"] == "running"
    assert run["started_at"] == "2026-08-28T09:00:00+00:00"
    assert json.loads(run["params"])
    assert run["snapshot_written"] == 0


def test_run_started_links_a_child_to_its_parent(store: SqliteStore) -> None:
    _apply(store, _fixture("run_started", parent_run_id="parent-1", parent_task_id="task_0002"))

    run = _row(store, "SELECT * FROM runs")
    assert (run["parent_run_id"], run["parent_task_id"]) == ("parent-1", "task_0002")
    edge = _row(store, "SELECT * FROM edges WHERE edge = 'child_of'")
    assert (edge["run_id"], edge["src_id"], edge["dst_id"]) == ("parent-1", RUN_ID, "task_0002")


def test_graph_node_registered_inserts_a_pending_task(started: SqliteStore) -> None:
    task = _row(started, "SELECT * FROM tasks")

    assert task["task_id"] == TASK_ID
    assert task["node_id"] == 7
    assert task["name"] == "analysis.fit"
    assert task["status"] == "pending"
    assert task["max_attempts"] == 3
    assert task["stdout_log"] == "logs/task_0007_analysis_fit.stdout.log"
    edge = _row(started, "SELECT * FROM edges WHERE edge = 'depends_on'")
    assert (edge["src_id"], edge["dst_id"]) == ("task_0001", TASK_ID)


def test_registering_the_same_node_twice_keeps_the_first_row(started: SqliteStore) -> None:
    _apply(started, _fixture("graph_node_registered", task_name="renamed"))

    assert _row(started, "SELECT * FROM tasks")["name"] == "analysis.fit"


def test_task_planned_records_the_cache_identity_and_inputs(started: SqliteStore) -> None:
    _apply(started, _fixture("task_planned"))

    task = _row(started, "SELECT * FROM tasks")
    assert task["cache_key"] == "cache_abcdef"
    assert task["source_hash"] == "src_abcdef"
    assert task["version"] == 3
    assert task["display_label"] == "fit[a]"
    row = _row(started, "SELECT * FROM task_inputs")
    assert row["param"] == "rows"
    assert json.loads(row["value_summary"]) == 10


def test_scalar_columns_are_stored_as_the_scalars_they_are(started: SqliteStore) -> None:
    """A join column holding a JSON-quoted string joins to nothing."""
    _apply(started, _fixture("task_planned", env_hash="env_deadbeef"))

    task = _row(started, "SELECT * FROM tasks")
    for column, expected in (
        ("env_hash", "env_deadbeef"),
        ("cache_key", "cache_abcdef"),
        ("source_hash", "src_abcdef"),
        ("extra_source_hash", "extra_abcdef"),
        ("display_label", "fit[a]"),
    ):
        assert task[column] == expected, column


def test_a_hashed_input_with_no_rendered_value_still_gets_a_row(
    started: SqliteStore,
) -> None:
    _apply(
        started,
        _fixture(
            "task_planned",
            inputs={},
            input_hashes=[{"param": "scratch", "digest": None, "type": "tmp_dir"}],
        ),
    )

    row = _row(started, "SELECT * FROM task_inputs")
    assert (row["param"], row["value_summary"], row["value_type"]) == ("scratch", None, "tmp_dir")


def test_a_phase_that_took_no_measurable_time_is_still_recorded(
    started: SqliteStore,
) -> None:
    _apply(started, _fixture("phase_timed", task_id=TASK_ID, phase="execute_seconds", seconds=0.0))

    assert json.loads(_row(started, "SELECT * FROM tasks")["timings"]) == {"execute_seconds": 0.0}


def test_a_negative_phase_reading_is_ignored(started: SqliteStore) -> None:
    _apply(started, _fixture("phase_timed", task_id=TASK_ID, phase="execute_seconds", seconds=-1))

    assert json.loads(_row(started, "SELECT * FROM tasks")["timings"]) == {}


def test_replanning_replaces_the_inputs_rather_than_adding_to_them(
    started: SqliteStore,
) -> None:
    _apply(started, _fixture("task_planned"))
    _apply(started, _fixture("task_planned", inputs={"columns": 4}))

    rows = started.query("SELECT param FROM task_inputs ORDER BY param")
    # "rows" came from the first plan and is gone; "table" is the hashed input
    # the fixture carries either way.
    assert [row["param"] for row in rows] == ["columns", "table"]


def test_task_started_opens_an_attempt(started: SqliteStore) -> None:
    _apply(started, _fixture("task_started"))

    task = _row(started, "SELECT * FROM tasks")
    assert task["status"] == "running"
    assert task["started_at"] == "2026-08-28T09:00:00+00:00"
    assert task["attempts"] == 1
    attempt = _row(started, "SELECT * FROM attempts")
    assert (attempt["attempt"], attempt["status"]) == (1, "running")


def test_task_completed_closes_the_task_and_indexes_its_outputs(started: SqliteStore) -> None:
    _apply(started, _fixture("task_started"), _fixture("task_completed"))

    task = _row(started, "SELECT * FROM tasks")
    assert task["status"] == "succeeded"
    assert task["exit_code"] == 0
    assert task["cached"] == 0
    assert json.loads(task["output_summary"])[0]["name"] == "table"
    assert json.loads(task["extra"])["assets"]
    output = _row(started, "SELECT * FROM task_outputs")
    assert (output["name"], output["value_type"]) == ("table", "table")


def test_a_cache_hit_marks_the_task_cached(started: SqliteStore) -> None:
    _apply(started, _fixture("task_cache_hit"))

    task = _row(started, "SELECT * FROM tasks")
    assert task["cached"] == 1
    assert task["cache_key"] == "cache_abcdef"


def test_task_failed_records_the_failure_and_closes_the_attempt(started: SqliteStore) -> None:
    _apply(started, _fixture("task_started"), _fixture("task_failed", attempt=1))

    task = _row(started, "SELECT * FROM tasks")
    assert task["status"] == "failed"
    assert task["exit_code"] == 1
    assert json.loads(task["failure"])["kind"] == "exception"
    attempt = _row(started, "SELECT * FROM attempts")
    assert attempt["status"] == "failed"


def test_task_retrying_reopens_the_task_and_closes_the_attempt(started: SqliteStore) -> None:
    _apply(started, _fixture("task_started"), _fixture("task_retrying"))

    task = _row(started, "SELECT * FROM tasks")
    assert task["status"] == "pending"
    assert task["finished_at"] is None
    attempt = _row(started, "SELECT * FROM attempts")
    assert attempt["status"] == "failed"
    assert attempt["retry_delay_s"] == 1.5


def test_task_annotated_promotes_known_keys_and_merges_the_rest(started: SqliteStore) -> None:
    _apply(
        started,
        _fixture(
            "task_annotated",
            fields={
                "resource_usage": {"measured": {"cpu_seconds": 1.0}},
                "remote_job_id": "job-1",
                "render_status": "ok",
            },
        ),
    )

    task = _row(started, "SELECT * FROM tasks")
    assert json.loads(task["resource_usage"])["measured"]["cpu_seconds"] == 1.0
    assert task["remote_job_id"] == "job-1"
    assert json.loads(task["extra"]) == {"render_status": "ok"}


def test_an_annotation_set_to_null_removes_the_field(started: SqliteStore) -> None:
    _apply(started, _fixture("task_annotated", fields={"render_error": "boom"}))
    _apply(started, _fixture("task_annotated", fields={"render_error": None}))

    assert json.loads(_row(started, "SELECT * FROM tasks")["extra"]) == {}


def test_phase_timings_accumulate_on_the_run_and_on_the_task(started: SqliteStore) -> None:
    _apply(
        started,
        _fixture("phase_timed", task_id=None, phase="workflow_load_seconds", seconds=1.5),
        _fixture("phase_timed", task_id=None, phase="workflow_load_seconds", seconds=0.25),
        _fixture("phase_timed", task_id=TASK_ID, phase="cache_lookup_seconds", seconds=0.5),
    )

    assert json.loads(_row(started, "SELECT * FROM runs")["timings"]) == {
        "workflow_load_seconds": 1.75
    }
    assert json.loads(_row(started, "SELECT * FROM tasks")["timings"]) == {
        "cache_lookup_seconds": 0.5
    }


def test_resource_samples_overwrite_each_other(started: SqliteStore) -> None:
    _apply(started, _fixture("run_resources_sampled", resources={"sample_count": 1}))
    _apply(started, _fixture("run_resources_sampled", resources={"sample_count": 9}))

    assert json.loads(_row(started, "SELECT * FROM runs")["resources"])["sample_count"] == 9


def test_run_completed_closes_the_run(started: SqliteStore) -> None:
    _apply(started, _fixture("run_completed"))

    run = _row(started, "SELECT * FROM runs")
    assert run["status"] == "succeeded"
    assert run["finished_at"] == "2026-08-28T09:00:00+00:00"


def test_a_failed_run_records_its_error(started: SqliteStore) -> None:
    _apply(started, _fixture("run_completed", status="failed", error="boom"))

    run = _row(started, "SELECT * FROM runs")
    assert (run["status"], run["error"]) == ("failed", "boom")


def test_graph_expansion_records_dynamic_dependencies(started: SqliteStore) -> None:
    _apply(started, _fixture("graph_expanded"))

    edge = _row(started, "SELECT * FROM edges WHERE edge = 'dynamic_depends_on'")
    assert (edge["src_id"], edge["dst_id"]) == ("task_0008", TASK_ID)


@pytest.mark.parametrize("name", ["task_log", "task_notice", "task_ready", "run_validated"])
def test_events_with_no_projection_leave_the_tables_alone(started: SqliteStore, name: str) -> None:
    before = started.query("SELECT * FROM tasks")
    _apply(started, _fixture(name))

    assert [dict(row) for row in started.query("SELECT * FROM tasks")] == [
        dict(row) for row in before
    ]
