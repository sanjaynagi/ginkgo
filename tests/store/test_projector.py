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
TS = "2026-08-28T09:00:00+00:00"


def _fixture(name: str, *, drop: str | None = None, **overrides: Any) -> StoredEvent:
    """Return the stored row for one golden event payload.

    ``drop`` removes a field, replaying the event as version 1 recorded it.
    """
    payload = {**json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8")), **overrides}
    if drop is not None:
        del payload[drop]
        payload["v"] = 1
    return StoredEvent(
        run_id=payload["run_id"],
        ts=payload["ts"],
        type=payload["event"],
        v=int(payload.get("v", 1)),
        task_id=payload.get("task_id"),
        attempt=payload.get("attempt"),
        cache_key=payload.get("cache_key"),
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


def _task(store: SqliteStore) -> dict[str, Any]:
    return _row(store, "SELECT * FROM tasks")


def _extra(store: SqliteStore) -> dict[str, Any]:
    return json.loads(_task(store)["extra"])


def test_run_started_inserts_the_run(store: SqliteStore) -> None:
    _apply(store, _fixture("run_started"))

    run = _row(store, "SELECT * FROM runs")
    assert (run["run_id"], run["status"], run["started_at"]) == (RUN_ID, "running", TS)
    assert json.loads(run["params"])
    assert run["snapshot_written"] == 0


def test_run_started_links_a_child_to_its_parent(store: SqliteStore) -> None:
    _apply(store, _fixture("run_started", parent_run_id="parent-1", parent_task_id="task_0002"))

    run = _row(store, "SELECT * FROM runs")
    assert (run["parent_run_id"], run["parent_task_id"]) == ("parent-1", "task_0002")
    edge = _row(store, "SELECT * FROM edges WHERE edge = 'child_of'")
    assert (edge["run_id"], edge["src_id"], edge["dst_id"]) == ("parent-1", RUN_ID, "task_0002")


def test_graph_node_registered_inserts_a_pending_task_once(started: SqliteStore) -> None:
    _apply(started, _fixture("graph_node_registered", task_name="renamed"))

    task = _task(started)
    assert (task["task_id"], task["node_id"], task["name"]) == (TASK_ID, 7, "analysis.fit")
    assert (task["status"], task["max_attempts"]) == ("pending", 3)
    assert task["stdout_log"] == "logs/task_0007_analysis_fit.stdout.log"
    assert task["display_label"] == "fit[north]"
    edge = _row(started, "SELECT * FROM edges WHERE edge = 'depends_on'")
    assert (edge["src_id"], edge["dst_id"]) == ("task_0001", TASK_ID)


def test_task_planned_records_the_cache_identity_as_plain_scalars(started: SqliteStore) -> None:
    """A join column holding a JSON-quoted string joins to nothing."""
    _apply(started, _fixture("task_planned", env_hash="env_deadbeef"))

    task = _task(started)
    assert task["version"] == 3
    for column, expected in (
        ("env_hash", "env_deadbeef"),
        ("cache_key", "cache_abcdef"),
        ("source_hash", "src_abcdef"),
        ("extra_source_hash", "extra_abcdef"),
        ("display_label", "fit[a]"),
    ):
        assert task[column] == expected, column
    row = _row(started, "SELECT * FROM task_inputs WHERE param = 'rows'")
    assert json.loads(row["value_summary"]) == 10


def test_task_planned_without_a_label_keeps_the_registered_one(started: SqliteStore) -> None:
    _apply(started, _fixture("task_planned", display_label=None))

    assert _task(started)["display_label"] == "fit[north]"


def test_a_hashed_input_with_no_rendered_value_still_gets_a_row(started: SqliteStore) -> None:
    hashes = [{"param": "scratch", "digest": None, "type": "tmp_dir"}]
    _apply(started, _fixture("task_planned", inputs={}, input_hashes=hashes))

    row = _row(started, "SELECT * FROM task_inputs")
    assert (row["param"], row["value_summary"], row["value_type"]) == ("scratch", None, "tmp_dir")


def test_replanning_replaces_the_inputs_rather_than_adding_to_them(started: SqliteStore) -> None:
    _apply(started, _fixture("task_planned"))
    _apply(started, _fixture("task_planned", inputs={"columns": 4}))

    rows = started.query("SELECT param FROM task_inputs ORDER BY param")
    # "rows" came from the first plan and is gone; "table" is the hashed input.
    assert [row["param"] for row in rows] == ["columns", "table"]


def test_task_started_opens_an_attempt(started: SqliteStore) -> None:
    _apply(started, _fixture("task_started"))

    task = _task(started)
    assert (task["status"], task["started_at"], task["attempts"]) == ("running", TS, 1)
    attempt = _row(started, "SELECT * FROM attempts")
    assert (attempt["attempt"], attempt["status"]) == (1, "running")


def test_task_completed_closes_the_task_and_indexes_its_outputs(started: SqliteStore) -> None:
    _apply(started, _fixture("task_started"), _fixture("task_completed"))

    task = _task(started)
    assert (task["status"], task["exit_code"], task["cached"]) == ("succeeded", 0, 0)
    assert json.loads(task["output_summary"])[0]["name"] == "return"
    output = _row(started, "SELECT * FROM task_outputs")
    assert (output["name"], output["value_type"], output["artifact_id"]) == (
        "return",
        "file",
        "artifact_abcdef",
    )
    edge = _row(started, "SELECT * FROM edges WHERE edge = 'produced'")
    assert (edge["src_id"], edge["dst_kind"], edge["dst_id"]) == (
        TASK_ID,
        "artifact",
        "artifact_abcdef",
    )


def test_a_cache_hit_marks_the_task_cached(started: SqliteStore) -> None:
    _apply(started, _fixture("task_cache_hit"))

    task = _task(started)
    assert (task["cached"], task["cache_key"]) == (1, "cache_abcdef")


@pytest.mark.parametrize(
    ("failed", "ignored"),
    [
        (_fixture("task_failed", attempt=1), None),
        (_fixture("task_failed", attempt=1, ignored=True), True),
        # A run recorded before the failure policy existed replays unchanged.
        (_fixture("task_failed", attempt=1, drop="ignored"), None),
    ],
    ids=["fatal", "ignored-by-policy", "v1-payload"],
)
def test_task_failed_records_the_failure_and_closes_the_attempt(
    started: SqliteStore, failed: StoredEvent, ignored: bool | None
) -> None:
    _apply(started, _fixture("task_started"), failed)

    task = _task(started)
    assert (task["status"], task["exit_code"]) == ("failed", 1)
    assert json.loads(task["failure"])["kind"] == "exception"
    assert _extra(started).get("ignored") is ignored
    assert _row(started, "SELECT * FROM attempts")["status"] == "failed"


def test_task_skipped_closes_the_task_and_names_the_failure(started: SqliteStore) -> None:
    _apply(started, _fixture("task_skipped"))

    task = _task(started)
    assert (task["status"], task["cached"], task["finished_at"]) == ("skipped", 0, TS)
    assert _extra(started)["skipped_because"] == {
        "reason": "upstream_failed",
        "task_id": "task_0001",
        "task_name": "analysis.load",
    }
    assert not started.query("SELECT * FROM attempts")


def test_a_v1_task_skipped_payload_replays_as_an_upstream_failure(started: SqliteStore) -> None:
    """A run recorded before skip reasons knew only one: a failed upstream task."""
    _apply(started, _fixture("task_skipped", drop="reason"))

    assert _extra(started)["skipped_because"]["reason"] == "upstream_failed"


def test_a_skipped_task_keeps_the_attempt_it_really_made(started: SqliteStore) -> None:
    """A task waiting on its own expansion ran; the skip must not erase that."""
    _apply(started, _fixture("task_started"), _fixture("task_skipped"))

    task = _task(started)
    assert (task["status"], task["started_at"], task["attempts"]) == ("skipped", TS, 1)
    assert _row(started, "SELECT * FROM attempts")["status"] == "running"


def test_task_retrying_reopens_the_task_and_closes_the_attempt(started: SqliteStore) -> None:
    _apply(started, _fixture("task_started"), _fixture("task_retrying"))

    task = _task(started)
    assert (task["status"], task["finished_at"]) == ("pending", None)
    attempt = _row(started, "SELECT * FROM attempts")
    assert (attempt["status"], attempt["retry_delay_s"]) == ("failed", 1.5)


def test_task_annotated_promotes_known_keys_and_merges_the_rest(started: SqliteStore) -> None:
    fields = {
        "resource_usage": {"measured": {"cpu_seconds": 1.0}},
        "remote_job_id": "job-1",
        "render_status": "ok",
        "render_error": "boom",
    }
    _apply(started, _fixture("task_annotated", fields=fields))
    # An annotation set to null removes the field.
    _apply(started, _fixture("task_annotated", fields={"render_error": None}))

    task = _task(started)
    assert json.loads(task["resource_usage"])["measured"]["cpu_seconds"] == 1.0
    assert task["remote_job_id"] == "job-1"
    assert _extra(started) == {"render_status": "ok"}


def test_phase_timings_accumulate_on_the_run_and_on_the_task(started: SqliteStore) -> None:
    _apply(
        started,
        _fixture("phase_timed", task_id=None, phase="workflow_load_seconds", seconds=1.5),
        _fixture("phase_timed", task_id=None, phase="workflow_load_seconds", seconds=0.25),
        _fixture("phase_timed", task_id=TASK_ID, phase="cache_lookup_seconds", seconds=0.5),
        # A phase that took no measurable time is still recorded; a negative one is not.
        _fixture("phase_timed", task_id=TASK_ID, phase="execute_seconds", seconds=0.0),
        _fixture("phase_timed", task_id=TASK_ID, phase="stage_seconds", seconds=-1),
    )

    assert json.loads(_row(started, "SELECT * FROM runs")["timings"]) == {
        "workflow_load_seconds": 1.75
    }
    assert json.loads(_task(started)["timings"]) == {
        "cache_lookup_seconds": 0.5,
        "execute_seconds": 0.0,
    }


def test_resource_samples_overwrite_each_other(started: SqliteStore) -> None:
    _apply(started, _fixture("run_resources_sampled", resources={"sample_count": 1}))
    _apply(started, _fixture("run_resources_sampled", resources={"sample_count": 9}))

    assert json.loads(_row(started, "SELECT * FROM runs")["resources"])["sample_count"] == 9


@pytest.mark.parametrize(
    ("status", "error", "projected"),
    [
        ("success", None, "succeeded"),
        ("failed", "boom", "failed"),
        ("cancelled", "Interrupted by SIGINT", "cancelled"),
        ("who-knows", None, "failed"),
    ],
)
def test_run_completed_closes_the_run(
    started: SqliteStore, status: str, error: str | None, projected: str
) -> None:
    _apply(started, _fixture("run_completed", status=status, error=error))

    run = _row(started, "SELECT * FROM runs")
    assert (run["status"], run["error"], run["finished_at"]) == (projected, error, TS)


def test_graph_expansion_records_dynamic_dependencies(started: SqliteStore) -> None:
    _apply(started, _fixture("graph_expanded"))

    edge = _row(started, "SELECT * FROM edges WHERE edge = 'dynamic_depends_on'")
    assert (edge["src_id"], edge["dst_id"]) == ("task_0008", TASK_ID)


@pytest.mark.parametrize("name", ["task_log", "task_notice", "task_ready", "run_validated"])
def test_events_with_no_projection_leave_the_tables_alone(started: SqliteStore, name: str) -> None:
    before = [dict(row) for row in started.query("SELECT * FROM tasks")]
    _apply(started, _fixture(name))

    assert [dict(row) for row in started.query("SELECT * FROM tasks")] == before


def _usage(memory_gb: int, peak: int, **declared: int) -> dict[str, Any]:
    return {
        "declared": {"threads": 4, "memory_gb": memory_gb, **declared},
        "measured": {"peak_rss_bytes": peak, "cpu_seconds": 2.0},
    }


@pytest.mark.parametrize(
    ("usage", "declared", "effective"),
    [
        (_usage(16, 3_400_000_000, effective_memory_gb=16), 16, 16),
        # An escalated retry ran against 32 GiB: a 30 GiB peak fitted, it did not overrun.
        (_usage(16, 30_000_000_000, effective_memory_gb=32), 16, 32),
        # A record predating escalation tracking ran against its declaration.
        (_usage(8, 1_000), 8, 8),
    ],
    ids=["plain", "escalated-retry", "pre-escalation-record"],
)
def test_a_completed_task_projects_its_measurements(
    started: SqliteStore, usage: dict[str, Any], declared: int, effective: int
) -> None:
    _apply(started, _fixture("task_completed", resource_usage=usage))

    task = _task(started)
    assert task["peak_rss_bytes"] == usage["measured"]["peak_rss_bytes"]
    assert (task["cpu_seconds"], task["declared_threads"]) == (2.0, 4)
    assert (task["declared_memory_gb"], task["effective_memory_gb"]) == (declared, effective)


def test_a_hit_with_no_measurement_leaves_the_columns_null(started: SqliteStore) -> None:
    _apply(started, _fixture("task_completed", status="cached", resource_usage=None))

    task = _task(started)
    assert task["cached"] == 1
    assert task["peak_rss_bytes"] is task["cpu_seconds"] is task["declared_memory_gb"] is None


def test_usage_annotated_on_a_failed_attempt_survives_a_bare_completion(
    started: SqliteStore,
) -> None:
    """The peak right before an OOM kill is the one worth having; a retry must keep it."""
    _apply(
        started,
        _fixture("task_annotated", fields={"resource_usage": _usage(16, 16_800_000_000)}),
    )
    assert _task(started)["peak_rss_bytes"] == 16_800_000_000

    _apply(started, _fixture("task_completed", resource_usage=None))

    task = _task(started)
    assert (task["status"], task["peak_rss_bytes"]) == ("succeeded", 16_800_000_000)
    assert json.loads(task["resource_usage"])["measured"]["cpu_seconds"] == 2.0
