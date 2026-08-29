"""Tests for the bus subscriber that persists a run."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml

from ginkgo.cli.commands.run import _close_unfinished_run
from ginkgo.runtime.events import (
    EventBus,
    GraphNodeRegistered,
    RunCompleted,
    RunStarted,
    TaskCompleted,
    TaskFailed,
    TaskLog,
)
from ginkgo.runtime.run_summary import RunSummary
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.store_recorder import StoreRecorder
from ginkgo.store.errors import StoreError
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


def _recorder(tmp_path: Path, *, run_id: str = "run-1") -> tuple[StoreRecorder, EventBus, Path]:
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    run_dir = RunDir.create(run_id=run_id, root=layout.runs)
    recorder = StoreRecorder(path=layout.db, run_dir=run_dir).start()
    bus = EventBus()
    bus.subscribe(recorder)
    return recorder, bus, layout.db


def test_the_manifest_is_exported_when_the_run_completes(tmp_path: Path) -> None:
    recorder, bus, db = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    bus.emit(GraphNodeRegistered(run_id="run-1", task_id="task_0000", node_id=0, task_name="demo"))
    bus.emit(TaskCompleted(run_id="run-1", task_id="task_0000", task_name="demo", attempt=1))
    bus.emit(RunCompleted(run_id="run-1", status="success"))
    recorder.close()

    manifest = yaml.safe_load(
        (tmp_path / ".ginkgo" / "runs" / "run-1" / "manifest.yaml").read_text(encoding="utf-8")
    )
    assert manifest["run_id"] == "run-1"
    assert manifest["status"] == "succeeded"
    assert [task["task_id"] for task in manifest["tasks"]] == ["task_0000"]

    with open_store(db, readonly=True) as store:
        assert store.query("SELECT snapshot_written FROM runs")[0]["snapshot_written"] == 1


def test_the_manifest_is_what_inspect_run_prints(tmp_path: Path) -> None:
    """One home for the run's serialised form: the file cannot drift from the CLI."""
    recorder, bus, db = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py", params={"seed": 7}))
    bus.emit(GraphNodeRegistered(run_id="run-1", task_id="task_0000", node_id=0, task_name="demo"))
    bus.emit(TaskCompleted(run_id="run-1", task_id="task_0000", task_name="demo", attempt=1))
    bus.emit(RunCompleted(run_id="run-1", status="success"))
    recorder.close()

    manifest = yaml.safe_load(
        (tmp_path / ".ginkgo" / "runs" / "run-1" / "manifest.yaml").read_text(encoding="utf-8")
    )
    with open_store(db, readonly=True) as store:
        summary = RunSummary.load(store, "run-1", runs_root=tmp_path / ".ginkgo" / "runs")

    assert manifest == summary.to_payload()
    assert manifest["params"] == {"seed": 7}


def test_a_committed_handler_can_read_what_it_is_reacting_to(tmp_path: Path) -> None:
    """The ordering guarantee is structural, not a note asking for care."""
    recorder, bus, db = _recorder(tmp_path)
    seen: list[tuple[str, list[str]]] = []

    def handler(event) -> None:  # noqa: ANN001 - any bus event
        with open_store(db, readonly=True) as store:
            rows = store.query(
                "SELECT task_id FROM tasks WHERE run_id = 'run-1' AND status = ?",
                ("failed" if event.event == "task_failed" else "succeeded",),
            )
        seen.append((event.event, [row["task_id"] for row in rows]))

    recorder.on_committed(handler)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    bus.emit(GraphNodeRegistered(run_id="run-1", task_id="task_0000", node_id=0, task_name="demo"))
    bus.emit(
        TaskFailed(run_id="run-1", task_id="task_0000", task_name="demo", attempt=1, exit_code=1)
    )
    bus.emit(
        GraphNodeRegistered(run_id="run-1", task_id="task_0001", node_id=1, task_name="other")
    )
    bus.emit(TaskCompleted(run_id="run-1", task_id="task_0001", task_name="other", attempt=1))
    bus.emit(RunCompleted(run_id="run-1", status="failed"))
    recorder.close()

    by_event = dict(seen)
    assert by_event["task_failed"] == ["task_0000"]
    assert by_event["task_completed"] == ["task_0001"]
    # Every event reaches the handler, in order, each after its own commit.
    assert [event for event, _ in seen] == [
        "run_started",
        "graph_node_registered",
        "task_failed",
        "graph_node_registered",
        "task_completed",
        "run_completed",
    ]


def test_log_chunks_are_not_stored(tmp_path: Path) -> None:
    recorder, bus, db = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    bus.emit(TaskLog(run_id="run-1", task_id="task_0000", task_name="demo", chunk="x" * 1000))
    recorder.close()

    with open_store(db, readonly=True) as store:
        types = [row["type"] for row in store.query("SELECT type FROM events")]
    assert types == ["run_started"]


def test_a_run_whose_ledger_cannot_be_written_fails_naming_the_database(
    tmp_path: Path,
) -> None:
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    layout.db.parent.mkdir(parents=True)
    # A directory where the database should be: nothing can open it.
    layout.db.mkdir()
    run_dir = RunDir.create(run_id="run-1", root=layout.runs)

    with pytest.raises(StoreError, match=str(layout.db)):
        StoreRecorder(path=layout.db, run_dir=run_dir).start()


def test_completing_twice_writes_the_manifest_once(tmp_path: Path) -> None:
    recorder, bus, _ = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    bus.emit(RunCompleted(run_id="run-1", status="success"))
    manifest_path = tmp_path / ".ginkgo" / "runs" / "run-1" / "manifest.yaml"
    written = manifest_path.read_text(encoding="utf-8")

    recorder.close()

    assert manifest_path.read_text(encoding="utf-8") == written


def test_a_run_that_unwinds_without_completing_is_marked_failed(tmp_path: Path) -> None:
    """A run can die before RunCompleted — a graph that will not build, an
    environment that will not prepare. Nothing comes back later to correct the
    ledger, so the guard on the way out has to."""
    recorder, bus, db = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))

    try:
        raise RuntimeError("Pixi environment 'ghost_env' not found.")
    except RuntimeError:
        _close_unfinished_run(bus=bus, recorder=recorder, run_id="run-1")
    recorder.close()

    with open_store(db, readonly=True) as store:
        run = dict(store.query("SELECT status, error, finished_at FROM runs")[0])
    assert run["status"] == "failed"
    assert "ghost_env" in run["error"]
    assert run["finished_at"] is not None
    assert (tmp_path / ".ginkgo" / "runs" / "run-1" / "manifest.yaml").is_file()


def test_the_guard_leaves_a_completed_run_alone(tmp_path: Path) -> None:
    recorder, bus, db = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    bus.emit(RunCompleted(run_id="run-1", status="success"))

    _close_unfinished_run(bus=bus, recorder=recorder, run_id="run-1")
    recorder.close()

    with open_store(db, readonly=True) as store:
        assert store.query("SELECT status FROM runs")[0]["status"] == "succeeded"


def test_a_dependency_on_an_unregistered_task_is_dropped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Collapsing unresolved ids onto one placeholder makes tasks each other's parent."""
    recorder, bus, db = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    for node_id in (0, 1):
        bus.emit(
            GraphNodeRegistered(
                run_id="run-1",
                task_id=f"task_{node_id:04d}",
                node_id=node_id,
                task_name="demo",
                dependency_ids=[99],
            )
        )
    bus.emit(RunCompleted(run_id="run-1", status="success"))
    recorder.close()

    with open_store(db, readonly=True) as store, caplog.at_level(logging.WARNING):
        summary = RunSummary.load(store, "run-1", runs_root=tmp_path / ".ginkgo" / "runs")

    assert [task.dependency_ids for task in summary.tasks] == [(), ()]
    assert "which the run has no task row for" in caplog.text
