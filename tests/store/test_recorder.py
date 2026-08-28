"""Tests for the bus subscriber that persists a run."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ginkgo.runtime.events import (
    EventBus,
    GraphNodeRegistered,
    RunCompleted,
    RunStarted,
    TaskCompleted,
    TaskLog,
)
from ginkgo.runtime.rundir import RunDir
from ginkgo.store.errors import StoreError
from ginkgo.store.recorder import StoreRecorder
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


def _recorder(tmp_path: Path, *, run_id: str = "run-1") -> tuple[StoreRecorder, EventBus, Path]:
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    run_dir = RunDir.create(run_id=run_id, root=layout.runs)
    recorder = StoreRecorder(path=layout.db, run_dir=run_dir).start()
    bus = EventBus()
    bus.subscribe(recorder)
    return recorder, bus, layout.db


def test_the_snapshot_is_exported_when_the_run_completes(tmp_path: Path) -> None:
    recorder, bus, db = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    bus.emit(GraphNodeRegistered(run_id="run-1", task_id="task_0000", task_name="demo"))
    bus.emit(TaskCompleted(run_id="run-1", task_id="task_0000", task_name="demo", attempt=1))
    bus.emit(RunCompleted(run_id="run-1", status="success"))
    recorder.close()

    manifest = yaml.safe_load(
        (tmp_path / ".ginkgo" / "runs" / "run-1" / "manifest.yaml").read_text(encoding="utf-8")
    )
    assert manifest["ginkgo_snapshot"] == 1
    assert manifest["runs"][0]["status"] == "succeeded"
    assert [task["task_id"] for task in manifest["tasks"]] == ["task_0000"]

    with open_store(db, readonly=True) as store:
        assert store.query("SELECT snapshot_written FROM runs")[0]["snapshot_written"] == 1


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


def test_completing_twice_writes_the_snapshot_once(tmp_path: Path) -> None:
    recorder, bus, _ = _recorder(tmp_path)
    bus.emit(RunStarted(run_id="run-1", workflow="flow.py"))
    bus.emit(RunCompleted(run_id="run-1", status="success"))
    manifest_path = tmp_path / ".ginkgo" / "runs" / "run-1" / "manifest.yaml"
    written = manifest_path.read_text(encoding="utf-8")

    recorder.close()

    assert manifest_path.read_text(encoding="utf-8") == written
