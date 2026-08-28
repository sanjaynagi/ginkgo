"""The snapshot on disk must reconstruct the projections exactly.

This is the permanent guard on the ledger: run, export, delete the database,
rebuild, export again, compare. It holds because the snapshot *is* the
projection rows serialised — anything the exporter drops shows up here as a
difference, and anything rebuild misreads shows up as one too.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from ginkgo.runtime.events import GraphNodeRegistered, TaskAnnotated, TaskCompleted, TaskPlanned
from ginkgo.store.export import export_manifest
from ginkgo.store.rebuild import rebuild
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

from tests.conftest import Ledger


def _populated(tmp_path: Path, *, run_id: str = "run-1") -> Ledger:
    """Record a two-task run with inputs, outputs, annotations and an edge."""
    ledger = Ledger.start(
        root=tmp_path,
        run_id=run_id,
        params={"seed": 7},
        param_sources={"seed": "cli"},
    )
    for node_id, dependencies in ((0, []), (1, ["task_0000"])):
        task_id = f"task_{node_id:04d}"
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id=run_id,
                task_id=task_id,
                task_name=f"demo.task{node_id}",
                env="analysis",
                retries=2,
                dependency_ids=dependencies,
                stdout_log=f"logs/{task_id}.stdout.log",
                stderr_log=f"logs/{task_id}.stderr.log",
            )
        )
        ledger.bus.emit(
            TaskPlanned(
                run_id=run_id,
                task_id=task_id,
                task_name=f"demo.task{node_id}",
                inputs={"n": node_id, "nested": {"a": [1, 2]}},
                input_hashes=[{"param": "n", "digest": f"b3:{node_id}"}],
                cache_key=f"key-{node_id}",
                dependency_ids=dependencies,
            )
        )
        ledger.bus.emit(
            TaskAnnotated(
                run_id=run_id,
                task_id=task_id,
                task_name=f"demo.task{node_id}",
                fields={"backend": "local", "env_lock": "envs/analysis.pixi.lock"},
            )
        )
        ledger.bus.emit(
            TaskCompleted(
                run_id=run_id,
                task_id=task_id,
                task_name=f"demo.task{node_id}",
                attempt=1,
                cache_key=f"key-{node_id}",
                outputs=[{"name": "return", "type": "file", "path": f"out{node_id}.txt"}],
            )
        )
    return ledger


def _export(db: Path, run_id: str) -> dict:
    with open_store(db, readonly=True) as store:
        return export_manifest(store, run_id)


def test_the_snapshot_round_trips_through_a_rebuilt_database(tmp_path: Path) -> None:
    ledger = _populated(tmp_path)
    ledger.finish()
    ledger.close()
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")

    before = yaml.safe_load(ledger.run_dir.manifest_path.read_text(encoding="utf-8"))
    assert before == _export(layout.db, "run-1")

    layout.db.unlink()
    with open_store(layout.db) as store:
        result = rebuild(store, layout=layout)

    assert result.runs == ["run-1"]
    assert _export(layout.db, "run-1") == before


def test_rebuilding_twice_leaves_the_same_rows(tmp_path: Path) -> None:
    ledger = _populated(tmp_path)
    ledger.finish()
    ledger.close()
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    before = _export(layout.db, "run-1")

    with open_store(layout.db) as store:
        rebuild(store, layout=layout)
        rebuild(store, layout=layout)

    assert _export(layout.db, "run-1") == before


def test_a_directory_holding_no_snapshot_is_skipped_with_one_warning(tmp_path: Path) -> None:
    ledger = _populated(tmp_path)
    ledger.finish()
    ledger.close()
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    (layout.runs / "not-a-run").mkdir()
    (layout.runs / "wrong-shape").mkdir()
    (layout.runs / "wrong-shape" / "manifest.yaml").write_text(
        "run_id: legacy\n", encoding="utf-8"
    )

    with open_store(layout.db) as store:
        result = rebuild(store, layout=layout)

    assert result.runs == ["run-1"]
    assert len(result.skipped) == 2
    assert any("not-a-run" in message for message in result.skipped)
    assert any("not a ginkgo snapshot" in message for message in result.skipped)


def test_a_dry_run_reports_without_writing(tmp_path: Path) -> None:
    ledger = _populated(tmp_path)
    ledger.finish()
    ledger.close()
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    layout.db.unlink()

    with open_store(layout.db) as store:
        result = rebuild(store, layout=layout, dry_run=True)
        assert result.runs == ["run-1"]
        assert store.query("SELECT run_id FROM runs") == []


def test_a_workspace_with_no_runs_rebuilds_to_nothing(tmp_path: Path) -> None:
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    with open_store(layout.db) as store:
        result = rebuild(store, layout=layout)

    assert result.runs == []
    assert result.skipped == []
