"""Regression tests for what a run records in the ledger."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import secret
import ginkgo.runtime.rundir as rundir_module
from ginkgo.runtime.rundir import make_run_id
from ginkgo.runtime.event_values import render_value
from ginkgo.runtime.events import (
    GraphNodeRegistered,
    PhaseTimed,
    RunResourcesSampled,
    TaskCompleted,
    TaskFailed,
    TaskPlanned,
    TaskSkipped,
)

from tests.conftest import Ledger


def test_make_run_id_remains_unique_under_fixed_clock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Freeze the clock so both ids share a timestamp. Uniqueness must then
    # come from the real random discriminator, not from the clock — so
    # token_hex is deliberately left unpatched.
    real_datetime = rundir_module.datetime

    class _FixedDatetime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001, ANN206
            return real_datetime(2026, 4, 1, 12, 0, 0, 123456, tzinfo=tz)

    monkeypatch.setattr(rundir_module, "datetime", _FixedDatetime)

    workflow_path = tmp_path / "workflow.py"
    first = make_run_id(workflow_path=workflow_path)
    second = make_run_id(workflow_path=workflow_path)

    assert first != second
    assert first.startswith("20260401_120000_123456_")
    assert second.startswith("20260401_120000_123456_")


def _register(ledger: Ledger, *, kind: str = "python") -> None:
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id=ledger.run_id,
            task_id="task_0000",
            node_id=0,
            task_name="demo.task",
            kind=kind,
            execution_mode="worker",
        )
    )


def test_marker_type_outputs_are_serialized_as_plain_strings(ledger: Ledger) -> None:
    _register(ledger)
    ledger.bus.emit(
        TaskCompleted(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="demo.task",
            attempt=1,
            outputs=[{"name": "return", "type": "file", "path": "results/out.txt"}],
        )
    )
    summary = ledger.finish()

    task = summary.tasks[0]
    assert task.outputs[0]["path"] == "results/out.txt"
    assert task.kind == "python"
    assert task.status == "succeeded"


def test_resources_and_memory_budget_are_recorded(tmp_path: Path) -> None:
    ledger = Ledger.start(root=tmp_path, jobs=4, cores=2, memory=32)
    ledger.bus.emit(
        RunResourcesSampled(
            run_id=ledger.run_id,
            resources={
                "status": "completed",
                "scope": "process_tree",
                "sample_count": 3,
                "peak": {"cpu_percent": 120.0, "rss_bytes": 4096, "process_count": 2},
            },
        )
    )
    summary = ledger.finish()
    ledger.close()

    assert summary.resources["status"] == "completed"
    assert summary.resources["peak"]["rss_bytes"] == 4096
    payload = summary.to_payload()
    assert payload["resources"]["peak"]["rss_bytes"] == 4096


def test_secret_inputs_are_redacted(ledger: Ledger) -> None:
    _register(ledger)
    ledger.bus.emit(
        TaskPlanned(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="demo.task",
            inputs=render_value({"token": secret("API_TOKEN")}),
        )
    )
    summary = ledger.summary()

    token = summary.tasks[0].inputs["token"]
    assert token["redacted"] is True
    assert token["secret"]["name"] == "API_TOKEN"


def test_timings_are_recorded_and_exposed_via_inspect(ledger: Ledger) -> None:
    _register(ledger)
    ledger.bus.emit(PhaseTimed(run_id=ledger.run_id, phase="workflow_load_seconds", seconds=1.25))
    ledger.bus.emit(
        PhaseTimed(
            run_id=ledger.run_id,
            task_id="task_0000",
            phase="cache_lookup_seconds",
            seconds=0.5,
        )
    )
    ledger.bus.emit(
        TaskCompleted(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="demo.task",
            status="cached",
        )
    )
    summary = ledger.finish()

    assert summary.timings["workflow_load_seconds"] == 1.25
    assert summary.tasks[0].timings["cache_lookup_seconds"] == 0.5

    payload = summary.to_payload()
    assert payload["timings"]["workflow_load_seconds"] == 1.25
    assert payload["tasks"][0]["timings"]["cache_lookup_seconds"] == 0.5


def test_the_payload_carries_skipped_tasks_and_ignored_failures(ledger: Ledger) -> None:
    _register(ledger)
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id=ledger.run_id,
            task_id="task_0001",
            node_id=1,
            task_name="demo.downstream",
            kind="python",
            execution_mode="worker",
            dependency_ids=["task_0000"],
        )
    )
    ledger.bus.emit(
        TaskFailed(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="demo.task",
            attempt=1,
            exit_code=1,
            failure={"kind": "exception", "message": "bad input"},
            ignored=True,
        )
    )
    ledger.bus.emit(
        TaskSkipped(
            run_id=ledger.run_id,
            task_id="task_0001",
            task_name="demo.downstream",
            blocked_by_task_id="task_0000",
            blocked_by_task_name="demo.task",
        )
    )
    summary = ledger.finish(status="failed")

    failed, skipped = summary.tasks
    assert (failed.status, failed.ignored) == ("failed", True)
    assert skipped.status == "skipped"
    assert skipped.skipped_because == {"task_id": "task_0000", "task_name": "demo.task"}
    assert skipped.cache_label == "—"
    assert summary.skipped_count == 1
    assert summary.ignored_failure_count == 1
    assert summary.failed_tasks == (failed,)

    payload = summary.to_payload()
    assert payload["tasks"][0]["ignored"] is True
    assert payload["tasks"][1]["status"] == "skipped"
    assert payload["tasks"][1]["skipped_because"]["task_name"] == "demo.task"
    assert "ignored" not in payload["tasks"][1]


def test_a_running_task_is_visible_before_the_run_finishes(ledger: Ledger) -> None:
    _register(ledger)
    assert ledger.summary().tasks[0].status == "pending"


def test_the_snapshot_is_written_when_the_run_completes(ledger: Ledger) -> None:
    _register(ledger)
    ledger.bus.emit(
        TaskCompleted(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="demo.task",
            status="cached",
        )
    )
    summary = ledger.finish()

    assert summary.status == "succeeded"
    assert summary.tasks[0].status == "cached"
    assert ledger.run_dir.manifest_path.is_file()


def test_secret_parameters_are_redacted_in_the_run_record(tmp_path: Path) -> None:
    """A run's parameters get the same rendering a task's arguments get."""
    ledger = Ledger.start(
        root=tmp_path,
        params=render_value({"token": secret("API_TOKEN"), "label": "base"}),
    )
    summary = ledger.finish()
    ledger.close()

    assert summary.params["token"]["redacted"] is True
    assert summary.params["token"]["secret"]["name"] == "API_TOKEN"
    assert summary.params["label"] == "base"
    recorded = ledger.run_dir.manifest_path.read_text(encoding="utf-8")
    assert "SecretRef" not in recorded


def test_a_task_with_no_environment_records_no_environment(ledger: Ledger) -> None:
    """``null`` rather than ``"local"``: no env is no env, and a label is the
    renderer's job, not the record's."""
    _register(ledger)
    payload = ledger.finish().to_payload()

    assert payload["tasks"][0]["env"] is None


def test_param_sources_are_recorded(tmp_path: Path) -> None:
    ledger = Ledger.start(
        root=tmp_path,
        params={"n_reps": 7, "label": "cfg"},
        param_sources={"n_reps": "cli", "label": "config"},
    )
    summary = ledger.finish()
    ledger.close()

    assert summary.param_sources == {"n_reps": "cli", "label": "config"}
    assert summary.params == {"n_reps": 7, "label": "cfg"}


def test_param_sources_default_to_empty(tmp_path: Path) -> None:
    ledger = Ledger.start(root=tmp_path)
    summary = ledger.finish()
    ledger.close()

    assert summary.param_sources == {}
    assert summary.params == {}
