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
    # come from the real random discriminator, not from the clock.
    real_datetime = rundir_module.datetime

    class _FixedDatetime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001, ANN206
            return real_datetime(2026, 4, 1, 12, 0, 0, 123456, tzinfo=tz)

    monkeypatch.setattr(rundir_module, "datetime", _FixedDatetime)

    first = make_run_id(workflow_path=tmp_path / "workflow.py")
    second = make_run_id(workflow_path=tmp_path / "workflow.py")

    assert first != second
    assert first.startswith("20260401_120000_123456_")
    assert second.startswith("20260401_120000_123456_")


def _register(ledger: Ledger, *, task_id: str = "task_0000", **fields) -> None:
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id=ledger.run_id,
            task_id=task_id,
            node_id=int(task_id[-4:]),
            task_name=fields.pop("task_name", "demo.task"),
            kind="python",
            execution_mode="worker",
            **fields,
        )
    )


def test_a_task_is_recorded_while_running_and_snapshotted_on_completion(ledger: Ledger) -> None:
    _register(ledger)
    assert ledger.summary().tasks[0].status == "pending"

    ledger.bus.emit(PhaseTimed(run_id=ledger.run_id, phase="workflow_load_seconds", seconds=1.25))
    ledger.bus.emit(
        PhaseTimed(
            run_id=ledger.run_id, task_id="task_0000", phase="cache_lookup_seconds", seconds=0.5
        )
    )
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
    assert (summary.status, task.status, task.kind) == ("succeeded", "succeeded", "python")
    assert task.outputs[0]["path"] == "results/out.txt"
    assert ledger.run_dir.manifest_path.is_file()
    payload = summary.to_payload()
    assert payload["timings"]["workflow_load_seconds"] == 1.25
    assert payload["tasks"][0]["timings"]["cache_lookup_seconds"] == 0.5
    # ``null`` rather than ``"local"``: a label is the renderer's job.
    assert payload["tasks"][0]["env"] is None


def test_run_resources_and_params_are_recorded(tmp_path: Path) -> None:
    ledger = Ledger.start(
        root=tmp_path,
        jobs=4,
        cores=2,
        memory=32,
        params=render_value({"token": secret("API_TOKEN"), "label": "base"}),
        param_sources={"label": "config"},
    )
    ledger.bus.emit(
        RunResourcesSampled(
            run_id=ledger.run_id,
            resources={"status": "completed", "peak": {"rss_bytes": 4096}},
        )
    )
    summary = ledger.finish()
    ledger.close()

    assert summary.to_payload()["resources"]["peak"]["rss_bytes"] == 4096
    assert summary.param_sources == {"label": "config"}
    # A run's parameters get the same rendering a task's arguments get.
    assert summary.params["token"]["redacted"] is True
    assert summary.params["label"] == "base"
    assert "SecretRef" not in ledger.run_dir.manifest_path.read_text(encoding="utf-8")


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

    token = ledger.summary().tasks[0].inputs["token"]
    assert token["redacted"] is True
    assert token["secret"]["name"] == "API_TOKEN"


def test_the_payload_carries_skipped_tasks_and_ignored_failures(ledger: Ledger) -> None:
    _register(ledger)
    _register(
        ledger, task_id="task_0001", task_name="demo.downstream", dependency_ids=["task_0000"]
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
    assert (failed.status, failed.ignored, skipped.status) == ("failed", True, "skipped")
    assert skipped.skipped_because == {
        "reason": "upstream_failed",
        "task_id": "task_0000",
        "task_name": "demo.task",
    }
    assert (summary.skipped_count, summary.ignored_failure_count) == (1, 1)
    assert summary.failed_tasks == (failed,)
    payload = summary.to_payload()
    assert payload["tasks"][0]["ignored"] is True
    assert payload["tasks"][1]["skipped_because"]["task_name"] == "demo.task"
    assert "ignored" not in payload["tasks"][1]
