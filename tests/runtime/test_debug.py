"""Tests for ``ginkgo debug`` run-level failure reporting."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ginkgo.cli.commands.debug import command_debug
from ginkgo.runtime.events import GraphNodeRegistered, TaskFailed

from tests.conftest import Ledger


RUN_ID = "20260810_120000_000000_deadbeef"


def _record_run(
    *,
    cwd: Path,
    status: str = "success",
    error: str | None = None,
    failed_task: bool = False,
    succeeded_task: bool = False,
) -> None:
    """Record one run in the workspace ``ginkgo debug`` will read."""
    ledger = Ledger.start(root=cwd, run_id=RUN_ID, workflow="wf.py")
    if failed_task or succeeded_task:
        name = "explode" if failed_task else "ok"
        ledger.bus.emit(
            GraphNodeRegistered(run_id=RUN_ID, task_id="task_0000", node_id=0, task_name=name)
        )
    if failed_task:
        ledger.bus.emit(
            TaskFailed(
                run_id=RUN_ID,
                task_id="task_0000",
                task_name="explode",
                attempt=1,
                exit_code=1,
                failure={"kind": "user_code_error", "message": "boom"},
            )
        )
    ledger.finish(status=status, error=error)
    ledger.close()


def _debug(*, json_output: bool = False) -> int:
    return command_debug(SimpleNamespace(run_id=RUN_ID, json=json_output))


@pytest.fixture(autouse=True)
def _in_tmp_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_run_level_failure_without_failed_tasks_reports_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _record_run(cwd=tmp_path, status="failed", error="Pixi environment 'ghost_env' not found.")

    # Exit code reports whether debug could produce a report, not whether the
    # inspected run failed, matching `runs show`.
    assert _debug() == 0
    stdout = capsys.readouterr().out
    assert "Debug Report" in stdout
    assert "Run Failure" in stdout
    assert "ghost_env" in stdout
    assert "No failed tasks found" not in stdout


def test_failed_task_and_run_level_error_are_both_rendered(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _record_run(cwd=tmp_path, status="failed", error="orchestrator exploded", failed_task=True)

    assert _debug() == 0
    stdout = capsys.readouterr().out
    assert "Failed Task: explode" in stdout
    assert "boom" in stdout
    assert "Run Failure" in stdout
    assert "orchestrator exploded" in stdout


def test_succeeded_run_keeps_empty_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _record_run(cwd=tmp_path, succeeded_task=True)

    assert _debug() == 0
    stdout = capsys.readouterr().out
    assert f"✓ No failed tasks found in {RUN_ID}" in stdout
    assert "Run Failure" not in stdout


def test_a_run_that_recorded_no_failure_is_treated_as_clean(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _record_run(cwd=tmp_path)

    assert _debug() == 0
    assert f"✓ No failed tasks found in {RUN_ID}" in capsys.readouterr().out


def test_failed_status_without_recorded_error_says_so(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _record_run(cwd=tmp_path, status="failed")

    assert _debug() == 0
    assert "No error recorded in the manifest." in capsys.readouterr().out


def test_json_payload_includes_run_level_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _record_run(cwd=tmp_path, status="failed", error="orchestrator exploded")

    assert _debug(json_output=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["error"] == "orchestrator exploded"
    assert payload["failures"] == []


def test_json_payload_carries_null_error_when_absent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _record_run(cwd=tmp_path)

    assert _debug(json_output=True) == 0
    assert json.loads(capsys.readouterr().out)["error"] is None
