"""Tests for ``ginkgo debug`` run-level failure reporting."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from ginkgo.cli.commands.debug import command_debug


RUN_ID = "20260810_120000_000000_deadbeef"


def _write_manifest(*, cwd: Path, manifest: dict[str, Any]) -> None:
    run_dir = cwd / ".ginkgo" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")


def _failed_task() -> dict[str, Any]:
    return {
        "node_id": 0,
        "task_id": "task-0",
        "task": "explode",
        "status": "failed",
        "exit_code": 1,
        "error": "boom",
    }


def _debug(*, json_output: bool = False) -> int:
    return command_debug(SimpleNamespace(run_id=RUN_ID, json=json_output))


@pytest.fixture(autouse=True)
def _in_tmp_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_run_level_failure_without_failed_tasks_reports_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_manifest(
        cwd=tmp_path,
        manifest={
            "run_id": RUN_ID,
            "workflow": "wf.py",
            "status": "failed",
            "error": "Pixi environment 'ghost_env' not found.",
            "tasks": {},
        },
    )

    assert _debug() == 1
    stdout = capsys.readouterr().out
    assert "Debug Report" in stdout
    assert "Run Failure" in stdout
    assert "ghost_env" in stdout
    assert "No failed tasks found" not in stdout


def test_failed_task_and_run_level_error_are_both_rendered(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_manifest(
        cwd=tmp_path,
        manifest={
            "run_id": RUN_ID,
            "workflow": "wf.py",
            "status": "failed",
            "error": "orchestrator exploded",
            "tasks": {"task-0": _failed_task()},
        },
    )

    assert _debug() == 0
    stdout = capsys.readouterr().out
    assert "Failed Task: explode" in stdout
    assert "boom" in stdout
    assert "Run Failure" in stdout
    assert "orchestrator exploded" in stdout


def test_succeeded_run_keeps_empty_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_manifest(
        cwd=tmp_path,
        manifest={
            "run_id": RUN_ID,
            "workflow": "wf.py",
            "status": "succeeded",
            "tasks": {"task-0": {"node_id": 0, "task": "ok", "status": "succeeded"}},
        },
    )

    assert _debug() == 0
    stdout = capsys.readouterr().out
    assert f"✓ No failed tasks found in {RUN_ID}" in stdout
    assert "Run Failure" not in stdout


def test_manifest_without_status_or_error_is_treated_as_clean(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_manifest(cwd=tmp_path, manifest={"run_id": RUN_ID, "tasks": {}})

    assert _debug() == 0
    assert f"✓ No failed tasks found in {RUN_ID}" in capsys.readouterr().out


def test_failed_status_without_recorded_error_still_exits_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_manifest(
        cwd=tmp_path,
        manifest={"run_id": RUN_ID, "status": "failed", "tasks": {}},
    )

    assert _debug() == 1
    assert "No error recorded in the manifest." in capsys.readouterr().out


def test_json_payload_includes_run_level_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_manifest(
        cwd=tmp_path,
        manifest={
            "run_id": RUN_ID,
            "workflow": "wf.py",
            "status": "failed",
            "error": "orchestrator exploded",
            "tasks": {},
        },
    )

    assert _debug(json_output=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["error"] == "orchestrator exploded"
    assert payload["failures"] == []


def test_json_payload_carries_null_error_when_absent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_manifest(cwd=tmp_path, manifest={"run_id": RUN_ID, "tasks": {}})

    assert _debug(json_output=True) == 0
    assert json.loads(capsys.readouterr().out)["error"] is None
