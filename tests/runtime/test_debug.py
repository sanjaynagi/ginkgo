"""Tests for ``ginkgo debug`` run-level failure reporting."""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from ginkgo.cli.commands.debug import command_debug
from ginkgo.runtime.events import GraphNodeRegistered, TaskFailed, TaskPlanned

from tests.conftest import Ledger


RUN_ID = "20260810_120000_000000_deadbeef"


def _record_run(
    *,
    cwd: Path,
    status: str = "success",
    error: str | None = None,
    failed_task: bool = False,
    succeeded_task: bool = False,
    message: str = "boom",
    display_label: str | None = None,
) -> None:
    """Record one run in the workspace ``ginkgo debug`` will read."""
    ledger = Ledger.start(root=cwd, run_id=RUN_ID, workflow="wf.py")
    if failed_task or succeeded_task:
        name = "explode" if failed_task else "ok"
        ledger.bus.emit(
            GraphNodeRegistered(run_id=RUN_ID, task_id="task_0000", node_id=0, task_name=name)
        )
    if display_label is not None:
        ledger.bus.emit(
            TaskPlanned(
                run_id=RUN_ID,
                task_id="task_0000",
                task_name="explode",
                display_label=display_label,
            )
        )
    if failed_task:
        ledger.bus.emit(
            TaskFailed(
                run_id=RUN_ID,
                task_id="task_0000",
                task_name="explode",
                attempt=1,
                exit_code=1,
                failure={"kind": "user_code_error", "message": message},
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


FAILED_TASK = {"status": "failed", "failed_task": True}


@pytest.mark.parametrize(
    ("run", "shown", "hidden"),
    [
        (
            {"status": "failed", "error": "Pixi environment 'ghost_env' not found."},
            ["Debug Report", "Run Failure", "ghost_env", r"Failures\s+1\s"],
            ["No failed tasks found"],
        ),
        ({**FAILED_TASK, "error": "boom"}, [r"Failures\s+1\s"], ["Run Failure"]),
        (
            {**FAILED_TASK, "error": "orchestrator exploded"},
            ["Failed Task: explode", "boom", "Run Failure", "orchestrator exploded"],
            [],
        ),
        (
            {**FAILED_TASK, "display_label": "explode[north]"},
            [r"Failed Task: explode\[north\]", r"Task +explode\[north\]"],
            [],
        ),
        ({**FAILED_TASK, "message": "bad region bad[/x] [bold]"}, [r"bad\[/x\] \[bold\]"], []),
        ({"succeeded_task": True}, [f"✓ No failed tasks found in {RUN_ID}"], ["Run Failure"]),
        ({}, [f"✓ No failed tasks found in {RUN_ID}"], []),
        ({"status": "failed"}, ["No error recorded in the manifest."], []),
    ],
    ids=[
        "run-error-only",
        "restated-error-counted-once",
        "task-and-run-error",
        "branch-label",
        "brackets-verbatim",
        "succeeded",
        "nothing-recorded",
        "failed-without-error",
    ],
)
def test_the_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], run, shown, hidden
) -> None:
    _record_run(cwd=tmp_path, **run)

    # The exit code says whether debug could report, not whether the run failed.
    assert _debug() == 0
    stdout = capsys.readouterr().out
    for pattern in shown:
        assert re.search(pattern, stdout), pattern
    for text in hidden:
        assert text not in stdout


@pytest.mark.parametrize(
    ("run", "error", "failure"),
    [
        ({"status": "failed", "error": "orchestrator exploded"}, "orchestrator exploded", None),
        ({}, None, None),
        # The run's error only restates the task's, which is already reported.
        ({**FAILED_TASK, "error": "boom"}, None, {"status": "failed", "message": "boom"}),
        ({**FAILED_TASK, "display_label": "explode[north]"}, None, {"label": "explode[north]"}),
    ],
)
def test_the_json_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], run, error, failure
) -> None:
    _record_run(cwd=tmp_path, **run)

    assert _debug(json_output=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == error
    if failure is None:
        assert payload["failures"] == []
        return
    (entry,) = payload["failures"]
    assert "error" not in entry
    if "label" in failure:
        assert (entry["task_name"], entry["display_label"]) == ("explode", failure["label"])
    else:
        assert entry["status"] == failure["status"]
        assert entry["failure"]["message"] == failure["message"]


def test_the_log_tail_prints_below_the_panel_and_unwrapped(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A traceback path folded at a panel border cannot be copied whole."""
    frame = 'File "/' + "nested/" * 40 + 'deep.py", line 3, in explode'
    ledger = Ledger.start(root=tmp_path, run_id=RUN_ID, workflow="wf.py")
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id=RUN_ID,
            task_id="task_0000",
            node_id=0,
            task_name="explode",
            stderr_log="logs/explode.stderr.log",
        )
    )
    ledger.bus.emit(
        TaskFailed(
            run_id=RUN_ID,
            task_id="task_0000",
            task_name="explode",
            attempt=1,
            failure={"kind": "user_code_error", "message": "boom"},
        )
    )
    (ledger.run_dir.logs_dir / "explode.stderr.log").write_text(frame + "\n", encoding="utf-8")
    ledger.finish(status="failed", error="boom")
    ledger.close()

    assert _debug() == 0
    assert frame in capsys.readouterr().out.splitlines()
