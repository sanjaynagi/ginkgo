"""What an agent reads about a run: the ``run_summary`` event, ``runs status`` and
``runs wait``, and a run started with ``run --detach`` (issues #416, #420)."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from ginkgo.runtime.rundir import make_run_id
from tests.cli.conftest import SIMPLE_FLOW, run_ok, write
from tests.conftest import Ledger, run_cli

_FAILING_FLOW = r"""
from ginkgo import file, flow, shell, task

@task()
def produce() -> file:
    with open("made.txt", "w") as handle:
        handle.write("x")
    return file("made.txt")

@task("shell")
def boom(source: file) -> file:
    return shell(cmd="printf '\\033[31mred line\\033[0m\\n'; exit 3", output="never.txt")

@task()
def after(source: file) -> str:
    return "unreached"

@flow
def main():
    made = produce()
    return [made, after(source=boom(source=made))]
"""


def test_an_agent_run_ends_with_its_outcome(tmp_path: Path) -> None:
    write(tmp_path / "flow.py", _FAILING_FLOW)

    result = run_cli("run", "flow.py", "--agent-output", "--keep-going")

    last = json.loads(result.stdout.splitlines()[-1])
    assert last["event"] == "run_summary" and last["v"] == 1
    assert last["status"] == "failed"
    assert last["counts"] == {
        "total": 3,
        "executed": 2,
        "cached": 0,
        "failed": 1,
        "ignored": 1,
        "skipped": 1,
    }
    assert Path(last["run_dir"]).is_absolute()
    [failure] = last["failures"]
    assert failure["task"] == "boom" and failure["exit_code"] == 3 and failure["error"]
    assert "red line" in failure["log_tail"] and "\x1b" not in "".join(failure["log_tail"])
    assert failure["logs"] and all(Path(log).is_file() for log in failure["logs"])
    assert last["skipped"] == [{"task": "after", "cause": "blocked by boom"}]
    assert {"task": "produce", "path": "made.txt"} in last["outputs"]

    # ``runs status`` prints the same payload from the ledger afterwards, and
    # ``runs wait`` exits as ``ginkgo run`` did for failures the policy let pass.
    status = run_cli("runs", "status", last["run_id"], "--json")
    assert status.returncode == 0
    assert json.loads(status.stdout) == {
        key: value for key, value in last.items() if key not in {"event", "v", "ts"}
    }
    assert run_cli("runs", "wait", last["run_id"]).returncode == result.returncode == 3


def test_status_of_a_succeeded_run() -> None:
    write("flow.py", SIMPLE_FLOW)
    run_ok("run", "flow.py")

    payload = json.loads(run_ok("runs", "status", "--json").stdout)
    assert payload["status"] == "succeeded"
    assert payload["failures"] == [] and payload["skipped"] == []
    assert payload["counts"]["executed"] == 1
    assert "succeeded" in run_ok("runs", "status").stdout


@pytest.mark.parametrize(("status", "code"), [("success", 0), ("failed", 1), ("cancelled", 130)])
def test_wait_exits_with_the_runs_status(ledger: Ledger, status: str, code: int) -> None:
    ledger.recorder.flush()
    timed_out = run_cli("runs", "wait", ledger.run_id, "--timeout", "0", "--json")
    assert timed_out.returncode == 124
    assert json.loads(timed_out.stdout)["status"] == "running"

    ledger.finish(status=status)
    assert run_cli("runs", "wait", ledger.run_id).returncode == code
    assert run_cli("runs", "wait", "no_such_run").returncode == 2


def test_a_detached_run_can_be_waited_for() -> None:
    write("flow.py", SIMPLE_FLOW)

    result = run_ok("run", "flow.py", "--detach", "--agent-output")

    [event] = [json.loads(line) for line in result.stdout.splitlines()]
    assert event["event"] == "run_detached" and Path(event["log"]).is_absolute()
    waited = run_cli("runs", "wait", event["run_id"], "--timeout", "60", "--json")
    assert waited.returncode == 0, waited.stdout + Path(event["log"]).read_text()
    assert json.loads(waited.stdout)["status"] == "succeeded"
    assert run_cli("run", "flow.py", "--detach", "--dry-run").returncode == 2


def test_a_detached_run_rejected_while_loading_is_reported_at_once() -> None:
    result = run_cli("run", "missing.py", "--detach", "--agent-output")

    assert result.returncode == 1
    # The child's whole log, its stderr included, after its ``run_rejected``.
    assert '"event": "run_rejected"' in result.stdout and "missing.py" in result.stdout


def test_a_detached_run_not_yet_recorded_is_starting_until_its_process_exits() -> None:
    run_id = make_run_id()
    write(f".ginkgo/detached/{run_id}.log", "Error: the workflow did not load\n")
    pid_file = write(f".ginkgo/detached/{run_id}.pid", str(os.getpid()))

    status = run_cli("runs", "status", run_id, "--json")
    assert status.returncode == 0 and json.loads(status.stdout)["status"] == "starting"
    assert run_cli("runs", "wait", run_id, "--timeout", "0").returncode == 124

    exited = subprocess.Popen(["true"])
    exited.wait()
    pid_file.write_text(str(exited.pid), encoding="utf-8")
    waited = run_cli("runs", "wait", run_id)
    assert waited.returncode == 1
    assert "was rejected before it started" in waited.stdout
    assert "the workflow did not load" in waited.stdout


def test_a_chosen_run_id_must_be_new_and_well_formed() -> None:
    write("flow.py", SIMPLE_FLOW)
    run_ok("run", "flow.py")
    recorded = json.loads(run_ok("runs", "status", "--json").stdout)["run_id"]

    assert run_cli("run", "flow.py", "--run-id", recorded).returncode == 2
    assert run_cli("run", "flow.py", "--run-id", "../elsewhere").returncode == 2
