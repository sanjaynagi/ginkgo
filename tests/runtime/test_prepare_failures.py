"""A failure while a task is prepared is recorded as that task's failure.

Preparation resolves a task's arguments, validates its inputs, runs a notebook
or script body to hash its source, and materialises its environment. A failure
in any of those used to escape the scheduler without a ``TaskFailed``: the run
summary said "0 failed", the task stayed ``pending`` in the record, and work
already in flight was left ``running`` forever.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.app import main as cli_main
from ginkgo.envs.container import ContainerBackend, ContainerPrepareError
from ginkgo.runtime.task_validation import TaskValidator

from tests.conftest import latest_run_view


_MISSING_NOTEBOOK_WORKFLOW = """
from ginkgo import flow, notebook, task

@task()
def prep() -> str:
    return "x"

@task(kind="notebook")
def report(x: str):
    return notebook("notebooks/missing.ipynb")

@flow
def main():
    return report(x=prep())
"""

_NOT_A_FILE_WORKFLOW = """
from ginkgo import file, flow, task

@task()
def make() -> str:
    return "does_not_exist.txt"

@task()
def use(p: file) -> str:
    return str(p)

@flow
def main():
    return use(p=make())
"""

_ENV_FAILURE_WORKFLOW = """
import time

from ginkgo import flow, shell, task

@task()
def fast(x: str) -> str:
    return x

@task()
def slow(x: str) -> str:
    time.sleep(2)
    return x

@task(kind="shell", env="docker://ubuntu:24.04")
def pack(src: str) -> str:
    return shell(cmd=f"echo {src}")

@flow
def main():
    return [pack(src=fast(x="q")), *slow().map(x=["a", "b"])]
"""


def _run(*, workflow: str) -> tuple[int, dict, dict[str, dict]]:
    """Run *workflow* here; return its exit status, run view, and tasks by label."""
    Path("wf.py").write_text(workflow.strip() + "\n", encoding="utf-8")
    status = cli_main(["run", "wf.py"])
    _, view = latest_run_view(root=Path.cwd())
    tasks_by_label = {
        task.get("display_label") or task["name"].rsplit(".", 1)[-1]: task
        for task in view["tasks"].values()
    }
    return status, view, tasks_by_label


def _combined(capsys: pytest.CaptureFixture[str]) -> str:
    captured = capsys.readouterr()
    return " ".join((captured.out + captured.err).split())


def test_a_missing_notebook_source_fails_the_notebook_task(
    capsys: pytest.CaptureFixture[str],
) -> None:
    status, view, tasks = _run(workflow=_MISSING_NOTEBOOK_WORKFLOW)

    assert status == 1
    assert view["status"] == "failed"
    assert tasks["report"]["status"] == "failed"
    assert tasks["report"]["failure"]["kind"] == "missing_input"
    assert "missing.ipynb" in tasks["report"]["failure"]["message"]
    output = _combined(capsys)
    assert "1 failed" in output
    assert "Failure Details: report" in output


def test_a_file_parameter_fed_a_non_file_fails_the_consuming_task(
    capsys: pytest.CaptureFixture[str],
) -> None:
    status, view, tasks = _run(workflow=_NOT_A_FILE_WORKFLOW)

    assert status == 1
    assert tasks["make"]["status"] == "succeeded"
    assert tasks["use"]["status"] == "failed"
    assert tasks["use"]["failure"]["kind"] == "missing_input"
    output = _combined(capsys)
    assert "1 failed" in output
    assert "Failure Details: use" in output


def test_an_environment_failure_fails_its_task_and_drains_work_in_flight(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fail_to_pull(self: ContainerBackend, *, env: str) -> None:
        raise ContainerPrepareError(image="ubuntu:24.04", output="daemon is not running")

    monkeypatch.setattr(ContainerBackend, "validate_envs", lambda self, *, env_names: None)
    monkeypatch.setattr(ContainerBackend, "prepare", _fail_to_pull)

    status, view, tasks = _run(workflow=_ENV_FAILURE_WORKFLOW)

    assert status == 1
    assert tasks["pack"]["status"] == "failed"
    assert tasks["pack"]["failure"]["kind"] == "env_mismatch"
    assert "running" not in {task["status"] for task in view["tasks"].values()}
    assert "pending" not in {task["status"] for task in view["tasks"].values()}
    output = _combined(capsys)
    assert "1 failed" in output
    assert "Failure Details: pack" in output


_SCHEDULER_FAILURE_WORKFLOW = """
import time

from ginkgo import flow, task

@task()
def slow(x: str) -> str:
    time.sleep(2)
    return x

@task()
def fast(x: str) -> str:
    return x

@task()
def boom(x: str) -> str:
    return x

@flow
def main():
    return [*slow().map(x=["a", "b"]), boom(x=fast(x="q"))]
"""


def test_a_scheduler_failure_closes_the_work_it_leaves_in_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validate_task_contract = TaskValidator.validate_task_contract

    def _fail_for_boom(self: TaskValidator, *, task_def, execution_args) -> None:
        if task_def.name.endswith(".boom"):
            raise RuntimeError("scheduler broke")
        validate_task_contract(self, task_def=task_def, execution_args=execution_args)

    monkeypatch.setattr(TaskValidator, "validate_task_contract", _fail_for_boom)

    status, view, _ = _run(workflow=_SCHEDULER_FAILURE_WORKFLOW)

    assert status == 1
    assert view["status"] == "failed"
    slow_tasks = [task for task in view["tasks"].values() if task["name"].endswith(".slow")]
    assert [task["status"] for task in slow_tasks] == ["failed", "failed"]
    assert [task["failure"]["kind"] for task in slow_tasks] == ["cancelled", "cancelled"]
    assert "running" not in {task["status"] for task in view["tasks"].values()}
