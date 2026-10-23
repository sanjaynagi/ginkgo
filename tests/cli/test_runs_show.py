"""``ginkgo runs show`` summarises a large run instead of listing every task (SC5-5)."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.app import main
from ginkgo.runtime.events import GraphNodeRegistered, TaskCompleted, TaskFailed
from tests.conftest import Ledger


def _run_with_tasks(*, root: Path, count: int) -> None:
    """Record a run of *count* tasks, the last of which failed."""
    ledger = Ledger.start(root=root, run_id="run-big")
    for index in range(count):
        task_id = f"task_{index:04d}"
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id="run-big",
                task_id=task_id,
                node_id=index,
                task_name="flow.step",
                display_label=f"step[{index}]",
            )
        )
        event = TaskFailed if index == count - 1 else TaskCompleted
        ledger.bus.emit(event(run_id="run-big", task_id=task_id, task_name="flow.step", attempt=1))
    ledger.finish()
    ledger.close()


def test_a_large_run_lists_its_first_tasks_and_every_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_with_tasks(root=tmp_path, count=80)

    assert main(["runs", "show", "run-big"]) == 0
    out = capsys.readouterr().out
    assert "Tasks: 80 (79 succeeded, 1 failed)" in out
    assert "step[0]" in out and "step[79]" in out and "step[60]" not in out
    assert "… 30 more tasks, all succeeded or cached — `--all` lists every one." in out

    assert main(["runs", "show", "run-big", "--all"]) == 0
    out = capsys.readouterr().out
    assert "step[60]" in out and "more tasks" not in out
