"""Tests for the run-summary "tracked by path only" count (issue #307 phase 1).

Builds a run's ledger through the same events the evaluator emits, rather than
running a real workflow, so the case is about the read side: which tasks and
labels the count includes.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.commands.run import _count_path_tracked_inputs
from ginkgo.runtime.events import GraphNodeRegistered, TaskCompleted, TaskFailed, TaskPlanned
from tests.conftest import Ledger

_TASK = {"task_id": "task_0000", "task_name": "analyze"}


@pytest.mark.parametrize(
    ("labels", "outcome", "count"),
    [
        ({"coords": "path", "threads": "value"}, "success", 1),
        ({"coords": "path"}, "cached", 1),
        ({"coords": "content", "threads": "value"}, "success", 0),
        # The label only matters for a task whose result the cache now holds.
        ({"coords": "path"}, "failed", 0),
        # A run recorded before labels existed: gracefully nothing, not a crash.
        (None, "success", 0),
    ],
)
def test_path_tracked_inputs_are_counted(
    tmp_path: Path, labels: dict | None, outcome: str, count: int
) -> None:
    ledger = Ledger.start(root=tmp_path)
    run_id = ledger.run_id
    ledger.bus.emit(
        GraphNodeRegistered(run_id=run_id, node_id=0, env="local", dependency_ids=[], **_TASK)
    )
    params = labels or {"coords": "x"}
    ledger.bus.emit(
        TaskPlanned(
            run_id=run_id,
            inputs={name: "x" for name in params},
            input_hashes=[{"param": name, "type": "str", "digest": "aa"} for name in params],
            cache_key="cache-task_0000",
            **({"input_labels": labels} if labels else {}),
            **_TASK,
        )
    )
    if outcome == "failed":
        ledger.bus.emit(
            TaskFailed(
                run_id=run_id,
                attempt=1,
                exit_code=1,
                failure={"kind": "user_code_error", "message": "boom"},
                **_TASK,
            )
        )
        summary = ledger.finish(status="failed", error="boom")
    else:
        ledger.bus.emit(TaskCompleted(run_id=run_id, attempt=1, status=outcome, **_TASK))
        summary = ledger.finish()

    assert _count_path_tracked_inputs(run_summary=summary) == count
