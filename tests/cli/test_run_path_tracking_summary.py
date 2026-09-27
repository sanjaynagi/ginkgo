"""Tests for the run-summary "tracked by path only" count (issue #307 phase 1).

Builds a run's ledger directly through the same events the evaluator emits
(as ``tests/reporting/test_reporting.py`` does), rather than running a real
workflow, so the case is about the read side: which tasks and labels the
count includes.
"""

from __future__ import annotations

from pathlib import Path

from ginkgo.cli.commands.run import _count_path_tracked_inputs
from ginkgo.runtime.events import GraphNodeRegistered, TaskCompleted, TaskFailed, TaskPlanned
from tests.conftest import Ledger


def _register_and_plan(
    ledger: Ledger,
    *,
    task_id: str,
    node_id: int,
    task_name: str,
    input_labels: dict[str, str],
) -> None:
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id=ledger.run_id,
            task_id=task_id,
            node_id=node_id,
            task_name=task_name,
            env="local",
            dependency_ids=[],
        )
    )
    ledger.bus.emit(
        TaskPlanned(
            run_id=ledger.run_id,
            task_id=task_id,
            task_name=task_name,
            inputs={name: "x" for name in input_labels},
            input_hashes=[{"param": name, "type": "str", "digest": "aa"} for name in input_labels],
            input_labels=input_labels,
            cache_key=f"cache-{task_id}",
        )
    )


def test_counts_path_labelled_inputs_of_a_succeeded_task(tmp_path: Path) -> None:
    ledger = Ledger.start(root=tmp_path)
    _register_and_plan(
        ledger,
        task_id="task_0000",
        node_id=0,
        task_name="analyze",
        input_labels={"coords": "path", "threads": "value"},
    )
    ledger.bus.emit(
        TaskCompleted(run_id=ledger.run_id, task_id="task_0000", task_name="analyze", attempt=1)
    )

    summary = ledger.finish()

    assert _count_path_tracked_inputs(run_summary=summary) == 1


def test_counts_path_labelled_inputs_of_a_cached_task(tmp_path: Path) -> None:
    ledger = Ledger.start(root=tmp_path)
    _register_and_plan(
        ledger,
        task_id="task_0000",
        node_id=0,
        task_name="analyze",
        input_labels={"coords": "path"},
    )
    ledger.bus.emit(
        TaskCompleted(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="analyze",
            attempt=1,
            status="cached",
        )
    )

    summary = ledger.finish()

    assert _count_path_tracked_inputs(run_summary=summary) == 1


def test_a_clean_workflow_counts_zero(tmp_path: Path) -> None:
    ledger = Ledger.start(root=tmp_path)
    _register_and_plan(
        ledger,
        task_id="task_0000",
        node_id=0,
        task_name="analyze",
        input_labels={"coords": "content", "threads": "value"},
    )
    ledger.bus.emit(
        TaskCompleted(run_id=ledger.run_id, task_id="task_0000", task_name="analyze", attempt=1)
    )

    summary = ledger.finish()

    assert _count_path_tracked_inputs(run_summary=summary) == 0


def test_a_failed_tasks_inputs_do_not_count(tmp_path: Path) -> None:
    """The label only matters for a task whose result the cache now holds."""
    ledger = Ledger.start(root=tmp_path)
    _register_and_plan(
        ledger,
        task_id="task_0000",
        node_id=0,
        task_name="analyze",
        input_labels={"coords": "path"},
    )
    ledger.bus.emit(
        TaskFailed(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="analyze",
            attempt=1,
            exit_code=1,
            failure={"kind": "user_code_error", "message": "boom"},
        )
    )

    summary = ledger.finish(status="failed", error="boom")

    assert _count_path_tracked_inputs(run_summary=summary) == 0


def test_a_run_recorded_before_labels_existed_counts_zero(tmp_path: Path) -> None:
    """No ``input_labels`` on the event: gracefully nothing, not a crash."""
    ledger = Ledger.start(root=tmp_path)
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id=ledger.run_id,
            task_id="task_0000",
            node_id=0,
            task_name="analyze",
            env="local",
            dependency_ids=[],
        )
    )
    ledger.bus.emit(
        TaskPlanned(
            run_id=ledger.run_id,
            task_id="task_0000",
            task_name="analyze",
            inputs={"coords": "x"},
            input_hashes=[{"param": "coords", "type": "str", "digest": "aa"}],
            cache_key="cache-task_0000",
        )
    )
    ledger.bus.emit(
        TaskCompleted(run_id=ledger.run_id, task_id="task_0000", task_name="analyze", attempt=1)
    )

    summary = ledger.finish()

    assert _count_path_tracked_inputs(run_summary=summary) == 0
