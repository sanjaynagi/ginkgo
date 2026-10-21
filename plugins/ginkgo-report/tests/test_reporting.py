"""Tests for ``ginkgo_report`` — the static HTML report exporter."""

from __future__ import annotations

from dataclasses import replace

from dataclasses import dataclass

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ginkgo.cli import main
from ginkgo_report.command import _resolve_destination
from ginkgo.formatting import format_bytes, format_duration
from ginkgo_report import command as report_command
from ginkgo_report.model import _memory_label, build_report_data
from ginkgo_report.notebook_page import static_notebook_page
from ginkgo_report.render import _MARKER_NAME, export_report
from ginkgo_report.sizing import SizingPolicy, build_log_tail
from ginkgo.runtime.events import (
    GraphNodeRegistered,
    RunResourcesSampled,
    TaskAnnotated,
    TaskCompleted,
    TaskFailed,
    TaskPlanned,
    TaskRetrying,
    TaskStarted,
)

from ginkgo.runtime.events import EventBus, RunCompleted, RunNotice, RunStarted
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.run_summary import RunSummary
from ginkgo.runtime.store_recorder import StoreRecorder
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


# ----- Fixtures ----------------------------------------------------------


@dataclass
class _Ledger:
    """A recorded run: events emitted on ``bus`` land in the run's ledger."""

    run_id: str
    run_dir: RunDir
    bus: EventBus
    recorder: StoreRecorder
    db: Path

    @classmethod
    def start(cls, *, root: Path, run_id: str, workflow: str = "workflow.py", **run_started: Any):
        layout = WorkspaceLayout(root=root / ".ginkgo")
        run_dir = RunDir.create(run_id=run_id, root=layout.runs)
        bus = EventBus()
        recorder = StoreRecorder(path=layout.db, run_dir=run_dir).start()
        bus.subscribe(recorder)
        bus.emit(RunStarted(run_id=run_id, workflow=workflow, **run_started))
        return cls(run_id=run_id, run_dir=run_dir, bus=bus, recorder=recorder, db=layout.db)

    @property
    def path(self) -> Path:
        return self.run_dir.path

    def finish(self, *, status: str, error: str | None = None) -> None:
        self.bus.emit(RunCompleted(run_id=self.run_id, status=status, error=error))

    def summary(self) -> RunSummary:
        self.recorder.flush()
        with open_store(self.db, readonly=True) as store:
            return RunSummary.load(store, self.run_id, runs_root=self.run_dir.root)

    def close(self) -> None:
        self.recorder.close()


@dataclass
class _Run:
    """A run under construction, finished the first time it is reported on."""

    ledger: _Ledger
    tmp_path: Path
    status: str = "success"
    error: str | None = None
    _finished: bool = False

    @property
    def run_id(self) -> str:
        return self.ledger.run_id

    @property
    def run_dir(self) -> Path:
        return self.ledger.path

    def summary(self) -> RunSummary:
        if not self._finished:
            self._finished = True
            self.ledger.finish(status=self.status, error=self.error)
        return self.ledger.summary()


def _make_run(
    *,
    tmp_path: Path,
    run_id: str,
    fail: bool,
    cached: bool = False,
) -> _Run:
    """Build a minimal terminal two-task run.

    ``cached`` records both tasks as cache hits instead of fresh executions,
    which is how a re-run of an unchanged workflow reaches the report.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# demo workflow\n@flow\ndef main():\n    pass\n", encoding="utf-8")
    ledger = _Ledger.start(
        root=tmp_path,
        run_id=run_id,
        workflow=str(workflow_path),
        jobs=4,
        cores=4,
        params={"seed": 42, "targets": ["a", "b"]},
    )
    run = _Run(
        ledger=ledger,
        tmp_path=tmp_path,
        status="failed" if fail else "success",
        error="boom" if fail else None,
    )
    status = "cached" if cached else "success"

    for node_id, (name, message, dependencies) in enumerate(
        [("demo.first", "hello", []), ("demo.second", "a.txt", ["task_0000"])]
    ):
        task_id = f"task_{node_id:04d}"
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id=run_id,
                task_id=task_id,
                node_id=node_id,
                task_name=name,
                env="local",
                dependency_ids=dependencies,
                stdout_log=f"logs/{task_id}.stdout.log",
                stderr_log=f"logs/{task_id}.stderr.log",
            )
        )
        ledger.bus.emit(
            TaskPlanned(
                run_id=run_id,
                task_id=task_id,
                task_name=name,
                inputs={"message" if node_id == 0 else "upstream": message},
                input_hashes=[{"param": "message", "type": "str", "digest": "aa"}],
                cache_key=f"cache-{name}",
                dependency_ids=dependencies,
            )
        )

    (ledger.run_dir.logs_dir / "task_0000.stdout.log").write_text(
        "starting first task\n" * 3, encoding="utf-8"
    )
    (ledger.run_dir.logs_dir / "task_0000.stderr.log").write_text(
        "\n".join(f"log line {i}" for i in range(50)) + "\n", encoding="utf-8"
    )
    (ledger.run_dir.logs_dir / "task_0001.stdout.log").write_text(
        "starting second task\n", encoding="utf-8"
    )
    (ledger.run_dir.logs_dir / "task_0001.stderr.log").write_text(
        "\n".join(f"err line {i}" for i in range(30)) + "\n", encoding="utf-8"
    )

    ledger.bus.emit(
        TaskCompleted(
            run_id=run_id,
            task_id="task_0000",
            task_name="demo.first",
            attempt=1,
            status=status,
            outputs=[{"name": "return", "type": "value", "summary": "results/a.txt"}],
        )
    )
    if fail:
        ledger.bus.emit(
            TaskFailed(
                run_id=run_id,
                task_id="task_0001",
                task_name="demo.second",
                attempt=1,
                exit_code=1,
                failure={"kind": "user_code_error", "message": "boom"},
            )
        )
    else:
        ledger.bus.emit(
            TaskCompleted(
                run_id=run_id,
                task_id="task_0001",
                task_name="demo.second",
                attempt=1,
                status=status,
                outputs=[{"name": "return", "type": "value", "summary": "results/b.txt"}],
            )
        )

    ledger.bus.emit(
        RunResourcesSampled(
            run_id=run_id,
            resources={
                "status": "completed",
                "scope": "process_tree",
                "sample_count": 2,
                "current": {"cpu_percent": 12.5, "rss_bytes": 1024, "process_count": 1},
                "peak": {"cpu_percent": 85.0, "rss_bytes": 4096, "process_count": 2},
                "average": {"cpu_percent": 48.0, "rss_bytes": 2048, "process_count": 1.5},
                "updated_at": "2026-03-13T00:00:00+00:00",
            },
        )
    )

    return run


def _marimo_page() -> str:
    """Return a tiny page shaped like ``marimo export html`` output.

    Its cells are drawn by marimo's frontend, loaded from a CDN; the cells'
    code and outputs travel in the mount config.
    """
    notebook = {
        "cells": [
            {"id": "a", "code": "mo.md('# Cases')", "config": {"hide_code": False}},
            {"id": "b", "code": "mo.ui.table(df)", "config": {"hide_code": False}},
            {"id": "c", "code": "mo.image(fig)", "config": {"hide_code": True}},
            {"id": "d", "code": "raise ValueError('boom')", "config": {"hide_code": False}},
        ]
    }
    rows = json.dumps(json.dumps([{"_marimo_row_id": 0, "region": "north", "cases": 3}]))
    table = (
        "<marimo-ui-element object-id='b-0'><marimo-table data-label='null' "
        f"data-data='{rows.replace(chr(34), '&quot;')}' data-total-rows='1'>"
        "</marimo-table></marimo-ui-element>"
    )
    session = {
        "cells": [
            {
                "id": "a",
                "console": [{"name": "stdout", "text": "loaded 3 rows\n", "type": "stream"}],
                "outputs": [{"type": "data", "data": {"text/markdown": "<h1>Cases</h1>"}}],
            },
            {
                "id": "b",
                "console": [],
                "outputs": [{"type": "data", "data": {"text/html": table}}],
            },
            {
                "id": "c",
                "console": [],
                "outputs": [
                    {
                        "type": "data",
                        "data": {"text/html": "<img src='data:image/png;base64,AA=='>"},
                    }
                ],
            },
            {
                "id": "d",
                "console": [],
                "outputs": [{"type": "error", "ename": "ValueError", "evalue": "boom"}],
            },
        ]
    }
    return (
        "<!doctype html><html><head>"
        '<script type="module" src="https://cdn.jsdelivr.net/npm/@marimo-team/frontend/i.js">'
        "</script></head><body><div id='root'></div><script data-marimo='true'>"
        'Object.defineProperty(window, "__MARIMO_MOUNT_CONFIG__", {value: Object.freeze({'
        f'"notebook": {json.dumps(notebook)},\n"session": {json.dumps(session)},\n'
        "})});</script></body></html>"
    )


def _make_page_run(*, tmp_path: Path, run_id: str, page: str = "<html>rendered</html>") -> _Run:
    """Build a terminal run whose single task rendered *page*."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# demo\n", encoding="utf-8")
    ledger = _Ledger.start(
        root=tmp_path, run_id=run_id, workflow=str(workflow_path), jobs=1, cores=1
    )
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id=run_id,
            task_id="task_0000",
            node_id=0,
            task_name="demo.report",
            env="local",
        )
    )
    ledger.bus.emit(
        TaskPlanned(
            run_id=run_id, task_id="task_0000", task_name="demo.report", cache_key="cache-nb"
        )
    )
    html_path = ledger.path / "notebooks" / "report.html"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(page, encoding="utf-8")
    ledger.bus.emit(
        TaskAnnotated(
            run_id=run_id,
            task_id="task_0000",
            task_name="demo.report",
            fields={"rendered_html": "notebooks/report.html", "rendered_html_run_id": run_id},
        )
    )
    ledger.bus.emit(
        TaskCompleted(
            run_id=run_id,
            task_id="task_0000",
            task_name="demo.report",
            attempt=1,
            outputs=[{"name": "return", "type": "file", "path": str(html_path)}],
        )
    )
    return _Run(ledger=ledger, tmp_path=tmp_path)


def _task_with_usage(resource_usage: dict[str, Any]) -> Any:
    """Return a stand-in task carrying only *resource_usage*."""

    @dataclass
    class _Task:
        resource_usage: dict[str, Any]

    return _Task(resource_usage=resource_usage)


# ----- Formatting --------------------------------------------------------


class TestFormatters:
    def test_duration_seconds(self) -> None:
        assert format_duration(0.42) == "0.4s"
        assert format_duration(4.2) == "4.2s"
        assert format_duration(12.4) == "12s"

    def test_duration_minutes(self) -> None:
        assert format_duration(74) == "1m 14s"
        assert format_duration(2834) == "47m 14s"

    def test_duration_hours(self) -> None:
        assert format_duration(3725) == "1h 02m 05s"

    def test_duration_none(self) -> None:
        assert format_duration(None) == "—"

    def test_bytes(self) -> None:
        assert format_bytes(512) == "512 B"
        assert format_bytes(2048) == "2.0 KiB"
        assert format_bytes(5_368_709_120) == "5.0 GiB"

    def test_bytes_none(self) -> None:
        assert format_bytes(None) == "—"


# ----- Sizing ------------------------------------------------------------


class TestSizing:
    def test_log_tail_truncation(self, tmp_path: Path) -> None:
        path = tmp_path / "big.log"
        path.write_text("\n".join(f"line {i}" for i in range(500)) + "\n", encoding="utf-8")
        tail = build_log_tail(path=path, policy=SizingPolicy(log_lines=50))
        assert tail is not None
        assert tail.shown_lines == 50
        assert tail.total_lines == 500
        assert tail.truncated is True
        assert tail.lines[-1] == "line 499"

    def test_log_tail_missing(self) -> None:
        assert build_log_tail(path=None, policy=SizingPolicy()) is None


# ----- ReportData --------------------------------------------------------


class TestReportData:
    def test_rejects_running_run(self, tmp_path: Path) -> None:
        ledger = _Ledger.start(root=tmp_path, run_id="run-live")
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id="run-live", task_id="task_0000", node_id=0, task_name="demo.t"
            )
        )
        # No RunCompleted — the run is still "running".
        with pytest.raises(ValueError, match="not terminal"):
            build_report_data(summary=ledger.summary())
        ledger.close()

    def test_basic_successful_run(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        report = build_report_data(
            summary=run.summary(),
            generated_at=datetime(2026, 4, 20, 0, 0, 0, tzinfo=UTC),
        )

        assert report.run_id == "run-ok"
        assert report.status_raw == "succeeded"
        assert report.has_failures is False
        assert len(report.tasks) == 2
        assert {task.label for task in report.tasks} == {"first", "second"}
        assert not any(task.failed for task in report.tasks)
        # Summary cards present.
        labels = [card.label for card in report.summary_cards]
        assert labels == ["Tasks", "Failures", "Cache hits"]
        # Masthead KV includes the status pill row.
        status_entries = [kv for kv in report.masthead_kv if kv.key == "status"]
        assert len(status_entries) == 1

    def test_attempts_label_counts_the_attempts_made(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-attempts", fail=False)
        report = build_report_data(
            summary=run.summary(),
            generated_at=datetime(2026, 4, 20, 0, 0, 0, tzinfo=UTC),
        )

        # Each fixture task completed on its first and only attempt.
        assert [task.attempts_label for task in report.tasks] == ["1", "1"]

    def test_attempts_label_for_an_exhausted_retry_budget(self, tmp_path: Path) -> None:
        ledger = _Ledger.start(root=tmp_path, run_id="run-retries")
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id="run-retries",
                task_id="task_0000",
                node_id=0,
                task_name="demo.flaky",
                retries=2,
            )
        )
        for attempt in (1, 2, 3):
            ledger.bus.emit(
                TaskStarted(
                    run_id="run-retries",
                    task_id="task_0000",
                    task_name="demo.flaky",
                    attempt=attempt,
                )
            )
            if attempt < 3:
                ledger.bus.emit(
                    TaskRetrying(
                        run_id="run-retries",
                        task_id="task_0000",
                        task_name="demo.flaky",
                        attempt=attempt,
                    )
                )
        ledger.bus.emit(
            TaskFailed(
                run_id="run-retries",
                task_id="task_0000",
                task_name="demo.flaky",
                attempt=3,
                exit_code=1,
                failure={"kind": "user_code_error", "message": "boom"},
            )
        )
        ledger.finish(status="failed", error="boom")
        summary = ledger.summary()
        ledger.close()

        report = build_report_data(
            summary=summary, generated_at=datetime(2026, 4, 20, 0, 0, 0, tzinfo=UTC)
        )

        assert [task.attempts_label for task in report.tasks] == ["3 / 3"]
        assert [card.attempts_label for card in report.failures] == ["3 / 3"]

    def test_a_retried_attempts_error_is_shown_after_the_retry_succeeds(
        self, tmp_path: Path
    ) -> None:
        ledger = _Ledger.start(root=tmp_path, run_id="run-retried")
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id="run-retried",
                task_id="task_0000",
                node_id=0,
                task_name="demo.flaky",
                retries=1,
            )
        )
        for attempt in (1, 2):
            ledger.bus.emit(
                TaskStarted(
                    run_id="run-retried",
                    task_id="task_0000",
                    task_name="demo.flaky",
                    attempt=attempt,
                )
            )
        ledger.bus.emit(
            TaskRetrying(
                run_id="run-retried",
                task_id="task_0000",
                task_name="demo.flaky",
                attempt=1,
                failure={"kind": "user_code_error", "message": "RuntimeError: transient"},
            )
        )
        ledger.bus.emit(
            TaskCompleted(
                run_id="run-retried", task_id="task_0000", task_name="demo.flaky", attempt=2
            )
        )
        ledger.finish(status="success")
        summary = ledger.summary()
        ledger.close()

        result = export_report(summary=summary, out=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        assert "Retried attempts" in html
        assert "RuntimeError: transient" in html

    def test_each_tasks_output_paths_are_listed(self, tmp_path: Path) -> None:
        run = _make_page_run(tmp_path=tmp_path, run_id="run-page")

        report = build_report_data(summary=run.summary())

        (row,) = report.outputs
        assert (row.label, row.kind) == ("report", "file")
        assert row.path.endswith("notebooks/report.html")

    def test_the_cache_card_says_what_share_of_tasks_was_reused(self, tmp_path: Path) -> None:
        executed = build_report_data(
            summary=_make_run(tmp_path=tmp_path / "a", run_id="run-a", fail=False).summary()
        )
        cached = build_report_data(
            summary=_make_run(
                tmp_path=tmp_path / "b", run_id="run-b", fail=False, cached=True
            ).summary()
        )

        assert executed.summary_cards[2].sub == "every task ran"
        assert cached.summary_cards[2].sub == "every task reused from the cache"

    def test_declared_memory_is_shown_as_written(self) -> None:
        task = _task_with_usage(
            {
                "declared": {"memory": "64Mi", "memory_gb": 1, "effective_memory_gb": 1},
                "measured": {"peak_rss_bytes": 2048, "baseline_rss_bytes": 120_000_000},
            }
        )

        assert _memory_label(task) == "2.0 KB / 64Mi"

    def test_an_escalated_budget_is_shown_as_the_budget_the_attempt_ran_against(self) -> None:
        task = _task_with_usage(
            {
                "declared": {"memory": "16Gi", "memory_gb": 16, "effective_memory_gb": 32},
                "measured": {"peak_rss_bytes": 0},
            }
        )

        assert _memory_label(task) == "0 B / 32 GiB"

    def test_a_run_notice_is_shown_in_the_summary(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.ledger.bus.emit(RunNotice(run_id="run-ok", message="Slack notification failed"))

        result = export_report(summary=run.summary(), out=tmp_path / "out")

        assert "<li>Slack notification failed</li>" in result.index_path.read_text(
            encoding="utf-8"
        )

    def test_the_run_status_uses_the_cli_vocabulary(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)

        report = build_report_data(summary=run.summary())

        assert report.status_label == "succeeded"
        assert report.sidebar_status_label == "succeeded"

    def test_the_workflow_is_shown_relative_to_the_project(self, tmp_path: Path) -> None:
        run = _make_page_run(tmp_path=tmp_path, run_id="run-nb")

        report = build_report_data(summary=run.summary())

        assert report.workflow == "workflow.py"

    def test_a_cancelled_run_can_be_reported_on(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-cancelled", fail=False)
        run.status = "cancelled"

        report = build_report_data(summary=run.summary())

        assert report.status_raw == "cancelled"
        assert report.status_label == "cancelled"

    def test_cached_run_reports_the_same_sections(self, tmp_path: Path) -> None:
        # A re-run that hits cache for every task must present exactly what the
        # executed run did; only the cache labels differ.
        executed = build_report_data(
            summary=_make_run(
                tmp_path=tmp_path / "executed", run_id="run-exec", fail=False
            ).summary()
        )
        cached = build_report_data(
            summary=_make_run(
                tmp_path=tmp_path / "cached", run_id="run-cached", fail=False, cached=True
            ).summary()
        )

        assert [task.cache_label for task in cached.tasks] == ["hit", "hit"]
        assert [task.cache_label for task in executed.tasks] == ["miss", "miss"]
        assert [task.status_label for task in cached.tasks] == ["cached", "cached"]
        assert cached.has_failures is False

        assert [(s.anchor, s.number, s.title) for s in cached.sections] == [
            (s.anchor, s.number, s.title) for s in executed.sections
        ]
        cache_card = next(card for card in cached.summary_cards if card.label == "Cache hits")
        assert cache_card.value == "2 / 2"

    def test_section_numbers_skip_sections_that_do_not_render(self, tmp_path: Path) -> None:
        clean = build_report_data(
            summary=_make_run(tmp_path=tmp_path / "clean", run_id="run-ok", fail=False).summary()
        )
        failed = build_report_data(
            summary=_make_run(tmp_path=tmp_path / "failed", run_id="run-fail", fail=True).summary()
        )

        assert [(s.number, s.anchor) for s in clean.sections] == [
            ("01", "summary"),
            ("02", "params"),
            ("03", "graph"),
            ("04", "tasks"),
            ("05", "env"),
        ]
        # The failure section takes 05, pushing everything after it along.
        assert [(s.number, s.anchor) for s in failed.sections] == [
            ("01", "summary"),
            ("02", "params"),
            ("03", "graph"),
            ("04", "tasks"),
            ("05", "failure"),
            ("06", "env"),
        ]
        # Results holds only rendered pages, so a run without one drops the group.
        assert [group.label for group in clean.section_groups] == ["Execution", "Appendix"]

    def test_section_lookup_rejects_an_unrendered_anchor(self, tmp_path: Path) -> None:
        report = build_report_data(
            summary=_make_run(tmp_path=tmp_path, run_id="run-ok", fail=False).summary()
        )
        assert report.section("tasks").title == "Tasks"
        with pytest.raises(KeyError, match="pages"):
            report.section("pages")

    def test_failed_run_produces_failure_card(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        report = build_report_data(summary=run.summary())

        assert report.status_raw == "failed"
        assert report.has_failures is True
        assert len(report.failures) == 1
        card = report.failures[0]
        assert card.label == "second"
        assert card.category == "user_code_error"
        assert card.log_tail is not None
        assert card.log_tail.total_lines > 0

    def test_a_rendered_page_gets_a_card_titled_by_its_task(self, tmp_path: Path) -> None:
        run = _make_page_run(tmp_path=tmp_path, run_id="run-page")

        report = build_report_data(summary=run.summary())

        (card,) = report.pages
        assert card.title == "report"
        assert card.status_tone == "ok"
        assert "from run" not in card.sub_line

    def test_graph_layout_places_all_tasks(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-graph", fail=False)
        report = build_report_data(summary=run.summary())
        assert len(report.graph.nodes) == 2
        assert len(report.graph.edges) == 1
        # Tasks should land in distinct columns because there's a dependency.
        xs = {node.x for node in report.graph.nodes}
        assert len(xs) == 2

    def test_a_cyclic_predecessor_map_lays_out_rather_than_recursing(self, tmp_path: Path) -> None:
        """A cycle cannot come from a task graph, so the report must not follow one."""
        run = _make_run(tmp_path=tmp_path, run_id="run-cycle", fail=False)
        summary = run.summary()
        node_ids = [task.node_id for task in summary.tasks]
        cyclic = replace(
            summary,
            tasks=tuple(
                replace(task, dependency_ids=(node_ids[(index + 1) % len(node_ids)],))
                for index, task in enumerate(summary.tasks)
            ),
        )

        report = build_report_data(summary=cyclic)

        assert len(report.graph.nodes) == len(summary.tasks)


# ----- Export ------------------------------------------------------------


def _make_fan_out_run(*, tmp_path: Path, run_id: str) -> _Run:
    """Build a failed run whose two fan-out branches of one task both failed."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    ledger = _Ledger.start(root=tmp_path, run_id=run_id, workflow=str(tmp_path / "workflow.py"))
    for node_id, label in enumerate(["clean[north]", "clean[south]"]):
        task_id = f"task_{node_id:04d}"
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id=run_id, task_id=task_id, node_id=node_id, task_name="demo.clean"
            )
        )
        ledger.bus.emit(
            TaskPlanned(
                run_id=run_id,
                task_id=task_id,
                task_name="demo.clean",
                display_label=label,
                inputs={"region": label[6:-1]},
                cache_key=f"cache-{label}",
            )
        )
        ledger.bus.emit(
            TaskFailed(
                run_id=run_id,
                task_id=task_id,
                task_name="demo.clean",
                display_label=label,
                attempt=1,
                exit_code=1,
                failure={"kind": "user_code_error", "message": "boom"},
            )
        )
    return _Run(ledger=ledger, tmp_path=tmp_path, status="failed", error="boom")


class TestExport:
    def test_fan_out_notebook_branches_keep_their_own_html(self, tmp_path: Path) -> None:
        ledger = _Ledger.start(root=tmp_path, run_id="run-nb-fan-out")
        for node_id, region in enumerate(["north", "south"]):
            task_id = f"task_{node_id:04d}"
            ledger.bus.emit(
                GraphNodeRegistered(
                    run_id=ledger.run_id, task_id=task_id, node_id=node_id, task_name="demo.report"
                )
            )
            html_path = ledger.path / "notebooks" / f"{task_id}.html"
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text(f"<html>{region}</html>", encoding="utf-8")
            ledger.bus.emit(
                TaskAnnotated(
                    run_id=ledger.run_id,
                    task_id=task_id,
                    task_name="demo.report",
                    fields={"rendered_html": f"notebooks/{task_id}.html"},
                )
            )
            ledger.bus.emit(
                TaskCompleted(
                    run_id=ledger.run_id, task_id=task_id, task_name="demo.report", attempt=1
                )
            )
        run = _Run(ledger=ledger, tmp_path=tmp_path)

        out_dir = tmp_path / "out"
        export_report(summary=run.summary(), out=out_dir)
        report = build_report_data(summary=run.summary())

        copies = [out_dir / card.link_relpath for card in report.pages]
        assert [path.read_text(encoding="utf-8") for path in copies] == [
            "<html>north</html>",
            "<html>south</html>",
        ]

    def test_fan_out_branches_are_named_by_their_labels(self, tmp_path: Path) -> None:
        run = _make_fan_out_run(tmp_path=tmp_path, run_id="run-fan-out")

        result = export_report(summary=run.summary(), out=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        task_names = re.findall(r'<td class="name">([^<]*)</td>', html)
        assert task_names == ["clean[north]", "clean[south]"]
        failure_headings = re.findall(r"<h4>([^<]*)</h4>", html)
        assert failure_headings == ["clean[north]", "clean[south]"]

    def test_bundle_mode_writes_index_and_assets(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "out"
        result = export_report(summary=run.summary(), out=out_dir)

        assert result.index_path == out_dir / "index.html"
        assert result.index_path.is_file()
        assert (out_dir / "assets" / "report.css").is_file()
        assert (out_dir / "assets" / "islands.js").is_file()
        assert (out_dir / "assets" / "fonts").is_dir()

        html = result.index_path.read_text(encoding="utf-8")
        assert "run-ok" in html
        assert "01</span>Summary" in html
        assert "first" in html
        assert "second" in html

    def test_failure_section_present_only_when_failures_exist(self, tmp_path: Path) -> None:
        ok_run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        ok_out = tmp_path / "ok-out"
        export_report(summary=ok_run.summary(), out=ok_out)
        assert 'id="failure"' not in ok_out.joinpath("index.html").read_text(encoding="utf-8")

        fail_run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        fail_out = tmp_path / "fail-out"
        export_report(summary=fail_run.summary(), out=fail_out)
        assert 'id="failure"' in fail_out.joinpath("index.html").read_text(encoding="utf-8")

    def test_single_file_inlines_css_and_fonts(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "sf"
        result = export_report(summary=run.summary(), out=out_dir, single_file=True)

        assert result.single_file is True
        html = result.index_path.read_text(encoding="utf-8")
        # CSS inlined (style block present, no <link rel="stylesheet">).
        assert "<style>" in html
        assert 'rel="stylesheet"' not in html
        # Font data URIs inlined.
        assert "data:font/woff2;base64," in html

    def test_rendered_section_numerals_are_contiguous(self, tmp_path: Path) -> None:
        # A run with no failures and no rendered pages renders five sections; the
        # numerals must run 01..05 with no hole where an omitted section sat.
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        result = export_report(summary=run.summary(), out=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        headings = re.findall(r'<span class="num">(\d+)</span>', html)
        sidebar = re.findall(r'<span class="idx">(\d+)</span>', html)

        assert headings == ["01", "02", "03", "04", "05"]
        assert sidebar == headings

    def test_rendered_section_numerals_include_the_failure_section(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        result = export_report(summary=run.summary(), out=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        assert re.findall(r'<span class="num">(\d+)</span>', html) == [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
        ]
        assert '<span class="num">05</span>Failure' in html

    def test_the_pages_section_renders_when_the_run_produced_one(self, tmp_path: Path) -> None:
        run = _make_page_run(tmp_path=tmp_path, run_id="run-nb")
        result = export_report(summary=run.summary(), out=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        assert '<span class="num">05</span>Outputs' in html
        assert '<span class="num">06</span>Rendered pages' in html
        assert re.findall(r'<span class="num">(\d+)</span>', html) == [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
        ]

    def test_recorded_values_are_escaped(self, tmp_path: Path) -> None:
        ledger = _Ledger.start(root=tmp_path, run_id="run-esc", params={"note": "<b>x</b>"})
        ledger.finish(status="success")

        result = export_report(summary=ledger.summary(), out=tmp_path / "out")
        ledger.close()

        html = result.index_path.read_text(encoding="utf-8")
        assert "&lt;b&gt;x&lt;/b&gt;" in html
        assert "<b>x</b>" not in html

    def test_no_network_references_in_rendered_html(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "out"
        export_report(summary=run.summary(), out=out_dir)
        html = (out_dir / "index.html").read_text(encoding="utf-8")

        # Allow HTTP namespace URIs (xmlns) but forbid external asset URLs.
        for needle in ("https://fonts.googleapis.com", "https://fonts.gstatic.com"):
            assert needle not in html

    def test_deterministic_reexport(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()

        # Freeze the only non-deterministic input — the generated-at timestamp
        # that build_report_data stamps via ``datetime.now`` — so two runs of
        # the real export_report pipeline must produce byte-identical HTML.
        import ginkgo_report.model as model_module

        frozen_ts = datetime(2026, 4, 20, 0, 0, 0, tzinfo=UTC)

        class _FixedDatetime:
            @classmethod
            def now(cls, tz=None):  # noqa: ANN001, ANN206
                return frozen_ts

        monkeypatch.setattr(model_module, "datetime", _FixedDatetime)

        first = export_report(summary=run.summary(), out=tmp_path / "a")
        second = export_report(summary=run.summary(), out=tmp_path / "b")
        assert first.index_path.read_bytes() == second.index_path.read_bytes()

    def test_refuses_to_overwrite_foreign_directory_by_default(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "precious"
        (out_dir / "subdir").mkdir(parents=True)
        (out_dir / "my_thesis.txt").write_text("important user data", encoding="utf-8")
        (out_dir / "subdir" / "notes.txt").write_text("more data", encoding="utf-8")

        with pytest.raises(FileExistsError):
            export_report(summary=run.summary(), out=out_dir)

        assert (out_dir / "my_thesis.txt").read_text(encoding="utf-8") == "important user data"
        assert (out_dir / "subdir" / "notes.txt").is_file()
        assert not (out_dir / "index.html").exists()

    def test_force_replaces_foreign_directory(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "precious"
        out_dir.mkdir()
        (out_dir / "existing.txt").write_text("goodbye", encoding="utf-8")

        result = export_report(summary=run.summary(), out=out_dir, force=True)

        assert result.index_path.is_file()
        assert not (out_dir / "existing.txt").exists()

    def test_rerender_into_own_report_dir_needs_no_flag(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "reports" / "run-ok"

        first = export_report(summary=run.summary(), out=out_dir)
        assert (out_dir / _MARKER_NAME).is_file()
        stale = out_dir / "assets" / "stale-figure.png"
        stale.write_bytes(b"stale")

        second = export_report(summary=run.summary(), out=out_dir)

        assert second.index_path == first.index_path
        assert second.index_path.is_file()
        assert not stale.exists()

    def test_managed_destination_replaces_an_unmarked_bundle(self, tmp_path: Path) -> None:
        # A report directory written before the ownership marker existed carries
        # no marker, but ginkgo derived the path and so owns it regardless.
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "reports" / "run-ok"
        (out_dir / "assets").mkdir(parents=True)
        (out_dir / "index.html").write_text("<html>old report</html>", encoding="utf-8")
        (out_dir / "assets" / "report.css").write_text("/* old */", encoding="utf-8")

        result = export_report(summary=run.summary(), out=out_dir, managed_destination=True)

        assert result.index_path.is_file()
        assert "old report" not in result.index_path.read_text(encoding="utf-8")
        assert (out_dir / _MARKER_NAME).is_file()

    def test_empty_directory_is_used_as_is(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "empty"
        out_dir.mkdir()

        result = export_report(summary=run.summary(), out=out_dir)

        assert result.index_path.is_file()

    def test_single_file_export_writes_only_its_file(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        out_dir = tmp_path / "shared"
        out_dir.mkdir()
        (out_dir / "notes.txt").write_text("keep me", encoding="utf-8")

        export_report(summary=run.summary(), out=out_dir / "report.html", single_file=True)
        # A second single-file export over its own file is allowed.
        result = export_report(
            summary=run.summary(), out=out_dir / "report.html", single_file=True
        )

        assert result.index_path == out_dir / "report.html"
        assert sorted(path.name for path in out_dir.iterdir()) == ["notes.txt", "report.html"]

    def test_single_file_export_refuses_to_replace_a_foreign_file(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        target = tmp_path / "page.html"
        target.write_text("<html>mine</html>", encoding="utf-8")

        with pytest.raises(FileExistsError):
            export_report(summary=run.summary(), out=target, single_file=True)

        assert target.read_text(encoding="utf-8") == "<html>mine</html>"

    def test_single_file_export_lists_the_links_it_omits(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)

        result = export_report(summary=run.summary(), out=tmp_path / "r.html", single_file=True)

        assert result.omitted_links == ("logs/task_0001.stderr.log",)

    def test_single_file_embeds_a_marimo_page_as_a_static_copy(self, tmp_path: Path) -> None:
        run = _make_page_run(tmp_path=tmp_path, run_id="run-nb", page=_marimo_page())

        result = export_report(summary=run.summary(), out=tmp_path / "r.html", single_file=True)
        html = result.index_path.read_text(encoding="utf-8")

        assert result.omitted_links == ()
        # The page's rows reach the file; marimo's CDN-loaded frontend does not.
        assert "&lt;td&gt;north&lt;/td&gt;" in html
        assert 'src="https://' not in html
        assert "<iframe" in html

    def test_single_file_embeds_another_page_as_it_is(self, tmp_path: Path) -> None:
        run = _make_page_run(tmp_path=tmp_path, run_id="run-nb")

        result = export_report(summary=run.summary(), out=tmp_path / "r.html", single_file=True)

        assert 'srcdoc="&lt;html&gt;rendered&lt;/html&gt;"' in result.index_path.read_text(
            encoding="utf-8"
        )

    def test_a_bundle_writes_a_static_copy_beside_a_marimo_page(self, tmp_path: Path) -> None:
        run = _make_page_run(tmp_path=tmp_path, run_id="run-nb", page=_marimo_page())

        export_report(summary=run.summary(), out=tmp_path / "out")

        (static,) = (tmp_path / "out" / "pages").glob("*.static.html")
        assert "<td>north</td>" in static.read_text(encoding="utf-8")


class TestNotebookPage:
    def test_a_marimo_page_is_drawn_without_its_frontend(self) -> None:
        page = static_notebook_page(page=_marimo_page(), title="cases")

        assert page is not None
        assert "cdn.jsdelivr" not in page
        assert "<h1>Cases</h1>" in page
        assert "<td>north</td><td>3</td>" in page
        assert "_marimo_row_id" not in page
        assert "data:image/png;base64,AA==" in page
        assert "loaded 3 rows" in page
        assert "ValueError: boom" in page

    def test_hidden_code_stays_hidden(self) -> None:
        page = static_notebook_page(page=_marimo_page(), title="cases")

        assert page is not None
        assert "mo.ui.table(df)" in page
        assert "mo.image(fig)" not in page

    def test_a_page_that_is_not_a_marimo_export_has_no_static_copy(self) -> None:
        assert static_notebook_page(page="<html>rendered</html>", title="x") is None


class TestOutputDir:
    def test_the_default_lands_in_the_workspace_reports_directory(self, tmp_path: Path) -> None:
        run_dir = tmp_path / ".ginkgo" / "runs" / "run-abc"

        resolved = _resolve_destination(run_dir=run_dir, out=None, single_file=False)

        assert resolved == (tmp_path / ".ginkgo" / "reports" / "run-abc").resolve()

    def test_a_single_file_defaults_beside_the_bundle(self, tmp_path: Path) -> None:
        run_dir = tmp_path / ".ginkgo" / "runs" / "run-abc"

        resolved = _resolve_destination(run_dir=run_dir, out=None, single_file=True)

        assert resolved == (tmp_path / ".ginkgo" / "reports" / "run-abc.html").resolve()

    def test_a_single_file_out_ending_in_html_is_the_file(self, tmp_path: Path) -> None:
        run_dir = tmp_path / ".ginkgo" / "runs" / "run-abc"

        resolved = _resolve_destination(
            run_dir=run_dir, out=str(tmp_path / "x.html"), single_file=True
        )

        assert resolved == (tmp_path / "x.html").resolve()


class TestReportCli:
    """``ginkgo report`` must never delete files it did not write."""

    @pytest.fixture(autouse=True)
    def _run_in_tmp_path(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # The command finds runs in the working directory's .ginkgo/.
        monkeypatch.chdir(tmp_path)

    def test_out_dir_with_unrelated_files_is_left_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        precious = tmp_path / "precious"
        (precious / "subdir").mkdir(parents=True)
        (precious / "my_thesis.txt").write_text("important user data", encoding="utf-8")
        (precious / "subdir" / "notes.txt").write_text("more data", encoding="utf-8")

        assert main(["report", "--out", str(precious), "--no-open"]) == 1

        assert (precious / "my_thesis.txt").read_text(encoding="utf-8") == "important user data"
        assert (precious / "subdir" / "notes.txt").is_file()
        assert not (precious / "index.html").exists()
        assert "--force" in capsys.readouterr().out

    def test_force_replaces_the_out_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        target = tmp_path / "target"
        target.mkdir()
        (target / "stale.txt").write_text("goodbye", encoding="utf-8")

        assert main(["report", "--out", str(target), "--no-open", "--force"]) == 0

        assert (target / "index.html").is_file()
        assert not (target / "stale.txt").exists()

    def test_managed_report_dir_rerenders_without_a_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()

        assert main(["report", "--no-open"]) == 0
        assert main(["report", "--no-open"]) == 0

    def test_a_single_file_report_leaves_the_bundle_intact(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        bundle = _resolve_destination(run_dir=run.run_dir, out=None, single_file=False)

        assert main(["report", "--no-open"]) == 0
        assert main(["report", "--single-file", "--no-open"]) == 0

        assert (bundle / "assets" / "report.css").is_file()
        assert (bundle.parent / "run-ok.html").is_file()

    def test_a_very_large_single_file_report_is_flagged(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        monkeypatch.setattr(report_command, "LARGE_SINGLE_FILE_BYTES", 1)

        assert main(["report", "--single-file", "--no-open"]) == 0

        assert "Export without --single-file" in " ".join(capsys.readouterr().out.split())

    def test_single_file_out_writes_that_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()

        assert main(["report", "--single-file", "--out", "x.html", "--no-open"]) == 0

        assert (tmp_path / "x.html").is_file()

    def test_unmarked_bundle_at_the_default_location_rerenders_without_a_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Reports written before the ownership marker existed must keep
        # re-rendering: the default destination needs no proof of ownership.
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        managed_dir = _resolve_destination(run_dir=run.run_dir, out=None, single_file=False)
        (managed_dir / "assets").mkdir(parents=True)
        (managed_dir / "index.html").write_text("<html>old report</html>", encoding="utf-8")
        (managed_dir / "assets" / "report.css").write_text("/* old */", encoding="utf-8")

        assert main(["report", "--no-open"]) == 0

        assert "old report" not in (managed_dir / "index.html").read_text(encoding="utf-8")
        assert (managed_dir / _MARKER_NAME).is_file()
