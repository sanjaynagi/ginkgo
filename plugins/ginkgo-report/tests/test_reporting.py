"""Tests for ``ginkgo_report`` — the static HTML report exporter."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ginkgo.cli import main
from ginkgo.formatting import format_bytes, format_duration
from ginkgo.query import TaskRow
from ginkgo.runtime.events import (
    GraphNodeRegistered,
    RunNotice,
    RunResourcesSampled,
    TaskAnnotated,
    TaskCompleted,
    TaskFailed,
    TaskPlanned,
    TaskRetrying,
    TaskSkipped,
    TaskStarted,
)
from ginkgo.runtime.run_summary import RunSummary
from ginkgo_report import command as report_command
from ginkgo_report.command import _resolve_destination
from ginkgo_report.model import _memory_label, build_report_data
from ginkgo_report.notebook_page import static_notebook_page
from ginkgo_report.render import _MARKER_NAME, export_report
from ginkgo_report.sizing import SizingPolicy, build_log_tail
from tests.conftest import Ledger

# ----- Fixtures ----------------------------------------------------------


@pytest.fixture
def ledger(tmp_path: Path):
    """Yield a started :class:`Ledger` rooted at ``tmp_path``."""
    started = Ledger.start(root=tmp_path, run_id="run-ledger")
    yield started
    started.close()


def _two_task_run(
    tmp_path: Path, *, run_id: str = "run-ok", fail: bool = False, cached: bool = False
) -> Ledger:
    """Record a two-task run, left open so a test can add events before finishing it.

    ``cached`` records both tasks as cache hits instead of fresh executions,
    which is how a re-run of an unchanged workflow reaches the report. With
    ``fail`` the second task fails.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# demo workflow\n@flow\ndef main():\n    pass\n", encoding="utf-8")
    ledger = Ledger.start(
        root=tmp_path,
        run_id=run_id,
        workflow=str(workflow_path),
        jobs=4,
        cores=4,
        params={"seed": 42, "targets": ["a", "b"]},
    )
    emit = ledger.bus.emit
    for node_id, (name, message, dependencies) in enumerate(
        [("demo.first", "hello", []), ("demo.second", "a.txt", ["task_0000"])]
    ):
        task_id = f"task_{node_id:04d}"
        emit(
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
        emit(
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
        logs = ledger.run_dir.logs_dir
        (logs / f"{task_id}.stdout.log").write_text(f"starting {name}\n", encoding="utf-8")
        (logs / f"{task_id}.stderr.log").write_text(
            "\n".join(f"log line {i}" for i in range(30)) + "\n", encoding="utf-8"
        )

    status = "cached" if cached else "success"
    for task_id, name in (("task_0000", "demo.first"), ("task_0001", "demo.second")):
        if fail and task_id == "task_0001":
            emit(
                TaskFailed(
                    run_id=run_id,
                    task_id=task_id,
                    task_name=name,
                    attempt=1,
                    exit_code=1,
                    failure={"kind": "user_code_error", "message": "boom"},
                )
            )
            continue
        emit(
            TaskCompleted(
                run_id=run_id,
                task_id=task_id,
                task_name=name,
                attempt=1,
                status=status,
                outputs=[{"name": "return", "type": "value", "summary": f"results/{name}.txt"}],
            )
        )
    emit(
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
    return ledger


def _summary(tmp_path: Path, *, run_id: str = "run-ok", fail: bool = False, **kw) -> RunSummary:
    """Record and finish a two-task run (see :func:`_two_task_run`)."""
    ledger = _two_task_run(tmp_path, run_id=run_id, fail=fail, **kw)
    return ledger.finish(status="failed" if fail else "success", error="boom" if fail else None)


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

    def cell(cell_id: str, output: dict[str, Any], console: list | None = None) -> dict:
        return {"id": cell_id, "console": console or [], "outputs": [output]}

    session = {
        "cells": [
            cell(
                "a",
                {"type": "data", "data": {"text/markdown": "<h1>Cases</h1>"}},
                [{"name": "stdout", "text": "loaded 3 rows\n", "type": "stream"}],
            ),
            cell("b", {"type": "data", "data": {"text/html": table}}),
            cell(
                "c",
                {"type": "data", "data": {"text/html": "<img src='data:image/png;base64,AA=='>"}},
            ),
            cell("d", {"type": "error", "ename": "ValueError", "evalue": "boom"}),
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


def _page_run(tmp_path: Path, *, run_id: str = "run-nb", page: str = "<html>rendered</html>"):
    """Record and finish a run whose single task rendered *page*."""
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# demo\n", encoding="utf-8")
    ledger = Ledger.start(
        root=tmp_path, run_id=run_id, workflow=str(workflow_path), jobs=1, cores=1
    )
    emit = ledger.bus.emit
    emit(
        GraphNodeRegistered(
            run_id=run_id, task_id="task_0000", node_id=0, task_name="demo.report", env="local"
        )
    )
    emit(TaskPlanned(run_id=run_id, task_id="task_0000", task_name="demo.report", cache_key="nb"))
    html_path = ledger.path / "notebooks" / "report.html"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(page, encoding="utf-8")
    emit(
        TaskAnnotated(
            run_id=run_id,
            task_id="task_0000",
            task_name="demo.report",
            fields={"rendered_html": "notebooks/report.html", "rendered_html_run_id": run_id},
        )
    )
    emit(
        TaskCompleted(
            run_id=run_id,
            task_id="task_0000",
            task_name="demo.report",
            attempt=1,
            outputs=[{"name": "return", "type": "file", "path": str(html_path)}],
        )
    )
    return ledger.finish()


def _flaky_run(tmp_path: Path, *, retries: int, succeed: bool) -> RunSummary:
    """Record one task that failed ``retries`` times, then succeeded or gave up."""
    ledger = Ledger.start(root=tmp_path, run_id="run-flaky")
    common = {"run_id": "run-flaky", "task_id": "task_0000", "task_name": "demo.flaky"}
    ledger.bus.emit(GraphNodeRegistered(**common, node_id=0, retries=retries))
    for attempt in range(1, retries + 2):
        ledger.bus.emit(TaskStarted(**common, attempt=attempt))
        if attempt <= retries:
            failure = {"kind": "user_code_error", "message": "RuntimeError: transient"}
            ledger.bus.emit(TaskRetrying(**common, attempt=attempt, failure=failure))
    last = retries + 1
    if succeed:
        ledger.bus.emit(TaskCompleted(**common, attempt=last))
        return ledger.finish()
    failure = {"kind": "user_code_error", "message": "boom"}
    ledger.bus.emit(TaskFailed(**common, attempt=last, exit_code=1, failure=failure))
    return ledger.finish(status="failed", error="boom")


def _html(summary: RunSummary, out: Path, **options: Any) -> str:
    return export_report(summary=summary, out=out, **options).index_path.read_text(
        encoding="utf-8"
    )


_FROZEN = datetime(2026, 4, 20, 0, 0, 0, tzinfo=UTC)


# ----- Formatting --------------------------------------------------------


@pytest.mark.parametrize(
    ("seconds", "text"),
    [(0.42, "0.4s"), (12.4, "12s"), (74, "1m 14s"), (2834, "47m 14s"), (3725, "1h 02m 05s")]
    + [(None, "—")],
)
def test_format_duration(seconds: float | None, text: str) -> None:
    assert format_duration(seconds) == text


@pytest.mark.parametrize(
    ("size", "text"), [(512, "512 B"), (2048, "2.0 KiB"), (5_368_709_120, "5.0 GiB"), (None, "—")]
)
def test_format_bytes(size: int | None, text: str) -> None:
    assert format_bytes(size) == text


def test_log_tail_truncation(tmp_path: Path) -> None:
    path = tmp_path / "big.log"
    path.write_text("\n".join(f"line {i}" for i in range(500)) + "\n", encoding="utf-8")
    tail = build_log_tail(path=path, policy=SizingPolicy(log_lines=50))
    assert tail is not None
    assert (tail.shown_lines, tail.total_lines, tail.truncated) == (50, 500, True)
    assert tail.lines[-1] == "line 499"
    assert build_log_tail(path=None, policy=SizingPolicy()) is None


# ----- ReportData --------------------------------------------------------


class TestReportData:
    def test_rejects_running_run(self, ledger: Ledger) -> None:
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id=ledger.run_id, task_id="task_0000", node_id=0, task_name="demo.t"
            )
        )
        # No RunCompleted — the run is still "running".
        with pytest.raises(ValueError, match="not terminal"):
            build_report_data(summary=ledger.summary())

    def test_basic_successful_run(self, tmp_path: Path) -> None:
        report = build_report_data(summary=_summary(tmp_path), generated_at=_FROZEN)

        assert report.run_id == "run-ok"
        assert report.status_raw == "succeeded"
        assert (report.status_label, report.sidebar_status_label) == ("succeeded", "succeeded")
        assert report.has_failures is False
        assert {task.label for task in report.tasks} == {"first", "second"}
        assert [task.attempts_label for task in report.tasks] == ["1", "1"]
        assert [card.label for card in report.summary_cards] == ["Tasks", "Failures", "Cache hits"]
        assert len([kv for kv in report.masthead_kv if kv.key == "status"]) == 1
        # Tasks land in distinct columns because there's a dependency.
        assert len(report.graph.edges) == 1
        assert len({node.x for node in report.graph.nodes}) == 2

    def test_a_cancelled_run_can_be_reported_on(self, tmp_path: Path) -> None:
        summary = _two_task_run(tmp_path, run_id="run-cancelled").finish(status="cancelled")

        report = build_report_data(summary=summary)

        assert (report.status_raw, report.status_label) == ("cancelled", "cancelled")

    def test_attempts_label_for_an_exhausted_retry_budget(self, tmp_path: Path) -> None:
        report = build_report_data(summary=_flaky_run(tmp_path, retries=2, succeed=False))

        assert [task.attempts_label for task in report.tasks] == ["3 of 3 allowed"]
        assert [card.attempts_label for card in report.failures] == ["3 of 3 allowed"]

    def test_a_retried_attempts_error_is_shown_after_the_retry_succeeds(
        self, tmp_path: Path
    ) -> None:
        html = _html(_flaky_run(tmp_path, retries=1, succeed=True), tmp_path / "out")

        assert "Retried attempts" in html
        assert "RuntimeError: transient" in html

    def test_a_rendered_page_run_lists_its_outputs_and_page(self, tmp_path: Path) -> None:
        report = build_report_data(summary=_page_run(tmp_path))

        (row,) = report.outputs
        assert (row.label, row.kind) == ("report", "file")
        assert row.path.endswith("notebooks/report.html")
        (card,) = report.pages
        assert (card.title, card.status_tone) == ("report", "ok")
        assert "from run" not in card.sub_line
        # The workflow is shown relative to the project.
        assert report.workflow == "workflow.py"

    def test_a_cached_run_reports_the_same_sections_as_an_executed_one(
        self, tmp_path: Path
    ) -> None:
        executed = build_report_data(summary=_summary(tmp_path / "a", run_id="run-a"))
        cached = build_report_data(summary=_summary(tmp_path / "b", run_id="run-b", cached=True))

        assert [task.cache_label for task in executed.tasks] == ["miss", "miss"]
        assert [task.cache_label for task in cached.tasks] == ["hit", "hit"]
        assert [task.status_label for task in cached.tasks] == ["cached", "cached"]
        assert cached.has_failures is False
        assert [(s.anchor, s.number, s.title) for s in cached.sections] == [
            (s.anchor, s.number, s.title) for s in executed.sections
        ]
        assert executed.summary_cards[2].sub == "every task ran"
        assert cached.summary_cards[2].value == "2 / 2"
        assert cached.summary_cards[2].sub == "every task reused from the cache"

    @pytest.mark.parametrize(
        ("declared", "peak", "label"),
        [
            # Declared memory is shown as written.
            ({"memory": "64Mi", "memory_gb": 1, "effective_memory_gb": 1}, 2048, "2.0 KiB / 64Mi"),
            # An escalated budget is the budget the attempt ran against.
            ({"memory": "16Gi", "memory_gb": 16, "effective_memory_gb": 32}, 0, "0 B / 32 GiB"),
        ],
    )
    def test_memory_label(self, declared: dict, peak: int, label: str) -> None:
        @dataclass
        class _Task:
            resource_usage: dict[str, Any]

        usage = {"declared": declared, "measured": {"peak_rss_bytes": peak}}
        assert _memory_label(_Task(resource_usage=usage)) == label

    def test_a_run_notice_is_shown_in_the_summary(self, tmp_path: Path) -> None:
        ledger = _two_task_run(tmp_path)
        ledger.bus.emit(RunNotice(run_id="run-ok", message="Slack notification failed"))

        assert "<li>Slack notification failed</li>" in _html(ledger.finish(), tmp_path / "out")

    def test_section_numbers_skip_sections_that_do_not_render(self, tmp_path: Path) -> None:
        clean = build_report_data(summary=_summary(tmp_path / "clean"))
        failed = build_report_data(summary=_summary(tmp_path / "failed", fail=True))

        anchors = ["summary", "params", "graph", "tasks", "env"]
        assert [(s.number, s.anchor) for s in clean.sections] == [
            (f"{i:02d}", anchor) for i, anchor in enumerate(anchors, 1)
        ]
        # The failure section takes 05, pushing everything after it along.
        assert [s.anchor for s in failed.sections] == [*anchors[:4], "failure", "env"]
        assert failed.sections[-1].number == "06"
        # Results holds only rendered pages, so a run without one drops the group.
        assert [group.label for group in clean.section_groups] == ["Execution", "Appendix"]
        assert clean.section("tasks").title == "Tasks"
        with pytest.raises(KeyError, match="pages"):
            clean.section("pages")

    def test_failed_run_produces_failure_card(self, tmp_path: Path) -> None:
        report = build_report_data(summary=_summary(tmp_path, fail=True))

        assert (report.status_raw, report.has_failures) == ("failed", True)
        (card,) = report.failures
        assert (card.label, card.category) == ("second", "user_code_error")
        assert card.log_tail is not None and card.log_tail.total_lines > 0

    def test_a_failed_run_leads_with_why_it_has_no_result(self, tmp_path: Path) -> None:
        reason = "The workflow result depends on second[b], which failed."
        summary = _two_task_run(tmp_path, fail=True).finish(status="failed", error=reason)

        html = _html(summary, tmp_path / "out")

        assert reason in html
        assert "Run failed:" in html

    def test_a_skipped_task_is_counted_and_says_what_blocked_it(self, tmp_path: Path) -> None:
        ledger = _two_task_run(tmp_path, run_id="run-fail", fail=True)
        common = {"run_id": "run-fail", "task_id": "task_0002", "task_name": "demo.third"}
        ledger.bus.emit(GraphNodeRegistered(**common, node_id=2))
        ledger.bus.emit(
            TaskSkipped(
                **common,
                reason="upstream_failed",
                blocked_by_task_id="task_0001",
                blocked_by_task_name="demo.second",
            )
        )

        report = build_report_data(summary=ledger.finish(status="failed", error="boom"))

        assert report.summary_cards[0].sub == "1 ok · 1 fail · 1 skipped"
        assert report.sidebar_status_label == "failed · 1 failed · 1 skipped"
        assert [(task.label, task.cause) for task in report.skipped] == [
            ("third", "blocked by second")
        ]

    def test_a_traceback_names_project_files_relative_to_the_project(self, tmp_path: Path) -> None:
        ledger = _two_task_run(tmp_path, fail=True)
        steps = tmp_path.resolve() / "workflow" / "steps.py"
        (ledger.run_dir.logs_dir / "task_0001.stderr.log").write_text(
            f'  File "{steps}", line 3, in second\n', encoding="utf-8"
        )

        report = build_report_data(summary=ledger.finish(status="failed", error="boom"))

        (card,) = report.failures
        assert card.log_tail is not None
        assert card.log_tail.lines == ('  File "workflow/steps.py", line 3, in second',)

    def test_a_cache_hit_shows_the_peak_of_the_run_that_measured_it(self, tmp_path: Path) -> None:
        measured = TaskRow(
            run_id="20260101_000000_000000_9e1f3e6c",
            task_id="task_0000",
            name="demo.first",
            display_label=None,
            status="succeeded",
            cached=False,
            cache_key="cache-demo.first",
            started_at=None,
            finished_at=None,
            duration_s=None,
            attempts=1,
            peak_rss_bytes=2048,
        )

        report = build_report_data(
            summary=_summary(tmp_path, cached=True), measured_runs={"cache-demo.first": measured}
        )

        assert [task.memory_label for task in report.tasks] == ["2.0 KiB (run 9e1f3e6c)", "—"]

    def test_a_cyclic_predecessor_map_lays_out_rather_than_recursing(self, tmp_path: Path) -> None:
        """A cycle cannot come from a task graph, so the report must not follow one."""
        summary = _summary(tmp_path)
        node_ids = [task.node_id for task in summary.tasks]
        cyclic = replace(
            summary,
            tasks=tuple(
                replace(task, dependency_ids=(node_ids[(index + 1) % len(node_ids)],))
                for index, task in enumerate(summary.tasks)
            ),
        )

        assert len(build_report_data(summary=cyclic).graph.nodes) == len(summary.tasks)


# ----- Export ------------------------------------------------------------


class TestExport:
    def test_fan_out_notebook_branches_keep_their_own_html(self, ledger: Ledger) -> None:
        for node_id, region in enumerate(["north", "south"]):
            common = {"run_id": ledger.run_id, "task_id": f"task_{node_id:04d}"}
            common["task_name"] = "demo.report"
            ledger.bus.emit(GraphNodeRegistered(**common, node_id=node_id))
            html_path = ledger.path / "notebooks" / f"task_{node_id:04d}.html"
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text(f"<html>{region}</html>", encoding="utf-8")
            fields = {"rendered_html": f"notebooks/{html_path.name}"}
            ledger.bus.emit(TaskAnnotated(**common, fields=fields))
            ledger.bus.emit(TaskCompleted(**common, attempt=1))
        summary = ledger.finish()

        out_dir = ledger.path.parent / "out"
        export_report(summary=summary, out=out_dir)

        copies = [out_dir / card.link_relpath for card in build_report_data(summary=summary).pages]
        assert [path.read_text(encoding="utf-8") for path in copies] == [
            "<html>north</html>",
            "<html>south</html>",
        ]

    def test_fan_out_branches_are_named_by_their_labels(self, ledger: Ledger) -> None:
        for node_id, label in enumerate(["clean[north]", "clean[south]"]):
            common = {"run_id": ledger.run_id, "task_id": f"task_{node_id:04d}"}
            common["task_name"] = "demo.clean"
            ledger.bus.emit(GraphNodeRegistered(**common, node_id=node_id))
            ledger.bus.emit(
                TaskPlanned(**common, display_label=label, inputs={"region": label[6:-1]})
            )
            failure = {"kind": "user_code_error", "message": "boom"}
            ledger.bus.emit(
                TaskFailed(**common, display_label=label, attempt=1, exit_code=1, failure=failure)
            )

        html = _html(ledger.finish(status="failed", error="boom"), ledger.path.parent / "out")

        assert re.findall(r'<td class="name">([^<]*)</td>', html) == [
            "clean[north]",
            "clean[south]",
        ]
        assert re.findall(r"<h4>([^<]*)</h4>", html) == ["clean[north]", "clean[south]"]

    def test_a_bundle_writes_index_and_assets_into_an_empty_directory(
        self, tmp_path: Path
    ) -> None:
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        result = export_report(summary=_summary(tmp_path), out=out_dir)

        assert result.index_path == out_dir / "index.html"
        assert (out_dir / "assets" / "report.css").is_file()
        assert (out_dir / "assets" / "islands.js").is_file()
        assert (out_dir / "assets" / "fonts").is_dir()
        html = result.index_path.read_text(encoding="utf-8")
        assert all(text in html for text in ("run-ok", "01</span>Summary", "first", "second"))
        assert 'id="failure"' not in html
        for needle in ("https://fonts.googleapis.com", "https://fonts.gstatic.com"):
            assert needle not in html

    def test_single_file_inlines_css_and_fonts(self, tmp_path: Path) -> None:
        result = export_report(summary=_summary(tmp_path), out=tmp_path / "sf", single_file=True)

        assert result.single_file is True
        html = result.index_path.read_text(encoding="utf-8")
        assert "<style>" in html
        assert 'rel="stylesheet"' not in html
        assert "data:font/woff2;base64," in html

    @pytest.mark.parametrize(
        ("kind", "count", "headings"),
        [
            ("clean", 5, []),
            ("failed", 6, ["05</span>Failure"]),
            ("page", 7, ["05</span>Outputs", "06</span>Rendered pages"]),
        ],
    )
    def test_rendered_section_numerals_are_contiguous(
        self, tmp_path: Path, kind: str, count: int, headings: list[str]
    ) -> None:
        if kind == "page":
            summary = _page_run(tmp_path)
        else:
            summary = _summary(tmp_path, fail=kind == "failed")

        html = _html(summary, tmp_path / "out")

        numerals = re.findall(r'<span class="num">(\d+)</span>', html)
        assert numerals == [f"{i:02d}" for i in range(1, count + 1)]
        assert re.findall(r'<span class="idx">(\d+)</span>', html) == numerals
        assert all(f'<span class="num">{heading}' in html for heading in headings)
        assert ('id="failure"' in html) is (kind == "failed")

    def test_recorded_values_are_escaped(self, tmp_path: Path) -> None:
        summary = Ledger.start(
            root=tmp_path, run_id="run-esc", params={"note": "<b>x</b>"}
        ).finish()

        html = _html(summary, tmp_path / "out")

        assert "&lt;b&gt;x&lt;/b&gt;" in html
        assert "<b>x</b>" not in html

    def test_deterministic_reexport(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Freeze the only non-deterministic input — the generated-at timestamp
        # that build_report_data stamps via ``datetime.now`` — so two runs of
        # the real export_report pipeline must produce byte-identical HTML.
        import ginkgo_report.model as model_module

        class _FixedDatetime:
            @classmethod
            def now(cls, tz=None):  # noqa: ANN001, ANN206
                return _FROZEN

        monkeypatch.setattr(model_module, "datetime", _FixedDatetime)
        summary = _summary(tmp_path)

        first = export_report(summary=summary, out=tmp_path / "a")
        second = export_report(summary=summary, out=tmp_path / "b")
        assert first.index_path.read_bytes() == second.index_path.read_bytes()

    def test_rerendering_its_own_bundle_refreshes_assets_and_keeps_other_files(
        self, tmp_path: Path
    ) -> None:
        summary = _summary(tmp_path)
        out_dir = tmp_path / "reports" / "run-ok"

        first = export_report(summary=summary, out=out_dir)
        assert (out_dir / _MARKER_NAME).is_file()
        stale = out_dir / "assets" / "stale-figure.png"
        stale.write_bytes(b"stale")
        (out_dir / "notes.txt").write_text("mine", encoding="utf-8")

        second = export_report(summary=summary, out=out_dir)

        assert second.index_path == first.index_path
        assert not stale.exists()
        assert (out_dir / "notes.txt").read_text(encoding="utf-8") == "mine"

    def test_single_file_export_writes_only_its_file(self, tmp_path: Path) -> None:
        summary = _summary(tmp_path)
        out_dir = tmp_path / "shared"
        out_dir.mkdir()
        (out_dir / "notes.txt").write_text("keep me", encoding="utf-8")

        export_report(summary=summary, out=out_dir / "report.html", single_file=True)
        # A second single-file export over its own file is allowed.
        result = export_report(summary=summary, out=out_dir / "report.html", single_file=True)

        assert result.index_path == out_dir / "report.html"
        assert sorted(path.name for path in out_dir.iterdir()) == ["notes.txt", "report.html"]

        foreign = tmp_path / "page.html"
        foreign.write_text("<html>mine</html>", encoding="utf-8")
        with pytest.raises(FileExistsError):
            export_report(summary=summary, out=foreign, single_file=True)
        assert foreign.read_text(encoding="utf-8") == "<html>mine</html>"

    def test_single_file_export_lists_the_links_it_omits(self, tmp_path: Path) -> None:
        result = export_report(
            summary=_summary(tmp_path, fail=True), out=tmp_path / "r.html", single_file=True
        )

        assert result.omitted_links == ("logs/task_0001.stderr.log",)

    def test_a_marimo_page_is_embedded_as_a_static_copy(self, tmp_path: Path) -> None:
        summary = _page_run(tmp_path, page=_marimo_page())

        result = export_report(summary=summary, out=tmp_path / "r.html", single_file=True)
        html = result.index_path.read_text(encoding="utf-8")

        assert result.omitted_links == ()
        # The page's rows reach the file; marimo's CDN-loaded frontend does not.
        assert "&lt;td&gt;north&lt;/td&gt;" in html
        assert 'src="https://' not in html
        assert "<iframe" in html

        export_report(summary=summary, out=tmp_path / "out")
        (static,) = (tmp_path / "out" / "pages").glob("*.static.html")
        assert "<td>north</td>" in static.read_text(encoding="utf-8")

    def test_single_file_embeds_another_page_as_it_is(self, tmp_path: Path) -> None:
        html = _html(_page_run(tmp_path), tmp_path / "r.html", single_file=True)

        assert 'srcdoc="&lt;html&gt;rendered&lt;/html&gt;"' in html


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
        # Hidden code stays hidden.
        assert "mo.ui.table(df)" in page
        assert "mo.image(fig)" not in page

        without_code = static_notebook_page(page=_marimo_page(), title="cases", include_code=False)
        assert without_code is not None
        assert "mo.md(" not in without_code
        assert "<h1>Cases</h1>" in without_code

    def test_a_page_that_is_not_a_marimo_export_has_no_static_copy(self) -> None:
        assert static_notebook_page(page="<html>rendered</html>", title="x") is None


@pytest.mark.parametrize(
    ("out", "single_file", "expected"),
    [
        (None, False, ".ginkgo/reports/run-abc"),
        (None, True, ".ginkgo/reports/run-abc.html"),
        ("x.html", True, "x.html"),
    ],
)
def test_resolve_destination(
    tmp_path: Path, out: str | None, single_file: bool, expected: str
) -> None:
    run_dir = tmp_path / ".ginkgo" / "runs" / "run-abc"
    resolved = _resolve_destination(
        run_dir=run_dir, out=None if out is None else str(tmp_path / out), single_file=single_file
    )

    assert resolved == (tmp_path / expected).resolve()


class TestReportCli:
    """``ginkgo report`` must never delete files it did not write."""

    @pytest.fixture(autouse=True)
    def _run_in_tmp_path(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # The command finds runs in the working directory's .ginkgo/.
        monkeypatch.chdir(tmp_path)

    def test_out_dir_with_unrelated_files_is_left_alone(self, tmp_path: Path, capsys) -> None:
        _summary(tmp_path)
        precious = tmp_path / "precious"
        (precious / "subdir").mkdir(parents=True)
        (precious / "my_thesis.txt").write_text("important user data", encoding="utf-8")
        (precious / "subdir" / "notes.txt").write_text("more data", encoding="utf-8")

        assert main(["report", "--out", str(precious), "--no-open"]) == 1

        assert (precious / "my_thesis.txt").read_text(encoding="utf-8") == "important user data"
        assert (precious / "subdir" / "notes.txt").is_file()
        assert not (precious / "index.html").exists()
        assert "--force" in capsys.readouterr().out

        assert main(["report", "--out", str(precious), "--no-open", "--force"]) == 0

        assert (precious / "index.html").is_file()
        assert (precious / "my_thesis.txt").read_text(encoding="utf-8") == "important user data"

    def test_single_file_out_must_name_an_html_file(self, tmp_path: Path, capsys) -> None:
        _summary(tmp_path)

        assert main(["report", "--single-file", "--out", "x.txt", "--no-open"]) == 1
        assert not (tmp_path / "x.txt").exists()
        assert ".html" in capsys.readouterr().out

        assert main(["report", "--single-file", "--out", "x.html", "--no-open"]) == 0
        assert (tmp_path / "x.html").is_file()

    def test_hide_code_leaves_notebook_code_out_of_the_static_copy(self, tmp_path: Path) -> None:
        _page_run(tmp_path, page=_marimo_page())

        assert main(["report", "--out", "out", "--hide-code", "--no-open"]) == 0

        (static,) = (tmp_path / "out" / "pages").glob("*.static.html")
        text = static.read_text(encoding="utf-8")
        assert "<td>north</td>" in text
        assert "mo.ui.table(df)" not in text

    def test_a_single_file_report_leaves_the_bundle_intact(self, tmp_path: Path) -> None:
        _summary(tmp_path)
        bundle = tmp_path / ".ginkgo" / "reports" / "run-ok"

        assert main(["report", "--no-open"]) == 0
        assert main(["report", "--single-file", "--no-open"]) == 0

        assert (bundle / "assets" / "report.css").is_file()
        assert (bundle.parent / "run-ok.html").is_file()

    def test_a_very_large_single_file_report_is_flagged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
    ) -> None:
        _summary(tmp_path)
        monkeypatch.setattr(report_command, "LARGE_SINGLE_FILE_BYTES", 1)

        assert main(["report", "--single-file", "--no-open"]) == 0

        assert "Export without --single-file" in " ".join(capsys.readouterr().out.split())

    def test_unmarked_bundle_at_the_default_location_rerenders_without_a_flag(
        self, tmp_path: Path
    ) -> None:
        # Reports written before the ownership marker existed must keep
        # re-rendering: the default destination needs no proof of ownership.
        _summary(tmp_path)
        managed_dir = tmp_path / ".ginkgo" / "reports" / "run-ok"
        (managed_dir / "assets").mkdir(parents=True)
        (managed_dir / "index.html").write_text("<html>old report</html>", encoding="utf-8")
        (managed_dir / "assets" / "report.css").write_text("/* old */", encoding="utf-8")

        assert main(["report", "--no-open"]) == 0
        assert main(["report", "--no-open"]) == 0

        assert "old report" not in (managed_dir / "index.html").read_text(encoding="utf-8")
        assert (managed_dir / _MARKER_NAME).is_file()
