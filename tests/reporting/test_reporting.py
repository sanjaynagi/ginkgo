"""Tests for ``ginkgo.reporting`` — the static HTML report exporter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ginkgo.cli import main
from ginkgo.cli.commands.report import _resolve_output_dir
from ginkgo.core.asset import AssetKey, make_asset_version
from ginkgo.formatting import format_bytes, format_duration
from ginkgo.reporting import SizingPolicy, build_report_data, export_report
from ginkgo.reporting.render import _MARKER_NAME
from ginkgo.reporting.sizing import build_log_tail, build_table_preview
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.events import (
    GraphNodeRegistered,
    RunResourcesSampled,
    TaskAnnotated,
    TaskCompleted,
    TaskFailed,
    TaskPlanned,
)

from ginkgo.runtime.run_summary import RunSummary

from tests.conftest import Ledger


# ----- Fixtures ----------------------------------------------------------


@dataclass
class _Run:
    """A run under construction, finished the first time it is reported on."""

    ledger: Ledger
    tmp_path: Path
    status: str = "success"
    error: str | None = None
    assets: list[dict[str, Any]] = field(default_factory=list)
    _finished: bool = False

    @property
    def run_id(self) -> str:
        return self.ledger.run_id

    @property
    def run_dir(self) -> Path:
        return self.ledger.path

    def add_asset(self, rendered: dict[str, Any], *, append: bool) -> None:
        """Attach one materialised asset to the run's first task."""
        self.assets = [*self.assets, rendered] if append else [rendered]
        self.ledger.bus.emit(
            TaskAnnotated(
                run_id=self.run_id,
                task_id="task_0000",
                task_name="demo.first",
                fields={"assets": list(self.assets)},
            )
        )

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
    """Build a minimal terminal run with a registered asset.

    ``cached`` records both tasks as cache hits instead of fresh executions,
    which is how a re-run of an unchanged workflow reaches the report.
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

    _register_asset(run=run)
    return run


def _make_notebook_run(
    *,
    tmp_path: Path,
    run_id: str,
    render_status: str = "ok",
    render_error: str | None = None,
) -> _Run:
    """Build a terminal run whose single task rendered a notebook."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# demo\n", encoding="utf-8")
    ledger = Ledger.start(
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
    html_path.write_text("<html>HTML export failed</html>", encoding="utf-8")
    ledger.bus.emit(
        TaskAnnotated(
            run_id=run_id,
            task_id="task_0000",
            task_name="demo.report",
            fields={
                "task_type": "notebook",
                "notebook_kind": "marimo",
                "notebook_path": str(tmp_path / "report.py"),
                "render_status": render_status,
                "render_error": render_error,
                "rendered_html": "notebooks/report.html",
            },
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


def _register_asset(
    *,
    run: _Run,
    name: str = "demo/output",
    text: str = "alpha\nbeta\ngamma\n",
    namespace: str = "file",
    suffix: str = ".txt",
    group: str | None = None,
    caption: str | None = None,
    checks: list[dict[str, bool | str]] | None = None,
    append: bool = False,
) -> None:
    """Register an asset and record it against the run's first task.

    ``namespace`` and ``suffix`` pick the asset kind and the stored artifact's
    extension — ``namespace="fig", suffix=".svg"`` registers a figure.
    """
    tmp_path = run.tmp_path
    asset_store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
    artifact_store = LocalArtifactStore(root=tmp_path / ".ginkgo" / "artifacts")
    source = tmp_path / f"{name.replace('/', '_')}{suffix}"
    source.write_text(text, encoding="utf-8")
    record = artifact_store.store(src_path=source)
    metadata = {"stage": "demo"}
    if group is not None:
        metadata["ginkgo_group"] = group
    if caption is not None:
        metadata["ginkgo_caption"] = caption
    if checks is not None:
        metadata["ginkgo_checks"] = checks
    version = make_asset_version(
        key=AssetKey(namespace=namespace, name=name),
        kind=namespace,
        artifact_id=record.artifact_id,
        content_hash=record.digest_hex,
        run_id=run.run_id,
        producer_task="demo.first",
        metadata=metadata,
    )
    asset_store.register_version(version=version)
    run.add_asset(
        {
            "asset_key": str(version.key),
            "version_id": version.version_id,
            "artifact_id": version.artifact_id,
            "name": version.key.name,
            "namespace": version.key.namespace,
            "kind": namespace,
            "metadata": dict(version.metadata),
        },
        append=append,
    )


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
        assert format_bytes(2048) == "2.0 KB"
        assert format_bytes(5_368_709_120) == "5.0 GB"

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

    def test_table_preview_csv(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("a,b\n1,2\n3,4\n5,6\n", encoding="utf-8")
        preview = build_table_preview(
            path=csv_path, extension=".csv", policy=SizingPolicy(table_rows=2)
        )
        assert preview is not None
        assert preview.columns == ("a", "b")
        assert preview.shown_rows == 2
        assert preview.truncated is True


# ----- ReportData --------------------------------------------------------


class TestReportData:
    def test_rejects_running_run(self, tmp_path: Path) -> None:
        ledger = Ledger.start(root=tmp_path, run_id="run-live")
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
        assert {task.base_name for task in report.tasks} == {"first", "second"}
        assert not any(task.failed for task in report.tasks)
        # Summary cards present.
        labels = [card.label for card in report.summary_cards]
        assert labels == ["Tasks", "Failures", "Assets", "Cache hits"]
        # Asset card surfaced.
        assert len(report.assets) == 1
        assert report.assets[0].title == "Ungrouped assets"
        assert report.assets[0].cards[0].asset_key == "file:demo/output"
        # Masthead KV includes the status pill row.
        status_entries = [kv for kv in report.masthead_kv if kv.key == "status"]
        assert len(status_entries) == 1

    def test_asset_checks_are_exposed_on_cards(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-checks", fail=False)
        _register_asset(
            run=run,
            name="demo/checked",
            checks=[{"name": "has_rows", "passed": True}],
            append=True,
        )

        report = build_report_data(summary=run.summary())
        checked_card = next(
            card
            for section in report.assets
            for card in section.cards
            if card.name == "demo/checked"
        )

        assert checked_card.checks[0].name == "has_rows"
        assert checked_card.checks[0].passed is True

    def test_grouped_assets_render_in_named_sections(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-assets", fail=False)
        _register_asset(
            run=run,
            name="demo/qc-a",
            text="qc a\n",
            group="QC metrics",
            caption="Variant counts after QC filtering",
            append=True,
        )
        _register_asset(
            run=run,
            name="demo/qc-b",
            text="qc b\n",
            group="QC metrics",
            append=True,
        )

        report = build_report_data(summary=run.summary())

        assert [section.title for section in report.assets] == [
            "Ungrouped assets",
            "QC metrics",
        ]
        assert [card.asset_key for card in report.assets[1].cards] == [
            "file:demo/qc-a",
            "file:demo/qc-b",
        ]
        assert report.assets[1].cards[0].caption == "Variant counts after QC filtering"
        assert report.assets[1].cards[1].caption is None
        asset_card = next(card for card in report.summary_cards if card.label == "Assets")
        assert asset_card.value == "3"

    def test_asset_cards_carry_anchors_derived_from_their_key(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-assets", fail=False)

        report = build_report_data(summary=run.summary())

        assert report.assets[0].cards[0].anchor == "asset-file-demo-output"

    def test_assets_sharing_a_slug_get_distinct_stable_anchors(self, tmp_path: Path) -> None:
        # "pca plot" and "pca-plot" reduce to the same slug; the ids must still
        # address one card each, and pick the same one on every re-render. The
        # suffix carries a doubled hyphen so it cannot take the id that
        # "pca-plot-2" — a key no other key collides with — claims for itself.
        run = _make_run(tmp_path=tmp_path, run_id="run-assets", fail=False)
        for name in ("demo/pca plot", "demo/pca-plot", "demo/pca-plot-2"):
            _register_asset(
                run=run,
                name=name,
                text=f"{name}\n",
                append=True,
            )

        anchors = [
            (card.asset_key, card.anchor)
            for section in build_report_data(summary=run.summary()).assets
            for card in section.cards
        ]

        assert dict(anchors) == {
            "file:demo/output": "asset-file-demo-output",
            "file:demo/pca plot": "asset-file-demo-pca-plot",
            "file:demo/pca-plot": "asset-file-demo-pca-plot--2",
            "file:demo/pca-plot-2": "asset-file-demo-pca-plot-2",
        }
        rebuilt = [
            (card.asset_key, card.anchor)
            for section in build_report_data(summary=run.summary()).assets
            for card in section.cards
        ]
        assert rebuilt == anchors

    def test_cached_run_reports_the_same_assets_and_sections(self, tmp_path: Path) -> None:
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

        def asset_keys(report) -> list[tuple[str, str]]:  # noqa: ANN001
            return [
                (section.title, card.asset_key)
                for section in report.assets
                for card in section.cards
            ]

        assert asset_keys(cached) == asset_keys(executed)
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
            ("05", "assets"),
            ("06", "env"),
        ]
        # The failure section takes 05, pushing everything after it along.
        assert [(s.number, s.anchor) for s in failed.sections] == [
            ("01", "summary"),
            ("02", "params"),
            ("03", "graph"),
            ("04", "tasks"),
            ("05", "failure"),
            ("06", "assets"),
            ("07", "env"),
        ]
        assert [group.label for group in clean.section_groups] == [
            "Execution",
            "Results",
            "Appendix",
        ]

    def test_section_lookup_rejects_an_unrendered_anchor(self, tmp_path: Path) -> None:
        report = build_report_data(
            summary=_make_run(tmp_path=tmp_path, run_id="run-ok", fail=False).summary()
        )
        assert report.section("assets").title == "Assets"
        with pytest.raises(KeyError, match="notebooks"):
            report.section("notebooks")

    def test_failed_run_produces_failure_card(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        report = build_report_data(summary=run.summary())

        assert report.status_raw == "failed"
        assert report.has_failures is True
        assert len(report.failures) == 1
        card = report.failures[0]
        assert card.base_name == "second"
        assert card.category == "user_code_error"
        assert card.log_tail is not None
        assert card.log_tail.total_lines > 0

    def test_notebook_render_failure_card_is_flagged(self, tmp_path: Path) -> None:
        run = _make_notebook_run(
            tmp_path=tmp_path,
            run_id="run-notebook-failed",
            render_status="failed",
            render_error="render blew up",
        )

        report = build_report_data(summary=run.summary())

        assert len(report.notebooks) == 1
        card = report.notebooks[0]
        assert card.status_tone == "warn"
        assert "HTML export failed" in card.sub_line

    def test_graph_layout_places_all_tasks(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-graph", fail=False)
        report = build_report_data(summary=run.summary())
        assert len(report.graph.nodes) == 2
        assert len(report.graph.edges) == 1
        # Tasks should land in distinct columns because there's a dependency.
        xs = {node.x for node in report.graph.nodes}
        assert len(xs) == 2


# ----- Export ------------------------------------------------------------


class TestExport:
    def test_bundle_mode_renders_asset_check_badges(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-checks", fail=False)
        _register_asset(
            run=run,
            name="demo/checked",
            checks=[{"name": "has_rows", "passed": True}],
            append=True,
        )

        result = export_report(summary=run.summary(), out_dir=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        assert "has_rows" in html
        assert "check-pass" in html

    def test_bundle_mode_writes_index_and_assets(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "out"
        result = export_report(summary=run.summary(), out_dir=out_dir)

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
        assert "<h3>Ungrouped assets</h3>" in html

    def test_bundle_mode_renders_grouped_asset_sections(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-assets", fail=False)
        _register_asset(
            run=run,
            name="demo/qc",
            text="qc\n",
            group="QC metrics",
            caption="Variant counts after QC filtering",
            append=True,
        )
        result = export_report(summary=run.summary(), out_dir=tmp_path / "out")

        html = result.index_path.read_text(encoding="utf-8")
        assert "<h3>Ungrouped assets</h3>" in html
        assert "<h3>QC metrics</h3>" in html
        assert "Variant counts after QC filtering" in html

    def test_bundle_mode_renders_asset_anchor_ids_and_links(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-assets", fail=False)
        _register_asset(
            run=run,
            name="demo/pca plot",
            text="pca\n",
            append=True,
        )
        result = export_report(summary=run.summary(), out_dir=tmp_path / "out")

        html = result.index_path.read_text(encoding="utf-8")
        assert '<div class="asset" id="asset-file-demo-output">' in html
        assert '<div class="asset" id="asset-file-demo-pca-plot">' in html
        assert 'href="#asset-file-demo-pca-plot"' in html

    def test_failure_section_present_only_when_failures_exist(self, tmp_path: Path) -> None:
        ok_run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        ok_out = tmp_path / "ok-out"
        export_report(summary=ok_run.summary(), out_dir=ok_out)
        assert 'id="failure"' not in ok_out.joinpath("index.html").read_text(encoding="utf-8")

        fail_run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        fail_out = tmp_path / "fail-out"
        export_report(summary=fail_run.summary(), out_dir=fail_out)
        assert 'id="failure"' in fail_out.joinpath("index.html").read_text(encoding="utf-8")

    def test_single_file_inlines_css_and_fonts(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "sf"
        result = export_report(summary=run.summary(), out_dir=out_dir, single_file=True)

        assert result.single_file is True
        html = result.index_path.read_text(encoding="utf-8")
        # CSS inlined (style block present, no <link rel="stylesheet">).
        assert "<style>" in html
        assert 'rel="stylesheet"' not in html
        # Font data URIs inlined.
        assert "data:font/woff2;base64," in html

    def test_rendered_section_numerals_are_contiguous(self, tmp_path: Path) -> None:
        # A run with no failures and no notebooks renders six sections; the
        # numerals must run 01..06 with no hole where an omitted section sat.
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        result = export_report(summary=run.summary(), out_dir=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        headings = re.findall(r'<span class="num">(\d+)</span>', html)
        sidebar = re.findall(r'<span class="idx">(\d+)</span>', html)

        assert headings == ["01", "02", "03", "04", "05", "06"]
        assert sidebar == headings

    def test_rendered_section_numerals_include_the_failure_section(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        result = export_report(summary=run.summary(), out_dir=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        assert re.findall(r'<span class="num">(\d+)</span>', html) == [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
        ]
        assert '<span class="num">05</span>Failure' in html

    def test_notebook_section_renders_when_the_run_produced_one(self, tmp_path: Path) -> None:
        run = _make_notebook_run(tmp_path=tmp_path, run_id="run-nb")
        result = export_report(summary=run.summary(), out_dir=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        assert '<span class="num">06</span>Notebooks' in html
        assert re.findall(r'<span class="num">(\d+)</span>', html) == [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
        ]

    def test_single_file_inlines_figures_with_an_image_mime_type(self, tmp_path: Path) -> None:
        # Figure sources are extensionless CAS blobs, so the MIME type has to
        # come from the bundle path. A generic octet-stream URI renders only by
        # browser content sniffing and breaks under a strict CSP.
        run = _make_run(tmp_path=tmp_path, run_id="run-fig", fail=False)
        _register_asset(
            run=run,
            name="demo/figure",
            text='<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"></svg>',
            namespace="fig",
            suffix=".svg",
            append=True,
        )

        result = export_report(summary=run.summary(), out_dir=tmp_path / "sf", single_file=True)
        html = result.index_path.read_text(encoding="utf-8")

        figure_uris = re.findall(r'<img src="data:([^;]+);base64,', html)
        assert figure_uris, "expected the figure to be inlined as a data URI"
        assert all(mime.startswith("image/") for mime in figure_uris), figure_uris
        assert "data:application/octet-stream" not in html

    def test_no_network_references_in_rendered_html(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "out"
        export_report(summary=run.summary(), out_dir=out_dir)
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
        import ginkgo.reporting.model as model_module

        frozen_ts = datetime(2026, 4, 20, 0, 0, 0, tzinfo=UTC)

        class _FixedDatetime:
            @classmethod
            def now(cls, tz=None):  # noqa: ANN001, ANN206
                return frozen_ts

        monkeypatch.setattr(model_module, "datetime", _FixedDatetime)

        first = export_report(summary=run.summary(), out_dir=tmp_path / "a")
        second = export_report(summary=run.summary(), out_dir=tmp_path / "b")
        assert first.index_path.read_bytes() == second.index_path.read_bytes()

    def test_refuses_to_overwrite_foreign_directory_by_default(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "precious"
        (out_dir / "subdir").mkdir(parents=True)
        (out_dir / "my_thesis.txt").write_text("important user data", encoding="utf-8")
        (out_dir / "subdir" / "notes.txt").write_text("more data", encoding="utf-8")

        with pytest.raises(FileExistsError):
            export_report(summary=run.summary(), out_dir=out_dir)

        assert (out_dir / "my_thesis.txt").read_text(encoding="utf-8") == "important user data"
        assert (out_dir / "subdir" / "notes.txt").is_file()
        assert not (out_dir / "index.html").exists()

    def test_force_replaces_foreign_directory(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "precious"
        out_dir.mkdir()
        (out_dir / "existing.txt").write_text("goodbye", encoding="utf-8")

        result = export_report(summary=run.summary(), out_dir=out_dir, force=True)

        assert result.index_path.is_file()
        assert not (out_dir / "existing.txt").exists()

    def test_rerender_into_own_report_dir_needs_no_flag(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "reports" / "run-ok"

        first = export_report(summary=run.summary(), out_dir=out_dir)
        assert (out_dir / _MARKER_NAME).is_file()
        stale = out_dir / "assets" / "stale-figure.png"
        stale.write_bytes(b"stale")

        second = export_report(summary=run.summary(), out_dir=out_dir)

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

        result = export_report(summary=run.summary(), out_dir=out_dir, managed_destination=True)

        assert result.index_path.is_file()
        assert "old report" not in result.index_path.read_text(encoding="utf-8")
        assert (out_dir / _MARKER_NAME).is_file()

    def test_empty_directory_is_used_as_is(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "empty"
        out_dir.mkdir()

        result = export_report(summary=run.summary(), out_dir=out_dir)

        assert result.index_path.is_file()

    def test_single_file_export_marks_its_directory(self, tmp_path: Path) -> None:
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        out_dir = tmp_path / "sf"

        export_report(summary=run.summary(), out_dir=out_dir, single_file=True)
        assert (out_dir / _MARKER_NAME).is_file()

        # A second single-file export over the same directory is allowed.
        result = export_report(summary=run.summary(), out_dir=out_dir, single_file=True)
        assert result.index_path.is_file()


class TestReportCli:
    """``ginkgo report`` must never delete files it did not write."""

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

    def test_unmarked_bundle_at_the_default_location_rerenders_without_a_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Reports written before the ownership marker existed must keep
        # re-rendering: the default destination needs no proof of ownership.
        run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        run.summary()
        managed_dir = _resolve_output_dir(run_dir=run.run_dir, out=None, single_file=False)
        (managed_dir / "assets").mkdir(parents=True)
        (managed_dir / "index.html").write_text("<html>old report</html>", encoding="utf-8")
        (managed_dir / "assets" / "report.css").write_text("/* old */", encoding="utf-8")

        assert main(["report", "--no-open"]) == 0

        assert "old report" not in (managed_dir / "index.html").read_text(encoding="utf-8")
        assert (managed_dir / _MARKER_NAME).is_file()
