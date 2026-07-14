"""Tests for ``ginkgo.reporting`` — the static HTML report exporter."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from ginkgo.core.asset import AssetKey, make_asset_version
from ginkgo.formatting import format_bytes, format_duration
from ginkgo.reporting import SizingPolicy, build_report_data, export_report
from ginkgo.reporting.sizing import build_log_tail, build_table_preview
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.caching.provenance import RunProvenanceRecorder


# ----- Fixtures ----------------------------------------------------------


def _make_run(
    *,
    tmp_path: Path,
    run_id: str,
    fail: bool,
) -> Path:
    """Build a minimal terminal run directory with a registered asset."""
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# demo workflow\n@flow\ndef main():\n    pass\n", encoding="utf-8")
    recorder = RunProvenanceRecorder(
        run_id=run_id,
        workflow_path=workflow_path,
        root_dir=tmp_path / ".ginkgo" / "runs",
        jobs=4,
        cores=4,
        params={"seed": 42, "targets": ["a", "b"]},
    )

    stdout_path, stderr_path = recorder.ensure_task(node_id=0, task_name="demo.first", env="local")
    stdout_path.write_text("starting first task\n" * 3, encoding="utf-8")
    stderr_path.write_text("\n".join(f"log line {i}" for i in range(50)) + "\n", encoding="utf-8")
    recorder.update_task_inputs(
        node_id=0,
        task_name="demo.first",
        env="local",
        resolved_args={"message": "hello"},
        input_hashes={"message": {"type": "str", "sha256": "aa"}},
        cache_key="cache-first",
        dependency_ids=[],
        dynamic_dependency_ids=[],
    )
    recorder.mark_succeeded(node_id=0, task_name="demo.first", env="local", value="results/a.txt")

    stdout_path_1, stderr_path_1 = recorder.ensure_task(
        node_id=1, task_name="demo.second", env="local"
    )
    stdout_path_1.write_text("starting second task\n", encoding="utf-8")
    stderr_path_1.write_text(
        "\n".join(f"err line {i}" for i in range(30)) + "\n", encoding="utf-8"
    )
    recorder.update_task_inputs(
        node_id=1,
        task_name="demo.second",
        env="local",
        resolved_args={"upstream": "a.txt"},
        input_hashes={"upstream": {"type": "str", "sha256": "bb"}},
        cache_key="cache-second",
        dependency_ids=[0],
        dynamic_dependency_ids=[],
    )
    if fail:
        exc = RuntimeError("boom")
        exc.exit_code = 1  # type: ignore[attr-defined]
        recorder.mark_failed(
            node_id=1,
            task_name="demo.second",
            env="local",
            exc=exc,
            failure={"kind": "user_code_error"},
        )
    else:
        recorder.mark_succeeded(
            node_id=1, task_name="demo.second", env="local", value="results/b.txt"
        )

    recorder.update_resources(
        {
            "status": "completed",
            "scope": "process_tree",
            "sample_count": 2,
            "current": {"cpu_percent": 12.5, "rss_bytes": 1024, "process_count": 1},
            "peak": {"cpu_percent": 85.0, "rss_bytes": 4096, "process_count": 2},
            "average": {"cpu_percent": 48.0, "rss_bytes": 2048, "process_count": 1.5},
            "updated_at": "2026-03-13T00:00:00+00:00",
        }
    )
    recorder.finalize(status="failed" if fail else "succeeded", error="boom" if fail else None)

    _register_asset(tmp_path=tmp_path, run_id=run_id, run_dir=recorder.run_dir)

    return recorder.run_dir


def _register_asset(
    *,
    tmp_path: Path,
    run_id: str,
    run_dir: Path,
    name: str = "demo/output",
    text: str = "alpha\nbeta\ngamma\n",
    group: str | None = None,
    caption: str | None = None,
    checks: list[dict[str, bool | str]] | None = None,
    append: bool = False,
) -> None:
    """Register a file asset and patch the manifest to reference it."""
    asset_store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
    artifact_store = LocalArtifactStore(root=tmp_path / ".ginkgo" / "artifacts")
    source = tmp_path / f"{name.replace('/', '_')}.txt"
    source.write_text(text, encoding="utf-8")
    record = artifact_store.store(src_path=source)
    metadata = {"stage": "demo"}
    if group is not None:
        metadata["ginkgo_group"] = group
    if caption is not None:
        metadata["ginkgo_caption"] = caption
    if checks is not None:
        metadata["_checks"] = checks
    version = make_asset_version(
        key=AssetKey(namespace="file", name=name),
        kind="file",
        artifact_id=record.artifact_id,
        content_hash=record.digest_hex,
        run_id=run_id,
        producer_task="demo.first",
        metadata=metadata,
    )
    asset_store.register_version(version=version)

    manifest_path = run_dir / "manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    task_0 = manifest["tasks"]["task_0000"]
    rendered = {
        "asset_key": str(version.key),
        "version_id": version.version_id,
        "artifact_id": version.artifact_id,
        "name": version.key.name,
        "namespace": version.key.namespace,
        "kind": "file",
        "metadata": dict(version.metadata),
    }
    existing = task_0.get("assets", []) if append else []
    task_0["assets"] = [*existing, rendered]
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


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
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# demo\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id="run-live",
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            params={},
        )
        recorder.ensure_task(node_id=0, task_name="demo.t", env="local")
        # No finalize — run is still "running".
        with pytest.raises(ValueError, match="not terminal"):
            build_report_data(run_dir=recorder.run_dir)

    def test_basic_successful_run(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        report = build_report_data(
            run_dir=run_dir,
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
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-checks", fail=False)
        _register_asset(
            tmp_path=tmp_path,
            run_id="run-checks",
            run_dir=run_dir,
            name="demo/checked",
            checks=[{"name": "has_rows", "passed": True}],
            append=True,
        )

        report = build_report_data(run_dir=run_dir)
        checked_card = next(
            card
            for section in report.assets
            for card in section.cards
            if card.name == "demo/checked"
        )

        assert checked_card.checks[0].name == "has_rows"
        assert checked_card.checks[0].passed is True

    def test_grouped_assets_render_in_named_sections(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-assets", fail=False)
        _register_asset(
            tmp_path=tmp_path,
            run_id="run-assets",
            run_dir=run_dir,
            name="demo/qc-a",
            text="qc a\n",
            group="QC metrics",
            caption="Variant counts after QC filtering",
            append=True,
        )
        _register_asset(
            tmp_path=tmp_path,
            run_id="run-assets",
            run_dir=run_dir,
            name="demo/qc-b",
            text="qc b\n",
            group="QC metrics",
            append=True,
        )

        report = build_report_data(run_dir=run_dir)

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

    def test_failed_run_produces_failure_card(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        report = build_report_data(run_dir=run_dir)

        assert report.status_raw == "failed"
        assert report.has_failures is True
        assert len(report.failures) == 1
        card = report.failures[0]
        assert card.base_name == "second"
        assert card.category == "user_code_error"
        assert card.log_tail is not None
        assert card.log_tail.total_lines > 0

    def test_graph_layout_places_all_tasks(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-graph", fail=False)
        report = build_report_data(run_dir=run_dir)
        assert len(report.graph.nodes) == 2
        assert len(report.graph.edges) == 1
        # Tasks should land in distinct columns because there's a dependency.
        xs = {node.x for node in report.graph.nodes}
        assert len(xs) == 2


# ----- Export ------------------------------------------------------------


class TestExport:
    def test_bundle_mode_renders_asset_check_badges(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-checks", fail=False)
        _register_asset(
            tmp_path=tmp_path,
            run_id="run-checks",
            run_dir=run_dir,
            name="demo/checked",
            checks=[{"name": "has_rows", "passed": True}],
            append=True,
        )

        result = export_report(run_dir=run_dir, out_dir=tmp_path / "out")
        html = result.index_path.read_text(encoding="utf-8")

        assert "has_rows" in html
        assert "check-pass" in html

    def test_bundle_mode_writes_index_and_assets(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        out_dir = tmp_path / "out"
        result = export_report(run_dir=run_dir, out_dir=out_dir)

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
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-assets", fail=False)
        _register_asset(
            tmp_path=tmp_path,
            run_id="run-assets",
            run_dir=run_dir,
            name="demo/qc",
            text="qc\n",
            group="QC metrics",
            caption="Variant counts after QC filtering",
            append=True,
        )
        result = export_report(run_dir=run_dir, out_dir=tmp_path / "out")

        html = result.index_path.read_text(encoding="utf-8")
        assert "<h3>Ungrouped assets</h3>" in html
        assert "<h3>QC metrics</h3>" in html
        assert "Variant counts after QC filtering" in html

    def test_failure_section_present_only_when_failures_exist(self, tmp_path: Path) -> None:
        ok_run = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        ok_out = tmp_path / "ok-out"
        export_report(run_dir=ok_run, out_dir=ok_out)
        assert 'id="failure"' not in ok_out.joinpath("index.html").read_text(encoding="utf-8")

        fail_run = _make_run(tmp_path=tmp_path, run_id="run-fail", fail=True)
        fail_out = tmp_path / "fail-out"
        export_report(run_dir=fail_run, out_dir=fail_out)
        assert 'id="failure"' in fail_out.joinpath("index.html").read_text(encoding="utf-8")

    def test_single_file_inlines_css_and_fonts(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        out_dir = tmp_path / "sf"
        result = export_report(run_dir=run_dir, out_dir=out_dir, single_file=True)

        assert result.single_file is True
        html = result.index_path.read_text(encoding="utf-8")
        # CSS inlined (style block present, no <link rel="stylesheet">).
        assert "<style>" in html
        assert 'rel="stylesheet"' not in html
        # Font data URIs inlined.
        assert "data:font/woff2;base64," in html

    def test_no_network_references_in_rendered_html(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        out_dir = tmp_path / "out"
        export_report(run_dir=run_dir, out_dir=out_dir)
        html = (out_dir / "index.html").read_text(encoding="utf-8")

        # Allow HTTP namespace URIs (xmlns) but forbid external asset URLs.
        for needle in ("https://fonts.googleapis.com", "https://fonts.gstatic.com"):
            assert needle not in html

    def test_deterministic_reexport(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)

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

        first = export_report(run_dir=run_dir, out_dir=tmp_path / "a")
        second = export_report(run_dir=run_dir, out_dir=tmp_path / "b")
        assert first.index_path.read_bytes() == second.index_path.read_bytes()

    def test_refuses_to_overwrite_without_flag(self, tmp_path: Path) -> None:
        run_dir = _make_run(tmp_path=tmp_path, run_id="run-ok", fail=False)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        (out_dir / "existing.txt").write_text("keep me", encoding="utf-8")

        with pytest.raises(FileExistsError):
            export_report(run_dir=run_dir, out_dir=out_dir, overwrite=False)
