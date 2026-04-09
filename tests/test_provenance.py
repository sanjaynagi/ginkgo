"""Regression tests for provenance serialization."""

from __future__ import annotations

from pathlib import Path

import yaml

from ginkgo import file, secret, task
from ginkgo.cli.commands.inspect import inspect_run
import ginkgo.runtime.caching.provenance as provenance_module
from ginkgo.runtime.caching.provenance import RunProvenanceRecorder, load_manifest, make_run_id


@task()
def fake_output_task(output_path: str) -> file:
    Path(output_path).write_text("ok", encoding="utf-8")
    return file(output_path)


class TestRunProvenanceRecorder:
    def test_make_run_id_remains_unique_under_fixed_clock(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        real_datetime = provenance_module.datetime

        class _FixedDatetime:
            @classmethod
            def now(cls, tz=None):
                return real_datetime(2026, 4, 1, 12, 0, 0, 123456, tzinfo=tz)

        tokens = iter(["aaaaaaaa", "bbbbbbbb"])
        monkeypatch.setattr(provenance_module, "datetime", _FixedDatetime)
        monkeypatch.setattr(provenance_module.secrets, "token_hex", lambda _: next(tokens))

        workflow_path = tmp_path / "workflow.py"
        first = make_run_id(workflow_path=workflow_path)
        second = make_run_id(workflow_path=workflow_path)

        assert first != second
        assert first.startswith("20260401_120000_123456_")
        assert second.startswith("20260401_120000_123456_")

    def test_marker_type_outputs_are_serialized_as_plain_strings(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id="20260312_000000_deadbeef",
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=None,
            cores=None,
            memory=None,
            params={},
        )

        output = file("results/out.txt")
        recorder.ensure_task(node_id=0, task_name="demo.task", env=None)
        recorder.mark_succeeded(
            node_id=0,
            task_name="demo.task",
            env=None,
            value=output,
        )
        recorder.finalize(status="succeeded")

        manifest = yaml.safe_load((recorder.run_dir / "manifest.yaml").read_text(encoding="utf-8"))
        assert manifest["tasks"]["task_0000"]["output"] == "results/out.txt"
        assert manifest["tasks"]["task_0000"]["kind"] == "python"
        assert manifest["tasks"]["task_0000"]["execution_mode"] == "worker"

    def test_resources_and_memory_budget_are_serialized(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id="20260312_000000_deadbeef",
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=4,
            cores=2,
            memory=32,
            params={},
        )

        recorder.update_resources(
            {
                "status": "completed",
                "scope": "process_tree",
                "sample_count": 3,
                "current": {"cpu_percent": 10.0, "rss_bytes": 1024, "process_count": 1},
                "peak": {"cpu_percent": 120.0, "rss_bytes": 4096, "process_count": 2},
                "average": {"cpu_percent": 55.0, "rss_bytes": 2048, "process_count": 1.5},
                "updated_at": "2026-03-13T00:00:00+00:00",
            }
        )
        recorder.finalize(status="succeeded")

        manifest = yaml.safe_load((recorder.run_dir / "manifest.yaml").read_text(encoding="utf-8"))
        assert manifest["memory"] == 32
        assert manifest["resources"]["status"] == "completed"
        assert manifest["resources"]["peak"]["rss_bytes"] == 4096

    def test_secret_inputs_are_redacted_in_manifest(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id="20260312_000000_deadbeef",
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=None,
            cores=None,
            memory=None,
            params={},
        )

        recorder.ensure_task(node_id=0, task_name="demo.task", env=None)
        recorder.update_task_inputs(
            node_id=0,
            task_name="demo.task",
            env=None,
            resolved_args={"token": secret("API_TOKEN")},
            input_hashes=None,
            cache_key=None,
        )

        manifest = load_manifest(recorder.run_dir)
        assert manifest["tasks"]["task_0000"]["inputs"]["token"]["redacted"] is True
        assert manifest["tasks"]["task_0000"]["inputs"]["token"]["secret"]["name"] == "API_TOKEN"

    def test_timings_are_serialized_and_exposed_via_inspect(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id="20260312_000000_deadbeef",
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=2,
            cores=2,
            memory=None,
            params={},
        )

        recorder.ensure_task(node_id=0, task_name="demo.task", env=None)
        recorder.add_run_timing(phase="workflow_load_seconds", seconds=1.25)
        recorder.add_task_timing(node_id=0, phase="cache_lookup_seconds", seconds=0.5)
        recorder.mark_cached(
            node_id=0,
            task_name="demo.task",
            env=None,
            value="ok",
        )
        recorder.finalize(status="succeeded")

        manifest = yaml.safe_load((recorder.run_dir / "manifest.yaml").read_text(encoding="utf-8"))
        assert manifest["timings"]["run"]["workflow_load_seconds"] == 1.25
        assert manifest["timings"]["task_phase_totals"]["cache_lookup_seconds"] == 0.5
        assert manifest["tasks"]["task_0000"]["timings"]["cache_lookup_seconds"] == 0.5

        payload = inspect_run(run_dir=recorder.run_dir)
        assert payload["timings"]["run"]["workflow_load_seconds"] == 1.25
        assert payload["tasks"][0]["timings"]["cache_lookup_seconds"] == 0.5

    def test_load_manifest_replays_task_updates_before_finalize(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id="20260312_000000_deadbeef",
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )

        recorder.ensure_task(node_id=0, task_name="demo.task", env=None)
        recorder.mark_running(
            node_id=0,
            task_name="demo.task",
            env=None,
            attempt=1,
            retries=0,
        )

        raw_manifest = yaml.safe_load(
            (recorder.run_dir / "manifest.yaml").read_text(encoding="utf-8")
        )
        assert raw_manifest["tasks"] == {}

        manifest = load_manifest(recorder.run_dir)
        assert manifest["tasks"]["task_0000"]["status"] == "running"
        assert manifest["tasks"]["task_0000"]["attempt"] == 1

    def test_manifest_is_flushed_with_latest_state_on_finalize(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id="20260312_000000_deadbeef",
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )

        recorder.ensure_task(node_id=0, task_name="demo.task", env=None)
        recorder.mark_cached(
            node_id=0,
            task_name="demo.task",
            env=None,
            value="ok",
        )
        recorder.finalize(status="succeeded")

        raw_manifest = yaml.safe_load(
            (recorder.run_dir / "manifest.yaml").read_text(encoding="utf-8")
        )
        assert raw_manifest["status"] == "succeeded"
        assert raw_manifest["tasks"]["task_0000"]["status"] == "cached"
