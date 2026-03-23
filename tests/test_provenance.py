"""Regression tests for provenance serialization."""

from __future__ import annotations

from pathlib import Path

import yaml

from ginkgo import file, secret, task
from ginkgo.runtime.provenance import RunProvenanceRecorder


@task()
def fake_output_task(output_path: str) -> file:
    Path(output_path).write_text("ok", encoding="utf-8")
    return file(output_path)


class TestRunProvenanceRecorder:
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

        manifest = yaml.safe_load((recorder.run_dir / "manifest.yaml").read_text(encoding="utf-8"))
        assert manifest["tasks"]["task_0000"]["inputs"]["token"]["redacted"] is True
        assert manifest["tasks"]["task_0000"]["inputs"]["token"]["secret"]["name"] == "API_TOKEN"
