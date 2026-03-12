"""Regression tests for provenance serialization."""

from __future__ import annotations

from pathlib import Path

import yaml

from ginkgo import file, task
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
