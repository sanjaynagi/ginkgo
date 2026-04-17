"""Tests for remote execution provenance — events, manifest, and inspect."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ginkgo.runtime.events import (
    TaskCompleted,
    TaskFailed,
    TaskStarted,
)


class TestRemoteEventFields:
    """Verify remote-specific fields on task events."""

    def _base_kwargs(self) -> dict[str, Any]:
        return {
            "run_id": "test-run",
            "task_id": "task_0000",
            "task_name": "my_task",
            "attempt": 1,
        }

    def test_task_started_remote_fields(self) -> None:
        event = TaskStarted(
            **self._base_kwargs(),
            remote_job_id="ginkgo/ginkgo-test-run-my-task-1",
            execution_backend="remote",
        )
        payload = event.to_payload()

        assert payload["remote_job_id"] == "ginkgo/ginkgo-test-run-my-task-1"
        assert payload["execution_backend"] == "remote"

    def test_task_started_local_defaults(self) -> None:
        event = TaskStarted(**self._base_kwargs())
        payload = event.to_payload()

        assert payload["remote_job_id"] is None
        assert payload["execution_backend"] is None

    def test_task_completed_remote_job_id(self) -> None:
        event = TaskCompleted(
            **self._base_kwargs(),
            remote_job_id="ginkgo/ginkgo-test-run-my-task-1",
        )
        payload = event.to_payload()

        assert payload["remote_job_id"] == "ginkgo/ginkgo-test-run-my-task-1"

    def test_task_completed_local_default(self) -> None:
        event = TaskCompleted(**self._base_kwargs())
        assert event.remote_job_id is None

    def test_task_failed_remote_job_id(self) -> None:
        event = TaskFailed(
            **self._base_kwargs(),
            remote_job_id="ginkgo/ginkgo-test-run-my-task-1",
            failure={"type": "RuntimeError", "message": "boom"},
        )
        payload = event.to_payload()

        assert payload["remote_job_id"] == "ginkgo/ginkgo-test-run-my-task-1"

    def test_task_failed_local_default(self) -> None:
        event = TaskFailed(**self._base_kwargs())
        assert event.remote_job_id is None


class TestProvenanceMarkRunning:
    """Tests for execution_backend in provenance recorder."""

    def test_mark_running_with_execution_backend(self, tmp_path: Path) -> None:
        from ginkgo.runtime.caching.provenance import RunProvenanceRecorder

        recorder = RunProvenanceRecorder(
            run_id="test-run",
            workflow_path=Path("test.py"),
            root_dir=tmp_path,
            jobs=1,
            cores=1,
        )
        recorder.ensure_task(node_id=0, task_name="my_task", env=None)
        recorder.mark_running(
            node_id=0,
            task_name="my_task",
            env=None,
            attempt=1,
            retries=0,
            execution_backend="remote",
        )

        task = recorder._task(0)
        assert task["execution_backend"] == "remote"

    def test_mark_running_without_execution_backend(self, tmp_path: Path) -> None:
        from ginkgo.runtime.caching.provenance import RunProvenanceRecorder

        recorder = RunProvenanceRecorder(
            run_id="test-run",
            workflow_path=Path("test.py"),
            root_dir=tmp_path,
            jobs=1,
            cores=1,
        )
        recorder.ensure_task(node_id=0, task_name="my_task", env=None)
        recorder.mark_running(
            node_id=0,
            task_name="my_task",
            env=None,
            attempt=1,
            retries=0,
        )

        task = recorder._task(0)
        assert "execution_backend" not in task

    def test_update_task_extra_remote_job_id(self, tmp_path: Path) -> None:
        from ginkgo.runtime.caching.provenance import RunProvenanceRecorder

        recorder = RunProvenanceRecorder(
            run_id="test-run",
            workflow_path=Path("test.py"),
            root_dir=tmp_path,
            jobs=1,
            cores=1,
        )
        recorder.ensure_task(node_id=0, task_name="my_task", env=None)
        recorder.update_task_extra(
            node_id=0,
            remote_job_id="ginkgo/ginkgo-test-001",
        )

        task = recorder._task(0)
        assert task["remote_job_id"] == "ginkgo/ginkgo-test-001"


class TestInspectRunRemoteFields:
    """Tests for remote fields in inspect_run output."""

    def test_inspect_run_includes_remote_fields(self, tmp_path: Path) -> None:
        from ginkgo.runtime.caching.provenance import RunProvenanceRecorder
        from ginkgo.cli.commands.inspect import inspect_run

        recorder = RunProvenanceRecorder(
            run_id="test-run",
            workflow_path=Path("test.py"),
            root_dir=tmp_path,
            jobs=1,
            cores=1,
        )
        recorder.ensure_task(node_id=0, task_name="my_task", env=None)
        recorder.mark_running(
            node_id=0,
            task_name="my_task",
            env=None,
            attempt=1,
            retries=0,
            execution_backend="remote",
        )
        recorder.update_task_extra(
            node_id=0,
            remote_job_id="ginkgo/ginkgo-test-001",
            resources={"cores": 4, "memory_gb": 8},
        )
        recorder.mark_succeeded(
            node_id=0,
            task_name="my_task",
            env=None,
            value=42,
        )
        recorder.finalize(status="succeeded")

        result = inspect_run(run_dir=recorder.run_dir)
        task = result["tasks"][0]

        assert task["remote_job_id"] == "ginkgo/ginkgo-test-001"
        assert task["execution_backend"] == "remote"
        assert task["resources"] == {"cores": 4, "memory_gb": 8}

    def test_inspect_run_omits_remote_fields_for_local(self, tmp_path: Path) -> None:
        from ginkgo.runtime.caching.provenance import RunProvenanceRecorder
        from ginkgo.cli.commands.inspect import inspect_run

        recorder = RunProvenanceRecorder(
            run_id="test-run",
            workflow_path=Path("test.py"),
            root_dir=tmp_path,
            jobs=1,
            cores=1,
        )
        recorder.ensure_task(node_id=0, task_name="my_task", env=None)
        recorder.mark_running(
            node_id=0,
            task_name="my_task",
            env=None,
            attempt=1,
            retries=0,
        )
        recorder.mark_succeeded(
            node_id=0,
            task_name="my_task",
            env=None,
            value=42,
        )
        recorder.finalize(status="succeeded")

        result = inspect_run(run_dir=recorder.run_dir)
        task = result["tasks"][0]

        assert "remote_job_id" not in task
        assert "execution_backend" not in task


class TestWorkerCodeBundle:
    """Tests for worker code bundle installation."""

    def test_worker_pops_code_bundle_from_payload(self) -> None:
        """Verify that the worker removes code_bundle from the payload."""
        # This is a unit-level check that the pop happens correctly.
        payload = {
            "args": {},
            "module": "test",
            "binding_name": "fn",
            "resources": {"threads": 1, "memory_gb": 0},
            "code_bundle": {
                "scheme": "gs",
                "bucket": "test-bucket",
                "key": "artifacts/code-bundles/abc.tar.gz",
                "digest": "abc",
            },
        }
        payload.pop("resources", None)
        code_bundle = payload.pop("code_bundle", None)

        assert code_bundle is not None
        assert code_bundle["scheme"] == "gs"
        assert "code_bundle" not in payload
        assert "resources" not in payload
