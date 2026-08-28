"""Tests for remote execution provenance — events, manifest, and inspect."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from ginkgo.runtime.events import (
    GraphNodeRegistered,
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


class TestTaskProjection:
    """What the ledger records about where a task ran."""

    def _register(self, ledger) -> None:
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id=ledger.run_id, task_id="task_0000", node_id=0, task_name="my_task"
            )
        )

    def test_task_started_records_execution_backend(self, ledger) -> None:
        self._register(ledger)
        ledger.bus.emit(
            TaskStarted(
                run_id=ledger.run_id,
                task_id="task_0000",
                task_name="my_task",
                attempt=1,
                execution_backend="remote",
            )
        )
        assert ledger.task()["execution_backend"] == "remote"

    def test_task_started_leaves_the_backend_unset_when_local(self, ledger) -> None:
        self._register(ledger)
        ledger.bus.emit(
            TaskStarted(run_id=ledger.run_id, task_id="task_0000", task_name="my_task", attempt=1)
        )
        assert ledger.task()["execution_backend"] is None

    def test_task_completed_records_the_remote_job_id(self, ledger) -> None:
        self._register(ledger)
        ledger.bus.emit(
            TaskCompleted(
                run_id=ledger.run_id,
                task_id="task_0000",
                task_name="my_task",
                attempt=1,
                remote_job_id="ginkgo/ginkgo-test-001",
            )
        )
        assert ledger.task()["remote_job_id"] == "ginkgo/ginkgo-test-001"


class TestInspectRunRemoteFields:
    """Tests for remote fields in the run's serialised form."""

    def _run(self, ledger, **started) -> dict[str, Any]:
        ledger.bus.emit(
            GraphNodeRegistered(
                run_id=ledger.run_id, task_id="task_0000", node_id=0, task_name="my_task"
            )
        )
        ledger.bus.emit(
            TaskStarted(
                run_id=ledger.run_id,
                task_id="task_0000",
                task_name="my_task",
                attempt=1,
                **started,
            )
        )
        ledger.bus.emit(
            TaskCompleted(
                run_id=ledger.run_id,
                task_id="task_0000",
                task_name="my_task",
                attempt=1,
                remote_job_id=started.get("remote_job_id"),
            )
        )
        return ledger.finish().to_payload()["tasks"][0]

    def test_inspect_run_includes_remote_fields(self, ledger) -> None:
        task = self._run(
            ledger, execution_backend="remote", remote_job_id="ginkgo/ginkgo-test-001"
        )

        assert task["remote_job_id"] == "ginkgo/ginkgo-test-001"
        assert task["execution_backend"] == "remote"

    def test_inspect_run_omits_remote_fields_for_local(self, ledger) -> None:
        task = self._run(ledger)

        assert "remote_job_id" not in task
        assert "execution_backend" not in task


class TestWorkerCodeBundle:
    """Tests for worker payload preparation in worker.main."""

    def test_main_strips_remote_only_keys_and_installs_bundle(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """worker.main installs the code bundle and strips remote-only keys."""
        import ginkgo.runtime.worker as runtime_worker
        from ginkgo.remote import worker

        code_bundle = {
            "scheme": "gs",
            "bucket": "test-bucket",
            "key": "artifacts/code-bundles/abc.tar.gz",
            "digest": "abc",
        }
        payload = {
            "args": {},
            "module": "test",
            "binding_name": "fn",
            "resources": {"threads": 1, "memory_gb": 0},
            "code_bundle": code_bundle,
        }
        monkeypatch.setenv(
            "GINKGO_WORKER_PAYLOAD",
            base64.b64encode(json.dumps(payload).encode()).decode(),
        )

        # Stub the side effects of bundle installation so no download happens.
        installed: list[dict] = []
        monkeypatch.setattr(
            worker, "_install_code_bundle", lambda cb: installed.append(cb) or tmp_path
        )
        monkeypatch.setattr(worker, "_rewrite_module_file", lambda *a, **k: None)

        # Capture the payload that the worker actually hands to run_task.
        captured: dict[str, Any] = {}

        def fake_run_task(received: dict[str, Any]) -> dict[str, Any]:
            captured["payload"] = received
            return {"ok": True, "result_encoding": "inline", "result": None}

        monkeypatch.setattr(runtime_worker, "run_task", fake_run_task)

        with pytest.raises(SystemExit) as exc_info:
            worker.main()
        assert exc_info.value.code == 0

        # The real code bundle reached _install_code_bundle, and run_task saw a
        # payload with every remote-only key removed.
        assert installed == [code_bundle]
        run_payload = captured["payload"]
        assert "code_bundle" not in run_payload
        assert "resources" not in run_payload
        assert "remote_artifact_store" not in run_payload
        assert run_payload["module"] == "test"
