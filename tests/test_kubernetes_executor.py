"""Tests for the Kubernetes executor with mocked K8s client."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from ginkgo.remote.kubernetes import (
    KubernetesExecutor,
    KubernetesJobHandle,
    _encode_payload,
    _generate_job_name,
    _parse_worker_output,
    _RefreshingApi,
)
from ginkgo.runtime.remote_executor import RemoteExecutor, RemoteJobState


class TestHelpers:
    """Tests for helper functions."""

    def test_encode_payload_roundtrip(self) -> None:
        import base64

        payload = {"task_id": "task_001", "args": {"x": 42}}
        encoded = _encode_payload(payload)
        decoded = json.loads(base64.b64decode(encoded))
        assert decoded == payload

    def test_generate_job_name(self) -> None:
        attempt = {"run_id": "20260410_ABC", "task_id": "task_001", "attempt": 1}
        name = _generate_job_name(attempt)
        assert name.startswith("ginkgo-")
        assert "_" not in name
        assert len(name) <= 63

    def test_generate_job_name_truncation(self) -> None:
        attempt = {"run_id": "a" * 50, "task_id": "b" * 50, "attempt": 1}
        name = _generate_job_name(attempt)
        assert len(name) <= 63

    def test_parse_worker_output_valid(self) -> None:
        logs = "Starting task...\nsome debug output\n" + json.dumps({"ok": True, "result": 42})
        result = _parse_worker_output(logs)
        assert result == {"ok": True, "result": 42}

    def test_parse_worker_output_no_json(self) -> None:
        logs = "no json here\njust text"
        result = _parse_worker_output(logs)
        assert result["ok"] is False

    def test_parse_worker_output_empty(self) -> None:
        result = _parse_worker_output("")
        assert result["ok"] is False


class TestKubernetesExecutor:
    """Tests for KubernetesExecutor with mocked K8s API."""

    def _make_executor(self) -> KubernetesExecutor:
        executor = KubernetesExecutor(
            namespace="ginkgo",
            image="my-image:latest",
            service_account="ginkgo-worker",
            ttl_seconds_after_finished=600,
        )
        executor._batch_api = MagicMock()
        executor._core_api = MagicMock()
        return executor

    def _make_attempt(self, **overrides: object) -> dict:
        base = {
            "run_id": "test-run",
            "task_id": "task_001",
            "attempt": 1,
            "args": {"x": 42},
            "resources": {"threads": 2, "memory_gb": 4},
        }
        base.update(overrides)
        return base

    def test_submit_creates_job(self) -> None:
        import base64

        # submit() builds a real V1Job; the kubernetes client lives in the
        # optional ``cloud`` extra (installed in CI). Guard so the test still
        # degrades gracefully in a minimal install without that extra.
        pytest.importorskip("kubernetes")

        executor = self._make_executor()
        attempt = self._make_attempt()

        # Only the API round-trip is mocked; submit() builds a real V1Job from
        # the installed kubernetes client, so we can assert on its actual spec.
        created_job = MagicMock()
        created_job.metadata.name = _generate_job_name(attempt)
        executor._batch_api.create_namespaced_job.return_value = created_job

        handle = executor.submit(attempt=attempt)
        assert handle.job_name == created_job.metadata.name

        call = executor._batch_api.create_namespaced_job.call_args
        assert call.kwargs["namespace"] == "ginkgo"

        job = call.kwargs["body"]
        assert job.metadata.namespace == "ginkgo"
        assert job.metadata.labels["ginkgo/task-id"] == "task_001"
        assert job.metadata.labels["ginkgo/run-id"] == "test-run"
        assert job.spec.ttl_seconds_after_finished == 600

        pod_spec = job.spec.template.spec
        assert pod_spec.restart_policy == "Never"
        assert pod_spec.service_account_name == "ginkgo-worker"

        container = pod_spec.containers[0]
        assert container.image == "my-image:latest"
        assert container.resources.requests["cpu"] == "2"
        assert container.resources.limits["cpu"] == "2"
        assert container.resources.requests["memory"] == "4Gi"
        assert container.resources.limits["memory"] == "4Gi"

        payload_env = next(e for e in container.env if e.name == "GINKGO_WORKER_PAYLOAD")
        decoded = json.loads(base64.b64decode(payload_env.value))
        assert decoded["task_id"] == "task_001"
        assert decoded["resources"]["threads"] == 2

    def test_satisfies_remote_executor_protocol(self) -> None:
        executor = self._make_executor()
        assert isinstance(executor, RemoteExecutor)


class TestKubernetesJobHandle:
    """Tests for KubernetesJobHandle with mocked K8s API."""

    def _make_handle(self) -> KubernetesJobHandle:
        return KubernetesJobHandle(
            job_name="ginkgo-test-001",
            namespace="ginkgo",
            _batch_api=MagicMock(),
            _core_api=MagicMock(),
        )

    def test_job_id(self) -> None:
        handle = self._make_handle()
        assert handle.job_id == "ginkgo/ginkgo-test-001"

    def _make_status(
        self,
        *,
        succeeded: int | None = None,
        active: int | None = None,
        failed_condition: bool = False,
    ) -> MagicMock:
        """Build a job status mock with explicit fields for state()."""
        status = MagicMock()
        status.succeeded = succeeded
        status.active = active
        status.failed = None
        if failed_condition:
            condition = MagicMock()
            condition.type = "Failed"
            condition.status = "True"
            status.conditions = [condition]
        else:
            status.conditions = []
        return status

    def test_state_succeeded(self) -> None:
        handle = self._make_handle()
        handle._batch_api.read_namespaced_job.return_value.status = self._make_status(succeeded=1)

        assert handle.state() == RemoteJobState.SUCCEEDED

    def test_state_failed(self) -> None:
        handle = self._make_handle()
        handle._batch_api.read_namespaced_job.return_value.status = self._make_status(
            failed_condition=True
        )

        assert handle.state() == RemoteJobState.FAILED

    def test_state_running(self) -> None:
        handle = self._make_handle()
        handle._batch_api.read_namespaced_job.return_value.status = self._make_status(active=1)

        assert handle.state() == RemoteJobState.RUNNING

    def test_state_pending(self) -> None:
        handle = self._make_handle()
        handle._batch_api.read_namespaced_job.return_value.status = self._make_status()
        handle._core_api.list_namespaced_pod.return_value.items = []

        assert handle.state() == RemoteJobState.PENDING

    def test_cancel_deletes_job(self) -> None:
        handle = self._make_handle()
        with patch.dict(
            "sys.modules", {"kubernetes": MagicMock(), "kubernetes.client": MagicMock()}
        ):
            handle.cancel()
            handle._batch_api.delete_namespaced_job.assert_called_once()

    def test_logs_tail_reads_pod_log(self) -> None:
        handle = self._make_handle()
        mock_pod = MagicMock()
        mock_pod.metadata.name = "ginkgo-test-001-xyz"
        handle._core_api.list_namespaced_pod.return_value.items = [mock_pod]
        mock_response = MagicMock()
        mock_response.read.return_value = b"line1\nline2\n"
        handle._core_api.read_namespaced_pod_log.return_value = mock_response

        logs = handle.logs_tail(lines=50)

        assert logs == "line1\nline2\n"
        handle._core_api.read_namespaced_pod_log.assert_called_once_with(
            name="ginkgo-test-001-xyz",
            namespace="ginkgo",
            tail_lines=50,
            _preload_content=False,
        )

    def test_logs_tail_empty_when_no_pods(self) -> None:
        handle = self._make_handle()
        handle._core_api.list_namespaced_pod.return_value.items = []

        assert handle.logs_tail() == ""


class TestRefreshingApi:
    """Verify that the 401-retry proxy rebuilds the inner API and retries."""

    def _make_api_exception(self, status: int) -> Exception:
        exc = Exception("api error")
        exc.status = status  # type: ignore[attr-defined]
        return exc

    def test_passthrough_when_no_error(self) -> None:
        inner = MagicMock()
        inner.read_namespaced_job.return_value = "job-body"
        factory = MagicMock()
        api = _RefreshingApi(inner=inner, factory=factory)

        assert api.read_namespaced_job(name="x") == "job-body"
        factory.assert_not_called()

    def test_retries_and_rebuilds_on_401(self) -> None:
        stale = MagicMock()
        stale.read_namespaced_job.side_effect = self._make_api_exception(401)
        fresh = MagicMock()
        fresh.read_namespaced_job.return_value = "job-body"
        factory = MagicMock(return_value=fresh)
        api = _RefreshingApi(inner=stale, factory=factory)

        assert api.read_namespaced_job(name="x") == "job-body"
        factory.assert_called_once()
        # The wrapper must have swapped the inner api to the fresh one.
        assert api._inner is fresh

    def test_non_401_propagates(self) -> None:
        inner = MagicMock()
        inner.read_namespaced_job.side_effect = self._make_api_exception(500)
        factory = MagicMock()
        api = _RefreshingApi(inner=inner, factory=factory)

        with pytest.raises(Exception, match="api error") as exc_info:
            api.read_namespaced_job(name="x")
        assert getattr(exc_info.value, "status", None) == 500
        factory.assert_not_called()
