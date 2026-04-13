"""Tests for the Kubernetes executor with mocked K8s client."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from ginkgo.remote.kubernetes import (
    KubernetesExecutor,
    KubernetesJobHandle,
    _encode_payload,
    _generate_job_name,
    _parse_worker_output,
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

    @patch("ginkgo.remote.kubernetes.k8s_client", create=True)
    def test_submit_creates_job(self, mock_k8s_client: MagicMock) -> None:
        executor = self._make_executor()
        attempt = self._make_attempt()

        # Mock the kubernetes.client module imports inside submit().
        with patch.dict(
            "sys.modules", {"kubernetes": MagicMock(), "kubernetes.client": MagicMock()}
        ):
            from unittest.mock import MagicMock as MM

            # Mock the create response.
            created_job = MM()
            created_job.metadata.name = "ginkgo-test-run-task-001-1"
            executor._batch_api.create_namespaced_job.return_value = created_job

            handle = executor.submit(attempt=attempt)

            assert executor._batch_api.create_namespaced_job.called
            call_kwargs = executor._batch_api.create_namespaced_job.call_args
            assert call_kwargs.kwargs["namespace"] == "ginkgo"
            assert handle.job_name == "ginkgo-test-run-task-001-1"

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

    def test_state_succeeded(self) -> None:
        handle = self._make_handle()
        job_status = MagicMock()
        job_status.succeeded = 1
        job_status.failed = None
        job_status.active = None
        handle._batch_api.read_namespaced_job.return_value.status = job_status

        assert handle.state() == RemoteJobState.SUCCEEDED

    def test_state_failed(self) -> None:
        handle = self._make_handle()
        job_status = MagicMock()
        job_status.succeeded = None
        job_status.failed = 1
        job_status.active = None
        handle._batch_api.read_namespaced_job.return_value.status = job_status

        assert handle.state() == RemoteJobState.FAILED

    def test_state_running(self) -> None:
        handle = self._make_handle()
        job_status = MagicMock()
        job_status.succeeded = None
        job_status.failed = None
        job_status.active = 1
        handle._batch_api.read_namespaced_job.return_value.status = job_status

        assert handle.state() == RemoteJobState.RUNNING

    def test_state_pending(self) -> None:
        handle = self._make_handle()
        job_status = MagicMock()
        job_status.succeeded = None
        job_status.failed = None
        job_status.active = None
        handle._batch_api.read_namespaced_job.return_value.status = job_status

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


class TestRemoteExecutorProtocol:
    """Verify protocol conformance."""

    def test_kubernetes_executor_is_remote_executor(self) -> None:
        executor = KubernetesExecutor(
            namespace="default",
            image="test:latest",
        )
        executor._batch_api = MagicMock()
        executor._core_api = MagicMock()
        assert isinstance(executor, RemoteExecutor)
