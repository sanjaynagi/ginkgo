"""Tests for the Kubernetes executor with mocked K8s client."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes import client as k8s

from ginkgo_k8s.executor import (
    KubernetesExecutor,
    KubernetesJobHandle,
    encode_payload,
    generate_job_name,
    parse_worker_output,
    _RefreshingApi,
)
from ginkgo.runtime.remote_executor import RemoteExecutor, RemoteJobState


class TestHelpers:
    """Tests for helper functions."""

    def test_encode_payload_roundtrip(self) -> None:
        import base64

        payload = {"task_id": "task_001", "args": {"x": 42}}
        encoded = encode_payload(payload)
        decoded = json.loads(base64.b64decode(encoded))
        assert decoded == payload

    def test_generate_job_name(self) -> None:
        attempt = {"run_id": "20260410_ABC", "task_id": "task_001", "attempt": 1}
        name = generate_job_name(attempt)
        assert name.startswith("ginkgo-")
        assert "_" not in name
        assert len(name) <= 63

    def test_generate_job_name_truncation(self) -> None:
        attempt = {"run_id": "a" * 50, "task_id": "b" * 50, "attempt": 1}
        name = generate_job_name(attempt)
        assert len(name) <= 63

    def test_parse_worker_output_valid(self) -> None:
        logs = "Starting task...\nsome debug output\n" + json.dumps({"ok": True, "result": 42})
        result = parse_worker_output(logs)
        assert result == {"ok": True, "result": 42}

    def test_parse_worker_output_no_json(self) -> None:
        logs = "no json here\njust text"
        result = parse_worker_output(logs)
        assert result["ok"] is False

    def test_parse_worker_output_empty(self) -> None:
        result = parse_worker_output("")
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
        created_job.metadata.name = generate_job_name(attempt)
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
        assert pod_spec.init_containers is None
        assert pod_spec.volumes is None

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
        handle._core_api.list_namespaced_pod.return_value.items = []

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

    def _pod(self, *, init: list[str], main: list[str]) -> MagicMock:
        pod = MagicMock()
        pod.metadata.name = "ginkgo-test-001-xyz"
        pod.spec.init_containers = [self._named(name) for name in init] or None
        pod.spec.containers = [self._named(name) for name in main]
        return pod

    def _named(self, name: str) -> MagicMock:
        container = MagicMock()
        container.name = name
        return container

    def test_logs_tail_reads_pod_log(self) -> None:
        handle = self._make_handle()
        mock_pod = self._pod(init=[], main=["ginkgo-worker"])
        handle._core_api.list_namespaced_pod.return_value.items = [mock_pod]
        mock_response = MagicMock()
        mock_response.read.return_value = b"line1\nline2\n"
        handle._core_api.read_namespaced_pod_log.return_value = mock_response

        logs = handle.logs_tail(lines=50)

        assert logs == "line1\nline2\n"
        handle._core_api.read_namespaced_pod_log.assert_called_once_with(
            name="ginkgo-test-001-xyz",
            namespace="ginkgo",
            container="ginkgo-worker",
            tail_lines=50,
            _preload_content=False,
        )

    def test_logs_tail_reads_every_container_in_the_order_they_ran(self) -> None:
        handle = self._make_handle()
        pod = self._pod(init=["stage-in", "task"], main=["stage-out"])
        handle._core_api.list_namespaced_pod.return_value.items = [pod]
        logs = {
            "stage-in": b"staged",
            "task": b"",
            "stage-out": b'from the command\n{"ok": true}\n',
        }

        def read(*, container: str, **_: object) -> MagicMock:
            response = MagicMock()
            response.read.return_value = logs[container]
            return response

        handle._core_api.read_namespaced_pod_log.side_effect = read

        assert handle.logs_tail() == 'staged\nfrom the command\n{"ok": true}\n'
        assert parse_worker_output(handle.logs_tail()) == {"ok": True}

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


class TestExecutorSettings:
    """Settings for clusters other than GKE: OKE with Karpenter, say."""

    def _built_job(self, executor: KubernetesExecutor, attempt: dict) -> object:
        pytest.importorskip("kubernetes")
        executor._batch_api = MagicMock()
        executor._core_api = MagicMock()
        executor.submit(attempt=attempt)
        return executor._batch_api.create_namespaced_job.call_args.kwargs["body"]

    def _attempt(self, **resources: object) -> dict:
        return {
            "run_id": "run-1",
            "task_id": "task_001",
            "attempt": 1,
            "args": {},
            "resources": {"threads": 1, "memory_gb": 1, **resources},
        }

    def test_env_annotations_labels_and_pull_secrets_reach_the_pod(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("GINKGO_PARENT_RUN_ID", raising=False)
        executor = KubernetesExecutor(
            image="worker:1",
            env={"GINKGO_REMOTE_OCI_AUTH": "oke_workload_identity"},
            pod_annotations={"karpenter.sh/do-not-disrupt": "true"},
            pod_labels={"team": "gpas"},
            image_pull_secrets=["ocir-pull"],
        )

        job = self._built_job(executor, self._attempt())

        template = job.spec.template
        env = {item.name: item.value for item in template.spec.containers[0].env}
        assert env["GINKGO_REMOTE_OCI_AUTH"] == "oke_workload_identity"
        assert "GINKGO_WORKER_PAYLOAD" in env
        assert template.metadata.annotations == {"karpenter.sh/do-not-disrupt": "true"}
        assert template.metadata.labels["team"] == "gpas"
        assert job.metadata.labels["team"] == "gpas"
        assert [secret.name for secret in template.spec.image_pull_secrets] == ["ocir-pull"]
        assert "ginkgo/parent-run-id" not in job.metadata.labels

    def test_jobs_carry_the_parent_run_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GINKGO_PARENT_RUN_ID", "dagster-run-7")

        job = self._built_job(KubernetesExecutor(image="worker:1"), self._attempt())

        assert job.metadata.labels["ginkgo/parent-run-id"] == "dagster-run-7"
        assert job.spec.template.metadata.labels["ginkgo/parent-run-id"] == "dagster-run-7"

    def test_the_gpu_selector_key_is_configurable(self) -> None:
        executor = KubernetesExecutor(
            image="worker:1", gpu_node_selector_key="nvidia.com/gpu.product"
        )

        job = self._built_job(executor, self._attempt(gpu=1, gpu_type="A10"))

        assert job.spec.template.spec.node_selector == {"nvidia.com/gpu.product": "A10"}

    def test_empty_fuse_annotations_mean_none(self) -> None:
        executor = KubernetesExecutor(image="worker:1", fuse_annotations={})
        attempt = self._attempt()
        attempt["args"] = {
            "data": {
                "__ginkgo_type__": "remote_input_file",
                "uri": "oci://b@ns/k",
                "scheme": "oci",
                "bucket": "b@ns",
                "key": "k",
                "policy": "fuse",
            }
        }

        job = self._built_job(executor, attempt)

        assert job.spec.template.metadata.annotations is None

    def test_a_container_task_runs_its_image_between_two_workers(self) -> None:
        executor = KubernetesExecutor(
            image="worker:1",
            pull_policy="Always",
            env={"GINKGO_REMOTE_OCI_AUTH": "oke_workload_identity"},
            image_pull_secrets=["ocir-pull"],
        )
        attempt = {**self._attempt(threads=2), "image": "busybox:1.36"}

        spec = self._built_job(executor, attempt).spec.template.spec

        stage_in, task = spec.init_containers
        [stage_out] = spec.containers
        steps = (stage_in, task, stage_out)
        assert [c.name for c in steps] == ["stage-in", "task", "stage-out"]
        assert [c.image for c in steps] == ["worker:1", "busybox:1.36", "worker:1"]
        assert {c.image_pull_policy for c in steps} == {"Always"}
        assert [s.name for s in spec.image_pull_secrets] == ["ocir-pull"]
        # The task's image keeps its ENTRYPOINT, as docker run keeps it locally,
        # and gets the command as its arguments.
        assert task.command is None
        assert task.args == ["sh", "/ginkgo-scratch/ginkgo-command/command.sh"]
        # The task's image gets neither the payload nor the executor's env.
        assert task.env is None
        for worker, phase in ((stage_in, "stage-in"), (stage_out, "stage-out")):
            env = {item.name: item.value for item in worker.env}
            assert worker.command == ["python", "-m", "ginkgo.remote.worker"]
            assert env["GINKGO_WORKER_PHASE"] == phase
            assert env["GINKGO_SCRATCH_ROOT"] == "/ginkgo-scratch"
            assert env["GINKGO_REMOTE_OCI_AUTH"] == "oke_workload_identity"
            assert "GINKGO_WORKER_PAYLOAD" in env
        [volume] = spec.volumes
        assert volume.empty_dir is not None
        for container in steps:
            assert [(m.name, m.mount_path) for m in container.volume_mounts] == [
                (volume.name, "/ginkgo-scratch")
            ]
            assert container.resources.limits["cpu"] == "2"


def _pod(*, scheduled: bool = True, waiting: dict[str, str] | None = None) -> k8s.V1Pod:
    """A container task's pod, whose containers named in *waiting* wait for that reason.

    Every container's image is named ``<container>:1``.
    """
    waiting = waiting or {}

    def status(name: str) -> k8s.V1ContainerStatus:
        reason = waiting.get(name)
        state = (
            k8s.V1ContainerState(waiting=k8s.V1ContainerStateWaiting(reason=reason, message="no"))
            if reason is not None
            else k8s.V1ContainerState(running=k8s.V1ContainerStateRunning())
        )
        return k8s.V1ContainerStatus(
            name=name, image=f"{name}:1", image_id="", ready=False, restart_count=0, state=state
        )

    scheduled_condition = k8s.V1PodCondition(
        type="PodScheduled",
        status="True" if scheduled else "False",
        reason=None if scheduled else "Unschedulable",
        message=None if scheduled else "0/3 nodes are available",
    )
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="ginkgo-test-001-xyz"),
        status=k8s.V1PodStatus(
            phase="Pending",
            conditions=[scheduled_condition],
            init_container_statuses=[status("stage-in"), status("task")],
            container_statuses=[status("stage-out")],
        ),
    )


class TestAPodThatCannotStart:
    """A Job counts a Pending pod as active, so its handle reads the pod itself."""

    @pytest.fixture
    def clock(self, monkeypatch: pytest.MonkeyPatch) -> list[float]:
        now = [1000.0]
        monkeypatch.setattr("ginkgo_k8s.executor.time", SimpleNamespace(monotonic=lambda: now[0]))
        return now

    def _handle(self, *pods: k8s.V1Pod) -> KubernetesJobHandle:
        """A handle to an active Job whose pod reads as each of *pods* in turn."""
        handle = KubernetesJobHandle(
            job_name="ginkgo-test-001",
            namespace="ginkgo",
            unschedulable_timeout=60.0,
            _batch_api=MagicMock(),
            _core_api=MagicMock(),
        )
        status = handle._batch_api.read_namespaced_job.return_value.status
        status.succeeded, status.active, status.conditions = None, 1, []
        handle._core_api.list_namespaced_pod.side_effect = [
            k8s.V1PodList(items=[pod]) for pod in pods
        ]
        handle._core_api.read_namespaced_pod_log.side_effect = Exception("no logs")
        return handle

    @pytest.mark.parametrize(
        ("pod", "reason"),
        [
            (
                _pod(waiting={"task": "ErrImagePull"}),
                "container 'task' cannot pull image 'task:1'",
            ),
            (
                _pod(waiting={"stage-in": "ImagePullBackOff"}),
                "container 'stage-in' cannot pull image 'stage-in:1'",
            ),
            (
                _pod(waiting={"stage-out": "InvalidImageName"}),
                "container 'stage-out' cannot pull image 'stage-out:1'",
            ),
            (_pod(scheduled=False), "unschedulable: Unschedulable: 0/3 nodes are available"),
        ],
    )
    def test_it_fails_once_the_timeout_passes(
        self, clock: list[float], pod: k8s.V1Pod, reason: str
    ) -> None:
        handle = self._handle(pod, pod, pod)

        assert handle.state() == RemoteJobState.RUNNING
        clock[0] += 59
        assert handle.state() == RemoteJobState.RUNNING
        clock[0] += 1
        assert handle.state() == RemoteJobState.FAILED

        error = handle.result().payload["error"]
        assert error["message"].startswith(f"Kubernetes pod could not start: {reason}")
        # Deleted, since it would otherwise hold its node or quota for good.
        handle._batch_api.delete_namespaced_job.assert_called_once()

    def test_a_registry_error_that_clears_restarts_the_clock(self, clock: list[float]) -> None:
        stuck = _pod(waiting={"task": "ErrImagePull"})
        handle = self._handle(stuck, _pod(), stuck, stuck)

        assert handle.state() == RemoteJobState.RUNNING
        clock[0] += 30
        assert handle.state() == RemoteJobState.RUNNING
        clock[0] += 30
        assert handle.state() == RemoteJobState.RUNNING
        clock[0] += 59
        assert handle.state() == RemoteJobState.RUNNING
        handle._batch_api.delete_namespaced_job.assert_not_called()


def test_an_unknown_setting_is_refused() -> None:
    from ginkgo_k8s.executor import build_kubernetes_executor

    with pytest.raises(ValueError, match="Unknown k8s executor setting\\(s\\): namespce"):
        build_kubernetes_executor({"image": "worker:1", "namespce": "nf"})
