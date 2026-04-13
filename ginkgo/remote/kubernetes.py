"""Kubernetes executor for remote task execution.

Submits individual Ginkgo task attempts as ``batch/v1`` Jobs on a
Kubernetes cluster.  One Job per attempt; Ginkgo handles retries.
"""

from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any

from ginkgo.runtime.remote_executor import (
    RemoteJobResult,
    RemoteJobState,
)


def _encode_payload(attempt: dict[str, Any]) -> str:
    """Encode a worker payload as a base64 JSON string."""
    payload_json = json.dumps(attempt, default=str)
    return base64.b64encode(payload_json.encode()).decode()


def _generate_job_name(attempt: dict[str, Any]) -> str:
    """Generate a unique Kubernetes Job name from the attempt payload."""
    run_id = attempt.get("run_id", "unknown")
    task_id = attempt.get("task_id", "unknown")
    attempt_num = attempt.get("attempt", 0)
    # K8s names must be <= 63 chars, lowercase alphanumeric + hyphens.
    name = f"ginkgo-{run_id}-{task_id}-{attempt_num}"
    name = name.lower().replace("_", "-")
    if len(name) > 63:
        name = name[:63]
    # Strip trailing hyphens.
    return name.rstrip("-")


def _parse_worker_output(logs: str) -> dict[str, Any]:
    """Parse the worker result from pod log output.

    The remote worker prints a single JSON line to stdout as its last
    output.  We search backwards from the end to find it.
    """
    for line in reversed(logs.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {
        "ok": False,
        "error": {
            "type": "RuntimeError",
            "module": "builtins",
            "message": "No worker output found in pod logs",
            "args": ["No worker output found in pod logs"],
        },
    }


@dataclass(kw_only=True)
class KubernetesExecutor:
    """Remote executor that submits tasks as Kubernetes batch/v1 Jobs.

    Parameters
    ----------
    namespace : str
        Kubernetes namespace for Job creation.
    image : str
        Container image for the worker pod.
    service_account : str | None
        Kubernetes service account for the pod.
    pull_policy : str
        Image pull policy (``IfNotPresent``, ``Always``, ``Never``).
    gpu_type : str | None
        GKE accelerator type for GPU tasks (e.g. ``"nvidia-l4"``,
        ``"nvidia-tesla-t4"``). When a task requests GPUs and this is set,
        the pod receives a ``cloud.google.com/gke-accelerator`` node
        selector so GKE Autopilot provisions the correct GPU node.
    node_selector : dict[str, str] | None
        Node selector labels for pod scheduling.
    tolerations : list[dict[str, Any]] | None
        Pod tolerations for scheduling on tainted nodes.
    ttl_seconds_after_finished : int
        Time before completed Jobs are cleaned up by the TTL controller.
    unschedulable_timeout : float
        Seconds to wait for a Pending pod to schedule before treating the
        job as failed. Catches quota, capacity, and node-affinity issues
        (e.g. GPU stockouts) that would otherwise leave pods Pending
        indefinitely.
    """

    namespace: str = "default"
    image: str = ""
    service_account: str | None = None
    pull_policy: str = "IfNotPresent"
    gpu_type: str | None = None
    node_selector: dict[str, str] | None = None
    tolerations: list[dict[str, Any]] | None = None
    ttl_seconds_after_finished: int = 300
    unschedulable_timeout: float = 300.0
    _batch_api: Any = field(default=None, init=False, repr=False)
    _core_api: Any = field(default=None, init=False, repr=False)

    def _get_apis(self) -> tuple[Any, Any]:
        """Return (BatchV1Api, CoreV1Api), lazily initialized."""
        if self._batch_api is not None:
            return self._batch_api, self._core_api

        # Suppress noisy gRPC fork-safety warnings on macOS.
        os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

        try:
            from kubernetes import client, config as k8s_config
        except ImportError as exc:
            raise ImportError(
                "kubernetes is required for the K8s executor. "
                "Install it with: pip install ginkgo[cloud]"
            ) from exc

        k8s_config.load_config()
        self._batch_api = client.BatchV1Api()
        self._core_api = client.CoreV1Api()
        return self._batch_api, self._core_api

    def submit(self, *, attempt: dict[str, Any]) -> KubernetesJobHandle:
        """Create a Kubernetes Job from the attempt payload.

        Parameters
        ----------
        attempt : dict[str, Any]
            Worker payload with ``resources`` key.

        Returns
        -------
        KubernetesJobHandle
        """
        from kubernetes import client as k8s_client

        batch_api, core_api = self._get_apis()
        resources = attempt.get("resources", {})
        threads = resources.get("threads", 1)
        memory_gb = resources.get("memory_gb", 0)

        # Build resource requests and limits.
        resource_requests: dict[str, str] = {"cpu": str(threads)}
        resource_limits: dict[str, str] = {"cpu": str(threads)}
        if memory_gb > 0:
            resource_requests["memory"] = f"{memory_gb}Gi"
            resource_limits["memory"] = f"{memory_gb}Gi"

        gpu = resources.get("gpu", 0)
        if gpu > 0:
            resource_limits["nvidia.com/gpu"] = str(gpu)

        container = k8s_client.V1Container(
            name="ginkgo-worker",
            image=self.image,
            image_pull_policy=self.pull_policy,
            command=["python", "-m", "ginkgo.remote.worker"],
            env=[
                k8s_client.V1EnvVar(
                    name="GINKGO_WORKER_PAYLOAD",
                    value=_encode_payload(attempt),
                ),
            ],
            resources=k8s_client.V1ResourceRequirements(
                requests=resource_requests,
                limits=resource_limits,
            ),
        )

        job_name = _generate_job_name(attempt)
        pod_spec_kwargs: dict[str, Any] = {
            "containers": [container],
            "restart_policy": "Never",
        }
        if self.service_account is not None:
            pod_spec_kwargs["service_account_name"] = self.service_account

        # Merge node selectors: start with user-configured selectors, then
        # add the GKE accelerator selector for GPU tasks.
        merged_node_selector: dict[str, str] = dict(self.node_selector or {})
        if gpu > 0 and self.gpu_type is not None:
            merged_node_selector["cloud.google.com/gke-accelerator"] = self.gpu_type
        if merged_node_selector:
            pod_spec_kwargs["node_selector"] = merged_node_selector
        if self.tolerations is not None:
            pod_spec_kwargs["tolerations"] = [
                k8s_client.V1Toleration(**t) for t in self.tolerations
            ]

        job = k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                namespace=self.namespace,
                labels={
                    "app": "ginkgo",
                    "ginkgo/task-id": str(attempt.get("task_id", "")),
                    "ginkgo/run-id": str(attempt.get("run_id", "")),
                },
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,
                ttl_seconds_after_finished=self.ttl_seconds_after_finished,
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(**pod_spec_kwargs),
                ),
            ),
        )

        created = batch_api.create_namespaced_job(namespace=self.namespace, body=job)
        return KubernetesJobHandle(
            job_name=created.metadata.name,
            namespace=self.namespace,
            unschedulable_timeout=self.unschedulable_timeout,
            _batch_api=batch_api,
            _core_api=core_api,
        )


@dataclass(kw_only=True)
class KubernetesJobHandle:
    """Handle to a running Kubernetes Job.

    Parameters
    ----------
    job_name : str
        Name of the Kubernetes Job.
    namespace : str
        Kubernetes namespace.
    """

    job_name: str
    namespace: str
    unschedulable_timeout: float = 300.0
    _batch_api: Any = field(repr=False)
    _core_api: Any = field(repr=False)
    _unschedulable_since: float | None = field(default=None, init=False, repr=False)
    _unschedulable_reason: str | None = field(default=None, init=False, repr=False)

    @property
    def job_id(self) -> str:
        """Stable identifier for the remote job."""
        return f"{self.namespace}/{self.job_name}"

    def state(self) -> RemoteJobState:
        """Poll the current Kubernetes Job status."""
        job = self._batch_api.read_namespaced_job(name=self.job_name, namespace=self.namespace)
        status = job.status
        if status.succeeded and status.succeeded > 0:
            return RemoteJobState.SUCCEEDED
        if status.failed and status.failed > 0:
            return RemoteJobState.FAILED
        if status.active and status.active > 0:
            self._unschedulable_since = None
            self._unschedulable_reason = None
            return RemoteJobState.RUNNING

        # Pending: check whether the pod is stuck unschedulable. A pod
        # stuck Pending beyond ``unschedulable_timeout`` (quota, capacity,
        # or node-affinity failure) is failed explicitly so ginkgo does
        # not wait forever.
        if self._check_unschedulable_timeout():
            return RemoteJobState.FAILED
        return RemoteJobState.PENDING

    def _check_unschedulable_timeout(self) -> bool:
        """Return True if the pod has been unschedulable for too long."""
        pods = self._core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"job-name={self.job_name}",
        )
        if not pods.items:
            return False
        pod = pods.items[0]
        conditions = pod.status.conditions or []
        scheduled_condition = next((c for c in conditions if c.type == "PodScheduled"), None)
        if scheduled_condition is None or scheduled_condition.status == "True":
            self._unschedulable_since = None
            self._unschedulable_reason = None
            return False

        now = time.monotonic()
        if self._unschedulable_since is None:
            self._unschedulable_since = now
        self._unschedulable_reason = f"{scheduled_condition.reason}: {scheduled_condition.message}"
        return now - self._unschedulable_since >= self.unschedulable_timeout

    def result(self) -> RemoteJobResult:
        """Read the worker result from pod logs.

        Blocks until the job reaches a terminal state, then parses the
        structured JSON result from the last line of pod output.
        """
        # Poll until terminal.
        poll_interval = 2.0
        while True:
            current = self.state()
            if current.is_terminal:
                break
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 30.0)

        logs = self.logs_tail(lines=1000)
        if current == RemoteJobState.FAILED and self._unschedulable_since is not None:
            reason = self._unschedulable_reason or "pod unschedulable"
            payload = {
                "ok": False,
                "error": {
                    "type": "RuntimeError",
                    "module": "builtins",
                    "message": f"Kubernetes pod unschedulable: {reason}",
                    "args": [f"Kubernetes pod unschedulable: {reason}"],
                },
            }
        else:
            payload = _parse_worker_output(logs)

        exit_code = None
        if current == RemoteJobState.FAILED:
            exit_code = 1

        return RemoteJobResult(
            state=current,
            payload=payload,
            exit_code=exit_code,
            logs=logs,
        )

    def cancel(self) -> None:
        """Delete the Kubernetes Job and its pods."""
        from kubernetes.client import V1DeleteOptions

        self._batch_api.delete_namespaced_job(
            name=self.job_name,
            namespace=self.namespace,
            body=V1DeleteOptions(propagation_policy="Foreground"),
        )

    def logs_tail(self, *, lines: int = 100) -> str:
        """Return the last *lines* lines from the pod's container log.

        Uses ``_preload_content=False`` because the kubernetes Python
        client's response deserializer mangles JSON log output into
        Python-repr format when preloading (single-quoted keys,
        capitalised ``False``), which breaks worker output parsing.
        """
        pods = self._core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"job-name={self.job_name}",
        )
        if not pods.items:
            return ""

        pod_name = pods.items[0].metadata.name
        try:
            resp = self._core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                tail_lines=lines,
                _preload_content=False,
            )
            return resp.read().decode("utf-8", errors="replace")
        except Exception:
            return ""
