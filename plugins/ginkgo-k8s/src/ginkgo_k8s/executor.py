"""Kubernetes executor for remote task execution.

Submits individual Ginkgo task attempts as ``batch/v1`` Jobs on a
Kubernetes cluster.  One Job per attempt; Ginkgo handles retries.
"""

from __future__ import annotations

import os
import time
from contextlib import suppress
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any

from ginkgo.remote.executor_support import (
    SHARED_SCRATCH,
    encode_payload,
    generate_job_name,
    job_steps,
    parse_worker_output,
    payload_requires_fuse,
)
from ginkgo.runtime.remote_executor import (
    RemoteJobResult,
    RemoteJobState,
)


class _RefreshingApi:
    """Kubernetes API proxy that reloads kube-config on 401.

    The kubernetes-python client caches the bearer token at client
    construction time. GKE tokens from the exec plugin expire after one
    hour, and the client does not re-invoke the plugin when they do — a
    multi-hour evaluator session would otherwise start failing every
    API call mid-run. This proxy catches ``ApiException(status=401)``
    from any method call, rebuilds the underlying API via the supplied
    factory (which re-reads ``~/.kube/config`` and so re-invokes the
    exec plugin for a fresh token), and retries the call once.
    """

    def __init__(self, *, inner: Any, factory: Any) -> None:
        self._inner = inner
        self._factory = factory

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            try:
                return attr(*args, **kwargs)
            except Exception as exc:
                if getattr(exc, "status", None) != 401:
                    raise
                self._inner = self._factory()
                return getattr(self._inner, name)(*args, **kwargs)

        return wrapped


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
        Image pull policy (``IfNotPresent``, ``Always``, ``Never``), for the
        worker image and a container-env task's own image alike.
    gpu_type : str | None
        Accelerator type for GPU tasks (e.g. ``"nvidia-l4"``). When a task
        requests GPUs and this is set, the pod gets a node selector
        ``gpu_node_selector_key: gpu_type``.
    gpu_node_selector_key : str
        Node label that names the accelerator type. GKE's by default; set it
        to your cluster's label elsewhere.
    node_selector : dict[str, str] | None
        Node selector labels for pod scheduling.
    tolerations : list[dict[str, Any]] | None
        Pod tolerations for scheduling on tainted nodes.
    ttl_seconds_after_finished : int
        Time before completed Jobs are cleaned up by the TTL controller.
    unschedulable_timeout : float
        Seconds a pod may stay unable to start before the job is treated as
        failed: unschedulable (quota, capacity, node affinity, e.g. GPU
        stockouts), or with a container that cannot pull its image (a bad
        tag, missing pull credentials). Either would otherwise leave the pod
        Pending indefinitely. A registry error that clears within this time
        is tolerated.
    ephemeral_storage : str
        Default ephemeral-storage request/limit for worker pods (e.g.
        ``"10Gi"``). Must be large enough to hold the code bundle plus
        any ``file``/``folder`` inputs hydrated from the remote artifact
        store into ``/tmp``, or into the ``emptyDir`` the containers of a
        container-env task share. GKE Autopilot's general-purpose compute
        class caps this at 10Gi per pod; workloads that need more must
        either raise it via a larger compute class (``balanced``,
        ``performance``) or mount a separate PVC for the scratch dir.
    backoff_limit : int
        Number of K8s-level pod retries the Job controller may perform
        before Ginkgo treats the attempt as failed. Defaults to 0: the Job
        controller cannot tell a failing task from a preempted pod, so any
        retries would re-run deterministic task errors too. Raise it on
        clusters that preempt pods (GKE Autopilot, spot nodes), or use
        task-level ``retries``.
    env : dict[str, str] | None
        Environment variables for every worker container, e.g. the object
        store's auth and region (``GINKGO_REMOTE_OCI_AUTH``).
    pod_annotations : dict[str, str] | None
        Annotations for every worker pod, e.g.
        ``karpenter.sh/do-not-disrupt = "true"``.
    pod_labels : dict[str, str] | None
        Labels for every worker pod and Job.
    image_pull_secrets : list[str] | None
        Names of the Secrets used to pull a private worker image, or a
        container-env task's own image.
    """

    namespace: str = "default"
    image: str = ""
    service_account: str | None = None
    pull_policy: str = "IfNotPresent"
    gpu_type: str | None = None
    gpu_node_selector_key: str = "cloud.google.com/gke-accelerator"
    node_selector: dict[str, str] | None = None
    tolerations: list[dict[str, Any]] | None = None
    ttl_seconds_after_finished: int = 3600
    unschedulable_timeout: float = 300.0
    ephemeral_storage: str = "10Gi"
    backoff_limit: int = 0
    fuse_image: str | None = None
    fuse_annotations: dict[str, str] | None = None
    fuse_privileged: bool = False
    env: dict[str, str] | None = None
    pod_annotations: dict[str, str] | None = None
    pod_labels: dict[str, str] | None = None
    image_pull_secrets: list[str] | None = None
    _batch_api: Any = field(default=None, init=False, repr=False)
    _core_api: Any = field(default=None, init=False, repr=False)

    def _get_apis(self) -> tuple[Any, Any]:
        """Return (BatchV1Api, CoreV1Api), lazily initialized.

        The returned objects are ``_RefreshingApi`` wrappers that transparently
        reload the kube-config and retry on 401 Unauthorized. GKE bearer
        tokens live for 1 hour, so long-running evaluator sessions would
        otherwise start failing API calls mid-run; this keeps the client
        alive indefinitely by re-invoking the exec plugin on auth loss.
        """
        if self._batch_api is not None:
            return self._batch_api, self._core_api

        # Suppress noisy gRPC fork-safety warnings on macOS.
        os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

        # Imported on first use: the plugin module loads on every CLI run.
        from kubernetes import client, config as k8s_config

        def build() -> tuple[Any, Any]:
            k8s_config.load_config()
            return client.BatchV1Api(), client.CoreV1Api()

        batch, core = build()
        self._batch_api = _RefreshingApi(inner=batch, factory=lambda: build()[0])
        self._core_api = _RefreshingApi(inner=core, factory=lambda: build()[1])
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
        fuse_required = payload_requires_fuse(attempt)

        # Build resource requests and limits.
        resource_requests: dict[str, str] = {"cpu": str(threads)}
        resource_limits: dict[str, str] = {"cpu": str(threads)}
        if memory_gb > 0:
            resource_requests["memory"] = f"{memory_gb}Gi"
            resource_limits["memory"] = f"{memory_gb}Gi"
        if self.ephemeral_storage:
            resource_requests["ephemeral-storage"] = self.ephemeral_storage
            resource_limits["ephemeral-storage"] = self.ephemeral_storage

        gpu = resources.get("gpu", 0)
        gpu_type = resources.get("gpu_type") or self.gpu_type
        if gpu > 0:
            resource_limits["nvidia.com/gpu"] = str(gpu)

        # Several steps run one after another as init containers, then the
        # last as the pod's one container, all mounting the scratch they
        # share. Each holds the task's resources only while it runs.
        steps = job_steps(attempt)
        shared = len(steps) > 1
        worker_env = [
            *(
                k8s_client.V1EnvVar(name=name, value=str(value))
                for name, value in (self.env or {}).items()
            ),
            k8s_client.V1EnvVar(name="GINKGO_WORKER_PAYLOAD", value=encode_payload(attempt)),
        ]
        worker_image = self.fuse_image if (fuse_required and self.fuse_image) else self.image
        privileged = (
            k8s_client.V1SecurityContext(privileged=True)
            if fuse_required and self.fuse_privileged
            else None
        )
        volume_mounts = (
            [k8s_client.V1VolumeMount(name="ginkgo-scratch", mount_path=SHARED_SCRATCH)]
            if shared
            else None
        )
        containers = []
        for step in steps:
            is_worker = step.image is None
            env = [
                *(worker_env if is_worker else ()),
                *(k8s_client.V1EnvVar(name=name, value=value) for name, value in step.env.items()),
            ]
            containers.append(
                k8s_client.V1Container(
                    name=step.name,
                    image=worker_image if is_worker else step.image,
                    image_pull_policy=self.pull_policy,
                    command=list(step.command),
                    env=env or None,
                    resources=k8s_client.V1ResourceRequirements(
                        requests=resource_requests,
                        limits=resource_limits,
                    ),
                    security_context=privileged if is_worker else None,
                    volume_mounts=volume_mounts,
                )
            )

        job_name = generate_job_name(attempt)
        pod_spec_kwargs: dict[str, Any] = {
            "init_containers": containers[:-1] or None,
            "containers": containers[-1:],
            "restart_policy": "Never",
        }
        if shared:
            pod_spec_kwargs["volumes"] = [
                k8s_client.V1Volume(
                    name="ginkgo-scratch", empty_dir=k8s_client.V1EmptyDirVolumeSource()
                )
            ]
        if self.service_account is not None:
            pod_spec_kwargs["service_account_name"] = self.service_account
        if self.image_pull_secrets:
            pod_spec_kwargs["image_pull_secrets"] = [
                k8s_client.V1LocalObjectReference(name=name) for name in self.image_pull_secrets
            ]

        # Merge node selectors: start with user-configured selectors, then
        # add the accelerator selector for GPU tasks.
        merged_node_selector: dict[str, str] = dict(self.node_selector or {})
        if gpu > 0 and gpu_type is not None:
            merged_node_selector[self.gpu_node_selector_key] = gpu_type
        if merged_node_selector:
            pod_spec_kwargs["node_selector"] = merged_node_selector
        if self.tolerations is not None:
            pod_spec_kwargs["tolerations"] = [
                k8s_client.V1Toleration(**t) for t in self.tolerations
            ]

        pod_annotations: dict[str, str] = dict(self.pod_annotations or {})
        if fuse_required:
            # An empty table means no annotation, for clusters that are not GKE.
            fuse_annotations = (
                {"gke-gcsfuse/volumes": "true"}
                if self.fuse_annotations is None
                else self.fuse_annotations
            )
            pod_annotations.update(fuse_annotations)

        labels = {
            **(self.pod_labels or {}),
            "app": "ginkgo",
            "ginkgo/task-id": str(attempt.get("task_id", "")),
            "ginkgo/run-id": str(attempt.get("run_id", "")),
        }
        # A caller that launched this run (Forge, say) can delete its Jobs by
        # selector, even if the driver is killed before it can.
        parent_run_id = os.environ.get("GINKGO_PARENT_RUN_ID")
        if parent_run_id:
            labels["ginkgo/parent-run-id"] = parent_run_id

        pod_template = k8s_client.V1PodTemplateSpec(
            metadata=k8s_client.V1ObjectMeta(annotations=pod_annotations or None, labels=labels),
            spec=k8s_client.V1PodSpec(**pod_spec_kwargs),
        )

        job = k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                namespace=self.namespace,
                labels=labels,
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=self.backoff_limit,
                ttl_seconds_after_finished=self.ttl_seconds_after_finished,
                template=pod_template,
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
    _stuck_since: float | None = field(default=None, init=False, repr=False)
    _stuck_reason: str | None = field(default=None, init=False, repr=False)
    _job_observed: bool = field(default=False, init=False, repr=False)
    _terminal_state: Any = field(default=None, init=False, repr=False)
    _terminal_logs: str | None = field(default=None, init=False, repr=False)
    _last_pod_check: float = field(default=0.0, init=False, repr=False)
    _last_pod_list: Any = field(default=None, init=False, repr=False)

    @property
    def job_id(self) -> str:
        """Stable identifier for the remote job."""
        return f"{self.namespace}/{self.job_name}"

    def state(self) -> RemoteJobState:
        """Poll the current Kubernetes Job status.

        Tolerates 404s before the job has ever been observed — the K8s
        API server is eventually consistent, so a freshly submitted Job
        can briefly read as missing. Once a terminal state has been
        cached, any later 404 is assumed to mean
        ``ttlSecondsAfterFinished`` has garbage-collected the Job and
        the cached state is returned instead of re-raising.
        """
        if self._terminal_state is not None:
            return self._terminal_state
        try:
            job = self._batch_api.read_namespaced_job(name=self.job_name, namespace=self.namespace)
        except Exception as exc:
            # Duck-type ApiException so the kubernetes client stays a
            # lazy import: any exception carrying ``status == 404``
            # before the job has been observed is treated as still
            # pending (eventual consistency on the K8s API server).
            if getattr(exc, "status", None) == 404 and not self._job_observed:
                return RemoteJobState.PENDING
            raise
        # Reach here with a fresh status; compute terminal transitions
        # below and cache them.
        self._job_observed = True

        status = job.status
        if status.succeeded and status.succeeded > 0:
            return self._mark_terminal(RemoteJobState.SUCCEEDED)

        # Check Job-level conditions instead of the pod-failure counter:
        # ``status.failed`` increments on every failed pod even while the
        # Job controller is still retrying within ``backoff_limit``. Only
        # the ``Failed`` or ``FailureTarget`` condition means the Job
        # itself is finished.
        for condition in status.conditions or []:
            if condition.type in {"Failed", "FailureTarget"} and condition.status == "True":
                return self._mark_terminal(RemoteJobState.FAILED)

        # A pod that cannot start is failed explicitly so ginkgo does not
        # wait forever, and deleted, since it would hold its quota or node
        # forever. ``status.active`` counts Pending pods too, so it is checked
        # whatever the Job reports.
        if self._check_stuck_timeout():
            state = self._mark_terminal(RemoteJobState.FAILED)
            with suppress(Exception):
                self.cancel()
            return state
        if status.active and status.active > 0:
            return RemoteJobState.RUNNING
        return RemoteJobState.PENDING

    def _mark_terminal(self, state: RemoteJobState) -> RemoteJobState:
        """Cache a terminal state + snapshot the pod logs before TTL GC."""
        if self._terminal_state is None:
            self._terminal_state = state
            # Capture logs now while the pod still exists; the Job's
            # ``ttlSecondsAfterFinished`` controller may delete the pod
            # before the caller next touches ``logs_tail``.
            try:
                self._terminal_logs = self.logs_tail(lines=1000)
            except Exception:
                self._terminal_logs = ""
        return state

    _POD_CHECK_INTERVAL = 15.0

    def _check_stuck_timeout(self) -> bool:
        """Return True once the pod has been unable to start for too long."""
        now = time.monotonic()
        if now - self._last_pod_check >= self._POD_CHECK_INTERVAL or self._last_pod_list is None:
            self._last_pod_list = self._core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"job-name={self.job_name}",
            )
            self._last_pod_check = now
        pods = self._last_pod_list
        reason = _stuck_reason(_latest_pod(pods.items)) if pods.items else None
        if reason is None:
            self._stuck_since = None
            self._stuck_reason = None
            return False

        if self._stuck_since is None:
            self._stuck_since = now
        self._stuck_reason = reason
        return now - self._stuck_since >= self.unschedulable_timeout

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

        # Prefer the logs snapshot captured at the moment state()
        # observed the terminal transition: the pod may already be GC'd.
        if self._terminal_logs is not None:
            logs = self._terminal_logs
        else:
            logs = self.logs_tail(lines=1000)
        if current == RemoteJobState.FAILED and self._stuck_reason is not None:
            message = f"Kubernetes pod could not start: {self._stuck_reason}"
            payload = {
                "ok": False,
                "error": {
                    "type": "RuntimeError",
                    "module": "builtins",
                    "message": message,
                    "args": [message],
                },
            }
        else:
            payload = parse_worker_output(logs, source_label="pod logs")

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
        """Return the last *lines* lines from the most recent pod's log.

        Uses ``_preload_content=False`` because the kubernetes Python
        client's response deserializer mangles JSON log output into
        Python-repr format when preloading (single-quoted keys,
        capitalised ``False``), which breaks worker output parsing.

        With ``backoff_limit > 0`` a single Job may have produced
        several pods (e.g. the first was preempted and retried). The
        worker output we care about lives on the most recent pod, so
        we sort by creation timestamp and read that one.

        A pod with init containers gets each container's log in the order
        they ran, so the worker's result line is still last.
        """
        pods = self._core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"job-name={self.job_name}",
        )
        if not pods.items:
            return ""

        latest_pod = _latest_pod(pods.items)
        spec = latest_pod.spec
        containers = [*(spec.init_containers or []), *spec.containers]
        logs = [
            self._container_log(pod=latest_pod.metadata.name, container=c.name, lines=lines)
            for c in containers
        ]
        return "".join(log if log.endswith("\n") else f"{log}\n" for log in logs if log)

    def _container_log(self, *, pod: str, container: str, lines: int) -> str:
        """Return the last *lines* lines of one container's log, or ``""`` if it has none."""
        try:
            resp = self._core_api.read_namespaced_pod_log(
                name=pod,
                namespace=self.namespace,
                container=container,
                tail_lines=lines,
                _preload_content=False,
            )
            return resp.read().decode("utf-8", errors="replace")
        except Exception:
            return ""


def _latest_pod(pods: list[Any]) -> Any:
    """Return the most recently created of a Job's *pods*, the one still running it."""
    return max(pods, key=lambda p: p.metadata.creation_timestamp or 0)


_IMAGE_PULL_FAILURES = frozenset(
    {"ErrImagePull", "ImagePullBackOff", "ErrImageNeverPull", "InvalidImageName"}
)
"""The reasons a container waits because it cannot pull its image."""


def _stuck_reason(pod: Any) -> str | None:
    """Return why *pod* cannot start, or ``None`` if nothing stops it.

    It cannot start while it is unschedulable, or while any of its containers,
    init or main, waits on an image it cannot pull.
    """
    for condition in pod.status.conditions or []:
        if condition.type == "PodScheduled" and condition.status != "True":
            return f"unschedulable: {condition.reason}: {condition.message}"
    statuses = [
        *(pod.status.init_container_statuses or []),
        *(pod.status.container_statuses or []),
    ]
    for status in statuses:
        waiting = status.state.waiting if status.state is not None else None
        if waiting is not None and waiting.reason in _IMAGE_PULL_FAILURES:
            return (
                f"container {status.name!r} cannot pull image {status.image!r}: "
                f"{waiting.reason}: {waiting.message}"
            )
    return None


def build_kubernetes_executor(settings: Mapping[str, Any]) -> KubernetesExecutor:
    """Construct a ``KubernetesExecutor`` from an executor table's settings.

    Raises
    ------
    ValueError
        If the table holds a setting the executor does not have, so a typo
        fails here rather than being silently ignored.
    """
    known = {item.name for item in fields(KubernetesExecutor) if item.init}
    unknown = sorted(set(settings) - known)
    if unknown:
        raise ValueError(
            f"Unknown k8s executor setting(s): {', '.join(unknown)}. "
            f"Known settings: {', '.join(sorted(known))}"
        )
    return KubernetesExecutor(**settings)
