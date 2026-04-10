"""GCP Batch executor for serverless remote task execution.

Submits individual Ginkgo task attempts as GCP Batch Jobs.  No cluster
required — each job runs as a standalone container on Google-managed
infrastructure.
"""

from __future__ import annotations

import base64
import json
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


def _generate_job_id(attempt: dict[str, Any]) -> str:
    """Generate a unique GCP Batch job ID from the attempt payload.

    GCP Batch job IDs must be lowercase, start with a letter, and contain
    only letters, numbers, and hyphens.  Max 63 characters.
    """
    run_id = attempt.get("run_id", "unknown")
    task_id = attempt.get("task_id", "unknown")
    attempt_num = attempt.get("attempt", 0)
    name = f"ginkgo-{run_id}-{task_id}-{attempt_num}"
    name = name.lower().replace("_", "-")
    if len(name) > 63:
        name = name[:63]
    return name.rstrip("-")


def _parse_worker_output(logs: str) -> dict[str, Any]:
    """Parse the worker result from container log output.

    The remote worker prints a single JSON line to stdout as its last
    output.  Search backwards from the end to find it.
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
            "message": "No worker output found in job logs",
            "args": ["No worker output found in job logs"],
        },
    }


@dataclass(kw_only=True)
class GCPBatchExecutor:
    """Remote executor that submits tasks as GCP Batch jobs.

    Parameters
    ----------
    project : str
        GCP project ID.
    region : str
        GCP region for job submission (e.g. ``"europe-west2"``).
    image : str
        Container image URI for the worker.
    service_account : str | None
        Service account email for the job. Uses the default compute
        service account when ``None``.
    gpu_type : str | None
        GPU accelerator type (e.g. ``"nvidia-l4"``, ``"nvidia-tesla-t4"``).
        Applied only when a task requests ``gpu > 0``.
    gpu_driver_version : str
        NVIDIA driver version to install. Defaults to ``"LATEST"``.
    max_run_duration : str
        Maximum wall-clock time for a job (e.g. ``"3600s"``).
    """

    project: str
    region: str
    image: str
    service_account: str | None = None
    gpu_type: str | None = None
    gpu_driver_version: str = "LATEST"
    max_run_duration: str = "3600s"
    _client: Any = field(default=None, init=False, repr=False)

    def _get_client(self) -> Any:
        """Return the BatchServiceClient, lazily initialized."""
        if self._client is not None:
            return self._client

        try:
            from google.cloud import batch_v1
        except ImportError as exc:
            raise ImportError(
                "google-cloud-batch is required for the GCP Batch executor. "
                "Install it with: pip install google-cloud-batch"
            ) from exc

        self._client = batch_v1.BatchServiceClient()
        return self._client

    def submit(self, *, attempt: dict[str, Any]) -> GCPBatchJobHandle:
        """Create a GCP Batch job from the attempt payload.

        Parameters
        ----------
        attempt : dict[str, Any]
            Worker payload with ``resources`` key.

        Returns
        -------
        GCPBatchJobHandle
        """
        from google.cloud import batch_v1

        client = self._get_client()
        resources = attempt.get("resources", {})
        threads = resources.get("threads", 1)
        memory_gb = resources.get("memory_gb", 0)
        gpu = resources.get("gpu", 0)

        # Container configuration.
        container = batch_v1.Runnable.Container(
            image_uri=self.image,
            commands=["python", "-m", "ginkgo.remote.worker"],
            environment=batch_v1.Environment(
                variables={"GINKGO_WORKER_PAYLOAD": _encode_payload(attempt)},
            ),
        )

        runnable = batch_v1.Runnable(container=container)

        # Compute resources.
        compute = batch_v1.ComputeResource(
            cpu_milli=threads * 1000,
        )
        if memory_gb > 0:
            compute.memory_mib = memory_gb * 1024

        task_spec = batch_v1.TaskSpec(
            runnables=[runnable],
            compute_resource=compute,
            max_retry_count=0,
            max_run_duration=self.max_run_duration,
        )

        task_group = batch_v1.TaskGroup(
            task_count=1,
            task_spec=task_spec,
        )

        # Allocation policy.
        instances = []
        policy_kwargs: dict[str, Any] = {}

        if gpu > 0 and self.gpu_type is not None:
            accelerator = batch_v1.AllocationPolicy.Accelerator(
                type_=self.gpu_type,
                count=gpu,
                install_gpu_drivers=True,
                driver_version=self.gpu_driver_version,
            )
            instance_policy = batch_v1.AllocationPolicy.InstancePolicy(
                accelerators=[accelerator],
            )
            instances.append(
                batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
                    policy=instance_policy,
                )
            )

        location = batch_v1.AllocationPolicy.LocationPolicy(
            allowed_locations=[f"regions/{self.region}"],
        )
        policy_kwargs["location"] = location
        if instances:
            policy_kwargs["instances"] = instances
        if self.service_account is not None:
            policy_kwargs["service_account"] = batch_v1.AllocationPolicy.ServiceAccount(
                email=self.service_account
            )

        allocation_policy = batch_v1.AllocationPolicy(**policy_kwargs)

        job = batch_v1.Job(
            task_groups=[task_group],
            allocation_policy=allocation_policy,
            logs_policy=batch_v1.LogsPolicy(
                destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING,
            ),
        )

        job_id = _generate_job_id(attempt)
        parent = f"projects/{self.project}/locations/{self.region}"

        created = client.create_job(
            request=batch_v1.CreateJobRequest(
                parent=parent,
                job_id=job_id,
                job=job,
            )
        )

        return GCPBatchJobHandle(
            job_name=created.name,
            _client=client,
        )


@dataclass(kw_only=True)
class GCPBatchJobHandle:
    """Handle to a running GCP Batch job.

    Parameters
    ----------
    job_name : str
        Fully qualified job resource name.
    """

    job_name: str
    _client: Any = field(repr=False)

    @property
    def job_id(self) -> str:
        """Stable identifier for the remote job."""
        return self.job_name

    def state(self) -> RemoteJobState:
        """Poll the current GCP Batch job status."""
        from google.cloud import batch_v1

        job = self._client.get_job(request=batch_v1.GetJobRequest(name=self.job_name))
        status = job.status.state

        if status == batch_v1.JobStatus.State.SUCCEEDED:
            return RemoteJobState.SUCCEEDED
        if status in {
            batch_v1.JobStatus.State.FAILED,
            batch_v1.JobStatus.State.DELETION_IN_PROGRESS,
        }:
            return RemoteJobState.FAILED
        if status == batch_v1.JobStatus.State.RUNNING:
            return RemoteJobState.RUNNING
        # QUEUED, SCHEDULED, STATE_UNSPECIFIED
        return RemoteJobState.PENDING

    def result(self) -> RemoteJobResult:
        """Block until the job reaches a terminal state and return the result."""
        poll_interval = 2.0
        while True:
            current = self.state()
            if current.is_terminal:
                break
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 30.0)

        logs = self.logs_tail(lines=1000)
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
        """Request cancellation of the GCP Batch job."""
        from google.cloud import batch_v1

        self._client.delete_job(request=batch_v1.DeleteJobRequest(name=self.job_name))

    def logs_tail(self, *, lines: int = 100) -> str:
        """Return the last *lines* lines from Cloud Logging for this job."""
        try:
            from google.cloud import logging as cloud_logging
        except ImportError:
            return ""

        try:
            client = cloud_logging.Client(project=self._project_from_name())
            job_uid = self.job_name.split("/")[-1]
            log_filter = f'resource.type="batch.googleapis.com/Job" labels.job_uid="{job_uid}"'
            entries = list(
                client.list_entries(
                    filter_=log_filter,
                    order_by="timestamp desc",
                    max_results=lines,
                )
            )
            # Entries come newest-first; reverse for chronological order.
            entries.reverse()
            return "\n".join(
                entry.payload if isinstance(entry.payload, str) else str(entry.payload)
                for entry in entries
            )
        except Exception:
            return ""

    def _project_from_name(self) -> str:
        """Extract the project ID from the fully qualified job name."""
        # Format: projects/{project}/locations/{region}/jobs/{job_id}
        parts = self.job_name.split("/")
        return parts[1] if len(parts) >= 2 else ""
