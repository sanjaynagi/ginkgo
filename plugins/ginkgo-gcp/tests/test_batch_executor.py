"""The Batch job a GCP Batch executor submits, with the Batch client mocked."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ginkgo_gcp.batch import GCPBatchExecutor

batch_v1 = pytest.importorskip("google.cloud.batch_v1")


def _runnables(attempt: dict) -> list:
    executor = GCPBatchExecutor(project="p", region="europe-west2", image="reg/worker:1")
    executor._client = MagicMock()
    executor.submit(attempt=attempt)
    job = executor._client.create_job.call_args.kwargs["request"].job
    return list(job.task_groups[0].task_spec.runnables)


def _attempt(**extra: object) -> dict:
    return {
        "run_id": "run-1",
        "task_id": "task_001",
        "attempt": 1,
        "args": {},
        "resources": {"threads": 1, "memory_gb": 1},
        **extra,
    }


def test_a_task_runs_in_one_worker_runnable() -> None:
    [runnable] = _runnables(_attempt())

    assert runnable.container.image_uri == "reg/worker:1"
    assert [runnable.container.entrypoint, *runnable.container.commands] == [
        "python",
        "-m",
        "ginkgo.remote.worker",
    ]
    assert list(runnable.container.volumes) == []
    assert "GINKGO_WORKER_PAYLOAD" in runnable.environment.variables
    assert not runnable.ignore_exit_status


def test_a_container_task_runs_its_image_between_two_workers() -> None:
    stage_in, task, stage_out = _runnables(_attempt(image="busybox:1.36"))

    assert [r.container.image_uri for r in (stage_in, task, stage_out)] == [
        "reg/worker:1",
        "busybox:1.36",
        "reg/worker:1",
    ]
    # The task's image keeps its ENTRYPOINT, as docker run keeps it locally,
    # and Batch passes it the commands. The workers replace theirs.
    assert task.container.entrypoint == ""
    assert list(task.container.commands) == ["sh", "/ginkgo-scratch/ginkgo-command/command.sh"]
    for worker in (stage_in, stage_out):
        assert [worker.container.entrypoint, *worker.container.commands] == [
            "python",
            "-m",
            "ginkgo.remote.worker",
        ]
    # Stage-out runs, and reports the result, whatever the task's image exits with.
    assert [r.ignore_exit_status for r in (stage_in, task, stage_out)] == [False, True, False]
    assert dict(task.environment.variables) == {}
    for worker, phase in ((stage_in, "stage-in"), (stage_out, "stage-out")):
        env = dict(worker.environment.variables)
        assert env["GINKGO_WORKER_PHASE"] == phase
        assert env["GINKGO_SCRATCH_ROOT"] == "/ginkgo-scratch"
        assert "GINKGO_WORKER_PAYLOAD" in env
    # The same host directory, at the same path in every container.
    volumes = {tuple(r.container.volumes) for r in (stage_in, task, stage_out)}
    [(volume,)] = volumes
    assert volume.endswith(":/ginkgo-scratch")
