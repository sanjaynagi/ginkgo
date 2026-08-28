"""Golden payloads for every runtime event.

The event stream is a wire format: agents parse it, and from the provenance
ledger onwards it is also the storage format. A field renamed or dropped by
accident is a silent break in both, so every event type has a checked-in
payload and a test that its shape has not moved. Regenerate a fixture only
when the change is deliberate.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pytest

from ginkgo.runtime import events as events_module
from ginkgo.runtime.events import (
    AssetMaterialized,
    EnvPrepareCompleted,
    EnvPrepareFailed,
    EnvPrepareStarted,
    GinkgoEvent,
    GraphExpanded,
    GraphNodeRegistered,
    PhaseTimed,
    RunCompleted,
    RunResourcesSampled,
    RunStarted,
    RunValidated,
    TaskAnnotated,
    TaskCacheHit,
    TaskCacheMiss,
    TaskCompleted,
    TaskFailed,
    TaskLog,
    TaskNotice,
    TaskPlanned,
    TaskReady,
    TaskRetrying,
    TaskRunning,
    TaskStaging,
    TaskStarted,
    TaskStreamingMounted,
    TaskStreamingUnmounted,
)

FIXTURES = Path(__file__).parent / "fixtures" / "events"

TS = "2026-08-28T09:00:00+00:00"
RUN_ID = "run_20260828_090000"
TASK = {"run_id": RUN_ID, "ts": TS, "task_id": "task_0007", "task_name": "analysis.fit"}
RUN = {"run_id": RUN_ID, "ts": TS}


def _events() -> list[GinkgoEvent]:
    """One fully populated instance of every event type."""
    return [
        GraphNodeRegistered(
            **RUN,
            task_id="task_0007",
            node_id=7,
            task_name="analysis.fit",
            kind="python",
            execution_mode="python",
            env="analysis",
            retries=2,
            dependency_ids=["task_0001"],
            stdout_log="logs/task_0007_analysis_fit.stdout.log",
            stderr_log="logs/task_0007_analysis_fit.stderr.log",
        ),
        GraphExpanded(**RUN, parent_task_id="task_0007", new_node_ids=["task_0008"]),
        TaskPlanned(
            **TASK,
            display_label="fit[a]",
            inputs={"rows": 10},
            input_hashes=[{"param": "table", "digest": "b3:1234"}],
            cache_key="cache_abcdef",
            source_hash="src_abcdef",
            version=3,
            env_hash="env_abcdef",
            extra_source_hash="extra_abcdef",
            dependency_ids=["task_0001"],
            dynamic_dependency_ids=["task_0008"],
        ),
        TaskCacheHit(**TASK, cache_key="cache_abcdef"),
        TaskCacheMiss(**TASK, cache_key="cache_abcdef"),
        TaskReady(**TASK, resources={"threads": 2}),
        TaskStarted(
            **TASK,
            attempt=1,
            kind="python",
            env="analysis",
            resources={"threads": 2, "max_attempts": 3},
            remote_job_id="job-1",
            execution_backend="local",
        ),
        TaskRunning(**TASK, attempt=1, remote_job_id="job-1"),
        TaskStaging(**TASK, remote_input_count=2, access_method="fuse"),
        TaskStreamingMounted(**TASK, scheme="s3", bucket="inputs", mount_seconds=0.5),
        TaskStreamingUnmounted(
            **TASK,
            scheme="s3",
            bucket="inputs",
            unmount_seconds=0.25,
            bytes_read=1024,
            range_requests=4,
        ),
        TaskLog(**TASK, stream="stdout", chunk="hello\n", sequence=1),
        TaskNotice(**TASK, message="notebook rendered"),
        TaskRetrying(
            **TASK,
            attempt=1,
            retries_remaining=2,
            failure={"kind": "exception", "message": "boom"},
            delay_seconds=1.5,
        ),
        TaskCompleted(
            **TASK,
            attempt=1,
            status="success",
            cache_key="cache_abcdef",
            outputs=[{"name": "table", "type": "table"}],
            assets=[{"asset_key": "table:rows", "version_id": "v1"}],
            output_summary={"kind": "table", "rows": 10},
            resource_usage={"max_rss_bytes": 1048576},
            remote_job_id="job-1",
        ),
        AssetMaterialized(
            **TASK,
            asset_key="table:rows",
            version_id="v1",
            kind="table",
            sub_kind="parquet",
            artifact_id="artifact_abcdef",
            content_hash="b3:abcdef",
            cache_key="cache_abcdef",
            metadata={"columns": 3},
            metrics={"rows": 10},
            checks=[{"name": "not_empty", "passed": True}],
            parents=[{"asset_key": "table:raw", "version_id": "v0"}],
        ),
        TaskAnnotated(**TASK, fields={"env_lock": "envs/analysis.lock"}),
        TaskFailed(
            **TASK,
            attempt=2,
            exit_code=1,
            failure={"kind": "exception", "message": "boom"},
            remote_job_id="job-1",
        ),
        EnvPrepareStarted(**TASK, env="analysis"),
        EnvPrepareCompleted(**TASK, env="analysis"),
        EnvPrepareFailed(**TASK, env="analysis", error="pixi install failed"),
        RunStarted(
            **RUN,
            workflow="workflow/flow.py",
            jobs=4,
            cores=8,
            memory=16,
            params={"seed": 1},
            param_sources={"seed": "cli"},
            ginkgo_version="0.1.0",
            parent_run_id="run_20260828_085900",
            parent_task_id="task_0002",
        ),
        RunValidated(**RUN, task_count=4, edge_count=3, env_count=1),
        PhaseTimed(**RUN, task_id="task_0007", phase="cache_lookup", seconds=0.125),
        RunResourcesSampled(**RUN, resources={"cpu_percent": 42.0, "rss_bytes": 1048576}),
        RunCompleted(
            **RUN,
            status="success",
            task_counts={"succeeded": 4},
            finished_at=TS,
            resources={"peak_rss_bytes": 1048576},
            error=None,
        ),
    ]


EVENTS = {event.event: event for event in _events()}


def _event_classes() -> list[type[GinkgoEvent]]:
    """Every concrete event type declared in ``runtime/events.py``."""
    abstract = {GinkgoEvent.__name__, "RunEvent", "TaskEvent"}
    return [
        member
        for _, member in inspect.getmembers(events_module, inspect.isclass)
        if issubclass(member, GinkgoEvent) and member.__name__ not in abstract
    ]


def _payload(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def test_every_event_type_has_a_golden_payload() -> None:
    covered = {type(event) for event in EVENTS.values()}

    assert set(_event_classes()) == covered


@pytest.mark.parametrize("name", sorted(EVENTS))
def test_payload_matches_the_golden_fixture(name: str) -> None:
    assert EVENTS[name].to_payload() == _payload(name)


@pytest.mark.parametrize("name", sorted(EVENTS))
def test_payload_is_json_serialisable(name: str) -> None:
    """The JSONL renderer's path: every field has to survive ``json.dumps``."""
    payload = EVENTS[name].to_payload()

    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
