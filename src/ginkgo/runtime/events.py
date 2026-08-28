"""Runtime event protocol and in-process event bus."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal


def event_timestamp() -> str:
    """Return the current timestamp in ISO 8601 UTC form."""
    return datetime.now(UTC).isoformat()


@dataclass(kw_only=True, frozen=True)
class GinkgoEvent:
    """Base runtime event."""

    run_id: str
    event: str
    ts: str = field(default_factory=event_timestamp)
    v: int = 1

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-serializable event payload."""
        return asdict(self)


@dataclass(kw_only=True, frozen=True)
class RunEvent(GinkgoEvent):
    """Run-scoped event."""


@dataclass(kw_only=True, frozen=True)
class GraphNodeRegistered(RunEvent):
    """Static or dynamic task-node registration."""

    event: str = "graph_node_registered"
    v: int = 3
    task_id: str = ""
    task_name: str = ""
    kind: str = "python"
    execution_mode: str = "python"
    env: str | None = None
    retries: int = 0
    dependency_ids: list[str] = field(default_factory=list)
    stdout_log: str | None = None
    """Where the task's stdout will be written, relative to the run directory."""
    stderr_log: str | None = None
    """Where the task's stderr will be written, relative to the run directory."""


@dataclass(kw_only=True, frozen=True)
class GraphExpanded(RunEvent):
    """Dynamic graph expansion event."""

    event: str = "graph_expanded"
    parent_task_id: str = ""
    new_node_ids: list[str] = field(default_factory=list)


@dataclass(kw_only=True, frozen=True)
class TaskEvent(GinkgoEvent):
    """Task-scoped runtime event."""

    task_id: str
    task_name: str
    attempt: int = 0
    display_label: str | None = None


@dataclass(kw_only=True, frozen=True)
class TaskCacheHit(TaskEvent):
    """Cache hit event."""

    event: str = "task_cache_hit"
    cache_key: str = ""


@dataclass(kw_only=True, frozen=True)
class TaskCacheMiss(TaskEvent):
    """Cache miss event."""

    event: str = "task_cache_miss"
    cache_key: str = ""


@dataclass(kw_only=True, frozen=True)
class TaskReady(TaskEvent):
    """Task became dispatch-ready."""

    event: str = "task_ready"
    resources: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True, frozen=True)
class TaskStarted(TaskEvent):
    """Task dispatch event."""

    event: str = "task_started"
    kind: str = "python"
    env: str | None = None
    resources: dict[str, Any] = field(default_factory=dict)
    remote_job_id: str | None = None
    execution_backend: str | None = None


@dataclass(kw_only=True, frozen=True)
class TaskRunning(TaskEvent):
    """Remote task pod has started executing."""

    event: str = "task_running"
    remote_job_id: str | None = None


@dataclass(kw_only=True, frozen=True)
class TaskStaging(TaskEvent):
    """Task is staging remote inputs before dispatch."""

    event: str = "task_staging"
    status: Literal["staging"] = "staging"
    remote_input_count: int = 0
    access_method: Literal["stage", "fuse", "hybrid"] = "stage"


@dataclass(kw_only=True, frozen=True)
class TaskStreamingMounted(TaskEvent):
    """A FUSE mount was established for a remote task input."""

    event: str = "task_streaming_mounted"
    scheme: str = ""
    bucket: str = ""
    mount_seconds: float = 0.0


@dataclass(kw_only=True, frozen=True)
class TaskStreamingUnmounted(TaskEvent):
    """A FUSE mount was torn down after the task body finished."""

    event: str = "task_streaming_unmounted"
    scheme: str = ""
    bucket: str = ""
    unmount_seconds: float = 0.0
    bytes_read: int = 0
    range_requests: int = 0


@dataclass(kw_only=True, frozen=True)
class TaskLog(TaskEvent):
    """Task log chunk event."""

    event: str = "task_log"
    stream: Literal["stdout", "stderr"] = "stdout"
    chunk: str = ""
    sequence: int = 0


@dataclass(kw_only=True, frozen=True)
class TaskNotice(TaskEvent):
    """Task-scoped runtime notice."""

    event: str = "task_notice"
    message: str = ""


@dataclass(kw_only=True, frozen=True)
class TaskRetrying(TaskEvent):
    """Task retry scheduling event."""

    event: str = "task_retrying"
    retries_remaining: int = 0
    failure: dict[str, Any] = field(default_factory=dict)
    delay_seconds: float = 0.0


@dataclass(kw_only=True, frozen=True)
class TaskCompleted(TaskEvent):
    """Task completion event."""

    event: str = "task_completed"
    v: int = 2
    status: Literal["success", "cached"] = "success"
    cache_key: str | None = None
    outputs: list[dict[str, Any]] = field(default_factory=list)
    assets: list[dict[str, Any]] = field(default_factory=list)
    output_summary: dict[str, Any] = field(default_factory=dict)
    resource_usage: dict[str, Any] = field(default_factory=dict)
    remote_job_id: str | None = None


@dataclass(kw_only=True, frozen=True)
class TaskPlanned(TaskEvent):
    """Arguments resolved and cache key built, before any cache probe.

    Everything the cache key was computed from is on this event, so the ledger
    can answer "why did this re-run" without the cache index being intact.
    """

    event: str = "task_planned"
    inputs: dict[str, Any] = field(default_factory=dict)
    input_hashes: list[dict[str, Any]] = field(default_factory=list)
    cache_key: str | None = None
    source_hash: str | None = None
    version: int | None = None
    env_hash: str | None = None
    extra_source_hash: str | None = None
    dependency_ids: list[str] = field(default_factory=list)
    dynamic_dependency_ids: list[str] = field(default_factory=list)


@dataclass(kw_only=True, frozen=True)
class AssetMaterialized(TaskEvent):
    """One asset version was written to the catalog by this task."""

    event: str = "asset_materialized"
    asset_key: str = ""
    version_id: str = ""
    kind: str = ""
    sub_kind: str | None = None
    artifact_id: str = ""
    content_hash: str = ""
    cache_key: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    checks: list[dict[str, Any]] = field(default_factory=list)
    parents: list[dict[str, Any]] = field(default_factory=list)


@dataclass(kw_only=True, frozen=True)
class TaskAnnotated(TaskEvent):
    """Facts about a task that have no lifecycle of their own.

    An environment lock file copied, a container image digest, remote access
    statistics, notebook artefact paths, a sub-run id: each is a field the
    projector merges into the task's ``extra``, and none of them deserves an
    event type.
    """

    event: str = "task_annotated"
    fields: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True, frozen=True)
class TaskFailed(TaskEvent):
    """Task failure event."""

    event: str = "task_failed"
    exit_code: int | None = None
    failure: dict[str, Any] = field(default_factory=dict)
    remote_job_id: str | None = None


@dataclass(kw_only=True, frozen=True)
class EnvPrepareStarted(TaskEvent):
    """Environment preparation started."""

    event: str = "env_prepare_started"
    env: str | None = None


@dataclass(kw_only=True, frozen=True)
class EnvPrepareCompleted(TaskEvent):
    """Environment preparation completed."""

    event: str = "env_prepare_completed"
    env: str | None = None


@dataclass(kw_only=True, frozen=True)
class EnvPrepareFailed(TaskEvent):
    """Environment preparation failed, aborting the task before it started."""

    event: str = "env_prepare_failed"
    env: str | None = None
    error: str | None = None


@dataclass(kw_only=True, frozen=True)
class RunStarted(RunEvent):
    """Run start event."""

    event: str = "run_started"
    v: int = 2
    workflow: str = ""
    jobs: int | None = None
    cores: int | None = None
    memory: int | None = None
    params: dict[str, Any] = field(default_factory=dict)
    param_sources: dict[str, str] = field(default_factory=dict)
    ginkgo_version: str | None = None
    parent_run_id: str | None = None
    parent_task_id: str | None = None


@dataclass(kw_only=True, frozen=True)
class RunValidated(RunEvent):
    """Run validation event."""

    event: str = "run_validated"
    task_count: int = 0
    edge_count: int = 0
    env_count: int = 0


@dataclass(kw_only=True, frozen=True)
class PhaseTimed(RunEvent):
    """How long one named phase took.

    ``task_id`` is what makes it a task's phase rather than the run's; there is
    no separate scope field, because two fields that must agree drift.
    """

    event: str = "phase_timed"
    task_id: str | None = None
    phase: str = ""
    seconds: float = 0.0


@dataclass(kw_only=True, frozen=True)
class RunResourcesSampled(RunEvent):
    """A snapshot of the run's CPU and memory usage; the latest one wins."""

    event: str = "run_resources_sampled"
    resources: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True, frozen=True)
class RunCompleted(RunEvent):
    """Run completion event."""

    event: str = "run_completed"
    v: int = 2
    status: Literal["success", "failed"] = "success"
    task_counts: dict[str, int] = field(default_factory=dict)
    finished_at: str | None = None
    resources: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class EventBus:
    """Simple in-process event dispatcher."""

    _handlers: list[Any] = field(default_factory=list)

    def subscribe(self, handler: Any) -> None:
        """Register a synchronous event handler."""
        self._handlers.append(handler)

    def emit(self, event: GinkgoEvent) -> None:
        """Deliver one event to all subscribers."""
        for handler in tuple(self._handlers):
            handler(event)


def task_id_for_node(node_id: int) -> str:
    """Return the stable task identifier for a scheduler node."""
    return f"task_{node_id:04d}"
