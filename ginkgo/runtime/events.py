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
    task_id: str = ""
    task_name: str = ""
    kind: str = "python"
    env: str | None = None
    dependency_ids: list[str] = field(default_factory=list)


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
    resources: dict[str, int] = field(default_factory=dict)


@dataclass(kw_only=True, frozen=True)
class TaskStarted(TaskEvent):
    """Task dispatch event."""

    event: str = "task_started"
    kind: str = "python"
    env: str | None = None
    resources: dict[str, int] = field(default_factory=dict)
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


@dataclass(kw_only=True, frozen=True)
class TaskCompleted(TaskEvent):
    """Task completion event."""

    event: str = "task_completed"
    status: Literal["success", "cached"] = "success"
    cache_key: str | None = None
    outputs: list[dict[str, Any]] = field(default_factory=list)
    remote_job_id: str | None = None


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
class RunStarted(RunEvent):
    """Run start event."""

    event: str = "run_started"
    workflow: str = ""


@dataclass(kw_only=True, frozen=True)
class RunValidated(RunEvent):
    """Run validation event."""

    event: str = "run_validated"
    task_count: int = 0
    edge_count: int = 0
    env_count: int = 0


@dataclass(kw_only=True, frozen=True)
class RunCompleted(RunEvent):
    """Run completion event."""

    event: str = "run_completed"
    status: Literal["success", "failed"] = "success"
    task_counts: dict[str, int] = field(default_factory=dict)
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
