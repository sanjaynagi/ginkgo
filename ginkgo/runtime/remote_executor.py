"""Async remote executor protocol for submitting tasks to remote backends.

Defines the ``RemoteExecutor`` protocol that remote backends (Kubernetes,
cloud batch services) implement to receive task submissions from the
evaluator.  The evaluator dispatches to remote backends through this
protocol instead of the local process-pool executor.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class RemoteJobState(Enum):
    """Lifecycle states for a remotely submitted job."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Return whether the state is final."""
        return self in {
            RemoteJobState.SUCCEEDED,
            RemoteJobState.FAILED,
            RemoteJobState.CANCELLED,
        }


@dataclass(frozen=True, kw_only=True)
class RemoteJobResult:
    """Result returned when a remote job reaches a terminal state.

    Parameters
    ----------
    state : RemoteJobState
        Final job state.
    payload : dict[str, Any]
        Worker result dictionary, same shape as ``run_task`` return value.
    exit_code : int | None
        Process exit code if available.
    logs : str | None
        Tail of job logs.
    """

    state: RemoteJobState
    payload: dict[str, Any]
    exit_code: int | None = None
    logs: str | None = None


@runtime_checkable
class RemoteJobHandle(Protocol):
    """Handle to a running remote job.

    Provides polling, blocking wait, cancellation, and log access.
    """

    @property
    def job_id(self) -> str:
        """Stable identifier for the remote job."""
        ...

    def state(self) -> RemoteJobState:
        """Poll the current job state."""
        ...

    def result(self) -> RemoteJobResult:
        """Block until the job reaches a terminal state and return the result."""
        ...

    def cancel(self) -> None:
        """Request cancellation of the remote job."""
        ...

    def logs_tail(self, *, lines: int = 100) -> str:
        """Return the last *lines* lines of job output."""
        ...


@runtime_checkable
class RemoteExecutor(Protocol):
    """Protocol for submitting tasks to remote execution backends.

    Implementations receive a worker payload dictionary (same shape as
    the local ``run_task`` input) augmented with a ``resources`` key
    containing scheduler resource declarations.
    """

    def submit(self, *, attempt: dict[str, Any]) -> RemoteJobHandle:
        """Submit a task attempt and return a handle for polling.

        Parameters
        ----------
        attempt : dict[str, Any]
            Worker payload dictionary with an additional ``resources``
            key mapping to ``{"threads": int, "memory_gb": int}``.

        Returns
        -------
        RemoteJobHandle
        """
        ...
