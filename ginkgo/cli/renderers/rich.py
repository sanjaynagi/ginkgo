"""Rich event adapter for run rendering."""

from __future__ import annotations

import json

from ginkgo.cli.renderers.run import _CliRunRenderer
from ginkgo.runtime.events import (
    GinkgoEvent,
    TaskCacheHit,
    TaskCompleted,
    TaskFailed,
    TaskRetrying,
    TaskStaging,
    TaskStarted,
)


class RichEventRenderer:
    """Translate runtime events into the existing Rich run renderer stream."""

    def __init__(self, *, renderer: _CliRunRenderer) -> None:
        self._renderer = renderer

    def __call__(self, event: GinkgoEvent) -> None:
        payload = self._event_payload(event=event)
        if payload is None:
            return
        self._renderer.write(json.dumps(payload, sort_keys=True) + "\n")

    def _event_payload(self, *, event: GinkgoEvent) -> dict[str, object] | None:
        if isinstance(event, TaskStarted):
            payload = {
                "task": event.task_name,
                "status": "running",
                "node_id": _node_id_from_task_id(event.task_id),
                "attempt": event.attempt,
                "max_attempts": event.resources.get("max_attempts"),
            }
            if event.display_label is not None:
                payload["display_label"] = event.display_label
            return payload

        if isinstance(event, TaskStaging):
            payload = {
                "task": event.task_name,
                "status": "staging",
                "node_id": _node_id_from_task_id(event.task_id),
                "attempt": event.attempt,
                "remote_input_count": event.remote_input_count,
            }
            if event.display_label is not None:
                payload["display_label"] = event.display_label
            return payload

        if isinstance(event, TaskCacheHit):
            payload = {
                "task": event.task_name,
                "status": "cached",
                "node_id": _node_id_from_task_id(event.task_id),
                "attempt": event.attempt,
            }
            if event.display_label is not None:
                payload["display_label"] = event.display_label
            return payload

        if isinstance(event, TaskCompleted):
            payload = {
                "task": event.task_name,
                "status": "cached" if event.status == "cached" else "succeeded",
                "node_id": _node_id_from_task_id(event.task_id),
                "attempt": event.attempt,
            }
            if event.display_label is not None:
                payload["display_label"] = event.display_label
            return payload

        if isinstance(event, TaskFailed):
            payload = {
                "task": event.task_name,
                "status": "failed",
                "node_id": _node_id_from_task_id(event.task_id),
                "attempt": event.attempt,
                "exit_code": event.exit_code,
            }
            if event.display_label is not None:
                payload["display_label"] = event.display_label
            return payload

        if isinstance(event, TaskRetrying):
            payload = {
                "task": event.task_name,
                "status": "waiting",
                "node_id": _node_id_from_task_id(event.task_id),
                "attempt": event.attempt,
                "retries_remaining": event.retries_remaining,
            }
            if event.display_label is not None:
                payload["display_label"] = event.display_label
            return payload

        return None


def _node_id_from_task_id(task_id: str) -> int:
    """Return the numeric node id from a manifest-style task id."""
    return int(task_id.split("_")[-1])
