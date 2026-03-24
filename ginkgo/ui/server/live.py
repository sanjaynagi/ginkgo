"""Runtime-event-backed live state helpers for the UI server."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from .payloads import (
    list_runs,
    read_log,
    run_payload,
    task_payload,
    workspace_payload,
)
from .workspaces import WorkspaceRecord, WorkspaceRegistry


def capture_live_state(
    *, registry: WorkspaceRegistry, selected_run_id: str | None
) -> dict[str, Any]:
    """Capture the current workspace/run state for event-log tailing.

    Parameters
    ----------
    registry : WorkspaceRegistry
        Loaded workspace registry.
    selected_run_id : str | None
        Optional selected run id.

    Returns
    -------
    dict[str, Any]
        Snapshot used to derive live UI events.
    """
    workspaces = registry.list_workspaces()
    meta: dict[str, Any] = {
        "active_workspace_id": registry.active_workspace_id(),
        "active_workspace": None,
        "project_root": None,
        "runs_root": None,
        "selected_run_id": selected_run_id,
        "latest_run_id": None,
        "workspaces": [],
    }

    runs_by_workspace: dict[str, list[dict[str, Any]]] = {}
    event_offsets: dict[str, dict[str, int]] = {}
    active_workspace = registry.active_workspace()

    # Capture serialized workspace meta and per-run signatures.
    for workspace in workspaces:
        payload = workspace_payload(
            workspace,
            active_workspace_id=registry.active_workspace_id(),
            selected_run_id=selected_run_id,
        )
        meta["workspaces"].append(payload)
        if payload["is_active"]:
            meta["active_workspace"] = payload
            meta["project_root"] = payload["project_root"]
            meta["runs_root"] = payload["runs_root"]
            meta["latest_run_id"] = payload["latest_run_id"]

        runs = list_runs(workspace)
        runs_by_workspace[workspace.workspace_id] = runs
        event_offsets[workspace.workspace_id] = {}

        for run in runs:
            run_id = cast(str, run["run_id"])
            events_path = workspace.runs_root / run_id / "events.jsonl"
            event_offsets[workspace.workspace_id][run_id] = (
                events_path.stat().st_size if events_path.is_file() else 0
            )

    if active_workspace is None:
        meta["active_workspace"] = None

    return {
        "meta": meta,
        "runs_by_workspace": runs_by_workspace,
        "event_offsets": event_offsets,
    }


def diff_live_state(*, previous: dict[str, Any], current: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert two captured live states into incremental UI events.

    Parameters
    ----------
    previous : dict[str, Any]
        Prior snapshot.
    current : dict[str, Any]
        Current snapshot.

    Returns
    -------
    list[dict[str, Any]]
        Ordered event payloads for the client.
    """
    events: list[dict[str, Any]] = []
    if previous["meta"] != current["meta"]:
        events.append(live_event(event_type="meta", payload={"meta": current["meta"]}))

    previous_runs = previous["runs_by_workspace"]
    current_runs = current["runs_by_workspace"]
    all_workspace_ids = sorted(set(previous_runs) | set(current_runs))

    # Emit summary-level run list changes first.
    for workspace_id in all_workspace_ids:
        prev_runs = previous_runs.get(workspace_id, [])
        curr_runs = current_runs.get(workspace_id, [])
        if prev_runs != curr_runs:
            events.append(
                live_event(
                    event_type="runs_updated",
                    payload={"workspace_id": workspace_id, "runs": curr_runs},
                )
            )

    # Tail runtime event logs for incremental run/task updates.
    previous_offsets = previous["event_offsets"]
    current_offsets = current["event_offsets"]
    for workspace_id in all_workspace_ids:
        prev_workspace_runs = previous_offsets.get(workspace_id, {})
        curr_workspace_runs = current_offsets.get(workspace_id, {})
        all_run_ids = sorted(set(prev_workspace_runs) | set(curr_workspace_runs))
        workspace = workspace_from_meta(meta=current["meta"], workspace_id=workspace_id)
        if workspace is None:
            continue
        for run_id in all_run_ids:
            prev_offset = prev_workspace_runs.get(run_id, 0)
            curr_offset = curr_workspace_runs.get(run_id)
            if curr_offset is None or curr_offset <= prev_offset:
                continue
            run_dir = workspace.runs_root / run_id
            runtime_events = read_runtime_events(
                run_dir=run_dir,
                start_offset=prev_offset,
                end_offset=curr_offset,
            )
            if not runtime_events:
                continue

            should_refresh_run = False
            for runtime_event in runtime_events:
                if runtime_event.get("event") == "task_log":
                    task_key = runtime_event.get("task_id")
                    if not isinstance(task_key, str):
                        continue
                    detail = task_payload(workspace=workspace, run_id=run_id, task_key=task_key)
                    if detail is None:
                        continue
                    events.append(
                        live_event(
                            event_type="task_log_updated",
                            payload={
                                "workspace_id": workspace_id,
                                "run_id": run_id,
                                "task_key": task_key,
                                "stdout": read_log(detail.get("stdout_path")),
                                "stderr": read_log(detail.get("stderr_path")),
                            },
                        )
                    )
                    continue
                should_refresh_run = True

            if should_refresh_run:
                payload = run_payload(workspace=workspace, run_id=run_id)
                if payload is None:
                    continue
                events.append(
                    live_event(
                        event_type="run_updated",
                        payload={"workspace_id": workspace_id, "run": payload},
                    )
                )

    return events


def workspace_from_meta(*, meta: dict[str, Any], workspace_id: str) -> WorkspaceRecord | None:
    """Reconstruct minimal workspace metadata from a serialized meta payload.

    Parameters
    ----------
    meta : dict[str, Any]
        Serialized workspace meta payload.
    workspace_id : str
        Workspace identifier to resolve.

    Returns
    -------
    WorkspaceRecord | None
        Matching workspace metadata when present.
    """
    for workspace in meta.get("workspaces", []):
        if workspace.get("workspace_id") != workspace_id:
            continue
        return WorkspaceRecord(
            workspace_id=workspace_id,
            label=cast(str, workspace["label"]),
            project_root=Path(cast(str, workspace["project_root"])),
            runs_root=Path(cast(str, workspace["runs_root"])),
            cache_root=Path(cast(str, workspace["cache_root"])),
        )
    return None


def live_event(*, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Build a structured live event payload.

    Parameters
    ----------
    event_type : str
        Event type emitted to the client.
    payload : dict[str, Any]
        Event-specific JSON payload.

    Returns
    -------
    dict[str, Any]
        Serialized event envelope.
    """
    return {
        "type": event_type,
        "timestamp": datetime.now(UTC).isoformat(),
        "payload": payload,
    }


def read_runtime_events(
    *, run_dir: Path, start_offset: int, end_offset: int
) -> list[dict[str, Any]]:
    """Read appended runtime events from one run-local JSONL stream."""
    events_path = run_dir / "events.jsonl"
    if not events_path.is_file() or end_offset <= start_offset:
        return []

    payloads: list[dict[str, Any]] = []
    with events_path.open("r", encoding="utf-8") as handle:
        handle.seek(start_offset)
        remaining = end_offset - start_offset
        text = handle.read(remaining)

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = cast(dict[str, Any], json.loads(line))
        except Exception:
            continue
        payloads.append(payload)
    return payloads
