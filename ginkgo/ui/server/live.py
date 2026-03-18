"""Live state capture and incremental event helpers for the UI server."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from ginkgo.runtime.provenance import load_manifest

from .payloads import (
    list_runs,
    read_log,
    resolve_log_path,
    run_payload,
    task_payload,
    workspace_payload,
)
from .utils import cache_signature, runs_signature
from .workspaces import WorkspaceRecord, WorkspaceRegistry


def workspaces_signature(workspaces: list[WorkspaceRecord]) -> str:
    """Build a change signature across all loaded workspaces.

    Parameters
    ----------
    workspaces : list[WorkspaceRecord]
        Loaded workspace metadata.

    Returns
    -------
    str
        Deterministic change signature for live refreshes.
    """
    parts: list[str] = []
    for workspace in workspaces:
        parts.append(
            "|".join(
                [
                    workspace.workspace_id,
                    runs_signature(workspace.runs_root),
                    cache_signature(workspace.cache_root),
                ]
            )
        )
    return "||".join(parts) or "no-workspaces"


def capture_live_state(
    *, registry: WorkspaceRegistry, selected_run_id: str | None
) -> dict[str, Any]:
    """Capture the current workspace/run/task state for live event diffing.

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
    run_signatures: dict[str, dict[str, str]] = {}
    task_log_signatures: dict[str, dict[str, dict[str, str]]] = {}
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
        run_signatures[workspace.workspace_id] = {}
        task_log_signatures[workspace.workspace_id] = {}

        for run in runs:
            run_id = cast(str, run["run_id"])
            run_dir = workspace.runs_root / run_id
            run_signatures[workspace.workspace_id][run_id] = run_dir_signature(run_dir)
            task_log_signatures[workspace.workspace_id][run_id] = run_task_log_signatures(run_dir)

    if active_workspace is None:
        meta["active_workspace"] = None

    return {
        "meta": meta,
        "runs_by_workspace": runs_by_workspace,
        "run_signatures": run_signatures,
        "task_log_signatures": task_log_signatures,
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

    # Then emit full run payload updates for changed run manifests/params.
    previous_signatures = previous["run_signatures"]
    current_signatures = current["run_signatures"]
    for workspace_id in all_workspace_ids:
        prev_workspace_runs = previous_signatures.get(workspace_id, {})
        curr_workspace_runs = current_signatures.get(workspace_id, {})
        all_run_ids = sorted(set(prev_workspace_runs) | set(curr_workspace_runs))
        for run_id in all_run_ids:
            prev_signature = prev_workspace_runs.get(run_id)
            curr_signature = curr_workspace_runs.get(run_id)
            if curr_signature is None or prev_signature == curr_signature:
                continue
            workspace = workspace_from_meta(meta=current["meta"], workspace_id=workspace_id)
            if workspace is None:
                continue
            payload = run_payload(workspace=workspace, run_id=run_id)
            if payload is None:
                continue
            events.append(
                live_event(
                    event_type="run_updated",
                    payload={"workspace_id": workspace_id, "run": payload},
                )
            )

    # Finally emit log updates for tasks whose log files changed.
    previous_logs = previous["task_log_signatures"]
    current_logs = current["task_log_signatures"]
    for workspace_id in all_workspace_ids:
        prev_workspace_logs = previous_logs.get(workspace_id, {})
        curr_workspace_logs = current_logs.get(workspace_id, {})
        all_run_ids = sorted(set(prev_workspace_logs) | set(curr_workspace_logs))
        workspace = workspace_from_meta(meta=current["meta"], workspace_id=workspace_id)
        if workspace is None:
            continue
        for run_id in all_run_ids:
            prev_run_logs = prev_workspace_logs.get(run_id, {})
            curr_run_logs = curr_workspace_logs.get(run_id, {})
            all_task_keys = sorted(set(prev_run_logs) | set(curr_run_logs))
            for task_key in all_task_keys:
                prev_signature = prev_run_logs.get(task_key)
                curr_signature = curr_run_logs.get(task_key)
                if curr_signature is None or prev_signature == curr_signature:
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


def run_dir_signature(run_dir: Path) -> str:
    """Return a signature for one run directory.

    Parameters
    ----------
    run_dir : Path
        Run directory to inspect.

    Returns
    -------
    str
        Deterministic file signature.
    """
    manifest_path = run_dir / "manifest.yaml"
    params_path = run_dir / "params.yaml"
    parts: list[str] = []
    for path in (manifest_path, params_path):
        if path.is_file():
            stat = path.stat()
            parts.append(f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}")
    return "|".join(parts) or "missing"


def run_task_log_signatures(run_dir: Path) -> dict[str, str]:
    """Return per-task log signatures for one run.

    Parameters
    ----------
    run_dir : Path
        Run directory to inspect.

    Returns
    -------
    dict[str, str]
        Per-task log signatures.
    """
    manifest = load_manifest(run_dir)
    tasks = manifest.get("tasks", {})
    if not isinstance(tasks, dict):
        return {}

    signatures: dict[str, str] = {}
    for task_key, task in tasks.items():
        if not isinstance(task_key, str) or not isinstance(task, dict):
            continue
        stdout_path = resolve_log_path(run_dir, task, "stdout_log")
        stderr_path = resolve_log_path(run_dir, task, "stderr_log")
        parts: list[str] = []
        for path in (stdout_path, stderr_path):
            if path is None or not path.is_file():
                continue
            stat = path.stat()
            parts.append(f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}")
        signatures[task_key] = "|".join(parts) or "no-logs"
    return signatures
