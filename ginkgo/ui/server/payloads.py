"""HTTP payload builders and file-backed data helpers for the UI server."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import yaml

from ginkgo.cli.workspace import list_workflow_paths
from ginkgo.runtime.provenance import load_manifest, tail_text

from .utils import (
    base_name,
    cached_count,
    dir_size,
    duration_seconds,
    format_age,
    format_size,
    latest_run_id,
    parse_timestamp,
    run_count,
    status_count,
    task_base_name,
)
from .workspaces import WorkspaceRecord

REPO_ROOT = Path(__file__).resolve().parents[3]


def workspace_payload(
    record: WorkspaceRecord,
    *,
    active_workspace_id: str | None,
    selected_run_id: str | None,
) -> dict[str, Any]:
    """Serialize workspace metadata for the UI."""
    latest_run = latest_run_id(record.runs_root)
    return {
        "workspace_id": record.workspace_id,
        "label": record.label,
        "project_root": str(record.project_root),
        "runs_root": str(record.runs_root),
        "cache_root": str(record.cache_root),
        "latest_run_id": latest_run,
        "selected_run_id": selected_run_id,
        "workflow_count": len(list_workflows(record.project_root)),
        "run_count": run_count(record.runs_root),
        "is_active": record.workspace_id == active_workspace_id,
    }


def list_runs(workspace: WorkspaceRecord) -> list[dict[str, Any]]:
    """List runs for one workspace."""
    runs: list[dict[str, Any]] = []
    if not workspace.runs_root.exists():
        return runs
    for run_dir in sorted(
        (path for path in workspace.runs_root.iterdir() if path.is_dir()),
        reverse=True,
    ):
        try:
            manifest = load_manifest(run_dir)
        except Exception:
            continue
        tasks = manifest.get("tasks", {})
        runs.append(
            {
                "workspace_id": workspace.workspace_id,
                "workspace_label": workspace.label,
                "project_root": str(workspace.project_root),
                "run_id": run_dir.name,
                "workflow": base_name(manifest.get("workflow")),
                "workflow_path": manifest.get("workflow"),
                "status": manifest.get("status", "unknown"),
                "started_at": manifest.get("started_at"),
                "finished_at": manifest.get("finished_at"),
                "task_count": len(tasks) if isinstance(tasks, dict) else 0,
                "failed_count": status_count(tasks, "failed"),
                "cached_count": cached_count(tasks),
                "succeeded_count": status_count(tasks, "succeeded"),
                "duration_seconds": duration_seconds(
                    manifest.get("started_at"),
                    manifest.get("finished_at"),
                ),
            }
        )
    return runs


def list_runs_across_workspaces(workspaces: list[WorkspaceRecord]) -> list[dict[str, Any]]:
    """Aggregate runs across all loaded workspaces."""
    runs: list[dict[str, Any]] = []
    for workspace in workspaces:
        runs.extend(list_runs(workspace))
    runs.sort(
        key=lambda item: (
            str(item.get("started_at") or ""),
            str(item.get("run_id") or ""),
        ),
        reverse=True,
    )
    return runs


def run_payload(workspace: WorkspaceRecord, run_id: str) -> dict[str, Any] | None:
    """Return one run detail payload."""
    run_dir = workspace.runs_root / run_id
    if not run_dir.is_dir():
        return None

    manifest = load_manifest(run_dir)
    params_path = run_dir / "params.yaml"
    params = (
        yaml.safe_load(params_path.read_text(encoding="utf-8")) if params_path.is_file() else {}
    )
    tasks_raw = manifest.get("tasks", {})
    tasks: list[dict[str, Any]] = []
    if isinstance(tasks_raw, dict):
        task_items: list[tuple[str, dict[str, Any]]] = []
        for task_key, task in tasks_raw.items():
            if not isinstance(task_key, str) or not isinstance(task, dict):
                continue
            task_items.append((task_key, cast(dict[str, Any], task)))

        for task_key, task in sorted(task_items, key=lambda item: int(item[1].get("node_id", -1))):
            tasks.append(
                {
                    "task_key": task_key,
                    "node_id": task.get("node_id"),
                    "task": task.get("task"),
                    "task_name": task_base_name(task.get("task")),
                    "status": task.get("status", "unknown"),
                    "env": task.get("env") or "local",
                    "cached": task.get("cached", False),
                    "exit_code": task.get("exit_code"),
                    "stdout_log": task.get("stdout_log"),
                    "stderr_log": task.get("stderr_log"),
                    "started_at": task.get("started_at"),
                    "finished_at": task.get("finished_at"),
                    "cache_key": task.get("cache_key"),
                    "dependency_ids": task.get("dependency_ids", []),
                    "dynamic_dependency_ids": task.get("dynamic_dependency_ids", []),
                }
            )
    return {
        "workspace_id": workspace.workspace_id,
        "workspace_label": workspace.label,
        "project_root": str(workspace.project_root),
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "resources": manifest.get("resources", {}),
        "manifest": manifest,
        "params": params,
        "tasks": tasks,
    }


def list_workflows(project_root: Path) -> list[str]:
    """List workflows for one project root."""
    if not project_root.exists():
        return []

    workflows: list[str] = []
    for path in list_workflow_paths(project_root=project_root):
        try:
            workflows.append(str(path.relative_to(project_root)))
        except ValueError:
            workflows.append(str(path))
    return workflows


def clear_cache_entries(cache_root: Path) -> int:
    """Delete all cache entries for one workspace."""
    if not cache_root.exists():
        return 0
    deleted = 0
    for entry in list(cache_root.iterdir()):
        if not entry.is_dir():
            continue
        shutil.rmtree(entry)
        deleted += 1
    return deleted


def _workflow_launch_command(*, project_root: Path, workflow_path: Path) -> list[str]:
    """Build the base subprocess command to launch a workflow.

    Uses the workspace's pixi environment when one is present, so that
    workspace-specific dependencies are available when the workflow module is
    imported.  Falls back to the current interpreter when pixi is unavailable.
    """
    if (project_root / ".pixi").is_dir():
        pixi = shutil.which("pixi")
        if pixi:
            return [pixi, "run", "python", "-m", "ginkgo.cli", "run", str(workflow_path)]
    return [sys.executable, "-m", "ginkgo.cli", "run", str(workflow_path)]


def launch_workflow_process(
    *,
    project_root: Path,
    workflow: str,
    config_paths: list[str],
    jobs: int | None,
    cores: int | None,
    memory: int | None,
) -> dict[str, Any]:
    """Launch a workflow process in one workspace."""
    workflow_path = Path(workflow)
    if not workflow_path.is_absolute():
        workflow_path = project_root / workflow_path
    workflow_path = workflow_path.resolve()
    if not workflow_path.is_file():
        raise FileNotFoundError(f"Workflow not found: {workflow}")

    resolved_configs: list[Path] = []
    for config_path in config_paths:
        path = Path(config_path)
        if not path.is_absolute():
            path = project_root / path
        path = path.resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Config not found: {config_path}")
        resolved_configs.append(path)

    command = _workflow_launch_command(project_root=project_root, workflow_path=workflow_path)
    for config_path in resolved_configs:
        command.extend(["--config", str(config_path)])
    if jobs is not None:
        command.extend(["--jobs", str(jobs)])
    if cores is not None:
        command.extend(["--cores", str(cores)])
    if memory is not None:
        command.extend(["--memory", str(memory)])

    # Ensure `python -m ginkgo.cli` resolves this checkout even when the spawned
    # process changes its cwd to a different workspace.
    env = os.environ.copy()
    pythonpath_entries = [str(REPO_ROOT)]
    existing_pythonpath = env.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)

    process = subprocess.Popen(  # noqa: S603 - controlled local CLI invocation
        command,
        cwd=project_root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        workflow_label = str(workflow_path.relative_to(project_root))
    except ValueError:
        workflow_label = str(workflow_path)
    return {"pid": process.pid, "workflow": workflow_label}


def resolve_log_path(run_dir: Path, task: dict[str, Any], key: str) -> Path | None:
    """Resolve a log path from a task manifest entry."""
    rel = task.get(key)
    return run_dir / rel if isinstance(rel, str) else None


def read_log(path: str | Path | None) -> str:
    """Read an entire log file, returning empty string on missing/error."""
    if path is None:
        return ""
    p = Path(path)
    return p.read_text(encoding="utf-8") if p.is_file() else ""


def task_payload(
    *, workspace: WorkspaceRecord, run_id: str, task_key: str
) -> dict[str, Any] | None:
    """Return one task detail payload."""
    run_dir = workspace.runs_root / run_id
    if not run_dir.is_dir():
        return None
    manifest = load_manifest(run_dir)
    tasks = manifest.get("tasks", {})
    if not isinstance(tasks, dict):
        return None
    task = tasks.get(task_key)
    if not isinstance(task, dict):
        return None

    stdout_path = resolve_log_path(run_dir, task, "stdout_log")
    stderr_path = resolve_log_path(run_dir, task, "stderr_log")
    legacy_path = resolve_log_path(run_dir, task, "log")
    if stdout_path is None and legacy_path is not None:
        stdout_path = legacy_path

    return {
        "workspace_id": workspace.workspace_id,
        "run_id": run_id,
        "task_key": task_key,
        "task": task,
        "stdout_path": str(stdout_path) if stdout_path is not None else None,
        "stderr_path": str(stderr_path) if stderr_path is not None else None,
        "stdout_tail": tail_text(stdout_path, lines=80) if stdout_path is not None else [],
        "stderr_tail": tail_text(stderr_path, lines=80) if stderr_path is not None else [],
    }


def list_cache_entries(root: Path) -> list[dict[str, Any]]:
    """List cache entries for one workspace."""
    if not root.exists():
        return []
    entries: list[dict[str, Any]] = []
    for entry in sorted((path for path in root.iterdir() if path.is_dir())):
        meta_path = entry / "meta.json"
        if meta_path.is_file():
            try:
                meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
            except Exception:
                meta = {}
        else:
            meta = {}

        function = str(meta.get("function") or "unknown")
        timestamp = str(meta.get("timestamp") or "")
        size_bytes = dir_size(entry)
        entries.append(
            {
                "cache_key": entry.name,
                "task": task_base_name(function),
                "size": format_size(size_bytes),
                "size_bytes": size_bytes,
                "age": format_age(parse_timestamp(timestamp)),
                "created": timestamp or "-",
                "function": function,
            }
        )
    return entries
