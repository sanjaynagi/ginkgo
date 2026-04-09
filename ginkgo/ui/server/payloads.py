"""HTTP payload builders and file-backed data helpers for the UI server."""

from __future__ import annotations

import mimetypes
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
import yaml

from ginkgo.cli.workspace import list_workflow_paths
from ginkgo.core.asset import AssetKey
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.caching.provenance import load_manifest, tail_text
from ginkgo.runtime.run_summary import RunSummary, TaskSummary

from .utils import (
    dir_size,
    format_age,
    format_size,
    latest_run_id,
    parse_timestamp,
    run_count,
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
            summary = RunSummary.load(run_dir)
        except Exception:
            continue
        runs.append(
            {
                "workspace_id": workspace.workspace_id,
                "workspace_label": workspace.label,
                "project_root": str(workspace.project_root),
                "run_id": summary.run_id,
                "workflow": summary.workflow_label,
                "workflow_path": summary.workflow,
                "status": summary.status,
                "started_at": summary.raw_manifest.get("started_at"),
                "finished_at": summary.raw_manifest.get("finished_at"),
                "task_count": summary.task_count,
                "failed_count": summary.failed_count,
                "cached_count": summary.cached_count,
                "succeeded_count": summary.succeeded_count,
                "duration_seconds": summary.duration_s,
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

    summary = RunSummary.load(run_dir)
    tasks: list[dict[str, Any]] = [
        _task_dict(workspace=workspace, run_id=run_id, run_dir=run_dir, task=task)
        for task in summary.tasks
    ]
    notebooks: list[dict[str, Any]] = [
        _notebook_dict(workspace=workspace, run_id=run_id, run_dir=run_dir, task=task)
        for task in summary.tasks
        if task.task_type == "notebook"
    ]

    return {
        "workspace_id": workspace.workspace_id,
        "workspace_label": workspace.label,
        "project_root": str(workspace.project_root),
        "run_id": summary.run_id,
        "run_dir": str(run_dir),
        "resources": summary.resources,
        "manifest": summary.raw_manifest,
        "params": summary.params,
        "tasks": tasks,
        "notebooks": notebooks,
    }


def _notebook_url(*, workspace: WorkspaceRecord, run_id: str, task_key: str) -> str:
    """Return the API URL for a rendered notebook artifact."""
    return f"/api/workspaces/{workspace.workspace_id}/runs/{run_id}/tasks/{task_key}/notebook"


def _task_dict(
    *,
    workspace: WorkspaceRecord,
    run_id: str,
    run_dir: Path,
    task: TaskSummary,
) -> dict[str, Any]:
    """Render one ``TaskSummary`` as a UI payload dict."""
    rendered_html = (run_dir / task.rendered_html) if task.rendered_html is not None else None
    executed_notebook = (
        (run_dir / task.executed_notebook) if task.executed_notebook is not None else None
    )
    return {
        "task_key": task.task_key,
        "node_id": task.node_id,
        "task": task.name,
        "task_name": task.base_name,
        "status": task.status,
        "env": task.env,
        "cached": task.cached,
        "exit_code": task.exit_code,
        "stdout_log": task.stdout_log,
        "stderr_log": task.stderr_log,
        "started_at": task.raw.get("started_at"),
        "finished_at": task.raw.get("finished_at"),
        "cache_key": task.cache_key,
        "dependency_ids": list(task.dependency_ids),
        "dynamic_dependency_ids": list(task.dynamic_dependency_ids),
        "task_type": task.task_type,
        "notebook_kind": task.notebook_kind,
        "notebook_description": task.notebook_description,
        "render_status": task.render_status,
        "rendered_html": str(rendered_html) if rendered_html is not None else None,
        "rendered_html_url": (
            _notebook_url(workspace=workspace, run_id=run_id, task_key=task.task_key)
            if rendered_html is not None
            else None
        ),
        "executed_notebook": str(executed_notebook) if executed_notebook is not None else None,
    }


def _notebook_dict(
    *,
    workspace: WorkspaceRecord,
    run_id: str,
    run_dir: Path,
    task: TaskSummary,
) -> dict[str, Any]:
    """Render one notebook task entry as a UI payload dict."""
    rendered_html = (run_dir / task.rendered_html) if task.rendered_html is not None else None
    return {
        "task_key": task.task_key,
        "task": task.name,
        "task_name": task.base_name,
        "description": task.notebook_description,
        "status": task.status,
        "render_status": task.render_status,
        "notebook_kind": task.notebook_kind,
        "notebook_path": task.notebook_path,
        "rendered_html": str(rendered_html) if rendered_html is not None else None,
        "rendered_html_url": (
            _notebook_url(workspace=workspace, run_id=run_id, task_key=task.task_key)
            if rendered_html is not None
            else None
        ),
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


def list_assets(workspace: WorkspaceRecord) -> list[dict[str, Any]]:
    """List cataloged assets for one workspace."""
    store = AssetStore(root=workspace.project_root / ".ginkgo" / "assets")
    artifact_store = LocalArtifactStore(root=workspace.project_root / ".ginkgo" / "artifacts")
    assets: list[dict[str, Any]] = []
    for key in store.list_asset_keys():
        versions = store.list_versions(key=key)
        latest = store.get_latest_version(key=key)
        if latest is None:
            continue
        artifact_record = _artifact_record_for(
            artifact_store=artifact_store,
            artifact_id=latest.artifact_id,
        )
        artifact_path = _artifact_path_for(
            artifact_store=artifact_store, artifact_id=latest.artifact_id
        )
        preview_kind = _infer_preview_kind(
            path=artifact_path,
            extension=artifact_record.extension if artifact_record is not None else "",
        )
        assets.append(
            {
                "asset_key": str(key),
                "name": key.name,
                "namespace": key.namespace,
                "kind": latest.kind,
                "latest_version_id": latest.version_id,
                "latest_created_at": latest.created_at,
                "latest_run_id": latest.run_id,
                "metadata": latest.metadata,
                "preview_kind": preview_kind,
                "version_count": len(versions),
            }
        )
    assets.sort(key=lambda item: (str(item["namespace"]), str(item["name"])))
    return assets


def asset_payload(
    workspace: WorkspaceRecord,
    *,
    asset_key_text: str,
    selector: str | None,
) -> dict[str, Any] | None:
    """Return one asset detail payload."""
    store = AssetStore(root=workspace.project_root / ".ginkgo" / "assets")
    artifact_store = LocalArtifactStore(root=workspace.project_root / ".ginkgo" / "artifacts")
    key = _parse_asset_key(asset_key_text)
    try:
        version = store.resolve_version(key=key, selector=selector)
    except FileNotFoundError:
        return None
    index = store._load_index(key)
    aliases_by_version: dict[str, list[str]] = {}
    for alias, version_id in dict(index.get("aliases", {})).items():
        aliases_by_version.setdefault(str(version_id), []).append(str(alias))

    artifact_path = _artifact_path_for(
        artifact_store=artifact_store, artifact_id=version.artifact_id
    )
    artifact_record = _artifact_record_for(
        artifact_store=artifact_store,
        artifact_id=version.artifact_id,
    )
    versions = []
    for item in store.list_versions(key=key):
        versions.append(
            {
                "aliases": aliases_by_version.get(item.version_id, []),
                "artifact_id": item.artifact_id,
                "content_hash": item.content_hash,
                "created_at": item.created_at,
                "metadata": item.metadata,
                "run_id": item.run_id,
                "version_id": item.version_id,
            }
        )

    lineage = store.lineage_for(key=key, version_id=version.version_id)
    parents = []
    if lineage is not None:
        for parent in lineage.parents:
            parents.append(
                {
                    "asset_key": str(parent.key),
                    "name": parent.name,
                    "namespace": parent.namespace,
                    "version_id": parent.version_id,
                }
            )

    content_url = (
        f"/api/workspaces/{workspace.workspace_id}/assets/{quote(str(key), safe='')}/content"
        f"?selector={quote(selector, safe='')}"
        if selector
        else f"/api/workspaces/{workspace.workspace_id}/assets/{quote(str(key), safe='')}/content"
    )
    extension = artifact_record.extension if artifact_record is not None else ""
    preview = _build_asset_preview(
        path=artifact_path, extension=extension, content_url=content_url
    )
    mime_source = (
        f"asset{extension}"
        if extension
        else str(artifact_path)
        if artifact_path is not None
        else ""
    )
    mime_type, _ = mimetypes.guess_type(mime_source)
    size_bytes = (
        artifact_record.size
        if artifact_record is not None
        else (
            artifact_path.stat().st_size
            if artifact_path is not None and artifact_path.exists()
            else None
        )
    )

    return {
        "asset_key": str(key),
        "artifact": {
            "artifact_id": version.artifact_id,
            "artifact_path": str(artifact_path) if artifact_path is not None else None,
            "extension": extension,
            "mime_type": mime_type,
            "size_bytes": size_bytes,
        },
        "kind": version.kind,
        "lineage": {"parents": parents},
        "metadata": version.metadata,
        "name": key.name,
        "namespace": key.namespace,
        "preview": preview,
        "selected_version": {
            "aliases": aliases_by_version.get(version.version_id, []),
            "artifact_id": version.artifact_id,
            "content_hash": version.content_hash,
            "created_at": version.created_at,
            "producer_task": version.producer_task,
            "run_id": version.run_id,
            "version_id": version.version_id,
        },
        "versions": versions,
    }


def asset_content_path(
    workspace: WorkspaceRecord,
    *,
    asset_key_text: str,
    selector: str | None,
) -> Path | None:
    """Return the filesystem path for a selected asset version."""
    payload = asset_payload(workspace, asset_key_text=asset_key_text, selector=selector)
    if payload is None:
        return None
    artifact_path = payload.get("artifact", {}).get("artifact_path")
    if not isinstance(artifact_path, str):
        return None
    path = Path(artifact_path)
    return path if path.is_file() else None


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


def resolve_task_path(run_dir: Path, task: dict[str, Any], key: str) -> Path | None:
    """Resolve one task-relative path from a manifest entry."""
    rel = task.get(key)
    return run_dir / rel if isinstance(rel, str) else None


def resolve_log_path(run_dir: Path, task: dict[str, Any], key: str) -> Path | None:
    """Resolve a log path from a task manifest entry."""
    return resolve_task_path(run_dir, task, key)


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
    notebook_html = resolve_task_path(run_dir, task, "rendered_html")

    return {
        "workspace_id": workspace.workspace_id,
        "run_id": run_id,
        "task_key": task_key,
        "task": task,
        "stdout_path": str(stdout_path) if stdout_path is not None else None,
        "stderr_path": str(stderr_path) if stderr_path is not None else None,
        "notebook_html_path": str(notebook_html) if notebook_html is not None else None,
        "notebook_html_url": (
            f"/api/workspaces/{workspace.workspace_id}/runs/{run_id}/tasks/{task_key}/notebook"
            if notebook_html is not None
            else None
        ),
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


def _parse_asset_key(value: str) -> AssetKey:
    """Parse ``namespace:name`` or a bare asset name."""
    namespace, separator, name = value.partition(":")
    if separator:
        if not namespace or not name:
            raise ValueError(f"Invalid asset key: {value!r}")
        return AssetKey(namespace=namespace, name=name)
    if not namespace:
        raise ValueError(f"Invalid asset key: {value!r}")
    return AssetKey(namespace="file", name=namespace)


def _artifact_path_for(*, artifact_store: LocalArtifactStore, artifact_id: str) -> Path | None:
    """Return the local artifact path when it exists."""
    if not artifact_store.exists(artifact_id=artifact_id):
        return None
    return artifact_store.artifact_path(artifact_id=artifact_id)


def _artifact_record_for(
    *,
    artifact_store: LocalArtifactStore,
    artifact_id: str,
) -> ArtifactRecord | None:
    """Return stored artifact metadata when it exists."""
    ref_path = artifact_store._refs_dir / f"{artifact_id}.json"
    if not ref_path.is_file():
        return None
    return ArtifactRecord.from_path(ref_path)


def _infer_preview_kind(*, path: Path | None, extension: str = "") -> str:
    """Infer the best preview strategy for a file."""
    suffix = extension.lower()
    if path is None:
        return "missing"
    if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        return "image"
    if suffix == ".pdf":
        return "pdf"
    if suffix in {".csv", ".tsv", ".parquet", ".json", ".jsonl", ".ndjson"}:
        return "table"
    if suffix in {".txt", ".log", ".md", ".yaml", ".yml", ".toml", ".py", ".sql"}:
        return "text"
    return "binary"


def _build_asset_preview(*, path: Path | None, extension: str, content_url: str) -> dict[str, Any]:
    """Build a preview payload for one asset artifact."""
    kind = _infer_preview_kind(path=path, extension=extension)
    if path is None or not path.exists():
        return {"kind": "missing", "message": "Artifact content is unavailable."}
    if kind == "image":
        return {"kind": "image", "url": content_url}
    if kind == "pdf":
        return {"kind": "pdf", "url": content_url}
    if kind == "table":
        preview = _table_preview(path=path, extension=extension)
        preview["kind"] = "table"
        return preview
    if kind == "text":
        text = path.read_text(encoding="utf-8", errors="replace")
        return {
            "kind": "text",
            "text": text[:4000],
            "truncated": len(text) > 4000,
        }
    return {
        "download_url": content_url,
        "kind": "binary",
        "message": "Preview unavailable for this file type.",
    }


def _table_preview(*, path: Path, extension: str) -> dict[str, Any]:
    """Return a compact row/column preview for a dataframe-like asset."""
    suffix = extension.lower()
    if suffix == ".csv":
        frame = pd.read_csv(path, nrows=50)
    elif suffix == ".tsv":
        frame = pd.read_csv(path, sep="\t", nrows=50)
    elif suffix == ".parquet":
        frame = pd.read_parquet(path).head(50)
    elif suffix == ".json":
        frame = pd.read_json(path).head(50)
    else:
        frame = pd.read_json(path, lines=True).head(50)

    frame = frame.where(pd.notnull(frame), None)
    return {
        "columns": [str(column) for column in frame.columns],
        "row_count": len(frame.index),
        "rows": frame.to_dict(orient="records"),
        "truncated": len(frame.index) == 50,
    }
