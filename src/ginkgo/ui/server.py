"""Local API and static asset server for the Ginkgo UI."""

from __future__ import annotations

import json
import mimetypes
import subprocess
import shutil
import sys
import time
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote

import yaml

from ginkgo.cli.workspace import list_workflow_paths
from ginkgo.runtime.provenance import latest_run_dir, load_manifest, tail_text

STATIC_DIR = Path(__file__).parent / "static"


def create_ui_server(
    *,
    host: str,
    port: int,
    runs_root: Path,
    selected_run_id: str | None = None,
) -> ThreadingHTTPServer:
    """Return a configured UI server bound to the given host and port."""
    project_root = runs_root.parent.parent
    cache_root = runs_root.parent / "cache"

    class _UiServer(ThreadingHTTPServer):
        allow_reuse_address = True

    class _UiHandler(BaseHTTPRequestHandler):
        server_version = "GinkgoUI/0.2"

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path.startswith("/api/"):
                self._handle_api(path)
                return

            if path.startswith("/assets/"):
                self._serve_static_asset(path)
                return

            if path == "/favicon.ico":
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return

            self._serve_spa()

        def do_DELETE(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            parts = [unquote(part) for part in path.strip("/").split("/")]
            if parts == ["api", "cache"]:
                deleted = _clear_cache_entries(cache_root)
                self._send_json({"ok": True, "deleted": deleted})
                return
            if len(parts) == 3 and parts[:2] == ["api", "cache"]:
                cache_key = parts[2]
                cache_dir = cache_root / cache_key
                if not cache_dir.is_dir():
                    self._send_json(
                        {"error": f"Cache entry not found: {cache_key}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                shutil.rmtree(cache_dir)
                self._send_json({"ok": True, "cache_key": cache_key})
                return
            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/api/run":
                content_length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(content_length) if content_length > 0 else b"{}"
                try:
                    payload = json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    self._send_json(
                        {"error": "Request body must be valid JSON."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                workflow = payload.get("workflow")
                if not isinstance(workflow, str) or not workflow.strip():
                    self._send_json(
                        {"error": "A workflow path is required."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                config_paths = payload.get("config_paths", [])
                if not isinstance(config_paths, list) or any(
                    not isinstance(item, str) or not item.strip() for item in config_paths
                ):
                    self._send_json(
                        {"error": "config_paths must be a list of non-empty strings."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                jobs = payload.get("jobs")
                cores = payload.get("cores")
                memory = payload.get("memory")
                if jobs is not None and not isinstance(jobs, int):
                    self._send_json(
                        {"error": "jobs must be an integer when provided."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                if cores is not None and not isinstance(cores, int):
                    self._send_json(
                        {"error": "cores must be an integer when provided."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                if memory is not None and not isinstance(memory, int):
                    self._send_json(
                        {"error": "memory must be an integer when provided."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                try:
                    launch = _launch_workflow_process(
                        project_root=project_root,
                        workflow=workflow,
                        config_paths=config_paths,
                        jobs=jobs,
                        cores=cores,
                        memory=memory,
                    )
                except FileNotFoundError as exc:
                    self._send_json(
                        {"error": str(exc)},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                self._send_json({"ok": True, **launch}, status=HTTPStatus.ACCEPTED)
                return

            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _handle_api(self, path: str) -> None:
            parts = [unquote(part) for part in path.strip("/").split("/")]
            if parts == ["api", "meta"]:
                latest_run = latest_run_dir(runs_root)
                self._send_json(
                    {
                        "project_root": str(project_root),
                        "runs_root": str(runs_root),
                        "selected_run_id": selected_run_id,
                        "latest_run_id": latest_run.name if latest_run is not None else None,
                    }
                )
                return

            if parts == ["api", "runs"]:
                self._send_json({"runs": _list_runs(runs_root)})
                return

            if parts == ["api", "cache"]:
                self._send_json({"entries": _list_cache_entries(cache_root)})
                return

            if parts == ["api", "workflows"]:
                self._send_json({"workflows": _list_workflows(project_root)})
                return

            if parts == ["api", "events"]:
                self._serve_events()
                return

            if len(parts) == 3 and parts[:2] == ["api", "runs"]:
                run_id = parts[2]
                run_dir = runs_root / run_id
                if not run_dir.is_dir():
                    self._send_json(
                        {"error": f"Run not found: {run_id}"}, status=HTTPStatus.NOT_FOUND
                    )
                    return
                self._send_json(_run_payload(run_dir))
                return

            if len(parts) == 5 and parts[:2] == ["api", "runs"] and parts[3] == "tasks":
                run_id = parts[2]
                task_key = parts[4]
                payload = _task_payload(runs_root, run_id, task_key)
                if payload is None:
                    self._send_json(
                        {"error": f"Task not found: {task_key}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(payload)
                return

            if (
                len(parts) == 6
                and parts[:2] == ["api", "runs"]
                and parts[3] == "tasks"
                and parts[5] == "log"
            ):
                run_id = parts[2]
                task_key = parts[4]
                payload = _task_payload(runs_root, run_id, task_key)
                if payload is None:
                    self._send_json(
                        {"error": f"Task not found: {task_key}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(
                    {
                        "stdout": _read_log(payload.get("stdout_path")),
                        "stderr": _read_log(payload.get("stderr_path")),
                        "task_key": task_key,
                    }
                )
                return

            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def _serve_static_asset(self, path: str) -> None:
            asset_path = (STATIC_DIR / path.lstrip("/")).resolve()
            try:
                asset_path.relative_to(STATIC_DIR.resolve())
            except ValueError:
                self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return
            if not asset_path.is_file():
                self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return

            content = asset_path.read_bytes()
            content_type, _ = mimetypes.guess_type(asset_path.name)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type or "application/octet-stream")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _serve_spa(self) -> None:
            index_path = STATIC_DIR / "index.html"
            if not index_path.is_file():
                self._send_json(
                    {
                        "error": "UI assets not built. Run the frontend build before starting `ginkgo ui`."
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            content = index_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _serve_events(self) -> None:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            last_signature = None
            try:
                while True:
                    latest = latest_run_dir(runs_root)
                    payload = {
                        "latest_run_id": latest.name if latest is not None else None,
                        "selected_run_id": selected_run_id,
                        "runs_signature": _runs_signature(runs_root),
                        "cache_signature": _cache_signature(cache_root),
                    }
                    signature = json.dumps(payload, sort_keys=True)
                    if signature != last_signature:
                        last_signature = signature
                        self.wfile.write(f"event: meta\ndata: {signature}\n\n".encode("utf-8"))
                        self.wfile.flush()
                    time.sleep(1.0)
            except (BrokenPipeError, ConnectionResetError):
                return

        def _send_json(
            self, payload: dict[str, Any], *, status: HTTPStatus = HTTPStatus.OK
        ) -> None:
            encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return _UiServer((host, port), _UiHandler)


def _list_runs(runs_root: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    if not runs_root.exists():
        return runs
    for run_dir in sorted((path for path in runs_root.iterdir() if path.is_dir()), reverse=True):
        try:
            manifest = load_manifest(run_dir)
        except Exception:
            continue
        tasks = manifest.get("tasks", {})
        runs.append(
            {
                "run_id": run_dir.name,
                "workflow": _base_name(manifest.get("workflow")),
                "workflow_path": manifest.get("workflow"),
                "status": manifest.get("status", "unknown"),
                "started_at": manifest.get("started_at"),
                "finished_at": manifest.get("finished_at"),
                "task_count": len(tasks) if isinstance(tasks, dict) else 0,
                "failed_count": _status_count(tasks, "failed"),
                "cached_count": _cached_count(tasks),
                "succeeded_count": _status_count(tasks, "succeeded"),
                "duration_seconds": _duration_seconds(
                    manifest.get("started_at"), manifest.get("finished_at")
                ),
            }
        )
    return runs


def _run_payload(run_dir: Path) -> dict[str, Any]:
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

        for task_key, task in sorted(
            task_items,
            key=lambda item: int(item[1].get("node_id", -1)),
        ):
            tasks.append(
                {
                    "task_key": task_key,
                    "node_id": task.get("node_id"),
                    "task": task.get("task"),
                    "task_name": _task_base_name(task.get("task")),
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
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "resources": manifest.get("resources", {}),
        "manifest": manifest,
        "params": params,
        "tasks": tasks,
    }


def _list_workflows(project_root: Path) -> list[str]:
    if not project_root.exists():
        return []

    workflows: list[str] = []
    for path in list_workflow_paths(project_root=project_root):
        try:
            workflows.append(str(path.relative_to(project_root)))
        except ValueError:
            workflows.append(str(path))
    return workflows


def _clear_cache_entries(cache_root: Path) -> int:
    if not cache_root.exists():
        return 0
    deleted = 0
    for entry in list(cache_root.iterdir()):
        if not entry.is_dir():
            continue
        shutil.rmtree(entry)
        deleted += 1
    return deleted


def _launch_workflow_process(
    *,
    project_root: Path,
    workflow: str,
    config_paths: list[str],
    jobs: int | None,
    cores: int | None,
    memory: int | None,
) -> dict[str, Any]:
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

    command = [sys.executable, "-m", "ginkgo.cli", "run", str(workflow_path)]
    for config_path in resolved_configs:
        command.extend(["--config", str(config_path)])
    if jobs is not None:
        command.extend(["--jobs", str(jobs)])
    if cores is not None:
        command.extend(["--cores", str(cores)])
    if memory is not None:
        command.extend(["--memory", str(memory)])

    process = subprocess.Popen(  # noqa: S603 - controlled local CLI invocation
        command,
        cwd=project_root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        workflow_label = str(workflow_path.relative_to(project_root))
    except ValueError:
        workflow_label = str(workflow_path)
    return {"pid": process.pid, "workflow": workflow_label}


def _resolve_log_path(run_dir: Path, task: dict[str, Any], key: str) -> Path | None:
    """Resolve a log path from a task manifest entry."""
    rel = task.get(key)
    return run_dir / rel if isinstance(rel, str) else None


def _read_log(path: str | Path | None) -> str:
    """Read an entire log file, returning empty string on missing/error."""
    if path is None:
        return ""
    p = Path(path)
    return p.read_text(encoding="utf-8") if p.is_file() else ""


def _task_payload(runs_root: Path, run_id: str, task_key: str) -> dict[str, Any] | None:
    run_dir = runs_root / run_id
    if not run_dir.is_dir():
        return None
    manifest = load_manifest(run_dir)
    tasks = manifest.get("tasks", {})
    if not isinstance(tasks, dict):
        return None
    task = tasks.get(task_key)
    if not isinstance(task, dict):
        return None

    stdout_path = _resolve_log_path(run_dir, task, "stdout_log")
    stderr_path = _resolve_log_path(run_dir, task, "stderr_log")

    # Backwards compatibility: fall back to legacy combined "log" field.
    legacy_path = _resolve_log_path(run_dir, task, "log")
    if stdout_path is None and legacy_path is not None:
        stdout_path = legacy_path

    return {
        "run_id": run_id,
        "task_key": task_key,
        "task": task,
        "stdout_path": str(stdout_path) if stdout_path is not None else None,
        "stderr_path": str(stderr_path) if stderr_path is not None else None,
        "stdout_tail": tail_text(stdout_path, lines=80) if stdout_path is not None else [],
        "stderr_tail": tail_text(stderr_path, lines=80) if stderr_path is not None else [],
    }


def _task_base_name(value: Any) -> str:
    if not isinstance(value, str) or not value:
        return "unknown"
    return value.rsplit(".", 1)[-1]


def _base_name(value: Any) -> str:
    if not isinstance(value, str) or not value:
        return "unknown"
    return Path(value).name


def _status_count(tasks: Any, status: str) -> int:
    if not isinstance(tasks, dict):
        return 0
    return sum(
        1 for item in tasks.values() if isinstance(item, dict) and item.get("status") == status
    )


def _cached_count(tasks: Any) -> int:
    if not isinstance(tasks, dict):
        return 0
    return sum(
        1 for item in tasks.values() if isinstance(item, dict) and item.get("cached") is True
    )


def _duration_seconds(started_at: Any, finished_at: Any) -> float | None:
    if not isinstance(started_at, str) or not isinstance(finished_at, str):
        return None
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at)
    except ValueError:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    return max(0.0, (end - start).total_seconds())


def _runs_signature(runs_root: Path) -> str:
    if not runs_root.exists():
        return "no-runs"
    parts: list[str] = []
    for run_dir in sorted(
        (path for path in runs_root.iterdir() if path.is_dir()), key=lambda item: item.name
    ):
        manifest_path = run_dir / "manifest.yaml"
        if manifest_path.is_file():
            stat = manifest_path.stat()
            parts.append(f"{run_dir.name}:{stat.st_mtime_ns}:{stat.st_size}")
        else:
            parts.append(f"{run_dir.name}:missing")
    return "|".join(parts) or "no-runs"


def _cache_signature(cache_root: Path) -> str:
    if not cache_root.exists():
        return "no-cache"
    parts: list[str] = []
    for entry in sorted(
        (path for path in cache_root.iterdir() if path.is_dir()), key=lambda item: item.name
    ):
        try:
            stat = entry.stat()
        except FileNotFoundError:
            continue
        parts.append(f"{entry.name}:{stat.st_mtime_ns}")
    return "|".join(parts) or "no-cache"


def _list_cache_entries(root: Path) -> list[dict[str, Any]]:
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
        entries.append(
            {
                "cache_key": entry.name,
                "task": _task_base_name(function),
                "size": _format_size(_dir_size(entry)),
                "size_bytes": _dir_size(entry),
                "age": _format_age(_parse_timestamp(timestamp)),
                "created": timestamp or "-",
                "function": function,
            }
        )
    return entries


def _dir_size(path: Path) -> int:
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            total += file_path.stat().st_size
    return total


def _format_size(size_bytes: int) -> str:
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size_bytes} B"


def _parse_timestamp(timestamp: str) -> datetime | None:
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _format_age(created_at: datetime | None) -> str:
    if created_at is None:
        return "-"
    delta = datetime.now(UTC) - created_at.astimezone(UTC)
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"
