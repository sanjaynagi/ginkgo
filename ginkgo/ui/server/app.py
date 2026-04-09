"""HTTP application and route handlers for the local Ginkgo UI server."""

from __future__ import annotations

import json
import mimetypes
import shutil
import time
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlsplit

from ginkgo.runtime.caching.provenance import load_manifest

from .live import capture_live_state, diff_live_state
from . import payloads
from .payloads import (
    asset_payload,
    clear_cache_entries,
    list_assets,
    list_cache_entries,
    list_runs,
    list_runs_across_workspaces,
    list_workflows,
    read_log,
    resolve_task_path,
    run_payload,
    task_payload,
    workspace_payload,
)
from .websocket import send_websocket_json, websocket_accept
from .workspaces import (
    WorkspaceLoadCancelledError,
    WorkspaceRecord,
    WorkspaceRegistry,
    pick_workspace_folder,
    resolve_launch_workspace,
)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


def create_ui_server(
    *,
    host: str,
    port: int,
    runs_root: Path,
    selected_run_id: str | None = None,
) -> ThreadingHTTPServer:
    """Return a configured UI server bound to the given host and port.

    Parameters
    ----------
    host : str
        Address to bind.
    port : int
        TCP port to bind.
    runs_root : Path
        Default run provenance directory used to seed the initial workspace.
    selected_run_id : str | None, default=None
        Optional run that should be highlighted initially.

    Returns
    -------
    ThreadingHTTPServer
        Configured UI server instance.
    """
    initial_project_root = runs_root.resolve().parent.parent
    registry = WorkspaceRegistry(initial_project_root=initial_project_root)

    class _UiServer(ThreadingHTTPServer):
        allow_reuse_address = True

    class _UiHandler(BaseHTTPRequestHandler):
        server_version = "GinkgoUI/0.3"

        def do_GET(self) -> None:  # noqa: N802
            split = urlsplit(self.path)
            path = split.path
            if path.startswith("/api/"):
                self._handle_api(path=path, query=parse_qs(split.query))
                return

            if path == "/ws":
                self._serve_websocket()
                return

            if path.startswith("/assets/") or path == "/resource-monitor.js":
                self._serve_static_asset(path)
                return

            if path == "/favicon.ico":
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return

            self._serve_spa()

        def do_DELETE(self) -> None:  # noqa: N802
            split = urlsplit(self.path)
            parts = [unquote(part) for part in split.path.strip("/").split("/") if part]

            # Clear cache for the active workspace.
            if parts == ["api", "cache"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                deleted = clear_cache_entries(workspace.cache_root)
                self._send_json({"ok": True, "deleted": deleted})
                return

            # Delete one cache entry from the active workspace.
            if len(parts) == 3 and parts[:2] == ["api", "cache"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._delete_cache_entry(workspace=workspace, cache_key=parts[2])
                return

            # Workspace-scoped cache clear.
            if len(parts) == 4 and parts[:2] == ["api", "workspaces"] and parts[3] == "cache":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                deleted = clear_cache_entries(workspace.cache_root)
                self._send_json({"ok": True, "deleted": deleted})
                return

            # Workspace-scoped cache entry delete.
            if len(parts) == 5 and parts[:2] == ["api", "workspaces"] and parts[3] == "cache":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._delete_cache_entry(workspace=workspace, cache_key=parts[4])
                return

            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802
            split = urlsplit(self.path)
            path = split.path
            parts = [unquote(part) for part in path.strip("/").split("/") if part]
            payload = self._json_body()
            if payload is None:
                return

            if path == "/api/run":
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._launch_workflow(workspace=workspace, payload=payload)
                return

            if path == "/api/workspaces/load":
                self._load_workspace(payload=payload)
                return

            if path == "/api/workspaces/activate":
                workspace_id = payload.get("workspace_id")
                if not isinstance(workspace_id, str) or not workspace_id.strip():
                    self._send_json(
                        {"error": "workspace_id is required."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    record = registry.set_active(workspace_id=workspace_id)
                except KeyError:
                    self._send_json(
                        {"error": f"Workspace not found: {workspace_id}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(
                    {
                        "ok": True,
                        "workspace": workspace_payload(
                            record,
                            active_workspace_id=registry.active_workspace_id(),
                            selected_run_id=selected_run_id,
                        ),
                    }
                )
                return

            if len(parts) == 4 and parts[:2] == ["api", "workspaces"] and parts[3] == "run":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._launch_workflow(workspace=workspace, payload=payload)
                return

            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _handle_api(self, *, path: str, query: dict[str, list[str]]) -> None:
            parts = [unquote(part) for part in path.strip("/").split("/") if part]

            if parts == ["api", "meta"]:
                self._send_json(self._meta_payload())
                return

            if parts == ["api", "workspaces"]:
                self._send_json(
                    {
                        "active_workspace_id": registry.active_workspace_id(),
                        "workspaces": self._workspace_payloads(),
                    }
                )
                return

            if query.get("scope") == ["all"] and parts == ["api", "runs"]:
                self._send_json({"runs": list_runs_across_workspaces(registry.list_workspaces())})
                return

            if parts == ["api", "runs"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._send_json({"runs": list_runs(workspace)})
                return

            if parts == ["api", "cache"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._send_json({"entries": list_cache_entries(workspace.cache_root)})
                return

            if parts == ["api", "assets"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._send_json({"assets": list_assets(workspace)})
                return

            if parts == ["api", "workflows"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._send_json({"workflows": list_workflows(workspace.project_root)})
                return

            if parts == ["api", "events"]:
                self._serve_events()
                return

            if len(parts) == 4 and parts[:2] == ["api", "workspaces"] and parts[3] == "runs":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._send_json({"runs": list_runs(workspace)})
                return

            if len(parts) == 4 and parts[:2] == ["api", "workspaces"] and parts[3] == "cache":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._send_json({"entries": list_cache_entries(workspace.cache_root)})
                return

            if len(parts) == 4 and parts[:2] == ["api", "workspaces"] and parts[3] == "assets":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._send_json({"assets": list_assets(workspace)})
                return

            if len(parts) == 4 and parts[:2] == ["api", "workspaces"] and parts[3] == "workflows":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._send_json({"workflows": list_workflows(workspace.project_root)})
                return

            if len(parts) == 3 and parts[:2] == ["api", "assets"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                payload = asset_payload(
                    workspace,
                    asset_key_text=parts[2],
                    selector=_query_scalar(query, "selector"),
                )
                if payload is None:
                    self._send_json(
                        {"error": f"Asset not found: {parts[2]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(payload)
                return

            if len(parts) == 4 and parts[:2] == ["api", "assets"] and parts[3] == "content":
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._serve_asset_content(
                    workspace=workspace,
                    asset_key_text=parts[2],
                    selector=_query_scalar(query, "selector"),
                )
                return

            if len(parts) == 5 and parts[:2] == ["api", "workspaces"] and parts[3] == "assets":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                payload = asset_payload(
                    workspace,
                    asset_key_text=parts[4],
                    selector=_query_scalar(query, "selector"),
                )
                if payload is None:
                    self._send_json(
                        {"error": f"Asset not found: {parts[4]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(payload)
                return

            if (
                len(parts) == 6
                and parts[:2] == ["api", "workspaces"]
                and parts[3] == "assets"
                and parts[5] == "content"
            ):
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._serve_asset_content(
                    workspace=workspace,
                    asset_key_text=parts[4],
                    selector=_query_scalar(query, "selector"),
                )
                return

            if len(parts) == 5 and parts[:2] == ["api", "workspaces"] and parts[3] == "runs":
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                payload = run_payload(workspace=workspace, run_id=parts[4])
                if payload is None:
                    self._send_json(
                        {"error": f"Run not found: {parts[4]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(payload)
                return

            if (
                len(parts) == 7
                and parts[:2] == ["api", "workspaces"]
                and parts[3] == "runs"
                and parts[5] == "tasks"
            ):
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                payload = task_payload(workspace=workspace, run_id=parts[4], task_key=parts[6])
                if payload is None:
                    self._send_json(
                        {"error": f"Task not found: {parts[6]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(payload)
                return

            if (
                len(parts) == 8
                and parts[:2] == ["api", "workspaces"]
                and parts[3] == "runs"
                and parts[5] == "tasks"
                and parts[7] == "notebook"
            ):
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                self._serve_notebook_html(workspace=workspace, run_id=parts[4], task_key=parts[6])
                return

            if (
                len(parts) == 8
                and parts[:2] == ["api", "workspaces"]
                and parts[3] == "runs"
                and parts[5] == "tasks"
                and parts[7] == "log"
            ):
                workspace = self._workspace_or_404(parts[2])
                if workspace is None:
                    return
                payload = task_payload(workspace=workspace, run_id=parts[4], task_key=parts[6])
                if payload is None:
                    self._send_json(
                        {"error": f"Task not found: {parts[6]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(
                    {
                        "stdout": read_log(payload.get("stdout_path")),
                        "stderr": read_log(payload.get("stderr_path")),
                        "task_key": parts[6],
                    }
                )
                return

            # Compatibility routes scoped to the active workspace.
            if len(parts) == 3 and parts[:2] == ["api", "runs"]:
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                payload = run_payload(workspace=workspace, run_id=parts[2])
                if payload is None:
                    self._send_json(
                        {"error": f"Run not found: {parts[2]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(payload)
                return

            if len(parts) == 5 and parts[:2] == ["api", "runs"] and parts[3] == "tasks":
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                payload = task_payload(workspace=workspace, run_id=parts[2], task_key=parts[4])
                if payload is None:
                    self._send_json(
                        {"error": f"Task not found: {parts[4]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(payload)
                return

            if (
                len(parts) == 6
                and parts[:2] == ["api", "runs"]
                and parts[3] == "tasks"
                and parts[5] == "notebook"
            ):
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                self._serve_notebook_html(workspace=workspace, run_id=parts[2], task_key=parts[4])
                return

            if (
                len(parts) == 6
                and parts[:2] == ["api", "runs"]
                and parts[3] == "tasks"
                and parts[5] == "log"
            ):
                workspace = self._active_workspace_or_404()
                if workspace is None:
                    return
                payload = task_payload(workspace=workspace, run_id=parts[2], task_key=parts[4])
                if payload is None:
                    self._send_json(
                        {"error": f"Task not found: {parts[4]}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json(
                    {
                        "stdout": read_log(payload.get("stdout_path")),
                        "stderr": read_log(payload.get("stderr_path")),
                        "task_key": parts[4],
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

        def _serve_notebook_html(
            self,
            *,
            workspace: WorkspaceRecord,
            run_id: str,
            task_key: str,
        ) -> None:
            run_dir = workspace.runs_root / run_id
            if not run_dir.is_dir():
                self._send_json({"error": f"Run not found: {run_id}"}, status=HTTPStatus.NOT_FOUND)
                return

            manifest = load_manifest(run_dir)
            tasks = manifest.get("tasks", {})
            if not isinstance(tasks, dict):
                self._send_json(
                    {"error": f"Task not found: {task_key}"}, status=HTTPStatus.NOT_FOUND
                )
                return
            task = tasks.get(task_key)
            if not isinstance(task, dict):
                self._send_json(
                    {"error": f"Task not found: {task_key}"}, status=HTTPStatus.NOT_FOUND
                )
                return

            html_path = resolve_task_path(run_dir, cast(dict[str, Any], task), "rendered_html")
            if html_path is None or not html_path.is_file():
                self._send_json(
                    {"error": f"Notebook HTML not found for task: {task_key}"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return

            content = html_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
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

        def _serve_asset_content(
            self,
            *,
            workspace: WorkspaceRecord,
            asset_key_text: str,
            selector: str | None,
        ) -> None:
            payload = asset_payload(
                workspace,
                asset_key_text=asset_key_text,
                selector=selector,
            )
            artifact = payload.get("artifact", {}) if payload is not None else {}
            artifact_path = artifact.get("artifact_path")
            path = Path(artifact_path) if isinstance(artifact_path, str) else None
            if path is None or not path.is_file():
                self._send_json(
                    {"error": f"Asset content not found: {asset_key_text}"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            content = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header(
                "Content-Type",
                str(artifact.get("mime_type") or "application/octet-stream"),
            )
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _serve_events(self) -> None:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            last_state = capture_live_state(registry=registry, selected_run_id=selected_run_id)
            try:
                while True:
                    current_state = capture_live_state(
                        registry=registry,
                        selected_run_id=selected_run_id,
                    )
                    for event in diff_live_state(previous=last_state, current=current_state):
                        encoded = json.dumps(event, sort_keys=True)
                        self.wfile.write(f"event: update\ndata: {encoded}\n\n".encode("utf-8"))
                        self.wfile.flush()
                    last_state = current_state
                    time.sleep(1.0)
            except (BrokenPipeError, ConnectionResetError):
                return

        def _serve_websocket(self) -> None:
            key = self.headers.get("Sec-WebSocket-Key")
            if not key:
                self._send_json(
                    {"error": "Missing Sec-WebSocket-Key header."},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            accept_key = websocket_accept(key)
            self.send_response(HTTPStatus.SWITCHING_PROTOCOLS)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept_key)
            self.end_headers()

            self.connection.settimeout(2.0)
            last_state = capture_live_state(registry=registry, selected_run_id=selected_run_id)
            initial_events = [
                {
                    "type": "connected",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "payload": {"transport": "websocket"},
                },
                {
                    "type": "meta",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "payload": {"meta": last_state["meta"]},
                },
            ]

            try:
                for event in initial_events:
                    send_websocket_json(self.connection, event)

                while True:
                    current_state = capture_live_state(
                        registry=registry,
                        selected_run_id=selected_run_id,
                    )
                    for event in diff_live_state(previous=last_state, current=current_state):
                        send_websocket_json(self.connection, event)
                    last_state = current_state
                    time.sleep(1.0)
            except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                return

        def _json_body(self) -> dict[str, Any] | None:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError:
                self._send_json(
                    {"error": "Request body must be valid JSON."},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return None
            if not isinstance(payload, dict):
                self._send_json(
                    {"error": "Request body must be a JSON object."},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return None
            return cast(dict[str, Any], payload)

        def _meta_payload(self) -> dict[str, Any]:
            active_workspace = registry.active_workspace()
            active_workspace_payload = self._active_workspace_payload()
            return {
                "project_root": (
                    str(active_workspace.project_root) if active_workspace is not None else None
                ),
                "runs_root": (
                    str(active_workspace.runs_root) if active_workspace is not None else None
                ),
                "selected_run_id": selected_run_id,
                "latest_run_id": (
                    active_workspace_payload.get("latest_run_id")
                    if active_workspace_payload is not None
                    else None
                ),
                "active_workspace_id": registry.active_workspace_id(),
                "active_workspace": active_workspace_payload,
                "workspaces": self._workspace_payloads(),
            }

        def _workspace_payloads(self) -> list[dict[str, Any]]:
            return [
                workspace_payload(
                    record,
                    active_workspace_id=registry.active_workspace_id(),
                    selected_run_id=selected_run_id,
                )
                for record in registry.list_workspaces()
            ]

        def _active_workspace_payload(self) -> dict[str, Any] | None:
            active_workspace = registry.active_workspace()
            if active_workspace is None:
                return None
            return workspace_payload(
                active_workspace,
                active_workspace_id=registry.active_workspace_id(),
                selected_run_id=selected_run_id,
            )

        def _active_workspace_or_404(self) -> WorkspaceRecord | None:
            workspace = registry.active_workspace()
            if workspace is None:
                self._send_json(
                    {"error": "No active workspace loaded."}, status=HTTPStatus.NOT_FOUND
                )
            return workspace

        def _workspace_or_404(self, workspace_id: str) -> WorkspaceRecord | None:
            workspace = registry.workspace(workspace_id)
            if workspace is None:
                self._send_json(
                    {"error": f"Workspace not found: {workspace_id}"},
                    status=HTTPStatus.NOT_FOUND,
                )
            return workspace

        def _delete_cache_entry(self, *, workspace: WorkspaceRecord, cache_key: str) -> None:
            cache_dir = workspace.cache_root / cache_key
            if not cache_dir.is_dir():
                self._send_json(
                    {"error": f"Cache entry not found: {cache_key}"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            shutil.rmtree(cache_dir)
            self._send_json({"ok": True, "cache_key": cache_key})

        def _load_workspace(self, *, payload: dict[str, Any]) -> None:
            path_value = payload.get("path")
            try:
                project_root = (
                    Path(path_value)
                    if isinstance(path_value, str) and path_value.strip()
                    else pick_workspace_folder()
                )
                record = registry.load_workspace(project_root=project_root, activate=True)
            except WorkspaceLoadCancelledError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            except (FileNotFoundError, RuntimeError) as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return

            self._send_json(
                {
                    "ok": True,
                    "workspace": workspace_payload(
                        record,
                        active_workspace_id=registry.active_workspace_id(),
                        selected_run_id=selected_run_id,
                    ),
                    "workspaces": self._workspace_payloads(),
                },
                status=HTTPStatus.ACCEPTED,
            )

        def _launch_workflow(self, *, workspace: WorkspaceRecord, payload: dict[str, Any]) -> None:
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
                launch_workspace, workflow_for_launch = resolve_launch_workspace(
                    registry=registry,
                    active_workspace=workspace,
                    workflow=workflow,
                )
                launch = payloads.launch_workflow_process(
                    project_root=launch_workspace.project_root,
                    workflow=workflow_for_launch,
                    config_paths=config_paths,
                    jobs=jobs,
                    cores=cores,
                    memory=memory,
                )
            except FileNotFoundError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return

            self._send_json(
                {
                    "ok": True,
                    **launch,
                    "workspace_id": launch_workspace.workspace_id,
                    "workspace_label": launch_workspace.label,
                    "workspace_changed": launch_workspace.workspace_id != workspace.workspace_id,
                    "workspace": workspace_payload(
                        launch_workspace,
                        active_workspace_id=registry.active_workspace_id(),
                        selected_run_id=selected_run_id,
                    ),
                    "workspaces": self._workspace_payloads(),
                },
                status=HTTPStatus.ACCEPTED,
            )

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


def _query_scalar(query: dict[str, list[str]], key: str) -> str | None:
    """Return the first query-string value for a key."""
    values = query.get(key)
    if not values:
        return None
    value = values[0]
    return value if value else None
