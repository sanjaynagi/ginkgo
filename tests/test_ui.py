"""Tests for the local Ginkgo UI server."""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from base64 import b64encode
from pathlib import Path
from urllib.parse import urlparse
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import yaml

from ginkgo.runtime.provenance import RunProvenanceRecorder
from ginkgo.ui import create_ui_server
from ginkgo.ui.server import payloads as server_payloads
from ginkgo.ui.server.workspaces import (
    WorkspaceRegistry,
    infer_workflow_project_root,
    resolve_launch_workspace,
    validate_workspace_root,
)


def _start_server(*, runs_root: Path, selected_run_id: str | None = None):
    runs_root.parent.mkdir(parents=True, exist_ok=True)
    server = create_ui_server(
        host="127.0.0.1",
        port=0,
        runs_root=runs_root,
        selected_run_id=selected_run_id,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    return server, thread, f"http://{host}:{port}"


def _fetch_text(url: str) -> tuple[int, str]:
    with urlopen(url) as response:  # noqa: S310 - local test server only
        return response.status, response.read().decode("utf-8")


def _fetch_json(url: str) -> tuple[int, dict]:
    status, body = _fetch_text(url)
    return status, json.loads(body)


def _fetch_json_request(request: Request) -> tuple[int, dict]:
    with urlopen(request) as response:  # noqa: S310 - local test server only
        return response.status, json.loads(response.read().decode("utf-8"))


def _open_websocket(base_url: str, *, path: str = "/ws") -> tuple[socket.socket, bytearray]:
    parsed = urlparse(base_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    client = socket.create_connection((host, port), timeout=5)
    key = b64encode(os.urandom(16)).decode("ascii")
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    client.sendall(request.encode("utf-8"))

    response = b""
    while b"\r\n\r\n" not in response:
        response += client.recv(4096)
    response, remainder = response.split(b"\r\n\r\n", 1)
    assert b"101 Switching Protocols" in response
    client.settimeout(6)
    return client, bytearray(remainder)


def _recv_exact(client: socket.socket, buffer: bytearray, size: int) -> bytes:
    while len(buffer) < size:
        buffer.extend(client.recv(4096))
    data = bytes(buffer[:size])
    del buffer[:size]
    return data


def _recv_ws_json(client: socket.socket, buffer: bytearray) -> dict:
    header = _recv_exact(client, buffer, 2)
    length = header[1] & 0x7F
    if length == 126:
        length = int.from_bytes(_recv_exact(client, buffer, 2), "big")
    elif length == 127:
        length = int.from_bytes(_recv_exact(client, buffer, 8), "big")
    payload = _recv_exact(client, buffer, length)
    return json.loads(payload.decode("utf-8"))


def _make_run(tmp_path: Path, *, run_id: str, status: str, fail: bool) -> Path:
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# workflow\n", encoding="utf-8")
    recorder = RunProvenanceRecorder(
        run_id=run_id,
        workflow_path=workflow_path,
        root_dir=tmp_path / ".ginkgo" / "runs",
        jobs=4,
        cores=4,
        params={"message": "hello"},
    )
    stdout_path, stderr_path = recorder.ensure_task(
        node_id=0, task_name="demo.write_output", env=None
    )
    stdout_path.write_text("task log line\n", encoding="utf-8")
    stderr_path.write_text("task error line\n", encoding="utf-8")
    recorder.update_task_inputs(
        node_id=0,
        task_name="demo.write_output",
        env=None,
        resolved_args={"output_path": "results/out.txt"},
        input_hashes={"output_path": {"type": "str", "sha256": "abc"}},
        cache_key="cache-key-123",
        dependency_ids=[],
        dynamic_dependency_ids=[],
    )
    recorder.update_resources(
        {
            "status": "completed",
            "scope": "process_tree",
            "sample_count": 2,
            "current": {"cpu_percent": 12.5, "rss_bytes": 1024, "process_count": 1},
            "peak": {"cpu_percent": 85.0, "rss_bytes": 4096, "process_count": 2},
            "average": {"cpu_percent": 48.0, "rss_bytes": 2048, "process_count": 1.5},
            "updated_at": "2026-03-13T00:00:00+00:00",
        }
    )
    if fail:
        recorder.mark_failed(
            node_id=0,
            task_name="demo.write_output",
            env=None,
            exc=RuntimeError("boom"),
        )
    else:
        recorder.mark_succeeded(
            node_id=0,
            task_name="demo.write_output",
            env=None,
            value="results/out.txt",
        )
    recorder.finalize(status=status, error="boom" if fail else None)
    return recorder.run_dir


def _add_notebook_artifact(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    notebook_dir = run_dir / "notebooks"
    notebook_dir.mkdir(parents=True, exist_ok=True)
    (notebook_dir / "task_0000.html").write_text(
        "<html><body>report</body></html>", encoding="utf-8"
    )
    manifest["tasks"]["task_0000"].update(
        {
            "task_type": "notebook",
            "notebook_kind": "ipynb",
            "notebook_description": "Notebook report.",
            "notebook_path": "/tmp/report.ipynb",
            "render_status": "succeeded",
            "rendered_html": "notebooks/task_0000.html",
        }
    )
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


class TestUiServer:
    def test_validate_workspace_root_accepts_pyproject_with_root_flow(
        self, tmp_path: Path
    ) -> None:
        workspace_root = tmp_path / "project"
        workspace_root.mkdir()
        (workspace_root / "pyproject.toml").write_text(
            "[project]\nname='demo'\n", encoding="utf-8"
        )
        (workspace_root / "ginkgo_workflow.py").write_text(
            "from ginkgo import flow\n\n@flow\ndef main():\n    return None\n",
            encoding="utf-8",
        )

        assert validate_workspace_root(workspace_root) == workspace_root.resolve()
        assert infer_workflow_project_root(workspace_root / "ginkgo_workflow.py") == workspace_root

    def test_resolve_launch_workspace_accepts_pyproject_root_flow(self, tmp_path: Path) -> None:
        active_root = tmp_path / "active"
        active_root.mkdir()
        (active_root / "ginkgo.toml").write_text("name = 'active'\n", encoding="utf-8")
        registry = WorkspaceRegistry(initial_project_root=active_root)
        active_workspace = registry.active_workspace()
        assert active_workspace is not None

        external_root = tmp_path / "external"
        external_root.mkdir()
        (external_root / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
        workflow_path = external_root / "ginkgo_workflow.py"
        workflow_path.write_text(
            "from ginkgo import flow\n\n@flow\ndef main():\n    return None\n",
            encoding="utf-8",
        )

        launch_workspace, workflow_label = resolve_launch_workspace(
            registry=registry,
            active_workspace=active_workspace,
            workflow=str(workflow_path),
        )

        assert launch_workspace.project_root == external_root.resolve()
        assert workflow_label == "ginkgo_workflow.py"

    def test_meta_reports_no_workspace_when_started_outside_workspace(
        self, tmp_path: Path
    ) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"

        server = create_ui_server(
            host="127.0.0.1",
            port=0,
            runs_root=runs_root,
            selected_run_id=None,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address[:2]
        base_url = f"http://{host}:{port}"

        try:
            status, payload = _fetch_json(f"{base_url}/api/meta")
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 200
        assert payload["active_workspace_id"] is None
        assert payload["active_workspace"] is None
        assert payload["project_root"] is None
        assert payload["runs_root"] is None
        assert payload["workspaces"] == []

    def test_launch_workflow_process_adds_repo_root_to_pythonpath(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        workspace_root = tmp_path / "workspace"
        workspace_root.mkdir()
        workflow_path = workspace_root / "workflow.py"
        workflow_path.write_text("from ginkgo import flow\n", encoding="utf-8")

        captured: dict[str, object] = {}

        class DummyProcess:
            pid = 9999

        def fake_popen(command, **kwargs):
            captured["command"] = command
            captured["kwargs"] = kwargs
            return DummyProcess()

        monkeypatch.setattr(server_payloads.subprocess, "Popen", fake_popen)
        monkeypatch.setenv("PYTHONPATH", "/existing/path")

        result = server_payloads.launch_workflow_process(
            project_root=workspace_root,
            workflow="workflow.py",
            config_paths=[],
            jobs=None,
            cores=None,
            memory=None,
        )

        assert result == {"pid": 9999, "workflow": "workflow.py"}
        assert captured["command"][:4] == [sys.executable, "-m", "ginkgo.cli", "run"]
        assert captured["kwargs"]["cwd"] == workspace_root
        assert captured["kwargs"]["env"]["PYTHONPATH"].split(os.pathsep)[:2] == [
            str(server_payloads.REPO_ROOT),
            "/existing/path",
        ]

    def test_root_serves_spa(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        _make_run(tmp_path, run_id="20260312_120000_deadbeef", status="succeeded", fail=False)

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            status, body = _fetch_text(f"{base_url}/")
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 200
        assert '<div id="root"></div>' in body
        assert "Ginkgo" in body
        assert "resource-monitor.js" in body

    def test_api_lists_runs(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        _make_run(tmp_path, run_id="20260312_120000_deadbeef", status="succeeded", fail=False)

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            status, payload = _fetch_json(f"{base_url}/api/runs")
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 200
        assert payload["runs"][0]["run_id"] == "20260312_120000_deadbeef"
        assert payload["runs"][0]["workflow"] == "workflow.py"
        assert payload["runs"][0]["status"] == "succeeded"

    def test_run_detail_task_detail_and_log_api(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        run_dir = _make_run(
            tmp_path, run_id="20260312_130000_deadbeef", status="failed", fail=True
        )

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            _, run_payload = _fetch_json(f"{base_url}/api/runs/{run_dir.name}")
            _, task_payload = _fetch_json(f"{base_url}/api/runs/{run_dir.name}/tasks/task_0000")
            _, log_payload = _fetch_json(f"{base_url}/api/runs/{run_dir.name}/tasks/task_0000/log")
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert run_payload["run_id"] == run_dir.name
        assert run_payload["resources"]["peak"]["rss_bytes"] == 4096
        assert run_payload["tasks"][0]["task"] == "demo.write_output"
        assert run_payload["tasks"][0]["dependency_ids"] == []
        assert task_payload["task"]["error"] == "boom"
        assert task_payload["task"]["cache_key"] == "cache-key-123"
        assert "task log line" in log_payload["stdout"]
        assert "task error line" in log_payload["stderr"]

    def test_run_payload_exposes_notebook_entries_and_html_route(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        run_dir = _make_run(
            tmp_path, run_id="20260312_130000_deadbeef", status="succeeded", fail=False
        )
        _add_notebook_artifact(run_dir)

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            _, run_payload = _fetch_json(f"{base_url}/api/runs/{run_dir.name}")
            status, html = _fetch_text(
                f"{base_url}/api/runs/{run_dir.name}/tasks/task_0000/notebook"
            )
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert run_payload["notebooks"][0]["task_key"] == "task_0000"
        assert run_payload["tasks"][0]["task_type"] == "notebook"
        assert run_payload["tasks"][0]["render_status"] == "succeeded"
        assert status == 200
        assert "<body>report</body>" in html

    def test_meta_reports_selected_run(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        _make_run(tmp_path, run_id="20260312_140000_deadbeef", status="succeeded", fail=False)

        server, thread, base_url = _start_server(
            runs_root=runs_root,
            selected_run_id="20260312_140000_deadbeef",
        )
        try:
            _, payload = _fetch_json(f"{base_url}/api/meta")
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert payload["selected_run_id"] == "20260312_140000_deadbeef"
        assert payload["latest_run_id"] == "20260312_140000_deadbeef"

    def test_cache_api_lists_and_deletes_entries(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        cache_root = tmp_path / ".ginkgo" / "cache"
        cache_entry = cache_root / "abc123"
        cache_entry.mkdir(parents=True)
        (cache_entry / "meta.json").write_text(
            json.dumps({"function": "demo.task", "timestamp": "2026-03-12T12:00:00+00:00"}),
            encoding="utf-8",
        )
        (cache_entry / "output.json").write_text("{}", encoding="utf-8")

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            _, payload = _fetch_json(f"{base_url}/api/cache")
            request = Request(
                f"{base_url}/api/cache/abc123",
                method="DELETE",
            )
            with urlopen(request) as response:  # noqa: S310 - local test server only
                delete_payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert payload["entries"][0]["cache_key"] == "abc123"
        assert delete_payload["ok"] is True
        assert not cache_entry.exists()

    def test_cache_api_clears_all_entries(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        cache_root = tmp_path / ".ginkgo" / "cache"
        for key in ("abc123", "def456"):
            entry = cache_root / key
            entry.mkdir(parents=True)
            (entry / "meta.json").write_text("{}", encoding="utf-8")

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            status, payload = _fetch_json_request(
                Request(f"{base_url}/api/cache", method="DELETE")
            )
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 200
        assert payload == {"deleted": 2, "ok": True}
        assert list(cache_root.iterdir()) == []

    def test_workflows_api_lists_flow_modules(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        package_dir = tmp_path / "demo_project"
        package_dir.mkdir()
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        workflow_path = package_dir / "workflow.py"
        workflow_path.write_text(
            "from demo_project.modules.pipeline import main\n",
            encoding="utf-8",
        )
        modules_dir = package_dir / "modules"
        modules_dir.mkdir()
        (modules_dir / "__init__.py").write_text("", encoding="utf-8")
        (modules_dir / "pipeline.py").write_text(
            "from ginkgo import flow\n\n@flow\ndef main():\n    return None\n",
            encoding="utf-8",
        )
        (tmp_path / "scripts").mkdir()
        (tmp_path / "scripts" / "helper.py").write_text(
            "print('no flow here')\n", encoding="utf-8"
        )

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            status, payload = _fetch_json(f"{base_url}/api/workflows")
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 200
        assert payload["workflows"] == [
            "demo_project/modules/pipeline.py",
            "demo_project/workflow.py",
        ]

    def test_run_api_launches_workflow_process(self, monkeypatch, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("from ginkgo import flow\n", encoding="utf-8")
        config_path = tmp_path / "ginkgo.toml"
        config_path.write_text('message = "hello"\n', encoding="utf-8")

        calls: list[dict[str, object]] = []

        def fake_launch_workflow_process(**kwargs):
            calls.append(kwargs)
            return {"pid": 4242, "workflow": kwargs["workflow"]}

        monkeypatch.setattr(
            server_payloads,
            "launch_workflow_process",
            fake_launch_workflow_process,
        )

        body = json.dumps(
            {
                "workflow": "workflow.py",
                "config_paths": ["ginkgo.toml"],
                "jobs": 4,
                "cores": 2,
                "memory": 12,
            }
        ).encode("utf-8")

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            status, payload = _fetch_json_request(
                Request(
                    f"{base_url}/api/run",
                    data=body,
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
            )
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 202
        assert payload["ok"] is True
        assert payload["pid"] == 4242
        assert payload["workflow"] == "workflow.py"
        assert payload["workspace_changed"] is False
        assert payload["workspace_label"] == tmp_path.name
        assert isinstance(payload["workspace_id"], str)
        assert payload["workspace"]["workspace_id"] == payload["workspace_id"]
        assert payload["workspace"]["project_root"] == str(tmp_path)
        assert len(payload["workspaces"]) == 1
        assert calls == [
            {
                "project_root": tmp_path,
                "workflow": "workflow.py",
                "config_paths": ["ginkgo.toml"],
                "jobs": 4,
                "cores": 2,
                "memory": 12,
            }
        ]

    def test_run_api_loads_external_workflow_workspace(self, monkeypatch, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        (tmp_path / "workflow.py").write_text("from ginkgo import flow\n", encoding="utf-8")

        other_root = tmp_path / "external_workspace"
        other_root.mkdir()
        (other_root / "ginkgo.toml").write_text('name = "external"\n', encoding="utf-8")
        external_workflow = other_root / "workflow.py"
        external_workflow.write_text("from ginkgo import flow\n", encoding="utf-8")

        calls: list[dict[str, object]] = []

        def fake_launch_workflow_process(**kwargs):
            calls.append(kwargs)
            return {"pid": 5252, "workflow": kwargs["workflow"]}

        monkeypatch.setattr(
            server_payloads,
            "launch_workflow_process",
            fake_launch_workflow_process,
        )

        body = json.dumps({"workflow": str(external_workflow), "config_paths": []}).encode("utf-8")

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            status, payload = _fetch_json_request(
                Request(
                    f"{base_url}/api/run",
                    data=body,
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
            )
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 202
        assert payload["ok"] is True
        assert payload["workspace_changed"] is True
        assert payload["workspace_label"] == "external_workspace"
        assert payload["workspace"]["workspace_id"] == payload["workspace_id"]
        assert payload["workspace"]["project_root"] == str(other_root)
        assert len(payload["workspaces"]) == 2
        assert calls == [
            {
                "project_root": other_root,
                "workflow": "workflow.py",
                "config_paths": [],
                "jobs": None,
                "cores": None,
                "memory": None,
            }
        ]

    def test_missing_run_returns_404(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            try:
                urlopen(f"{base_url}/api/runs/missing-run")  # noqa: S310 - local test server only
            except HTTPError as exc:
                status = exc.code
                body = exc.read().decode("utf-8")
            else:  # pragma: no cover
                raise AssertionError("Expected HTTPError for missing run")
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert status == 404
        assert "Run not found: missing-run" in body

    def test_workspace_load_and_activate_updates_active_workspace(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        primary_workflow = tmp_path / "workflow.py"
        primary_workflow.write_text("from ginkgo import flow\n", encoding="utf-8")

        other_root = tmp_path / "second"
        other_root.mkdir()
        (other_root / "ginkgo.toml").write_text('name = "second"\n', encoding="utf-8")
        other_package = other_root / "second_project"
        other_package.mkdir()
        (other_package / "__init__.py").write_text("", encoding="utf-8")
        (other_package / "workflow.py").write_text(
            "from ginkgo import flow\n\n@flow\ndef main():\n    return None\n",
            encoding="utf-8",
        )

        server, thread, base_url = _start_server(runs_root=runs_root)
        try:
            load_status, load_payload = _fetch_json_request(
                Request(
                    f"{base_url}/api/workspaces/load",
                    data=json.dumps({"path": str(other_root)}).encode("utf-8"),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
            )
            _, meta_payload = _fetch_json(f"{base_url}/api/meta")
            initial_workspace = next(
                item
                for item in meta_payload["workspaces"]
                if item["project_root"] == str(tmp_path)
            )
            activate_status, activate_payload = _fetch_json_request(
                Request(
                    f"{base_url}/api/workspaces/activate",
                    data=json.dumps({"workspace_id": initial_workspace["workspace_id"]}).encode(
                        "utf-8"
                    ),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
            )
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert load_status == 202
        assert load_payload["workspace"]["project_root"] == str(other_root)
        assert meta_payload["active_workspace"]["project_root"] == str(other_root)
        assert len(meta_payload["workspaces"]) == 2
        assert activate_status == 200
        assert activate_payload["workspace"]["project_root"] == str(tmp_path)

    def test_websocket_emits_initial_and_run_update_events(self, tmp_path: Path) -> None:
        runs_root = tmp_path / ".ginkgo" / "runs"
        _make_run(tmp_path, run_id="20260312_120000_deadbeef", status="succeeded", fail=False)

        server, thread, base_url = _start_server(runs_root=runs_root)
        client, buffer = _open_websocket(base_url)
        try:
            connected = _recv_ws_json(client, buffer)
            meta_event = _recv_ws_json(client, buffer)
            _make_run(tmp_path, run_id="20260312_120100_feedface", status="succeeded", fail=False)

            deadline = time.time() + 6
            events: list[dict] = []
            while time.time() < deadline:
                event = _recv_ws_json(client, buffer)
                events.append(event)
                if event["type"] == "runs_updated":
                    break
        finally:
            client.close()
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()

        assert connected["type"] == "connected"
        assert meta_event["type"] == "meta"
        assert meta_event["payload"]["meta"]["active_workspace_id"] is not None
        assert any(
            event["type"] == "runs_updated"
            and any(
                run["run_id"] == "20260312_120100_feedface" for run in event["payload"]["runs"]
            )
            for event in events
        )
