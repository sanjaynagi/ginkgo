"""Tests for the local Ginkgo UI server."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ginkgo.runtime.provenance import RunProvenanceRecorder
from ginkgo.ui import server as ui_server
from ginkgo.ui import create_ui_server


def _start_server(*, runs_root: Path, selected_run_id: str | None = None):
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


class TestUiServer:
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

        monkeypatch.setattr(ui_server, "_launch_workflow_process", fake_launch_workflow_process)

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
        assert payload == {"ok": True, "pid": 4242, "workflow": "workflow.py"}
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
