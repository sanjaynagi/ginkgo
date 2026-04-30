"""Slack notification tests."""

from __future__ import annotations

import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from ginkgo.runtime.notifications.notifications import parse_notification_config


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = REPO_ROOT / ".pixi" / "envs" / "default" / "bin" / "python"


def _run_cli(
    *args: str, cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


class _WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        self.server.requests.append(json.loads(body))  # type: ignore[attr-defined]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args) -> None:
        return


class _WebhookServer:
    def __init__(self) -> None:
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _WebhookHandler)
        self._server.requests = []  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}/hook"

    @property
    def requests(self) -> list[dict[str, object]]:
        return list(self._server.requests)  # type: ignore[attr-defined]

    def __enter__(self) -> _WebhookServer:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def _payload_text(payload: dict[str, object]) -> str:
    blocks = payload.get("blocks", [])
    if not isinstance(blocks, list):
        return str(payload.get("text", ""))

    fragments: list[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        text = block.get("text")
        if isinstance(text, dict):
            value = text.get("text")
            if isinstance(value, str):
                fragments.append(value)
    return "\n".join(fragments)


class TestNotificationConfig:
    def test_parse_notification_config_supports_aliases_and_secret_refs(self) -> None:
        config = parse_notification_config(
            config={
                "notifications": {
                    "slack": {
                        "enabled": True,
                        "webhook": {"env": "GINKGO_SLACK_WEBHOOK"},
                        "events": ["run_started", "run_completed", "retry_exhausted"],
                        "log_tail_lines": 50,
                        "max_failed_tasks": 20,
                    },
                }
            }
        )

        assert config.slack.enabled is True
        assert config.slack.webhook is not None
        assert config.slack.webhook.backend == "env"
        assert config.slack.webhook.name == "GINKGO_SLACK_WEBHOOK"
        # "run_completed" is an alias for "run_succeeded"; the parser normalises it.
        assert config.slack.events == frozenset(
            {"run_started", "run_succeeded", "retry_exhausted"}
        )
        # Values are capped at the schema maximums (50 → 25, 20 → 10).
        assert config.slack.log_tail_lines == 25
        assert config.slack.max_failed_tasks == 10


@pytest.mark.integration
class TestSlackNotifications:
    def test_cli_run_sends_start_and_success_notifications(self, monkeypatch) -> None:
        with _WebhookServer() as server:
            monkeypatch.setenv("GINKGO_SLACK_WEBHOOK", server.url)
            Path("ginkgo.toml").write_text(
                """
[notifications.slack]
enabled = true
webhook = { env = "GINKGO_SLACK_WEBHOOK" }
events = ["run_started", "run_completed"]
""".strip()
                + "\n",
                encoding="utf-8",
            )
            Path("workflow.py").write_text(
                """
from ginkgo import flow, task

@task()
def succeed() -> str:
    return "ok"

@flow
def main():
    return succeed()
""".strip()
                + "\n",
                encoding="utf-8",
            )

            result = _run_cli("run", "workflow.py", cwd=Path.cwd(), env=dict(os.environ))
            assert result.returncode == 0, result.stderr

            requests = server.requests
            assert len(requests) == 2
            assert "Ginkgo Run Started" in _payload_text(requests[0])
            assert "workflow.py" in _payload_text(requests[0])
            assert "/api/runs/" in _payload_text(requests[0])
            assert "Ginkgo Run Succeeded" in _payload_text(requests[1])
            assert "succeeded=1" in _payload_text(requests[1])

    def test_cli_run_sends_retry_exhausted_and_failure_notifications(self, monkeypatch) -> None:
        with _WebhookServer() as server:
            monkeypatch.setenv("GINKGO_SLACK_WEBHOOK", server.url)
            Path("ginkgo.toml").write_text(
                """
[notifications.slack]
enabled = true
webhook = { env = "GINKGO_SLACK_WEBHOOK" }
events = ["retry_exhausted", "run_failed"]
log_tail_lines = 5
""".strip()
                + "\n",
                encoding="utf-8",
            )
            Path("workflow.py").write_text(
                """
from ginkgo import flow, task

@task(retries=1)
def fail() -> str:
    raise RuntimeError("boom from task")

@flow
def main():
    return fail()
""".strip()
                + "\n",
                encoding="utf-8",
            )

            result = _run_cli("run", "workflow.py", cwd=Path.cwd(), env=dict(os.environ))
            assert result.returncode == 1

            requests = server.requests
            assert len(requests) == 2
            assert "Ginkgo Retries Exhausted" in _payload_text(requests[0])
            assert "boom from task" in _payload_text(requests[0])
            assert "Ginkgo Run Failed" in _payload_text(requests[1])
            assert "fail" in _payload_text(requests[1])

    def test_cli_run_warns_when_slack_webhook_is_unreachable(self, monkeypatch) -> None:
        monkeypatch.setenv("GINKGO_SLACK_WEBHOOK", "http://127.0.0.1:1/hook")
        Path("ginkgo.toml").write_text(
            """
[notifications.slack]
enabled = true
webhook = { env = "GINKGO_SLACK_WEBHOOK" }
events = ["run_completed"]
""".strip()
            + "\n",
            encoding="utf-8",
        )
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def succeed() -> str:
    return "ok"

@flow
def main():
    return succeed()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", cwd=Path.cwd(), env=dict(os.environ))
        assert result.returncode == 0, result.stderr
        assert "Slack notification failed:" in result.stderr
