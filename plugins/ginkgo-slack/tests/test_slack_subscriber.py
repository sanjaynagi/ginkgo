"""Slack notification tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from ginkgo.core.secret import SecretRef
from ginkgo.plugins.api import EventSubscriberCheckContext
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo_slack.subscriber import check_slack_config, parse_notification_config
from ginkgo_slack.webhook import build_run_failed_payload


def _run_cli(
    *args: str, cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ginkgo.cli", *args],
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


def _slack(**settings: object) -> dict[str, object]:
    return {"notifications": {"slack": settings}}


def _check(config: dict[str, object], *, environ: dict[str, str]) -> list[str]:
    resolver = build_secret_resolver(project_root=Path.cwd(), config=config, environ=environ)
    return check_slack_config(EventSubscriberCheckContext(config=config, secret_resolver=resolver))


class TestNotificationConfig:
    def test_parse_notification_config_supports_aliases_and_secret_refs(self) -> None:
        config, problems = parse_notification_config(
            config=_slack(
                enabled=True,
                webhook={"env": "GINKGO_SLACK_WEBHOOK"},
                events=["run_started", "run_completed", "retry_exhausted"],
                log_tail_lines=25,
                max_failed_tasks=10,
            )
        )

        assert problems == []
        assert config.webhook == SecretRef(name="GINKGO_SLACK_WEBHOOK", backend="env")
        # "run_completed" is an alias for "run_succeeded"; the parser normalises it.
        assert config.events == frozenset({"run_started", "run_succeeded", "retry_exhausted"})
        assert (config.log_tail_lines, config.max_failed_tasks) == (25, 10)

    def test_every_invalid_setting_is_named(self) -> None:
        _, problems = parse_notification_config(
            config=_slack(
                enabled=True,
                webhook={"env": "GINKGO_SLACK_WEBHOOK"},
                events=["run_finished"],
                channel="#alerts",
                log_tail_lines=-3,
            )
        )

        assert len(problems) == 3
        assert "'channel'" in problems[0]
        assert "'run_finished'" in problems[1]
        assert "log_tail_lines" in problems[2]

    def test_a_literal_webhook_url_is_explained(self) -> None:
        _, problems = parse_notification_config(
            config=_slack(enabled=True, webhook="https://hooks.slack.com/services/x")
        )

        assert len(problems) == 1
        assert 'webhook = { env = "GINKGO_SLACK_WEBHOOK" }' in problems[0]

    def test_the_check_names_an_unset_environment_variable(self) -> None:
        config = _slack(enabled=True, webhook={"env": "GINKGO_SLACK_WEBHOOK"})

        assert _check(config, environ={"GINKGO_SLACK_WEBHOOK": ""}) == [
            "[notifications.slack] webhook reads the environment variable "
            "GINKGO_SLACK_WEBHOOK, which is unset or empty."
        ]
        assert _check(config, environ={"GINKGO_SLACK_WEBHOOK": "http://127.0.0.1:1/"}) == []


@pytest.mark.integration
class TestFailedRunCard:
    def test_the_card_counts_the_tasks_a_failure_left_unrun(self) -> None:
        """Three failures beside five thousand skips is a different run."""
        payload = build_run_failed_payload(
            workflow_label="flow.py",
            run_id="run_1",
            ts="2026-09-02T09:00:00+00:00",
            task_counts={"succeeded": 4997, "failed": 3, "skipped": 4997},
            failed_tasks=[],
            error="3 tasks failed; the run continued under its failure policy.",
        )

        text = _payload_text(payload)
        assert "failed=3" in text
        assert "skipped=4997" in text


class TestSlackNotifications:
    @pytest.fixture(autouse=True)
    def _run_in_tmp_path(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Each test writes a workflow and config into the working directory.
        monkeypatch.chdir(tmp_path)

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

    def test_doctor_reports_an_invalid_slack_table(self, monkeypatch) -> None:
        monkeypatch.delenv("GINKGO_SLACK_WEBHOOK", raising=False)
        Path("ginkgo.toml").write_text(
            """
[notifications.slack]
enabled = true
webhook = { env = "GINKGO_SLACK_WEBHOOK" }
events = ["run_finished"]
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

        result = _run_cli("doctor", "workflow.py", cwd=Path.cwd(), env=dict(os.environ))

        assert result.returncode == 1
        assert "notification_config" in result.stderr
        assert "'run_finished'" in result.stderr

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

        # The failure is kept with the run, not only printed while it ran.
        shown = _run_cli("runs", "show", cwd=Path.cwd(), env=dict(os.environ))
        assert "Notice: Slack notification failed:" in shown.stdout
