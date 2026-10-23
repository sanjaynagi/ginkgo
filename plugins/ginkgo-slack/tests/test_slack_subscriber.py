"""Slack notification tests."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from ginkgo.core.secret import SecretRef
from ginkgo.plugins.api import EventSubscriberCheckContext, EventSubscriberConfigProblem
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo_slack.subscriber import (
    SlackNotificationConfig,
    SlackSubscriber,
    check_slack_config,
    parse_notification_config,
)
from ginkgo_slack.webhook import SlackUnreachableError, build_run_failed_payload
from tests.conftest import run_cli

_WORKFLOW = """
from ginkgo import flow, task

@task(retries=1)
def fail() -> str:
    raise RuntimeError("boom from task")

@task()
def succeed() -> str:
    return "ok"

@flow
def main():
    return {body}
"""


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
    return "\n".join(
        block["text"]["text"]
        for block in blocks
        if isinstance(block, dict) and isinstance(block.get("text"), dict)
    )


def _slack(**settings: object) -> dict[str, object]:
    return {"notifications": {"slack": {"enabled": True, **settings}}}


def _check(
    config: dict[str, object], *, environ: dict[str, str]
) -> list[EventSubscriberConfigProblem]:
    resolver = build_secret_resolver(project_root=Path.cwd(), config=config, environ=environ)
    return check_slack_config(EventSubscriberCheckContext(config=config, secret_resolver=resolver))


_WEBHOOK = {"env": "GINKGO_SLACK_WEBHOOK"}


class TestNotificationConfig:
    def test_parse_notification_config_supports_aliases_and_secret_refs(self) -> None:
        config, problems = parse_notification_config(
            config=_slack(
                webhook=_WEBHOOK,
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
                webhook=_WEBHOOK, events=["run_finished"], channel="#alerts", log_tail_lines=-3
            )
        )

        assert len(problems) == 3
        assert "'channel'" in problems[0]
        assert "'run_finished'" in problems[1]
        assert "log_tail_lines" in problems[2]

    def test_a_misspelt_event_beside_valid_ones_is_ignored_not_fatal(self) -> None:
        """RP4-4: one typo must not turn off the alerts that were spelt right."""
        settings = _slack(webhook=_WEBHOOK, events=["run_faild", "run_failed"])
        config, problems = parse_notification_config(config=settings)

        assert problems == []
        assert config.events == frozenset({"run_failed"})
        [ignored] = config.ignored_events
        assert "'run_faild', so it is ignored" in ignored
        # doctor still names it, as a warning: the run still sends.
        environ = {"GINKGO_SLACK_WEBHOOK": "http://127.0.0.1:1/"}
        assert _check(settings, environ=environ) == [
            EventSubscriberConfigProblem(message=ignored, blocking=False)
        ]

    def test_a_literal_webhook_url_is_explained(self) -> None:
        _, problems = parse_notification_config(
            config=_slack(webhook="https://hooks.slack.com/services/x")
        )

        assert len(problems) == 1
        assert 'webhook = { env = "GINKGO_SLACK_WEBHOOK" }' in problems[0]

    def test_the_check_names_an_unset_environment_variable(self) -> None:
        config = _slack(webhook=_WEBHOOK)

        [problem] = _check(config, environ={"GINKGO_SLACK_WEBHOOK": ""})
        assert "GINKGO_SLACK_WEBHOOK, which is unset or empty" in problem.message
        assert _check(config, environ={"GINKGO_SLACK_WEBHOOK": "http://127.0.0.1:1/"}) == []


def test_once_slack_is_unreachable_later_notifications_are_not_attempted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    attempts: list[dict[str, object]] = []

    def unreachable(*, webhook_url: str, payload: dict[str, object]) -> None:
        attempts.append(payload)
        raise SlackUnreachableError("Slack webhook request failed: timed out.")

    monkeypatch.setattr("ginkgo_slack.subscriber.post_slack_message", unreachable)
    warnings: list[str] = []
    subscriber = SlackSubscriber(
        config=SlackNotificationConfig(enabled=True),
        webhook_url="http://127.0.0.1:1/hook",
        store=None,  # type: ignore[arg-type]
        run_id="run-1",
        run_dir=tmp_path,
        workflow_path=tmp_path / "workflow.py",
        logger=warnings.append,
    )

    subscriber._submit(event_name="run_started", payload={"text": "a"})
    subscriber._submit(event_name="run_failed", payload={"text": "b"})
    subscriber.close()

    assert attempts == [{"text": "a"}]
    assert len(warnings) == 2
    assert "run_failed" in warnings[1] and "not sent" in warnings[1]


def test_the_failed_run_card_counts_the_tasks_a_failure_left_unrun() -> None:
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

    def _project(self, *, events: list[str], body: str = "succeed()") -> None:
        Path("ginkgo.toml").write_text(
            "[notifications.slack]\nenabled = true\n"
            'webhook = { env = "GINKGO_SLACK_WEBHOOK" }\n'
            f"events = {json.dumps(events)}\nlog_tail_lines = 5\n",
            encoding="utf-8",
        )
        Path("workflow.py").write_text(_WORKFLOW.format(body=body), encoding="utf-8")

    @pytest.mark.parametrize(
        ("events", "body", "code", "expected"),
        [
            (
                ["run_started", "run_completed"],
                "succeed()",
                0,
                [("Ginkgo Run Started", "workflow.py"), ("Ginkgo Run Succeeded", "succeeded=1")],
            ),
            (
                ["retry_exhausted", "run_failed"],
                "fail()",
                1,
                [("Ginkgo Retries Exhausted", "boom from task"), ("Ginkgo Run Failed", "fail")],
            ),
        ],
    )
    def test_cli_run_sends_the_configured_notifications(
        self, monkeypatch, events, body, code, expected
    ) -> None:
        with _WebhookServer() as server:
            monkeypatch.setenv("GINKGO_SLACK_WEBHOOK", server.url)
            self._project(events=events, body=body)

            result = run_cli("run", "workflow.py")
            assert result.returncode == code, result.stderr
            texts = [_payload_text(request) for request in server.requests]

        assert len(texts) == len(expected)
        for text, fragments in zip(texts, expected):
            assert all(fragment in text for fragment in fragments)

    def test_doctor_blocks_an_invalid_slack_table(self, monkeypatch) -> None:
        monkeypatch.delenv("GINKGO_SLACK_WEBHOOK", raising=False)
        self._project(events=["run_finished"])

        result = run_cli("doctor", "workflow.py")

        assert result.returncode == 1
        assert "notification_config" in result.stderr
        assert "'run_finished'" in result.stderr

    def test_doctor_warns_but_passes_on_an_unknown_event_beside_valid_ones(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("GINKGO_SLACK_WEBHOOK", "http://127.0.0.1:1/hook")
        self._project(events=["run_faild", "run_failed"])

        result = run_cli("doctor", "workflow.py")

        assert result.returncode == 0, result.stdout + result.stderr
        assert "'run_faild'" in result.stdout + result.stderr

    def test_cli_run_warns_when_slack_webhook_is_unreachable(self, monkeypatch) -> None:
        monkeypatch.setenv("GINKGO_SLACK_WEBHOOK", "http://127.0.0.1:1/hook")
        self._project(events=["run_completed"])

        result = run_cli("run", "workflow.py")
        assert result.returncode == 0, result.stderr
        assert "Slack notification for run_succeeded failed:" in result.stderr

        # The failure is kept with the run, not only printed while it ran.
        shown = run_cli("runs", "show")
        assert "Notice: Slack notification for run_succeeded failed:" in shown.stdout
