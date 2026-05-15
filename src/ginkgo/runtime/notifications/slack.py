"""Slack notification transport and payload helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class SlackDeliveryError(RuntimeError):
    """Raised when a Slack webhook request cannot be delivered."""


@dataclass(frozen=True, kw_only=True)
class SlackTaskFailure:
    """Compact failed-task summary for Slack payloads."""

    task_name: str
    exit_code: int | None
    attempt: int | None = None
    max_attempts: int | None = None
    log_tail: tuple[str, ...] = ()


def post_slack_message(
    *,
    webhook_url: str,
    payload: dict[str, Any],
    timeout_seconds: float = 5.0,
) -> None:
    """Send one JSON payload to a Slack incoming webhook."""
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            status_code = getattr(response, "status", response.getcode())
    except HTTPError as exc:
        raise SlackDeliveryError(
            f"Slack webhook rejected the request with HTTP {exc.code}."
        ) from exc
    except URLError as exc:
        reason = getattr(exc, "reason", None)
        detail = str(reason) if reason else exc.__class__.__name__
        raise SlackDeliveryError(f"Slack webhook request failed: {detail}.") from exc
    except Exception as exc:  # pragma: no cover - defensive wrapper
        raise SlackDeliveryError("Slack webhook request failed.") from exc

    if not 200 <= status_code < 300:
        raise SlackDeliveryError(f"Slack webhook rejected the request with HTTP {status_code}.")


def build_run_started_payload(
    *,
    workflow_label: str,
    run_id: str,
    ts: str,
) -> dict[str, Any]:
    """Build a Slack payload for run start notifications."""
    summary_lines = [
        f"*Workflow*: `{workflow_label}`",
        f"*Run*: `{run_id}`",
        f"*Started*: `{ts}`",
    ]

    return _build_payload(
        fallback=f"Ginkgo run started: {workflow_label} ({run_id})",
        title="Ginkgo Run Started",
        body_lines=summary_lines,
    )


def build_run_succeeded_payload(
    *,
    workflow_label: str,
    run_id: str,
    ts: str,
    task_counts: dict[str, int],
) -> dict[str, Any]:
    """Build a Slack payload for successful run completion."""
    summary_lines = [
        f"*Workflow*: `{workflow_label}`",
        f"*Run*: `{run_id}`",
        f"*Completed*: `{ts}`",
        f"*Task counts*: {_format_task_counts(task_counts)}",
    ]

    return _build_payload(
        fallback=f"Ginkgo run succeeded: {workflow_label} ({run_id})",
        title="Ginkgo Run Succeeded",
        body_lines=summary_lines,
    )


def build_run_failed_payload(
    *,
    workflow_label: str,
    run_id: str,
    ts: str,
    failed_tasks: list[SlackTaskFailure],
    error: str | None,
) -> dict[str, Any]:
    """Build a Slack payload for failed run completion."""
    summary_lines = [
        f"*Workflow*: `{workflow_label}`",
        f"*Run*: `{run_id}`",
        f"*Failed*: `{ts}`",
    ]
    if error:
        summary_lines.append(f"*Error*: `{_truncate(error, limit=180)}`")

    return _build_payload(
        fallback=f"Ginkgo run failed: {workflow_label} ({run_id})",
        title="Ginkgo Run Failed",
        body_lines=summary_lines,
        failure_lines=_format_failure_lines(failed_tasks),
    )


def build_retry_exhausted_payload(
    *,
    workflow_label: str,
    run_id: str,
    ts: str,
    failed_task: SlackTaskFailure,
) -> dict[str, Any]:
    """Build a Slack payload for retry exhaustion notifications."""
    summary_lines = [
        f"*Workflow*: `{workflow_label}`",
        f"*Run*: `{run_id}`",
        f"*Task*: `{failed_task.task_name}`",
        f"*Failed*: `{ts}`",
    ]
    attempt_line = _format_attempts(failed_task)
    if attempt_line is not None:
        summary_lines.append(f"*Attempts*: `{attempt_line}`")
    if failed_task.exit_code is not None:
        summary_lines.append(f"*Exit code*: `{failed_task.exit_code}`")

    return _build_payload(
        fallback=f"Ginkgo task exhausted retries: {failed_task.task_name} ({run_id})",
        title="Ginkgo Retries Exhausted",
        body_lines=summary_lines,
        failure_lines=_format_failure_lines([failed_task]),
    )


def _build_payload(
    *,
    fallback: str,
    title: str,
    body_lines: list[str],
    failure_lines: list[str] | None = None,
) -> dict[str, Any]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": title},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(body_lines)},
        },
    ]
    if failure_lines:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(failure_lines)},
            }
        )

    return {"text": fallback, "blocks": blocks}


def _format_task_counts(task_counts: dict[str, int]) -> str:
    ordered = [
        (name, count)
        for name, count in (
            ("succeeded", task_counts.get("succeeded", 0)),
            ("cached", task_counts.get("cached", 0)),
            ("failed", task_counts.get("failed", 0)),
            ("running", task_counts.get("running", 0)),
            ("pending", task_counts.get("pending", 0)),
        )
        if count
    ]
    if not ordered:
        return "none"
    return ", ".join(f"{name}={count}" for name, count in ordered)


def _format_failure_lines(failed_tasks: list[SlackTaskFailure]) -> list[str]:
    lines: list[str] = []
    for item in failed_tasks:
        detail_parts = [f"*Task*: `{item.task_name}`"]
        if item.exit_code is not None:
            detail_parts.append(f"*Exit*: `{item.exit_code}`")
        attempt_line = _format_attempts(item)
        if attempt_line is not None:
            detail_parts.append(f"*Attempts*: `{attempt_line}`")
        lines.append(" | ".join(detail_parts))
        if item.log_tail:
            excerpt = "\n".join(_truncate(line, limit=140) for line in item.log_tail)
            lines.append(f"```{_truncate(excerpt, limit=900)}```")
    return lines


def _format_attempts(item: SlackTaskFailure) -> str | None:
    if item.attempt is None:
        return None
    if item.max_attempts is None:
        return str(item.attempt)
    return f"{item.attempt}/{item.max_attempts}"


def _truncate(value: str, *, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."
