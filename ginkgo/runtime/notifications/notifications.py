"""Runtime Slack notification configuration and delivery service."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from ginkgo.core.secret import SecretRef
from ginkgo.runtime.events import GinkgoEvent, RunCompleted, RunStarted, TaskFailed
from ginkgo.runtime.notifications.slack import (
    SlackTaskFailure,
    build_retry_exhausted_payload,
    build_run_failed_payload,
    build_run_started_payload,
    build_run_succeeded_payload,
    post_slack_message,
)
from ginkgo.runtime.caching.provenance import load_manifest, tail_text
from ginkgo.runtime.environment.secrets import SecretResolutionError, SecretResolver


VALID_NOTIFICATION_EVENTS = frozenset(
    {"run_started", "run_succeeded", "run_failed", "retry_exhausted"}
)
EVENT_ALIASES = {"run_completed": "run_succeeded"}


@dataclass(frozen=True, kw_only=True)
class SlackNotificationConfig:
    """Resolved Slack notification settings."""

    enabled: bool = False
    webhook: SecretRef | None = None
    events: frozenset[str] = frozenset({"run_failed"})
    log_tail_lines: int = 10
    max_failed_tasks: int = 3


@dataclass(frozen=True, kw_only=True)
class NotificationConfig:
    """Top-level notification settings."""

    ui_base_url: str | None = None
    slack: SlackNotificationConfig = field(default_factory=SlackNotificationConfig)


@dataclass(kw_only=True)
class NotificationService:
    """Subscribe to runtime events and deliver Slack notifications."""

    config: NotificationConfig
    webhook_url: str
    run_dir: Path
    workflow_path: Path
    logger: Callable[[str], None]
    _executor: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ginkgo-slack"
        ),
        init=False,
        repr=False,
    )
    _futures: list[Future[None]] = field(default_factory=list, init=False, repr=False)

    def handle(self, event: GinkgoEvent) -> None:
        """Handle one runtime event."""
        if isinstance(event, RunStarted):
            self._handle_run_started(event=event)
            return
        if isinstance(event, TaskFailed):
            self._handle_task_failed(event=event)
            return
        if isinstance(event, RunCompleted):
            self._handle_run_completed(event=event)

    def close(self) -> None:
        """Wait for outstanding deliveries and emit warnings for failures."""
        self._executor.shutdown(wait=True)
        for future in self._futures:
            try:
                future.result()
            except Exception as exc:
                self.logger(f"Slack notification failed: {exc}")

    def _handle_run_started(self, *, event: RunStarted) -> None:
        if "run_started" not in self.config.slack.events:
            return
        payload = build_run_started_payload(
            workflow_label=self._workflow_label,
            run_id=event.run_id,
            ts=event.ts,
            ui_url=self._run_ui_url,
        )
        self._submit(payload=payload)

    def _handle_task_failed(self, *, event: TaskFailed) -> None:
        if "retry_exhausted" not in self.config.slack.events:
            return

        task_failure = self._retry_exhausted_failure(task_id=event.task_id)
        if task_failure is None:
            return

        payload = build_retry_exhausted_payload(
            workflow_label=self._workflow_label,
            run_id=event.run_id,
            ts=event.ts,
            failed_task=task_failure,
            ui_url=self._run_ui_url,
        )
        self._submit(payload=payload)

    def _handle_run_completed(self, *, event: RunCompleted) -> None:
        if event.status == "success":
            if "run_succeeded" not in self.config.slack.events:
                return
            payload = build_run_succeeded_payload(
                workflow_label=self._workflow_label,
                run_id=event.run_id,
                ts=event.ts,
                task_counts=event.task_counts,
                ui_url=self._run_ui_url,
            )
            self._submit(payload=payload)
            return

        if "run_failed" not in self.config.slack.events:
            return

        payload = build_run_failed_payload(
            workflow_label=self._workflow_label,
            run_id=event.run_id,
            ts=event.ts,
            failed_tasks=self._failed_tasks(),
            ui_url=self._run_ui_url,
            error=event.error,
        )
        self._submit(payload=payload)

    def _submit(self, *, payload: dict[str, Any]) -> None:
        future = self._executor.submit(
            post_slack_message,
            webhook_url=self.webhook_url,
            payload=payload,
        )
        self._futures.append(future)

    def _failed_tasks(self) -> list[SlackTaskFailure]:
        manifest = load_manifest(self.run_dir)
        tasks = manifest.get("tasks", {})
        if not isinstance(tasks, dict):
            return []

        failed_tasks = sorted(
            (
                task
                for task in tasks.values()
                if isinstance(task, dict) and task.get("status") == "failed"
            ),
            key=lambda item: int(item.get("node_id", -1)),
        )
        return [
            _task_failure_from_manifest(
                run_dir=self.run_dir,
                task=task,
                log_tail_lines=self.config.slack.log_tail_lines,
            )
            for task in failed_tasks[: self.config.slack.max_failed_tasks]
        ]

    def _retry_exhausted_failure(self, *, task_id: str) -> SlackTaskFailure | None:
        manifest = load_manifest(self.run_dir)
        tasks = manifest.get("tasks", {})
        if not isinstance(tasks, dict):
            return None

        task = tasks.get(task_id)
        if not isinstance(task, dict):
            return None

        attempts = _int_or_none(task.get("attempt"))
        max_attempts = _int_or_none(task.get("max_attempts"))
        retries = _int_or_none(task.get("retries")) or 0
        if retries < 1:
            return None
        if attempts is None or max_attempts is None or attempts < max_attempts:
            return None
        return _task_failure_from_manifest(
            run_dir=self.run_dir,
            task=task,
            log_tail_lines=self.config.slack.log_tail_lines,
        )

    @property
    def _workflow_label(self) -> str:
        try:
            return str(self.workflow_path.relative_to(Path.cwd()))
        except ValueError:
            return str(self.workflow_path)

    @property
    def _run_ui_url(self) -> str | None:
        if self.config.ui_base_url is None:
            return None
        base = self.config.ui_base_url.rstrip("/") + "/"
        return urljoin(base, f"api/runs/{self.run_dir.name}")


def build_notification_service(
    *,
    config: Mapping[str, Any] | None,
    resolver: SecretResolver | None,
    run_dir: Path,
    workflow_path: Path,
    logger: Callable[[str], None],
) -> NotificationService | None:
    """Return a ready-to-use notification service when Slack is configured."""
    notification_config = parse_notification_config(config=config)
    slack = notification_config.slack
    if not slack.enabled:
        return None
    if slack.webhook is None:
        logger("Slack notifications are enabled but notifications.slack.webhook is missing.")
        return None
    if resolver is None:
        logger("Slack notifications are enabled but no secret resolver is available.")
        return None

    try:
        webhook_url = resolver.resolve(ref=slack.webhook)
    except SecretResolutionError:
        logger(
            "Slack notifications are enabled but the configured webhook secret could not be resolved."
        )
        return None

    return NotificationService(
        config=notification_config,
        webhook_url=webhook_url,
        run_dir=run_dir,
        workflow_path=workflow_path,
        logger=logger,
    )


def parse_notification_config(*, config: Mapping[str, Any] | None) -> NotificationConfig:
    """Parse notifications from the loaded config mapping."""
    notifications = config.get("notifications", {}) if isinstance(config, Mapping) else {}
    if not isinstance(notifications, Mapping):
        return NotificationConfig()

    raw_ui_base_url = notifications.get("ui_base_url")
    ui_base_url = str(raw_ui_base_url).strip() if isinstance(raw_ui_base_url, str) else None
    if ui_base_url == "":
        ui_base_url = None

    raw_slack = notifications.get("slack", {})
    if not isinstance(raw_slack, Mapping):
        return NotificationConfig(ui_base_url=ui_base_url)

    enabled = bool(raw_slack.get("enabled", False))
    raw_events = raw_slack.get("events")
    events = _parse_notification_events(raw_events)
    log_tail_lines = _bounded_int(
        raw_slack.get("log_tail_lines"), default=10, minimum=1, maximum=25
    )
    max_failed_tasks = _bounded_int(
        raw_slack.get("max_failed_tasks"), default=3, minimum=1, maximum=10
    )

    return NotificationConfig(
        ui_base_url=ui_base_url,
        slack=SlackNotificationConfig(
            enabled=enabled,
            webhook=_parse_secret_ref(raw_slack.get("webhook")),
            events=events,
            log_tail_lines=log_tail_lines,
            max_failed_tasks=max_failed_tasks,
        ),
    )


def _parse_notification_events(value: Any) -> frozenset[str]:
    if not isinstance(value, list):
        return frozenset({"run_failed"})

    parsed: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        normalized = EVENT_ALIASES.get(item.strip(), item.strip())
        if normalized in VALID_NOTIFICATION_EVENTS:
            parsed.add(normalized)

    return frozenset(parsed or {"run_failed"})


def _parse_secret_ref(value: Any) -> SecretRef | None:
    if not isinstance(value, Mapping):
        return None

    for backend, secret_name in value.items():
        if isinstance(backend, str) and isinstance(secret_name, str):
            backend_name = backend.strip()
            secret_value = secret_name.strip()
            if backend_name and secret_value:
                return SecretRef(name=secret_value, backend=backend_name)
    return None


def _task_failure_from_manifest(
    *,
    run_dir: Path,
    task: Mapping[str, Any],
    log_tail_lines: int,
) -> SlackTaskFailure:
    return SlackTaskFailure(
        task_name=str(task.get("task", "unknown")),
        exit_code=_int_or_none(task.get("exit_code")),
        attempt=_int_or_none(task.get("attempt")),
        max_attempts=_int_or_none(task.get("max_attempts")),
        log_tail=tuple(_combined_log_tail(run_dir=run_dir, task=task, lines=log_tail_lines)),
    )


def _combined_log_tail(*, run_dir: Path, task: Mapping[str, Any], lines: int) -> list[str]:
    stdout_rel = task.get("stdout_log")
    stderr_rel = task.get("stderr_log")

    combined: list[str] = []
    if isinstance(stdout_rel, str):
        combined.extend(tail_text(run_dir / stdout_rel, lines=lines))
    if isinstance(stderr_rel, str):
        combined.extend(tail_text(run_dir / stderr_rel, lines=lines))
    return combined[-lines:]


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    if not isinstance(value, int):
        return default
    return max(minimum, min(maximum, value))


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) else None
