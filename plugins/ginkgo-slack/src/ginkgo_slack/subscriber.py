"""Slack run notifications: configuration and the delivery subscriber."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.core.secret import SecretRef
from ginkgo.plugins.api import EventSubscriberCheckContext, EventSubscriberContext
from ginkgo.plugins.registry import PluginNotFoundError
from ginkgo.runtime.events import GinkgoEvent, RunCompleted, RunStarted, TaskFailed
from ginkgo_slack.webhook import (
    SlackTaskFailure,
    build_retry_exhausted_payload,
    build_run_failed_payload,
    build_run_started_payload,
    build_run_succeeded_payload,
    post_slack_message,
)
from ginkgo.runtime.rundir import combined_log_tail
from ginkgo.runtime.environment.secrets import SecretResolutionError, SecretResolver
from ginkgo.runtime.run_summary import RunSummary, TaskSummary
from ginkgo.store.protocol import ProvenanceStore


VALID_NOTIFICATION_EVENTS = frozenset(
    {"run_started", "run_succeeded", "run_failed", "retry_exhausted"}
)
EVENT_ALIASES = {"run_completed": "run_succeeded"}

#: The settings ``[notifications.slack]`` accepts, with each integer's bounds.
_INT_SETTINGS = {"log_tail_lines": (1, 25), "max_failed_tasks": (1, 10)}
_SETTINGS = frozenset({"enabled", "webhook", "events", *_INT_SETTINGS})
_TABLE = "[notifications.slack]"
_WEBHOOK_EXAMPLE = 'webhook = { env = "GINKGO_SLACK_WEBHOOK" }'


@dataclass(frozen=True, kw_only=True)
class SlackNotificationConfig:
    """Resolved Slack notification settings.

    ``ignored_events`` holds one message per ``events`` entry that names no
    event. A misspelt entry beside valid ones is ignored rather than turning
    every notification off, so the run still sends the events it can.
    """

    enabled: bool = False
    webhook: SecretRef | None = None
    events: frozenset[str] = frozenset({"run_failed"})
    log_tail_lines: int = 10
    max_failed_tasks: int = 3
    ignored_events: tuple[str, ...] = ()


@dataclass(kw_only=True)
class SlackSubscriber:
    """Subscribe to runtime events and deliver Slack notifications."""

    config: SlackNotificationConfig
    webhook_url: str
    store: ProvenanceStore
    run_id: str
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
    _futures: list[tuple[str, Future[None]]] = field(default_factory=list, init=False, repr=False)

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
        for event_name, future in self._futures:
            try:
                future.result()
            except Exception as exc:
                self.logger(f"Slack notification for {event_name} failed: {exc}")

    def _handle_run_started(self, *, event: RunStarted) -> None:
        if "run_started" not in self.config.events:
            return
        payload = build_run_started_payload(
            workflow_label=self._workflow_label,
            run_id=event.run_id,
            ts=event.ts,
        )
        self._submit(event_name="run_started", payload=payload)

    def _handle_task_failed(self, *, event: TaskFailed) -> None:
        if "retry_exhausted" not in self.config.events:
            return

        task_failure = self._retry_exhausted_failure(task_id=event.task_id)
        if task_failure is None:
            return

        payload = build_retry_exhausted_payload(
            workflow_label=self._workflow_label,
            run_id=event.run_id,
            ts=event.ts,
            failed_task=task_failure,
        )
        self._submit(event_name=f"retry_exhausted ({task_failure.task_name})", payload=payload)

    def _handle_run_completed(self, *, event: RunCompleted) -> None:
        # A run someone interrupted needs no announcement: the person who
        # stopped it is watching the terminal it stopped in, and posting it as
        # a failure would report a deliberate act as an incident.
        if event.status == "cancelled":
            return

        if event.status == "success":
            if "run_succeeded" not in self.config.events:
                return
            payload = build_run_succeeded_payload(
                workflow_label=self._workflow_label,
                run_id=event.run_id,
                ts=event.ts,
                task_counts=event.task_counts,
            )
            self._submit(event_name="run_succeeded", payload=payload)
            return

        if "run_failed" not in self.config.events:
            return

        payload = build_run_failed_payload(
            workflow_label=self._workflow_label,
            run_id=event.run_id,
            ts=event.ts,
            task_counts=event.task_counts,
            failed_tasks=self._failed_tasks(),
            error=event.error,
        )
        self._submit(event_name="run_failed", payload=payload)

    def _submit(self, *, event_name: str, payload: dict[str, Any]) -> None:
        """Post *payload* in the background; *event_name* names it if delivery fails."""
        future = self._executor.submit(
            post_slack_message,
            webhook_url=self.webhook_url,
            payload=payload,
        )
        self._futures.append((event_name, future))

    def _summary(self) -> RunSummary:
        """Reload the run from the ledger, which the recorder has committed."""
        return RunSummary.load(self.store, self.run_id, runs_root=self.run_dir.parent)

    def _failed_tasks(self) -> list[SlackTaskFailure]:
        failed = self._summary().failed_tasks
        return [self._failure(task=task) for task in failed[: self.config.max_failed_tasks]]

    def _retry_exhausted_failure(self, *, task_id: str) -> SlackTaskFailure | None:
        task = next(
            (item for item in self._summary().tasks if item.task_key == task_id),
            None,
        )
        if task is None or task.max_attempts is None or task.max_attempts < 2:
            return None
        if task.attempts < task.max_attempts:
            return None
        return self._failure(task=task)

    def _failure(self, *, task: TaskSummary) -> SlackTaskFailure:
        return SlackTaskFailure(
            task_name=task.label,
            exit_code=task.exit_code,
            attempt=task.attempts,
            max_attempts=task.max_attempts,
            log_tail=tuple(
                combined_log_tail(
                    run_dir=self.run_dir,
                    stdout_log=task.stdout_log,
                    stderr_log=task.stderr_log,
                    lines=self.config.log_tail_lines,
                )
            ),
        )

    @property
    def _workflow_label(self) -> str:
        try:
            return str(self.workflow_path.relative_to(Path.cwd()))
        except ValueError:
            return str(self.workflow_path)


def build_slack_subscriber(context: EventSubscriberContext) -> SlackSubscriber | None:
    """Return a ready-to-use notification service when Slack is configured.

    Parameters
    ----------
    context : EventSubscriberContext
        The run being observed.

    Returns
    -------
    SlackSubscriber | None
        ``None`` when ``[notifications.slack]`` is disabled or has problems;
        each problem is reported through ``context.warn``.
    """
    config, problems = parse_notification_config(config=context.config)
    if not config.enabled:
        return None
    webhook_url = None
    if not problems:
        webhook_url, problem = _resolve_webhook(
            ref=config.webhook, resolver=context.secret_resolver
        )
        problems = [] if problem is None else [problem]
    if webhook_url is None:
        for problem in problems:
            context.warn(f"Slack notifications are off: {problem}")
        return None
    for message in config.ignored_events:
        context.warn(message)

    return SlackSubscriber(
        config=config,
        webhook_url=webhook_url,
        store=context.store,
        run_id=context.run_id,
        run_dir=context.run_dir,
        workflow_path=context.workflow_path,
        logger=context.warn,
    )


def check_slack_config(context: EventSubscriberCheckContext) -> list[str]:
    """Return every problem with ``[notifications.slack]``, for ``ginkgo doctor``.

    Parameters
    ----------
    context : EventSubscriberCheckContext
        The runtime config and the secret resolver a run would use.

    Returns
    -------
    list[str]
        One message per problem; empty when a run would send notifications as
        configured, or when they are disabled and otherwise valid.
    """
    config, problems = parse_notification_config(config=context.config)
    if config.enabled and not problems:
        _, problem = _resolve_webhook(ref=config.webhook, resolver=context.secret_resolver)
        if problem is not None:
            problems.append(problem)
    return [*problems, *config.ignored_events]


def parse_notification_config(
    *, config: Mapping[str, Any] | None
) -> tuple[SlackNotificationConfig, list[str]]:
    """Parse ``[notifications.slack]`` from the loaded config mapping.

    Parameters
    ----------
    config : Mapping[str, Any] | None
        The merged runtime config.

    Returns
    -------
    tuple[SlackNotificationConfig, list[str]]
        The settings, and one message per invalid or unknown setting. A setting
        with a problem keeps its default, so the settings are only to be used
        when there are no problems.
    """
    notifications = config.get("notifications", {}) if isinstance(config, Mapping) else {}
    raw = notifications.get("slack", {}) if isinstance(notifications, Mapping) else {}
    if not isinstance(raw, Mapping):
        return SlackNotificationConfig(), [f"{_TABLE} must be a table."]

    problems = [
        f"{_TABLE} has no setting {key!r}; the settings are {', '.join(sorted(_SETTINGS))}."
        for key in sorted(set(raw) - _SETTINGS, key=str)
    ]
    settings: dict[str, Any] = {}

    enabled = raw.get("enabled", False)
    if isinstance(enabled, bool):
        settings["enabled"] = enabled
    else:
        problems.append(f"{_TABLE} enabled must be true or false, not {enabled!r}.")

    if "events" in raw:
        events, ignored, event_problems = _parse_events(raw["events"])
        settings["events"] = events
        settings["ignored_events"] = ignored
        problems.extend(event_problems)

    for key, (minimum, maximum) in _INT_SETTINGS.items():
        if key not in raw:
            continue
        value = raw[key]
        if isinstance(value, int) and not isinstance(value, bool) and minimum <= value <= maximum:
            settings[key] = value
        else:
            problems.append(
                f"{_TABLE} {key} must be a whole number from {minimum} to {maximum}, "
                f"not {value!r}."
            )

    # A disabled table may leave the webhook out; one it gives must still be valid.
    if "webhook" in raw or enabled is True:
        webhook, problem = _parse_webhook(raw.get("webhook"))
        settings["webhook"] = webhook
        if problem is not None:
            problems.append(problem)

    return SlackNotificationConfig(**settings), problems


def _parse_events(value: Any) -> tuple[frozenset[str], tuple[str, ...], list[str]]:
    """Parse the ``events`` list.

    Returns the events, one message per entry that names no event (ignored
    while another entry is valid), and the problems that leave nothing to send.
    """
    if not isinstance(value, list):
        return (
            frozenset(),
            (),
            [f"{_TABLE} events must be a list of event names, not {value!r}."],
        )
    if not value:
        return frozenset(), (), [f"{_TABLE} events is empty, so nothing would be sent."]
    known = ", ".join(sorted({*VALID_NOTIFICATION_EVENTS, *EVENT_ALIASES}))
    parsed: set[str] = set()
    unknown: list[Any] = []
    for item in value:
        name = EVENT_ALIASES.get(item, item) if isinstance(item, str) else item
        if name in VALID_NOTIFICATION_EVENTS:
            parsed.add(name)
        else:
            unknown.append(item)
    if not parsed:
        return (
            frozenset(),
            (),
            [
                f"{_TABLE} events has no event {item!r}; the events are {known}."
                for item in unknown
            ],
        )
    ignored = tuple(
        f"{_TABLE} events has no event {item!r}, so it is ignored and the other events "
        f"are still sent; the events are {known}."
        for item in unknown
    )
    return frozenset(parsed), ignored, []


def _parse_webhook(value: Any) -> tuple[SecretRef | None, str | None]:
    """Parse the webhook's secret reference, explaining the shape when it is wrong."""
    if value is None:
        return None, f"{_TABLE} webhook is missing; add {_WEBHOOK_EXAMPLE}."
    if isinstance(value, str):
        return None, (
            f"{_TABLE} webhook must be a secret reference such as {_WEBHOOK_EXAMPLE}, "
            "naming where the URL is kept rather than the URL itself: a webhook URL "
            "is a credential and does not belong in config."
        )
    if isinstance(value, Mapping) and len(value) == 1:
        backend, name = next(iter(value.items()))
        if isinstance(backend, str) and isinstance(name, str) and backend.strip() and name.strip():
            return SecretRef(name=name.strip(), backend=backend.strip()), None
    return None, f"{_TABLE} webhook must name one secret, such as {_WEBHOOK_EXAMPLE}."


def _resolve_webhook(
    *, ref: SecretRef | None, resolver: SecretResolver | None
) -> tuple[str | None, str | None]:
    """Resolve the webhook URL, or say which secret is unset."""
    assert ref is not None
    if resolver is None:
        return None, f"{_TABLE} webhook cannot be read: no secret resolver is available."
    where = (
        f"the environment variable {ref.name}"
        if ref.backend == "env"
        else f"the {ref.backend} secret {ref.name!r}"
    )
    try:
        url = resolver.resolve(ref=ref)
    except (SecretResolutionError, PluginNotFoundError):
        url = ""
    if not url.strip():
        return None, f"{_TABLE} webhook reads {where}, which is unset or empty."
    return url, None
