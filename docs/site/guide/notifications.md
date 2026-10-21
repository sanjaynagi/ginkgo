# Notifications

Ginkgo can send Slack notifications for workflow lifecycle events during
`ginkgo run`. Notifications come from the `ginkgo-slack` plugin; see
[Plugins](../getting-started/installation.md#plugins) to install it. Without it,
`ginkgo run` warns that the `[notifications.slack]` table will not be sent.

## Supported Channel

The current implementation supports Slack incoming webhooks only.

Supported notification events:

- `run_started`
- `run_succeeded` (`run_completed` is accepted as another name for it)
- `run_failed`
- `retry_exhausted`

`run_succeeded` is the success notification event. `run_failed` is the terminal
run-level failure notification. `retry_exhausted` is a task-level notification
sent when a task fails after using all configured retries.

## Configure Slack Notifications

Add notification settings to `ginkgo.toml`:

```toml
[notifications.slack]
enabled = true
webhook = { env = "GINKGO_SLACK_WEBHOOK" }
events = ["run_started", "run_succeeded", "run_failed", "retry_exhausted"]
log_tail_lines = 10
max_failed_tasks = 3
```

Then export the webhook secret before running:

```bash
export GINKGO_SLACK_WEBHOOK="https://hooks.slack.com/services/..."
ginkgo run flow.py
```

Notes:

- `webhook` must be a secret reference such as `{ env = "GINKGO_SLACK_WEBHOOK" }`,
  naming where the URL is kept. A literal URL is refused: the URL is a
  credential and does not belong in config.
- `log_tail_lines` (1 to 25, default 10) controls how many lines of task log
  tail are included in failure notifications.
- `max_failed_tasks` (1 to 10, default 3) controls how many failed tasks are
  summarized in one failure notification.
- `events` defaults to `["run_failed"]`.
- A table with an unknown setting, an out-of-range number, no valid event, or
  a webhook secret that is unset or empty sends nothing. `ginkgo run` warns with
  each problem, and `ginkgo doctor` reports them as errors before you run.
- An unknown name in `events` beside valid ones is ignored: `ginkgo run` warns
  about it and still sends the valid events, and `ginkgo doctor` reports it.
- Notification delivery is best-effort. A Slack failure warns, naming the event
  it was for, but does not fail the run, and the warning is kept with the run:
  `ginkgo runs show` and `ginkgo report` list it as a notice.

## Set Up The Slack Webhook

Slack incoming webhooks are configured in Slack, not in Ginkgo.

1. Create a Slack app for your workspace.
2. Open the app settings page and go to `Incoming Webhooks`.
3. Enable `Activate Incoming Webhooks`.
4. Click `Add New Webhook to Workspace`.
5. Choose the channel that should receive Ginkgo notifications.
6. Authorize the app and copy the generated webhook URL.
7. Store that URL in `GINKGO_SLACK_WEBHOOK` before running Ginkgo.

For private channels, the Slack app must already be allowed in that channel.

## Choosing A Slack Channel

Ginkgo does not currently support a `channel = ...` override in config.

The destination channel is controlled by the Slack webhook itself. If you want
notifications in a different channel, create a webhook for that channel and use
that webhook URL in `GINKGO_SLACK_WEBHOOK`.
