# Notifications

Ginkgo can send Slack notifications for workflow lifecycle events during
`ginkgo run`.

## Supported Channel

The current implementation supports Slack incoming webhooks only.

Supported notification events:

- `run_started`
- `run_completed`
- `run_failed`
- `retry_exhausted`

`run_completed` is the success notification event. `run_failed` is the terminal
run-level failure notification. `retry_exhausted` is a task-level notification
sent when a task fails after using all configured retries.

## Configure Slack Notifications

Add notification settings to `ginkgo.toml`:

```toml
[notifications]
ui_base_url = "http://127.0.0.1:8000"

[notifications.slack]
enabled = true
webhook = { env = "GINKGO_SLACK_WEBHOOK" }
events = ["run_started", "run_completed", "run_failed", "retry_exhausted"]
log_tail_lines = 10
max_failed_tasks = 3
```

Then export the webhook secret before running:

```bash
export GINKGO_SLACK_WEBHOOK="https://hooks.slack.com/services/..."
ginkgo run workflow.py
```

Notes:

- `webhook` must be provided through the secrets layer as a secret reference.
- If `ui_base_url` is set, notifications include a link to the run.
- `log_tail_lines` controls how many lines of task log tail are included in
  failure notifications.
- `max_failed_tasks` controls how many failed tasks are summarized in one
  failure notification.
- Notification delivery is best-effort. Slack failures log a warning but do not
  fail the run.

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
