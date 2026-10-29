---
title: "Notifications"
---

Ginkgo can post a message to Slack while `ginkgo run` runs a workflow: when the
run starts, succeeds or fails, and when a task runs out of retries.
Notifications come from the `ginkgo-slack` [plugin](/ginkgo/guide/concepts/#glossary). See
[Plugins](/ginkgo/getting-started/installation/#plugins) to install it. Without
the plugin, `ginkgo run` warns that the `[notifications.slack]` settings will
not be used.

## What Ginkgo Can Notify

Ginkgo posts to Slack through an incoming webhook: a private URL that posts
into one Slack channel. No other service is supported.

You choose which events send a message:

- `run_started`: the run began.
- `run_succeeded`: the run finished with no failures.
- `run_failed`: the run ended with a failure, including failures that
  `--keep-going` or `on_failure="ignore"` let pass.
- `retry_exhausted`: a task that has retries failed on its last attempt. It is
  sent straight away, before the run ends.

A run you stop with Ctrl-C sends no message.

### What A Message Looks Like

Each message names the workflow and the run. A `run_failed` message, for
example, adds the task counts, the run's error, and each failed task with its
exit code, attempts and last log lines:

```text
Ginkgo Run Failed
Workflow: workflow/flow.py
Run: 20261028_161834_486728_8fe7e264
Failed: 2026-10-28T16:21:07.118230+00:00
Task counts: succeeded=40, cached=12, failed=1, skipped=3
Error: …

Task: align[sample_c] | Exit: 1 | Attempts: 3/3
…the last lines of the task's log…
```

## Set Up Slack Notifications

### 1. Create The Slack Webhook

You create the webhook in Slack, not in Ginkgo. The steps below follow Slack's
app settings. If Slack's menus have changed, follow Slack's own guide to
incoming webhooks.

1. Create a Slack app for your Slack workspace.
2. Open the app's settings page and go to `Incoming Webhooks`.
3. Turn on `Activate Incoming Webhooks`.
4. Click `Add New Webhook to Workspace`.
5. Choose the channel that should receive Ginkgo's messages. For a private
   channel, add the Slack app to the channel first.
6. Authorize the app and copy the webhook URL it creates.

To post to a different channel, create another webhook.

### 2. Add The Settings

Add a `[notifications.slack]` section to `ginkgo.toml`:

```toml
[notifications.slack]
enabled = true
webhook = { env = "GINKGO_SLACK_WEBHOOK" }
events = ["run_started", "run_succeeded", "run_failed", "retry_exhausted"]  # every event
log_tail_lines = 10
max_failed_tasks = 3
```

`webhook = { env = "GINKGO_SLACK_WEBHOOK" }` tells Ginkgo to read the URL from
the `GINKGO_SLACK_WEBHOOK` environment variable. Don't paste the URL itself
into `ginkgo.toml`. Ginkgo refuses it, because the URL lets anyone post to your
channel.

### 3. Export The Webhook And Run

```bash
export GINKGO_SLACK_WEBHOOK="https://hooks.slack.com/services/..."
ginkgo doctor
ginkgo run flow.py
```

`ginkgo doctor` checks the settings before your first run.

## Settings

| Setting | Meaning | Default |
| --- | --- | --- |
| `enabled` | Send notifications. | `false` |
| `webhook` | Where the webhook URL is kept, such as `{ env = "GINKGO_SLACK_WEBHOOK" }`. Required when `enabled` is `true`. | none |
| `events` | The events that send a message. | `["run_failed"]` |
| `log_tail_lines` | How many lines of a failed task's log to include, from 1 to 25. | `10` |
| `max_failed_tasks` | How many failed tasks one `run_failed` message lists, from 1 to 10. | `3` |

## If The Settings Are Wrong

An invalid setting, such as an unknown setting name, a number outside its
range, or an unset webhook secret, turns off all notifications. `ginkgo run`
warns about each problem, and `ginkgo doctor` reports it as an error. An unknown name in `events` is only a warning, and the valid events
are still sent.

## If Slack Cannot Be Reached

A Slack failure never fails your run, and adds at most about 3 seconds to it.
Ginkgo warns, and records the warning with the run, so `ginkgo runs show` and
`ginkgo report` list it.
