# Notifications

Ginkgo can post a message to Slack while `ginkgo run` runs a workflow: when the
run starts, succeeds or fails, and when a task runs out of retries.
Notifications come from the `ginkgo-slack` plugin. See
[Plugins](../getting-started/installation.md#plugins) to install it. Without
the plugin, `ginkgo run` warns that the `[notifications.slack]` settings will
not be used.

## What Ginkgo Can Notify

Ginkgo sends notifications to Slack, through an incoming webhook. No other
service is supported. A Slack incoming webhook is a private URL. Anything
posted to it appears as a message in one Slack channel.

You choose which events send a message:

- `run_started`: the run began.
- `run_succeeded`: the run finished with no failures. `run_completed` is
  another name for it.
- `run_failed`: the run ended with a failure. This includes a run that
  finished with failures that `--keep-going` or `on_failure="ignore"` let
  pass.
- `retry_exhausted`: a task that has retries failed on its last attempt. It is
  sent when the task fails, before the run ends. If that failure fails the
  run, `run_failed` follows.

A run you stop with Ctrl-C sends no message.

### What A Message Looks Like

Each message names the workflow and the run, and gives the time. A
`run_succeeded` or `run_failed` message also gives the task counts. A
`run_failed` message adds the run's error, and lists the failed tasks. A
`retry_exhausted` message names the one task. For each failed task, the
message shows its exit code, its attempts, and the last lines of its log.

A `run_failed` message looks like this:

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

Each webhook posts to the one channel you chose when you created it. To post to
a different channel, create another webhook and use its URL instead.

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

If a setting is invalid, Ginkgo sends no notifications at all. These make a
setting invalid:

- an unknown setting name
- a number outside its range
- an `events` list with no valid event
- a webhook secret that is unset or empty

`ginkgo run` warns about each problem, and `ginkgo doctor` reports each one as
an error.

An unknown name in `events` next to valid ones is not fatal. Ginkgo ignores
it, warns, and still sends the valid events. `ginkgo doctor` reports it as a
warning.

## If Slack Cannot Be Reached

A Slack failure never fails your run. Ginkgo warns, naming the event the
message was for. It also records the warning with the run, and
`ginkgo runs show` and `ginkgo report` list it as a notice.

Ginkgo posts messages while the workflow runs, and waits for any that are
still being sent when the run ends. Each message waits at most 3 seconds for
Slack. If Slack can't be reached, Ginkgo skips the run's later messages and
warns for each one. So a Slack outage adds at most about 3 seconds to a run.
