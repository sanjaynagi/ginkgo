# Agent Operability

Phase 4 introduced a machine-readable operability layer for AI agents and
other programmatic clients.

## Runtime Event Protocol

The evaluator emits typed runtime events through an in-process event bus in
`ginkgo/runtime/events.py`. These events cover:

- run lifecycle
- task lifecycle
- cache hits and misses
- environment preparation
- dynamic graph expansion

This keeps runtime state changes explicit and lets multiple consumers observe
the same execution facts without duplicating scheduler logic.

## Human and Agent Output Modes

Rich CLI output and agent-mode JSONL output are separate renderings of the
same runtime event stream.

- Human operators continue to use the Rich run renderer.
- Agents can use `ginkgo run --agent` to receive one JSON event per line on
  stdout.

The legacy structured stderr task stream used by direct `evaluate(...)`
callers remains available when no event bus is attached, preserving backward
compatibility for existing tests and programmatic use.

## Structured Inspection and Diagnostics

Ginkgo exposes machine-readable post-hoc inspection and diagnostics:

- `ginkgo inspect workflow` returns a static task graph snapshot without
  execution.
- `ginkgo inspect run <run_id>` reconstructs a run snapshot from provenance.
- `ginkgo debug --json` returns failed-task diagnostics, including failure
  summaries and log tails.
- `ginkgo doctor --json` returns structured validation diagnostics.
- `ginkgo cache explain --run <run_id>` provides best-effort rerun reasons from
  cache metadata.

To support these surfaces, task provenance records structured failure
summaries and a compact typed output index alongside the existing manifest
fields.

## Runtime Notifications

Ginkgo includes a Slack notification path built on the same runtime event
stream used by CLI and agent renderers.

- Notification config is loaded from `ginkgo.toml` or explicit CLI config
  overlays, independent of whether the workflow module calls `ginkgo.config(...)`.
- Slack webhook credentials are resolved through the existing secrets resolver
  using secret references such as `{ env = "GINKGO_SLACK_WEBHOOK" }`.
- Supported events are:
  - run started
  - run completed successfully
  - run failed
  - task retry exhaustion
- Failure notifications are enriched from run provenance so they can include
  failed task names, exit codes, and truncated log tails.
- Notification dispatch is non-blocking and warning-only. Slack delivery
  failures do not affect workflow execution or provenance recording.

The implementation is intentionally narrow for now: Slack incoming webhooks are
the only supported notification channel, and channel routing is controlled by
the webhook configured in Slack rather than by a per-run channel override in
Ginkgo.
