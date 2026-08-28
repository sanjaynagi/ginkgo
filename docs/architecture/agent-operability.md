# Agent Operability

Phase 4 introduced a machine-readable operability layer for AI agents and
other programmatic clients.

## Runtime Event Protocol

The evaluator emits typed runtime events through an in-process event bus in
`ginkgo/runtime/events.py`. These events cover:

- run lifecycle — `RunStarted`, `RunValidated`, `RunResourcesSampled`,
  `RunCompleted`
- task lifecycle — `TaskPlanned`, `TaskStarted`, `TaskRunning`, `TaskRetrying`,
  `TaskCompleted`, `TaskFailed`
- cache hits and misses — `TaskCacheHit`, `TaskCacheMiss`
- environment preparation — `EnvPrepare*`
- dynamic graph expansion — `GraphNodeRegistered`, `GraphExpanded`
- phase timings — `PhaseTimed`, one per named phase, of the run when it carries
  no `task_id` and of the task when it does
- open-ended facts about a task — `TaskAnnotated`, whose `fields` carry things
  with no lifecycle of their own: a container image digest, a copied lockfile,
  remote access statistics, notebook artefact pointers, a sub-run id
- assets materialised — `AssetMaterialized`

This keeps runtime state changes explicit and lets multiple consumers observe
the same execution facts without duplicating scheduler logic.

The protocol is now also the storage format: the same events are the ledger at
`.ginkgo/ginkgo.db`, and every projection the CLI reads is derived from them
(see [Provenance Store](store.md)). An event a renderer can show is an event
`ginkgo.query` can answer questions about. `TaskLog` is the one event that is
not stored — log chunks are bytes, and the log files already hold them.

## Human and Agent Output Modes

Rich CLI output and agent-mode JSONL output are separate renderings of the
same runtime event stream.

- Human operators continue to use the Rich run renderer.
- Agents use `ginkgo run --agent-output` to receive one JSON event per line on stdout.
- `ginkgo run --agent-output --verbose` extends the JSONL stream with per-task log
  output, which is omitted from the default agent stream.

Environment preparation is visible in both renderings. `EnvPrepareStarted`,
`EnvPrepareCompleted`, and `EnvPrepareFailed` reach agents verbatim in the JSONL
stream; the Rich renderer maps them onto the task status cell, showing
`preparing env` while `pixi install` runs and then returning the row to
`waiting` (prepared) or `failed` (install failed). Every started preparation is
closed by exactly one of the two outcome events, so neither renderer is left
with a task stuck mid-preparation.

Preparation time is tracked separately from task duration: the row clock starts
when the task itself starts, and the accumulated preparation time is reported
once in the run summary when it is material. A slow or failed first run is then
attributed to environment installation rather than to the workflow.

The legacy structured stderr task stream used by direct `evaluate(...)`
callers remains available when no event bus is attached, preserving backward
compatibility for existing tests and programmatic use.

## Structured Inspection and Diagnostics

Ginkgo exposes machine-readable post-hoc inspection and diagnostics:

- `ginkgo inspect workflow` returns a static task graph snapshot without
  execution.
- `ginkgo inspect run <run_id>` returns a run snapshot from the ledger, for a
  live run as readily as a finished one.
- `ginkgo debug --json` returns failed-task diagnostics, including failure
  summaries and log tails.
- `ginkgo doctor --json` returns structured validation diagnostics.
- `ginkgo cache explain <run_id>` provides best-effort rerun reasons from
  cache metadata, naming the cache-key components that differ from the previous
  entry (`inputs.<parameter>`, `source_hash`, `env_hash.pixi_lock`, …) under the
  summary reason. `--run <run_id>` remains accepted as an alias.

To support these surfaces, the ledger records structured failure summaries and
a compact typed output index against each task.

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
