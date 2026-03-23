# Ginkgo Implementation Plan

This document describes work that has not yet been implemented. Phases are
grouped into dependency tiers — each tier builds on the foundations established
by the previous one.

Each phase is independently testable and follows the same structure:

- Goal
- Deliverables
- Key design points
- Validation

## Dependency Tiers

```
Tier 1 (foundations):   Phase 2  Cache Integrity
                        Phase 3  Output Directory Control
                        Phase 13 Secrets and Credentials

Tier 2 (build on T1):  Phase 4  Agent Workflow Tooling      ← builds on Phase 2
                        Phase 5  Notebook Tasks
                        Phase 6  Remote Artifact Store       ← builds on Phase 2
                        Phase 10 Alerts and Notifications    ← builds on Phase 13

Tier 3 (build on T2):  Phase 7  Asset Catalog and Lineage   ← builds on Phases 2, 6
                        Phase 8  Versioned DataFrame Assets  ← builds on Phases 2, 6, 7
                        Phase 9  Model Assets and Registry   ← builds on Phases 2, 6, 7

Tier 4 (build on T3):  Phase 12 Workflow Publishing          ← builds on Phases 3, 5, 7
                        Phase 14 Kubernetes / Batch Executor  ← builds on Phases 6, 13
                        Phase 15 Workflow Composition         ← builds on Phase 13

Cross-cutting:          Phase 1  Remaining Hardening and UI Polish
                        Phase 11 Public Documentation
```

### Cross-Phase Integration Notes

The following integration points span multiple phases. Each upstream phase
should design its contracts with these downstream consumers in mind.

- **Artifact identity is the shared spine.** Phase 2's content-addressed
  `artifact_id` (`<sha256>.<ext>`) is consumed directly by Phases 6, 7, 8, and
  9. No phase should introduce a separate identity scheme.

- **Run manifest as the common metadata surface.** Phases 1 (benchmarks), 2
  (artifact IDs), 3 (outdir paths), 5 (notebook HTML paths), and 7 (asset
  keys) all enrich the same run manifest. Each phase should extend the manifest
  rather than storing metadata in a side channel, so Phase 12 (Publishing) can
  assemble a complete bundle from the manifest alone.

- **Phase 2 → Phase 4.** Cache explanation tooling (Phase 4) should report
  against Phase 2's richer cache key structure — distinguishing source hash
  changes, version bumps, input changes, and env lock changes as separate
  rerun reasons.

- **Phase 3 → Phase 12.** Phase 3's outdir manifest entries should record
  enough metadata (logical output ID, resolved path, artifact ID) that Phase 12
  can locate and bundle outputs without filesystem scanning.

- **Phase 5 → Phase 12.** Phase 5 should store rendered notebook HTML at a
  stable, discoverable path convention so Phase 12 can collect it from the
  manifest without heuristics.

- **Phase 13 → Phase 10.** Phase 10 (Alerts) requires credential resolution
  for webhook URLs and SMTP passwords. Phase 13 (Secrets) must land first or
  concurrently. Phase 10 should consume credentials exclusively through the
  secrets resolver, not through its own config mechanism.

- **Phase 6 → Phase 14.** Remote execution (Phase 14) is a hard dependency on
  Phase 6 — a K8s job cannot read from a local `.ginkgo/cache/`. Phase 6 must
  be complete and stable before Phase 14 work begins.

- **Phase 4 → Phase 11.** Phase 4's structured inspection output, DAG export,
  and doctor commands should be documented as first-class reference material in
  Phase 11.

- **Phase 1 benchmarks → Phases 7, 12.** Phase 1's benchmark data should be
  recorded in run provenance in a structured format so Phase 7 (Asset Catalog)
  can surface it as asset metadata and Phase 12 (Publishing) can include it in
  bundles without parsing benchmark files.

---

## Tier 1 — Foundations

### Phase 2 — Cache Integrity

**Goal:** Close two silent cache corruption gaps — task source code changes going undetected, and file outputs being mutable without the cache knowing — and in doing so establish a formal `ArtifactStore` abstraction that later phases (remote storage, versioned assets, model registry) will extend.

#### Deliverables

- Include a SHA-256 hash of the task function's source code in the cache key, so any change to the function body automatically invalidates prior cache entries without requiring a manual `version=` bump.
- Retain `version=` as an explicit override for cases where the user wants to force invalidation independently of source changes.
- Raise a clear error at task registration time if `inspect.getsource()` cannot extract the source (e.g. dynamically-defined functions), rather than silently falling back.
- Extract the artifact storage logic from `cache.py` into a formal `ArtifactStore` abstraction with a `LocalArtifactStore` backend. The interface must cover:
  - `store(src_path) → artifact_id` — copies bytes in, returns a stable content-addressed identity
  - `retrieve(artifact_id, dest_path)` — materialises the artifact at a destination path (via symlink for local, staging copy for remote)
  - `exists(artifact_id) → bool`
  - `delete(artifact_id)`
- Artifact identity is a content-addressed string: `<sha256>.<ext>`. This scheme is the stable contract that all later phases depend on — it maps directly to object-store keys in Phase 6 and snapshot IDs in Phase 8.
- `LocalArtifactStore.store()` copies the output bytes into `.ginkgo/cache/<cache-key>/artifacts/<artifact_id>`, sets them read-only (`chmod 444` for files, recursively for directory contents), then calls `retrieve()` to place a symlink at the declared output path.
- On cache hit, `retrieve()` validates the output path is a symlink pointing to the correct artifact:
  - Correct symlink: serve from cache.
  - Missing symlink: recreate it silently from the intact cache artifact.
  - Path exists but is a regular file or points elsewhere: treat as external modification, invalidate, re-execute.
- Record `artifact_id` values explicitly in `meta.json` alongside relative paths, so Phase 6 can use the same identity as remote object keys without re-deriving them.
- Ensure `ginkgo cache prune` restores write permissions on artifacts before deletion to avoid `PermissionError` during cleanup.

#### Key design points

- The `ArtifactStore` interface is the key output of this phase for future phases. The local backend is the only implementation here, but the interface must be designed so Phase 6 can add a remote backend without touching the cache or evaluator.
- The cache becomes the source of truth for file and folder outputs. Attempting to write to a symlinked output fails with `PermissionError`, making corruption loud rather than silent.
- `retrieve()` is the single point where "how an artifact becomes available at a path" is decided — locally this means a symlink; remotely this means a staged download. No other code should make that decision.
- Source hashing invalidates the cache on any source change, including whitespace and comments. This is intentional: correctness over convenience.
- Source hashing covers only the top-level task function. Changes to helper functions called by a task are not detected. This is a known limitation and should be documented.
- Symlinking the directory as a whole (rather than individual files within it) keeps folder output handling consistent with file handling.
- All existing cache entries are invalidated on first run after this change due to the new `source_hash` field in the key. This is unavoidable and a one-time cost.

#### Validation

- Modify a task function body without bumping `version=` and assert the next run is a cache miss.
- Assert that attempting to write to a symlinked output file raises `PermissionError`.
- Delete a symlinked output and replace it with a regular file; assert the next run is a cache miss.
- Delete a symlinked output without replacing it; assert the symlink is silently recreated from the cache artifact on the next run.
- Assert `ginkgo cache prune` removes read-only artifacts without errors.
- Assert `ginkgo doctor` reports a clear error for tasks whose source cannot be extracted.
- Assert `meta.json` records explicit `artifact_id` values for all file and folder outputs.

---

### Phase 3 — Output Directory Control

**Goal:** Give users explicit control over where workflow outputs land, and establish the output-routing foundation that Phase 12 (Publishing) will build on.

**Downstream consumers:** Phase 12 reads outdir manifest entries to locate and bundle outputs.

#### Deliverables

- Add a top-level `--outdir <path>` flag to `ginkgo run` that redirects all declared task output files and folders to the specified directory, appending each file under a stable, collision-free subdirectory structure (e.g. `<outdir>/<run-id>/<task-id>/`).
- Ensure tasks that do not declare their outputs explicitly are unaffected; `--outdir` applies only to declared outputs.
- Record the resolved outdir path in the run manifest so downstream consumers and `ginkgo publish` can locate outputs without scanning the filesystem. Each output entry must include the logical output identifier, the resolved filesystem path, and the `artifact_id` from Phase 2.
- Expose outdir configuration in the Ginkgo project config as a persistent default so users do not need to pass `--outdir` on every invocation.
- Ensure the UI run detail view links to outputs correctly whether outputs are in the default cache layout or a custom outdir.

#### Key design points

- `--outdir` should not change task cache semantics: cache keys remain content-addressed regardless of where the output bytes land.
- The layout inside outdir must be deterministic and human-navigable without tooling.
- When `--outdir` is set, the run manifest must record both the logical output identifier and the resolved filesystem path so nothing is lost if the outdir is later moved.
- This feature is intentionally simple: it routes outputs to a flat, readable directory. It is not a remote artifact store (that is Phase 6).

#### Validation

- Run a workflow with `--outdir ./results` and assert all declared outputs appear under the expected `<run-id>/<task-id>/` subdirectory.
- Assert cache hit behavior is unchanged when the same workflow is re-run with `--outdir` pointing to a different location.
- Assert the run manifest records the resolved outdir paths for each task.
- Assert a persistent outdir default in project config is respected when `--outdir` is not passed on the CLI.
- Assert the UI links to outputs correctly for both default-layout and outdir-layout runs.

---

### Phase 13 — Secrets and Credentials Management

**Goal:** Provide a first-class, auditable mechanism for supplying secrets to workflows without embedding credentials in code, config files, or provenance records.

**Downstream consumers:** Phase 10 (Alerts) resolves channel credentials through the secrets system. Phase 14 (K8s Executor) resolves remote backend credentials. Phase 15 (Composition) passes secrets through sub-workflow call boundaries.

#### Deliverables

- Define a `secret` reference type that tasks can declare as an input alongside ordinary parameters.
- Add a secrets resolver layer with pluggable backends, starting with:
  - environment variable pass-through (zero-config, for local use)
  - `.env` file sourcing with explicit opt-in
  - future-ready interface for external vaults (e.g. HashiCorp Vault, AWS Secrets Manager)
- Ensure secrets are never written to:
  - run manifests or provenance records
  - cache keys (use a stable secret identifier instead of the secret value)
  - log output (redact on output, not just on storage)
- Add a `ginkgo secrets` CLI group with subcommands to:
  - list declared secrets across a workflow
  - validate that required secrets are resolvable in the current environment
- Extend `ginkgo doctor` to report missing or unresolvable secret declarations.

#### Key design points

- Secret values must not flow through the task DAG as ordinary Python values; they should be injected at execution time, after the DAG is constructed.
- Cache key stability requires that secret identity (name/path) be used for invalidation, not the secret value itself, so rotating a credential does not unnecessarily invalidate cache entries.
- The resolver backend should be swappable via config so teams can move from env-var sourcing to a vault without changing task code.

#### Validation

- Assert that a task declaring a secret receives the correct runtime value and that the value never appears in the run manifest, log output, or cache key.
- Assert `ginkgo secrets validate` reports resolution failures for missing env vars and passes for correctly configured secrets.
- Assert rotating a secret value (same name, new value) does not invalidate the cache entry for an otherwise unchanged task.
- Assert `ginkgo doctor` flags workflows with declared but unresolvable secrets before execution begins.

---

## Tier 2 — Build on Foundations

### Phase 4 — Agent Workflow Tooling

**Goal:** Treat Ginkgo as an event-emitting state machine and make agents first-class consumers — alongside the Rich renderer, provenance writer, and UI broadcaster. Humans use Rich. Agents use JSONL events. Both are generated from the same internal events.

**Depends on:** Phase 2 (cache key structure for cache explanation).

**Detailed design:** [`docs/phase5-agent-tooling-plan.md`](phase5-agent-tooling-plan.md)

#### Deliverables

**4A — Internal Event Bus:**

- Add a typed, in-process event bus (`ginkgo/runtime/events.py`) that the evaluator emits to at each state transition. All downstream consumers — Rich renderer, JSONL sink, provenance writer, UI WebSocket — subscribe to this bus rather than generating output independently.
- Event families: run lifecycle (`run_started`, `run_validated`, `run_completed`), graph structure (`graph_node_registered`, `graph_edge_registered`, `graph_expanded`), task lifecycle (`task_ready`, `task_blocked`, `task_started`, `task_completed`, `task_failed`), cache (`task_cache_hit`, `task_cache_miss`), retry (`task_retrying`), environment (`env_prepare_started`, `env_prepare_completed`), resource (`scheduler_idle`).
- Every event is a versioned frozen dataclass carrying `v` (protocol version), `ts` (ISO 8601 UTC), `event` (type name), `run_id`. Protocol is append-only within a major version.
- Extract the evaluator's current inline Rich output into a `RichEventRenderer` handler that subscribes to the bus and reproduces the same human output as today.

**4B — `ginkgo run --agent`: JSONL Event Stream:**

- Add an `--agent` flag that switches stdout from Rich to JSONL — one JSON object per line. No Rich output anywhere; agents own the process.
- `--agent --verbose` also includes `task_log` events (stdout/stderr chunks). Default is state events only.

**4C — Typed Failure Classification and Artifact Index:**

- Enrich `task_failed` events with structured failure metadata: `kind` (taxonomy: `user_code_error`, `shell_command_error`, `missing_input`, `output_validation_error`, `environment_error`, `resource_exhaustion`, `serialization_error`, `cache_error`, etc.), `retryable` flag, `locus`, and `suggested_actions`.
- Enrich `task_completed` events with a typed artifact index: each output carries `name`, `type`, `path`, and type-specific metadata (`shape`/`dtype` for arrays, `format` for DataFrames).
- `run_completed` includes task counts by status, cache reuse stats, failed task roots, and resource bottleneck summary.

**4D — `ginkgo inspect`: Static and Run-Based Introspection:**

- `ginkgo inspect workflow` — load a workflow module, build the expression tree without executing, emit the static graph as JSON (tasks, edges, envs, resource declarations).
- `ginkgo inspect run <run_id>` — emit a complete run snapshot: status, per-task status/duration/cache decision/failure summary, dynamic expansions, and output artifact index. This gives agents a stateless recovery path without the live stream.

**4E — `ginkgo doctor`: Workflow Linter and Diagnostics:**

- Extract validation logic from the evaluator into a shared `WorkflowDiagnostics` class so runtime and linter share the same checks.
- Checks: flow discovery (error), env resolution (error), nested/closure task bodies (error), `shell()` without explicit `output=` (warning), unquoted variable interpolation (warning), `file`/`folder` annotation mismatches (warning), cycle detection (error), missing config keys (warning).
- Output: Rich table by default, `--json` for structured diagnostic array.

**4F — Cache Explanation:**

- Live: `task_cache_hit` and `task_cache_miss` events carry the cache key component that caused the miss.
- Post-hoc: `ginkgo cache explain --run <run_id>` compares per-task cache key components against stored `meta.json` using Phase 2's `build_cache_key()` function. Rerun reason categories: `all_inputs_match` (cached), `no_prior_entry`, `source_hash_changed`, `version_bump`, `env_lock_changed`, `input_changed` (names the changed inputs).

**4G — `ginkgo init` Agent Bootstrapping:**

- Scaffold `.claude/commands/` with Ginkgo-aware agent skills: `ginkgo-run.md`, `ginkgo-debug.md`, `ginkgo-add-task.md`, `ginkgo-inspect.md`, `ginkgo-doctor.md`. Skills are thin prompts referencing the CLI.
- Create `workflow/notebooks/` with a starter `.ipynb` for prototyping.
- `ginkgo init --force` upgrades skills idempotently.

**DAG export:**

- `ginkgo export --format mermaid` produces a valid Mermaid graph from the expression tree.

#### Key design points

- The event bus is the architectural spine — all other deliverables are consumers. The evaluator emits events; renderers, sinks, and the UI subscribe.
- Human Rich output and agent JSONL output are generated from the same events. No output drift.
- Failure classification uses exception types and exit codes the evaluator already catches — this is dispatch, not heuristic guessing.
- Cache explanation compares using `build_cache_key()`, not by parsing key strings.
- Diagnostics are shared between `ginkgo doctor` and the runtime evaluator so checks stay in sync.
- Agent skills are kept thin (prompts referencing CLI commands) so they do not go stale.

#### Implementation Sequence

```
5A (event bus) ──→ 5B (--agent JSONL) ──→ 5C (failure classification + artifacts)
                          │
                          ├──→ 5D (inspect: static + run snapshot)
                          │
                          └──→ 5F (cache explain, live + post-hoc)

5E (doctor/diagnostics) ──→ 5G (init: skills + notebooks)
```

5A and 5E are independent and can proceed in parallel. Everything else builds on 5A.

#### Validation

- `ginkgo run --agent` emits a valid JSONL stream covering run lifecycle, task lifecycle, cache decisions, graph expansions, and typed failure summaries for all validation workflows.
- The JSONL stream and the Rich CLI produce semantically equivalent information from the same underlying events.
- `ginkgo inspect workflow` returns deterministic JSON with task metadata, dependency edges, and env references.
- `ginkgo inspect run <id>` returns a complete run snapshot usable for agent recovery without the live stream.
- `ginkgo doctor` catches missing flows, unresolvable envs, closure tasks, shell tasks without explicit outputs, and cycles — zero false positives on valid example workflows.
- `ginkgo cache explain` correctly distinguishes source hash change, version bump, env lock change, input file change, and new-task as distinct rerun reasons.
- `task_failed` events classify failures into the defined taxonomy with suggested next actions.
- `task_completed` events include typed artifact metadata (type, path, shape, dtype where applicable).
- `ginkgo init` produces a project with agent skills and notebook directory immediately usable by a coding agent.
- `ginkgo export --format mermaid` produces a valid Mermaid graph that renders without errors.

---

### Phase 5 — Notebook Tasks

**Goal:** Allow users to parameterise and execute Jupyter (`.ipynb`) and marimo notebooks as first-class workflow tasks, with automatic HTML export and a dedicated UI view.

**Downstream consumers:** Phase 12 (Publishing) bundles rendered notebook HTML. The HTML storage path and provenance entry format should be stable and discoverable from the run manifest.

#### Deliverables

- Add a `@notebook` decorator that registers a notebook file as a workflow task:
  - For `.ipynb` notebooks: execute via Papermill, injecting parameters through the standard Papermill parameters cell mechanism.
  - For marimo notebooks: execute as a standard Python script with CLI arguments forwarded.
- Execute notebooks as shell tasks under the hood so they integrate cleanly with existing task scheduling, caching, and provenance without a separate execution path.
- After each successful execution, render the executed notebook to HTML and store it at a stable path alongside run provenance (e.g. `.ginkgo/runs/<run-id>/notebooks/<task-id>.html`). Record this path in the run manifest so Phase 12 can discover it without heuristics.
- Expose the `@notebook` docstring as the human-readable description surfaced in the UI.
- Add a **Notebooks** tab to the UI that:
  - Lists all registered notebooks with their descriptions.
  - Links to the rendered HTML output for each completed run.
  - Shows per-notebook execution status (pending, running, completed, failed).

#### Key design points

- Notebook tasks are first-class DAG nodes: they participate in dependency resolution, are cached by their input parameters and notebook content hash, and appear in run provenance identically to Python tasks.
- The `@notebook` decorator should accept typed parameters in the same style as `@task` so inputs can be validated before execution.
- Papermill is used for `.ipynb` execution because it provides a well-defined injection mechanism and produces an executed output notebook suitable for HTML conversion.
- Marimo notebooks receive their arguments via CLI args consistent with marimo's own execution model.
- HTML rendering is a post-execution step and must not block cache recording or provenance writes if rendering fails.
- The Notebooks tab is additive and must not disrupt existing Tasks or DAG views.

#### Validation

- Define a `.ipynb` notebook with a Papermill parameters cell, register it with `@notebook`, and assert that parameter injection, execution, and HTML export complete correctly and appear in run provenance.
- Define a marimo notebook with CLI args, register it with `@notebook`, and assert it executes with the correct arguments and produces an HTML export.
- Run a workflow containing a notebook task twice with identical inputs and assert the second run is served from cache.
- Assert the Notebooks tab lists all registered notebooks with their descriptions and links to the rendered HTML for each completed run.
- Assert a notebook task failure is reflected in the Notebooks tab with the correct failure state and does not leave a stale HTML artifact from a prior successful run.

---

### Phase 6 — Remote Artifact Store

**Goal:** Add a remote backend to the `ArtifactStore` abstraction introduced in Phase 2, removing the assumption that artifact bytes live on the local filesystem.

**Depends on:** Phase 2 (`ArtifactStore` interface and `artifact_id` scheme).

**Downstream consumers:** Phase 7 (Asset Catalog), Phase 8 (DataFrame Assets), Phase 9 (Model Assets), and Phase 14 (K8s Executor) all depend on remote storage being available.

#### Deliverables

- Implement a `RemoteArtifactStore` backend targeting S3-compatible object storage, conforming to the `ArtifactStore` interface from Phase 2.
- `store()` uploads artifact bytes to the remote store keyed by the same content-addressed `artifact_id` (`<sha256>.<ext>`) used locally — no key translation is needed.
- `retrieve()` downloads the artifact to a local staging path under `.ginkgo/staging/` and places a symlink at the declared output path, exactly as the local backend does. The staging path is recorded in provenance alongside the remote artifact ID.
- `exists()` checks the remote store without downloading.
- Add configurable local staging cache so repeated `retrieve()` calls for the same `artifact_id` skip re-downloading.
- Extend support to all artifact types the local backend handles: task output files, folders, run manifests, and task logs.
- Add backend configuration to the Ginkgo project config so users can switch from local to remote storage without changing task code.

#### Key design points

- This phase is entirely additive to the `ArtifactStore` interface — no changes to the cache, evaluator, or task code are required. The interface from Phase 2 was designed for exactly this extension.
- Artifact identity (`artifact_id`) is unchanged: the same content-addressed string used locally becomes the remote object key. Cache keys remain content-addressed whether bytes live locally or remotely.
- `retrieve()` is the only place that knows whether an artifact came from local or remote storage. Everything above it (cache hit logic, provenance recording, symlink creation) is unaffected.
- The local backend remains the default. Remote is opt-in via project config.
- Provenance records both the logical `artifact_id` and the resolved local staging path so downstream tools can locate artifacts without knowing the backend.

#### Validation

- Configure a remote backend and run a workflow; assert all declared output artifacts are uploaded and `artifact_id` values match their content hashes.
- Re-run the same workflow and assert cache hits are served from the remote store without re-executing tasks.
- Assert `retrieve()` uses the local staging cache on repeated accesses and does not re-download the same `artifact_id` twice.
- Re-run `VW-5` and assert selective cache invalidation still works when artifacts are stored remotely.
- Assert `ginkgo debug` can retrieve task logs when the log backend is remote.
- Assert switching from local to remote backend in config requires no changes to workflow or task code.

---

### Phase 10 — Alerts and Notifications

**Goal:** Notify users and teams when workflow runs complete, fail, or breach configurable thresholds — without requiring constant UI monitoring.

**Depends on:** Phase 13 (Secrets) for credential resolution. Channel credentials must be sourced exclusively through the secrets resolver.

#### Deliverables

- Add a notification system that fires on run lifecycle events:
  - run started
  - run completed successfully
  - run failed (with task-level failure detail)
  - task retry threshold exceeded
- Support the following notification channels with pluggable backends:
  - **Email**: SMTP-based with configurable recipients and templates
  - **Slack**: webhook-based messages with run summary and link to local UI
- Add notification configuration at the workflow level and as global defaults in Ginkgo config:
  - per-channel enable/disable
  - event filter (e.g. only notify on failure)
  - recipient list or webhook URL (resolved via the secrets system from Phase 13)
- Include enough context in each notification to be actionable without opening the UI:
  - workflow name, run id, trigger timestamp
  - list of failed tasks with exit codes and truncated log tails
  - direct link to the local UI run detail view where available

#### Key design points

- Notification dispatch must be non-blocking and must not affect run execution or provenance recording if a notification channel is unavailable.
- Webhook URLs, SMTP passwords, and other channel credentials must be resolved through the secrets system (Phase 13), never stored in plaintext config.
- Notification templates should be user-overridable so teams can adapt message content to their conventions.
- Channel backends should be pluggable so additional channels (PagerDuty, Teams, etc.) can be added without core changes.

#### Validation

- Run a workflow that succeeds and assert a Slack webhook receives a well-formed success notification with the correct run id and summary.
- Run a workflow that fails and assert the failure notification includes the failed task name, exit code, and a truncated log tail.
- Configure failure-only filtering and assert no notification is sent for a successful run.
- Assert that a misconfigured or unavailable notification channel logs a warning but does not prevent the run from completing or provenance from being recorded.
- Assert that channel credentials are sourced from the secrets layer and never appear in config files or log output.

---

## Tier 3 — Asset Layer

### Phase 7 — Asset Catalog and Lineage

**Goal:** Introduce durable asset identity and lineage as a thin indexing layer over Phase 2's cache and artifact store, without changing Ginkgo's run-centric execution model.

**Depends on:** Phase 2 (`ArtifactStore`, `artifact_id`). Benefits from Phase 6 (remote storage) when active.

**Downstream consumers:** Phase 8 (DataFrame Assets) and Phase 9 (Model Assets) extend the catalog with type-specific backends. Phase 12 (Publishing) includes asset metadata in bundles.

#### Deliverables

- Add a first-class asset abstraction that can be attached to task outputs:
  - stable logical asset key (user-defined name)
  - pointer to the producing cache entry (cache key + `artifact_id` from Phase 2)
  - materialization metadata (timestamp, run id, task id)
  - optional storage backend metadata
- Introduce an asset catalog under `.ginkgo/assets/`:
  - current materialization per asset key: a pointer to the latest cache entry and `artifact_id`
  - historical materialization records, ordered by run
  - lineage edges: links to upstream asset keys consumed by the producing task
- Extend run provenance so task manifests record asset keys alongside the existing `artifact_id` and cache key.
- Add CLI and UI read paths for:
  - list assets
  - inspect current materialization state (resolves to a specific cache entry and `artifact_id`)
  - inspect upstream and downstream lineage

#### Key design points

- The catalog is a pure index: it stores metadata and pointers, never artifact bytes. All bytes remain in the `ArtifactStore` from Phase 2 and are referenced by `artifact_id`.
- "Current materialization" is a pointer to a specific Phase 2 cache entry. Resolving an asset key to a file path goes through `ArtifactStore.retrieve()`, keeping the backend abstraction intact for Phase 6 remote storage.
- The catalog must distinguish three separate things: logical asset identity (the key), physical materialization (the `artifact_id`), and the task-run cache entry (the cache key). These are not the same thing.
- This phase does not introduce Dagster-style asset-driven scheduling.

#### Validation

- Define a workflow where two tasks materialize named assets and a downstream task consumes them. Assert the catalog records the correct asset keys, `artifact_id` values, and lineage edges.
- Re-run with unchanged inputs and assert the catalog points to the same current materialization (same `artifact_id`) while provenance records cached task reuse.
- Update one upstream input and assert only the affected downstream asset lineage chain receives a new materialization with a new `artifact_id`.
- Assert the UI/API renders an asset detail view showing current state, `artifact_id`, and upstream/downstream dependencies.

---

### Phase 8 — Versioned DataFrame Assets

**Goal:** Give `pandas.DataFrame` assets Iceberg-like snapshot behavior by extending Phase 2's immutable artifact storage with a lineage manifest layer.

**Depends on:** Phase 2 (`ArtifactStore`, immutable artifacts), Phase 7 (asset catalog for asset key resolution). Benefits from Phase 6 (remote storage) for time-travel reads.

#### Deliverables

- Add a versioned tabular asset backend for DataFrame-producing tasks. Each successful materialization calls `ArtifactStore.store()` to write an immutable Parquet artifact, then records a snapshot manifest entry alongside it.
- Snapshot manifest entries extend Phase 2's `meta.json` structure with tabular-specific fields:
  - snapshot id (derived from the `artifact_id`)
  - parent snapshot id
  - asset key
  - schema summary
  - row count
  - producing run id and task id
- Add a head-pointer file per asset key that records the latest snapshot id, enabling resolution of:
  - latest snapshot
  - specific snapshot id by lookup
  - historical lineage chain by following parent pointers
- Downstream task cache keys consume snapshot identity (`artifact_id`) rather than re-hashing the full DataFrame in memory, so cache invalidation happens exactly when the upstream data changes.
- Add time-travel reads for tabular assets by snapshot id, resolved via `ArtifactStore.retrieve()`.

#### Key design points

- Snapshot immutability is inherited directly from Phase 2: artifacts are read-only once written to the store. This phase adds the lineage manifest on top; it does not re-implement storage.
- The snapshot id is the `artifact_id` from Phase 2 (`<sha256>.parquet`). No separate identity scheme is needed.
- When Phase 6 (Remote Artifact Store) is active, snapshot Parquet files are stored remotely via the same `ArtifactStore` interface. Time-travel reads go through `retrieve()` and benefit from local staging cache automatically.
- The snapshot manifest is intentionally minimal — it is not Iceberg. The storage contract is immutable blobs plus a lightweight manifest, not a full table format.
- The snapshot store is an implementation detail behind the asset abstraction so larger backends (e.g. Delta Lake, Iceberg) can be substituted later.

#### Validation

- A task materializing a DataFrame asset twice with different inputs produces two distinct snapshots with the correct parent-child relationship and distinct `artifact_id` values.
- A downstream task pinned to an older snapshot id reads the historical data correctly even after a newer snapshot exists, via `ArtifactStore.retrieve()`.
- Re-running a consumer task against the same snapshot id hits the cache without re-hashing the DataFrame.
- Schema summaries and row counts are recorded in both the snapshot manifest and run provenance.
- Assert that with a remote backend active (Phase 6), time-travel reads are served from the remote store and local staging cache correctly.

---

### Phase 9 — ML Model and Evaluation Support

**Goal:** Add three ML-specific capabilities through the existing `kind=` extension point: versioned model assets (`kind="model"`), structured evaluation records (`kind="eval"`), and parameter sweep fan-out (`.sweep()`). Together these let practitioners train, evaluate, compare, and promote models without manual metric logging or version tracking.

**Depends on:** Phase 7 (asset catalog — provides asset identity, versioning, alias resolution, and the asset store that this phase registers model and eval assets into). Phase 2 (`ArtifactStore`, immutable artifacts). Benefits from Phase 6 (remote storage) and Phase 8 (upstream dataset snapshot IDs for lineage).

**Detailed design:** [`docs/phase9-ml-support-plan.md`](phase9-ml-support-plan.md)

#### Target DSL

```python
from ginkgo import task, flow, model, eval, file

@task(kind="model")
def train(data: file, *, lr: float, epochs: int):
    clf = fit(load(data), lr=lr, epochs=epochs)
    return model(clf, framework="sklearn")

@task(kind="eval")
def evaluate(m: model, test_data: file):
    clf = m.load()
    preds = clf.predict(load(test_data))
    return eval(metrics={"accuracy": acc, "f1": f1, "auc": auc})

@flow
def main():
    data = prepare_data(raw=file("data/raw.csv"))
    test = prepare_test(raw=file("data/test.csv"))
    models = train.sweep(data=data, lr=[0.001, 0.01, 0.1], epochs=[10, 50, 100])
    evals = evaluate.map(m=models, test_data=test)
    return evals
```

#### Deliverables

**`kind="model"` — versioned model assets:**

- Add a `ModelResult` sentinel (following the `shell()` / `ShellExpr` pattern) returned from `kind="model"` task bodies via a `model()` builder function. The sentinel carries the model object, framework name, optional metrics, and optional metadata.
- Add a `ModelRef` resolved output type that downstream tasks receive, carrying the asset key, version ID, artifact path, metrics, and a `.load()` method for deserialization.
- Add a `model.ref("name@alias")` factory for resolving a model by alias or version ID at graph build time, enabling downstream consumption of promoted models.
- Add a pluggable `ModelSerializer` protocol with initial implementations for pickle (universal fallback), sklearn (joblib), and torch (state_dict). Serializers are registered by framework name; unknown names raise a clear error at task completion time.
- On task completion, the evaluator serializes the model object, computes a content hash, registers an immutable asset version in the Phase 7 asset catalog (namespace `"model"`), and returns a `ModelRef` as the resolved output.
- Auto-capture resolved task input arguments as `params` in the asset version metadata — the practitioner does not need to pass params explicitly.
- When a task input is a `ModelRef`, hash the `version_id` for cache keys (not the serialized bytes), giving cheap and stable cache invalidation.

**`kind="eval"` — structured evaluation records:**

- Add an `EvalResult` sentinel returned from `kind="eval"` task bodies via an `eval()` builder function. The sentinel carries structured metrics and optional artifact paths (confusion matrices, plots, etc.).
- Add an `EvalRecord` resolved output type carrying the asset key, version ID, metrics, auto-captured params, linked model version (if any input was a `ModelRef`), and artifact paths.
- On task completion, the evaluator inspects resolved inputs — if any is a `ModelRef`, it records the model's `version_id` as `model_version`, linking the eval to the model automatically. Register an immutable asset version in the catalog (namespace `"eval"`).

**`.sweep()` — parameter exploration:**

- Add a `.sweep()` method on `TaskDef` / `PartialCall`, parallel to `.map()`. It partitions kwargs into fixed (scalar) and swept (list) arguments, computes combinations via `itertools.product` (grid) or `zip` (positional), and delegates to `.map()` to produce an `ExprList`.
- Attach `SweepMeta` (strategy, axes, combination count) to the `ExprList` so the evaluator can record sweep provenance on each constituent task.
- Support `strategy="grid"` (Cartesian product, default) and `strategy="zip"` (positional pairing, equal-length lists required).

**CLI:**

- `ginkgo model ls` — list model asset keys.
- `ginkgo model versions <name>` — list versions with metrics summary.
- `ginkgo model inspect <name>@<ver|alias>` — full metadata, params, lineage.
- `ginkgo model promote <name> <ver> <alias>` — move alias pointer.
- `ginkgo eval ls` — list eval asset keys.
- `ginkgo eval compare <name>` — tabular comparison of all versions (metrics columns from eval records, param columns from auto-captured inputs, model version from linked `ModelRef`).
- `ginkgo eval inspect <name>@<ver>` — full detail.

**UI:**

- Add a **Models** sidebar section: list view with latest version summary, version detail with metrics/params/alias badges, and a promote action.
- Add an **Evals** sidebar section: sortable comparison table (rows = eval versions, columns = metrics + params), run/model linkage per row, and version detail view.

#### Key design points

- Model and eval assets are registered in the Phase 7 asset catalog — this phase does not build a separate asset store. Phase 7 provides identity, versioning, alias resolution, and storage layout; this phase adds ML-specific sentinels, serializers, and evaluator dispatch.
- `kind="model"` and `kind="eval"` both use `execution_mode = "driver"` (same as shell) — the task body runs on the scheduler, produces a sentinel, and the evaluator handles serialization and storage.
- Model versions are immutable once written — immutability is inherited from Phase 2's read-only artifact store. Promotion is alias movement, not mutation.
- When Phase 6 (Remote Artifact Store) is active, model and eval artifacts are stored and retrieved remotely via the same `ArtifactStore` interface, with no changes to registry logic.
- Serializer logic is plugin-driven so framework-specific handling does not bloat the runtime core. Only pickle is zero-dependency; framework serializers use lazy imports and fail clearly if the framework is not installed.
- `.sweep()` is deliberately simple (grid/zip only) — it is not a Bayesian optimization framework. Complex HPO should use external tools (Optuna, etc.) with Ginkgo tasks as the execution substrate.
- Auto param capture records scalar inputs only; file, folder, and model ref inputs are skipped to avoid capturing large objects in the metadata dict.
- The initial scope is offline training and batch inference composition, not online serving or deployment orchestration.

#### Validation

- A `kind="model"` task with `framework="sklearn"` serializes and registers an immutable model version with correct auto-captured params and metrics.
- Re-running with identical inputs hits the cache and does not create a duplicate version.
- Re-running with changed inputs creates a new version; latest version pointer updates.
- A downstream task consuming `model` receives a `ModelRef` with a working `.load()` method.
- `model.ref("train@production")` resolves to the promoted version and invalidates downstream cache when the alias moves.
- `train.sweep(data=d, lr=[0.01, 0.1], epochs=[10, 50], strategy="grid")` produces 4 tasks with correct parameter combinations and sweep metadata in provenance.
- `strategy="zip"` with equal-length lists produces N tasks; unequal lengths raise a clear error.
- A `kind="eval"` task stores structured metrics and automatically links to the upstream model version.
- `ginkgo eval compare` renders correct columns from metrics and inherited params without manual metric logging.
- Promote a model version to `staging`, then another, and assert alias resolution changes without mutating historical model versions.
- Framework-aware serialization and deserialization round-trips correctly for at least sklearn and records the serializer metadata in provenance.
- UI Models sidebar lists model keys and versions; Evals comparison table renders sortable metric columns with model version and run linkage.

---

## Tier 4 — Composition, Publishing, and Remote Execution

### Phase 12 — Workflow Publishing

**Goal:** Produce a self-contained, human-readable bundle of a workflow run — including outputs, notebook renders, assets, and a structured summary — that can be shared or archived without requiring a running Ginkgo instance.

**Depends on:** Phase 3 (outdir manifest entries for output location), Phase 5 (notebook HTML paths in provenance), Phase 7 (asset metadata in provenance). Degrades gracefully when optional upstream phases are not active.

#### Deliverables

- Add a `ginkgo publish [run-id]` command that collects all artifacts from a completed run and writes them into a portable bundle directory:
  - declared task output files and folders
  - rendered notebook HTML files (from Phase 5)
  - benchmark files for any tasks run with `benchmark=True` (from Phase 1)
  - run manifest and provenance JSON
  - a generated `index.html` that provides a human-readable summary of the run: workflow name, run id, task list with statuses, timings, and links to each artifact
- Resolve artifacts from either the default cache layout or an outdir (Phase 3) transparently, using the manifest entries recorded by Phase 3.
- Support asset metadata in the bundle summary if the asset catalog (Phase 7) is present; degrade gracefully if it is not.
- Add a `--bundle-dir` flag to control where the published bundle is written (default: `./<workflow-name>-<run-id>/`).
- Add a `--open` flag that opens `index.html` in the default browser after publishing.

#### Key design points

- The bundle must be fully self-contained: no running server, no Ginkgo installation, and no network access should be required to read it.
- `ginkgo publish` is a read-only operation on provenance; it must never modify run state or cache entries.
- The `index.html` should be navigable without JavaScript where possible, falling back to minimal static HTML for maximum portability.
- Asset and model metadata are included as a best-effort enhancement; the command should not fail if those phases are not yet active.
- The bundle layout should be stable and documented so external tools can consume it without parsing generated HTML.

#### Validation

- Run a workflow with multiple tasks, including at least one notebook task and one benchmarked task, then run `ginkgo publish` and assert the bundle directory contains all expected output files, notebook HTML, benchmark files, and provenance JSON.
- Assert `index.html` correctly links to every artifact in the bundle and is viewable in a browser without a running server.
- Assert `ginkgo publish` works correctly whether outputs were written to the default cache layout or an outdir.
- Assert the command is idempotent: running `ginkgo publish` twice on the same run id produces the same bundle.
- Assert that `ginkgo publish` on a failed run still produces a valid bundle with partial outputs and a clear failure summary in `index.html`.

---

### Phase 14 — Kubernetes / Batch Executor

**Goal:** Run tasks on a remote scheduler such as Kubernetes Jobs or cloud batch services while preserving Ginkgo's dynamic DAG and cache semantics.

**Depends on:** Phase 6 (Remote Artifact Store — hard prerequisite; remote jobs cannot access local `.ginkgo/cache/`), Phase 13 (Secrets — remote backends require credentials).

#### Deliverables

- Implement a remote executor that can submit one task run as one remote job.
- Add resource mapping from Ginkgo task declarations onto remote job specs:
  - CPU
  - memory
  - optional GPU
- Package workflow code so remote workers can import task functions safely.
- Collect remote status, exit codes, and logs back into Ginkgo provenance.
- Support cancellation and retry of remote jobs.

#### Key design points

- The main evaluator can remain the control plane, but it must treat remote jobs as asynchronous task futures.
- Dynamic DAG expansion should still happen in the scheduler after parent-task results return.
- Remote execution makes artifact storage mandatory; Phase 6 (Remote Artifact Store) must be complete and stable before this phase begins.

#### Validation

- Re-run `VW-2`, `VW-3`, `VW-6`, `VW-7`, and `VW-8` through the remote executor.
- Assert remote logs, exit codes, and declared resources are reflected in the local run manifest.
- Assert cancellation from the CLI propagates to in-flight remote jobs.

---

### Phase 15 — Workflow Composition

**Goal:** Allow Ginkgo workflows to invoke other Ginkgo workflows as first-class sub-workflows, enabling reuse and composition without duplicating task logic.

**Depends on:** Phase 13 (Secrets — secrets must pass through sub-workflow call boundaries).

#### Deliverables

- Add a `call_workflow` primitive that invokes a named Ginkgo workflow from within a parent workflow task.
- Support two composition modes:
  - **Inline expansion**: the sub-workflow's DAG is expanded into the parent DAG at plan time, making its tasks visible in the parent's provenance and UI.
  - **Opaque invocation**: the sub-workflow runs as a self-contained execution unit and its result is returned as an artifact to the parent.
- Pass parameters, secrets, and resource declarations through the call boundary consistently.
- Propagate sub-workflow run ids and provenance back into the parent run manifest so lineage is fully traceable.
- Detect and reject circular workflow dependencies at plan time.
- Extend the UI and `ginkgo inspect` to show sub-workflow boundaries and nested task graphs.

#### Key design points

- Inline expansion is preferred for small, reusable task groups where joint caching and visibility matter.
- Opaque invocation is preferred for independently versioned or cross-team workflows where internal structure should be encapsulated.
- Sub-workflow cache semantics must be consistent with top-level workflow semantics: the same inputs should hit cache regardless of call depth.
- Recursive or indirect circular dependencies must be caught before any execution begins.

#### Validation

- Define a parent workflow that calls a sub-workflow in inline mode and assert that sub-workflow tasks appear in the parent DAG, share the same run manifest, and are individually cached.
- Define a parent workflow that calls a sub-workflow in opaque mode and assert that only the sub-workflow's result artifact appears in the parent provenance, not its internal tasks.
- Assert that circular workflow references are detected at plan time with a clear error message.
- Assert that parameters and secrets passed to a sub-workflow are correctly scoped and do not leak into unrelated tasks in the parent workflow.
- Re-run the parent workflow with unchanged inputs and assert that sub-workflow tasks are served from cache at the appropriate granularity for each composition mode.

---

## Cross-Cutting Phases

These phases are not gated by a specific tier and can be worked on incrementally alongside any other work.

### Phase 1 — Remaining Hardening and UI Polish

**Goal:** Finish the production-readiness and local UI work that remains.

Completed in this phase: sidebar shell, multi-workspace aggregation and
workspace switching, pixi-aware workflow launch from external workspaces,
live WebSocket event channel, structured live-state diffing, UI server package
refactor, workspace validation from non-workspace directories, age-based
`ginkgo cache prune`.

**Integration note:** The `benchmark` deliverable below produces structured per-task performance data. This data should be recorded in run provenance in a format that Phase 7 (Asset Catalog) can surface as asset metadata and Phase 12 (Publishing) can include in bundles without parsing benchmark files.

#### Remaining Deliverables

- Extend retry support with:
  - selective retry policies
  - retry backoff
- Broaden cache-management policy beyond age-based pruning (size- or
  count-based eviction).
- Polish the UI task-graph experience:
  - richer DAG layout (fit-to-view, failure focus, better spacing)
- Add task priority declarations so users can express relative urgency between
  tasks in the same DAG tier; the scheduler should respect priority when
  multiple tasks are ready to run concurrently.
- Add a `benchmark` argument to `@task()`: when set, the task runner collects
  wall-clock time, CPU usage, and peak memory for the task execution and writes
  a structured benchmark file alongside the run log. Benchmark data should also
  be captured in run provenance so it is queryable without reading the file.
- Tighten documentation around partial resume, dry-run behavior, and resource
  declarations.

#### Key design points

- This phase is explicitly for remaining gaps in areas that already exist.
- The goal is to reduce ambiguity and operational rough edges before the runtime surface area expands further.
- UI work should remain local-first and should build on the current file-backed provenance model.

#### Validation

- Re-run `VW-4`, `VW-5`, `VW-6`, and `VW-8` through the polished CLI and UI paths and assert the richer retry, cache, and resource behavior is visible in both CLI output and persisted provenance.
- Assert the improved diagnostics distinguish common classes of failure such as env mismatch, invalid paths, and packaging/importability errors.

---

### Phase 11 — Public Documentation

**Goal:** Produce clear, maintainable, and complete public documentation so new users can adopt Ginkgo without needing to read source code or ask for help.

**Integration note:** Phase 4's structured inspection output, DAG export schemas, and doctor commands should be documented as first-class reference material. Documentation should be updated incrementally as each phase lands.

#### Deliverables

- Publish a documentation site built with MyST Markdown covering:
  - **Getting started**: installation, first workflow, running and inspecting results
  - **Core concepts**: tasks, DAGs, caching, provenance, resources, environments
  - **How-to guides**: one topic per common use case (retry, dry-run, partial resume, secrets, notifications, workflow composition, asset materialization)
  - **Reference**: full CLI command reference, config schema, Python API surface
  - **Architecture**: internal design overview for contributors
- Write a changelog that captures major version milestones and breaking changes.
- Add inline docstrings to all public Python APIs that do not already have them (consistent with the project's numpydoc convention).
- Add `--help` text review pass to ensure every CLI command and flag has accurate, up-to-date help text.
- Establish a documentation CI check so undocumented public APIs and broken internal links are caught automatically.

#### Key design points

- Documentation should be written for users first, contributors second.
- How-to guides should be task-oriented and runnable end-to-end from a clean checkout.
- Reference documentation should be generated from source where possible to avoid drift.
- The documentation site should be deployable from the existing `pixi` environment without requiring separate tooling.

#### Validation

- A new user following only the Getting Started guide can install Ginkgo, write a two-task workflow, run it, and inspect the cached result without consulting any other source.
- All CLI commands and flags have non-empty `--help` text that matches current behavior.
- The documentation CI check catches at least one intentionally introduced undocumented public function and one broken internal link.
- The full documentation site builds without warnings from a clean `pixi` environment.
