# Ginkgo Implementation Plan

This document describes work that has not yet been implemented. It is ordered by recommended implementation sequence rather than by the historical order in which the existing system was built.

Each phase is independently testable and follows the same structure:

- Goal
- Deliverables
- Key design points
- Validation

## Phase 4 — Remaining Hardening and UI Polish

**Goal:** Finish the production-readiness and local UI work that remains after the currently implemented hardening and UI slices.

### Deliverables

- Extend retry support with:
  - selective retry policies
  - retry backoff
  - clearer retry reporting
- Broaden cache-management policy beyond age-based pruning.
- Improve CLI diagnostics and runtime error messages further where current reporting is still thin.
- Polish the UI task-graph experience:
  - richer DAG layout
  - better live-run visualization
  - a true WebSocket-based event channel
- Tighten documentation around partial resume, dry-run behavior, and resource declarations.

### Key design points

- This phase is explicitly for remaining gaps in areas that already exist.
- The goal is to reduce ambiguity and operational rough edges before the runtime surface area expands further.
- UI work should remain local-first and should build on the current file-backed provenance model.

### Validation

- Re-run `VW-4`, `VW-5`, `VW-6`, and `VW-8` through the polished CLI and UI paths and assert the richer retry, cache, and resource behavior is visible in both CLI output and persisted provenance.
- Assert live UI updates over the final event channel remain consistent with the run manifest and log files.
- Assert the improved diagnostics distinguish common classes of failure such as env mismatch, invalid paths, and packaging/importability errors.

---

## Phase 5 — Agent Workflow Tooling

**Goal:** Make Ginkgo easier for coding agents and agent-assisted users to inspect, scaffold, debug, and modify safely.

### Deliverables

- Workflow linter and doctor commands that expose:
  - task discovery
  - env references
  - resource declarations
  - process-safety issues
  - suspicious shell-task patterns
- Cache explanation tooling that reports why a task was cached or rerun.
- Machine-readable inspection output for workflows, tasks, dependencies, envs, resources, and cache metadata.
- Extend `ginkgo init` so project scaffolding also creates or installs a standard set of agent skills alongside the project template.
- Add agent-facing notebook prototyping support based on the [`datalayer/jupyter-mcp-server`](https://github.com/datalayer/jupyter-mcp-server), with project-local notebooks stored under `workflow/notebooks/`.
- Ongoing improvements to scaffolding templates and installed skills for common Ginkgo project types.

### Key design points

- Agent tooling should expose structured state, not force agents to scrape human-readable logs.
- The linter and doctor commands should share the same validation logic as the runtime.
- Cache explanation should rely on real cache-key components, not heuristic guesses.
- Agent-skill bootstrapping should be deterministic and idempotent so rerunning `ginkgo init` or upgrading templates does not leave projects in a half-installed state.
- Agent-oriented prototyping should happen in Jupyter notebooks accessed through the Jupyter MCP server, with a standard on-disk location at `workflow/notebooks/` so both humans and agents can discover and reuse exploratory work consistently.

### Validation

- Run `ginkgo doctor` on valid and invalid workflows and assert it reports task discovery, env references, and process-safety issues correctly.
- Assert the linter catches nested tasks, closures, missing explicit shell outputs, and invalid path annotations.
- Assert cache explain reports a version bump, env lock hash change, and changed input file contents as distinct rerun reasons.
- Assert the inspect API returns dependency edges, task metadata, and resource declarations in deterministic JSON.
- Assert `ginkgo init` creates or installs the expected agent-skill set and that the resulting project is immediately usable by the agent without manual skill setup.
- Assert initialized projects expose the expected notebook prototyping path at `workflow/notebooks/` and can be connected to the configured Jupyter MCP workflow without ad hoc directory setup.

---

## Phase 6 — Container Execution Backend

**Goal:** Decouple task execution from the scheduler host by introducing a container-first runtime that still works locally.

### Deliverables

- Add a backend abstraction for task execution instead of hard-coding local subprocesses and process-pool workers.
- Implement a Docker or OCI-container executor for both:
  - shell tasks
  - Python tasks
- Add first-class environment metadata that can represent either:
  - Pixi envs for local development
  - container images for cloud-oriented execution
- Capture container image identity in cache keys and run provenance, preferably by immutable image digest.
- Define host/container mount conventions for:
  - workflow source
  - input artifacts
  - output artifacts
  - cache artifacts
  - temp directories
  - logs

### Key design points

- The container backend should reuse the existing evaluator, dependency graph, cache logic, and provenance recorder.
- The hard problem is path semantics, not command execution.
- Pixi remains useful as a local authoring and testing environment even after containers are added.

### Validation

- Run `VW-1` through `VW-8` through the container executor and assert results match the local executor.
- Assert cache invalidation occurs when the container image digest changes.
- Assert per-task logs and manifests still record resolved inputs, outputs, and environment identity.

---

## Phase 7 — Remote Artifact Store

**Goal:** Remove the assumption that all task inputs, outputs, cache entries, and logs live on the scheduler's local filesystem.

### Deliverables

- Introduce an artifact store abstraction with an initial object-store backend such as S3-compatible storage.
- Support remote storage for:
  - task outputs
  - cache artifacts
  - run manifests
  - task logs
  - worker transport payloads when needed
- Extend `file` and `folder` handling so task inputs can be staged from remote storage and outputs can be materialized back into it.
- Add configurable local caching of remote artifacts for repeated access.

### Key design points

- Local paths should remain supported, but the runtime should stop assuming POSIX paths are the only artifact identity.
- Cache keys should remain content-addressed even when the bytes live in object storage.
- Provenance should record both logical artifact identifiers and any local staging path used during execution.

### Validation

- Re-run `VW-5` and assert selective cache invalidation still works when cache artifacts are stored remotely.
- Run a shell pipeline whose outputs are written to the remote artifact store and assert downstream tasks can consume them without manual staging.
- Assert `ginkgo debug` can still retrieve task logs when logs are remote-backed.

---

## Phase 8 — Kubernetes / Batch Executor

**Goal:** Run tasks on a remote scheduler such as Kubernetes Jobs or cloud batch services while preserving Ginkgo's dynamic DAG and cache semantics.

### Deliverables

- Implement a remote executor that can submit one task run as one remote job.
- Add resource mapping from Ginkgo task declarations onto remote job specs:
  - CPU
  - memory
  - optional GPU
- Package workflow code so remote workers can import task functions safely.
- Collect remote status, exit codes, and logs back into Ginkgo provenance.
- Support cancellation and retry of remote jobs.

### Key design points

- The main evaluator can remain the control plane, but it must treat remote jobs as asynchronous task futures.
- Dynamic DAG expansion should still happen in the scheduler after parent-task results return.
- Remote execution makes artifact storage mandatory; this phase depends on the remote artifact store.

### Validation

- Re-run `VW-2`, `VW-3`, `VW-6`, `VW-7`, and `VW-8` through the remote executor.
- Assert remote logs, exit codes, and declared resources are reflected in the local run manifest.
- Assert cancellation from the CLI propagates to in-flight remote jobs.

---

## Phase 9 — Persistent Control Plane

**Goal:** Make Ginkgo resilient to scheduler restarts and suitable for longer-running multi-user or cloud-hosted orchestration.

### Deliverables

- Persist run state in a database instead of keeping the authoritative scheduler state only in memory.
- Add durable records for:
  - task lifecycle state
  - dependency relationships
  - cache decisions
  - remote job ids
  - retry counts
- Expose a backend API for the UI instead of reading only directly from local files.
- Add scheduler recovery so an interrupted Ginkgo process can reconnect to active remote jobs and continue the run.

### Key design points

- The YAML manifest can remain a user-friendly export, but it should no longer be the only source of truth once runs span multiple processes or hosts.
- Idempotent dispatch becomes critical: a recovered scheduler must distinguish "not submitted", "submitted", and "finished" tasks reliably.

### Validation

- Start a run, terminate the scheduler process mid-execution, restart it, and assert the run resumes correctly without duplicating completed work.
- Assert the UI can query both live and historical runs through the API.
- Assert remote executor state and local provenance remain consistent after recovery.

---

## Phase 10 — Asset Catalog and Lineage

**Goal:** Introduce durable asset identity and lineage without changing Ginkgo's run-centric execution model.

### Deliverables

- Add a first-class asset abstraction that can be attached to task inputs and outputs:
  - stable asset key
  - materialization metadata
  - optional storage backend metadata
- Introduce an asset catalog under `.ginkgo/assets/` or equivalent metadata storage:
  - current materialization per asset key
  - historical materialization records
  - links back to producing run id, task id, and cache key
- Extend run provenance so task manifests can record asset-aware inputs and outputs in addition to ordinary task inputs and outputs.
- Add CLI and UI read paths for:
  - list assets
  - inspect current asset state
  - inspect upstream and downstream lineage

### Key design points

- Asset identity should be a thin layer over existing cache and provenance primitives, not a second scheduler.
- The catalog must distinguish:
  - logical asset identity
  - physical materialization
  - task-run cache entry
- This phase does not introduce Dagster-style asset-driven scheduling.

### Validation

- Define a workflow where two tasks materialize named assets and a downstream task consumes them. Assert the run manifest and asset catalog record the correct asset keys, producer tasks, and lineage edges.
- Re-run the same workflow with unchanged inputs and assert the asset catalog points to the same current materialization while the run manifest records cached task reuse correctly.
- Update one upstream input and assert only the affected downstream asset lineage chain receives a new materialization.
- Assert the UI/API can render an asset detail view showing current state plus upstream and downstream dependencies.

---

## Phase 11 — Versioned DataFrame Assets

**Goal:** Give `pandas.DataFrame` assets Iceberg-like snapshot behavior through a lightweight local snapshot store.

### Deliverables

- Add a versioned tabular asset backend for DataFrame-producing tasks:
  - each successful materialization is written as an immutable snapshot
  - snapshots are stored as Parquet plus a small manifest file
- Snapshot metadata records:
  - snapshot id
  - parent snapshot id
  - asset key
  - schema summary
  - row count
  - content hash
  - producing run id and task id
- Add head-pointer metadata so Ginkgo can resolve:
  - latest snapshot
  - specific snapshot id
  - historical lineage chain
- Extend cache keys and downstream provenance so tabular asset consumers depend on snapshot identity instead of re-hashing the whole in-memory DataFrame on every downstream task.
- Add basic time-travel reads for tabular assets by snapshot id.

### Key design points

- This phase is intentionally not Iceberg.
- The storage contract is immutable snapshots plus metadata, not in-place mutation.
- The snapshot store should remain an implementation detail behind the asset abstraction so larger backends can be added later.

### Validation

- A task materializing a DataFrame asset twice with different inputs produces two distinct snapshots with the correct parent-child relationship.
- A downstream task pinned to an older snapshot id reads the historical data correctly even after a newer snapshot exists.
- Re-running a consumer task against the same snapshot id hits the cache.
- Schema summaries and row counts are recorded in both the snapshot manifest and run provenance.

---

## Phase 12 — Model Assets and Registry Semantics

**Goal:** Add first-class ML model assets with immutable versions, rich metadata, and lightweight promotion flows.

### Deliverables

- Add a `model` asset/backend type for training outputs and downstream inference inputs.
- Extend the asset catalog to store immutable model versions with:
  - model asset key
  - version id
  - producing run id and task id
  - upstream dataset and feature snapshot ids
  - metrics
  - hyperparameters
  - input and output schema summaries
  - framework, serializer format, and framework version
- Add framework-aware model artifact serialization through a pluggable registry with initial support for a focused set of frameworks.
- Add mutable model aliases or promotion pointers such as:
  - `dev`
  - `staging`
  - `production`
- Add CLI and UI support to:
  - list model versions
  - inspect model metrics and lineage
  - promote an immutable model version to an alias
- Allow downstream batch inference tasks to consume model assets by version id or alias.

### Key design points

- Model versions are immutable asset materializations; promotion is implemented as alias movement, not model mutation.
- Serializer logic must be plugin-driven so framework-specific handling does not bloat the runtime core.
- The initial scope is offline and batch inference composition, not online serving or deployment orchestration.

### Validation

- Train the same logical model asset twice with different hyperparameters and assert two distinct immutable model versions are recorded with metrics and lineage to the exact training data snapshot.
- Promote one version to `staging`, then another, and assert alias resolution changes without mutating historical model versions.
- Run a downstream batch-scoring task against `model("...@production")`, change the promoted version, and assert the scoring task invalidates correctly even when the input data snapshot is unchanged.
- Assert framework-aware serialization and deserialization round-trips correctly for at least one initial supported framework and records the serializer metadata in provenance.

---

## Phase 13 — Iceberg Asset Backend (Read-Only Integration)

**Goal:** Support Iceberg-backed assets as an optional storage backend for large or shared analytic tables.

### Deliverables

- Add an `iceberg_table` asset/backend type that can be used for task inputs and asset declarations.
- Integrate PyIceberg for metadata inspection:
  - load table by catalog and identifier
  - resolve current snapshot id
  - record schema and table metadata needed for provenance
- Extend cache hashing so Iceberg-backed asset inputs use snapshot identity rather than hashing table contents.
- Add provenance fields for:
  - catalog identifier
  - table identifier or location
  - snapshot id
  - optional schema id
- Add configuration for Iceberg catalogs in Ginkgo config.
- Expose Iceberg-backed assets in the same asset catalog and UI views introduced in earlier asset phases.

### Key design points

- Iceberg is an optional backend for versioned tabular assets, not the default for all DataFrame assets.
- This phase is read-only from Ginkgo's perspective.
- The main value is cheap, stable snapshot-based invalidation plus strong provenance for external tables.

### Validation

- Point a task input at an Iceberg table, run the workflow twice without table changes, and assert the second run is cached via the same snapshot id.
- Advance the Iceberg table to a new snapshot and assert only the dependent tasks are invalidated.
- Assert the run manifest records the exact Iceberg snapshot id consumed by each task.
- Assert the asset catalog can display local snapshot-backed DataFrame assets, model assets, and Iceberg-backed assets through the same logical asset interface.

---

## Phase 14 — Advanced Asset Materialization Semantics

**Goal:** Add higher-risk asset features that require stronger commit, cursor, and retry guarantees.

### Deliverables

- Add incremental asset-consumption cursors based on prior materialized asset state rather than only "last successful run".
- Define asset update modes explicitly for tabular assets:
  - append
  - overwrite
  - keyed merge
- Add optional write support for supported backends, starting only after commit semantics are explicit and testable.
- Extend provenance to record:
  - prior snapshot id
  - new snapshot id
  - materialization mode
  - incremental cursor state
- Add failure-recovery and retry rules for asset writes so Ginkgo can distinguish:
  - write not attempted
  - write committed but run recording incomplete
  - write fully committed and recorded

### Key design points

- This phase is intentionally separated because writes are materially riskier than reads.
- Incremental processing must be based on asset snapshot lineage or explicit cursors, not a loose "since last run" heuristic.
- Iceberg write support belongs here, not in the read-only Iceberg phase.

### Validation

- Define a workflow that incrementally consumes only new tabular asset snapshots and assert the recorded cursor advances correctly across repeated runs.
- Simulate a failure after an asset backend reports a successful commit but before Ginkgo finishes writing run provenance; assert recovery logic prevents duplicate writes on retry.
- Assert append, overwrite, and merge modes produce distinct provenance records and expected downstream invalidation behavior.
- For Iceberg-backed outputs, assert the recorded snapshot ids and recovery state remain consistent across retries.
