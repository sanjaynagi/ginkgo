# Ginkgo Implementation Plan

This document describes work that has not yet been implemented. Phases are
grouped into dependency tiers — each tier builds on the foundations established
by the previous one.

Each phase is independently testable and follows the same structure:

- Goal
- Deliverables
- Key design points
- Validation

---

## Tier 1 — Runtime Maturity

These phases polish and extend existing subsystems without introducing new
architectural layers.

### Phase 3 — UI Performance and Responsiveness

**Goal:** Keep the local web UI fast and predictable as run history, task
counts, event volume, and artifact metadata grow.

#### Deliverables

- Profile the UI server and frontend against larger synthetic and real
  workspaces to identify dominant latency and rendering costs.
- Reduce run-list and run-detail load latency by avoiding full manifest reads
  when summary data is sufficient.
- Add pagination, windowing, or incremental loading for large run collections
  and task lists.
- Make live updates cheaper by sending targeted diffs and limiting unnecessary
  client-side recomputation.
- Improve graph rendering performance for large DAGs through layout caching,
  viewport-aware rendering, or progressive expansion.
- Ensure notebook and asset detail views remain responsive even when runs
  contain many artifacts.
- Add benchmark-style regression checks for UI server payload construction and
  selected frontend interactions.

#### Key design points

- Performance work should preserve the current provenance-first architecture;
  optimisations should improve data access patterns rather than introduce a
  second source of truth.
- The server should distinguish between summary payloads and detail payloads so
  the frontend does not pay full-cost reads for overview screens.
- Frontend rendering should favour incremental rendering and bounded DOM size
  over cosmetic complexity.
- UI performance should be measured with repeatable fixtures and checked into the
  repository where practical, so regressions are visible.

#### Validation

- Large workspaces with many historical runs remain navigable without noticeable
  stalls in the run list.
- Opening a run with a large task graph or many artifacts does not block the UI
  for multiple seconds on routine hardware.
- Live runs continue to update smoothly without flooding the browser with
  redundant state changes.
- Notebook and artifact views remain responsive on real workflow runs rather than
  only toy examples.

---

## Tier 2 — Asset Layer

These phases extend the existing asset catalog (file-backed, with `AssetKey`,
`AssetVersion`, alias pointers, and lineage) into richer asset kinds and
lifecycle tooling.

### Phase 6 — Asset Read Paths and Lifecycle Tooling

**Goal:** Add the user-facing read surfaces and maintenance workflows that sit
on top of the asset catalog.

**Depends on:** the existing asset catalog (Phase 4 and Phase 5 complete).

#### Deliverables

- Add a programmatic asset API for workspace-scoped inspection and loading:
  - list asset keys
  - inspect versions
  - resolve aliases and version ids
  - load materialized assets through kind-specific loaders
- Add staleness reporting based on lineage and version timestamps.
- Extend cache pruning to become asset-aware:
  - preserve alias-pinned versions
  - remove old unpinned versions by retention policy
  - garbage-collect unreferenced artifacts
- Extend the existing asset CLI and UI with the remaining lifecycle features:
  - alias management and promotion flows
  - lineage navigation beyond the current detail views
  - staleness indicators and explanations

#### Key design points

- Phase 6 is read- and lifecycle-oriented. It should not change the core asset
  storage model.
- The programmatic API and CLI should be thin wrappers over the shared
  `AssetStore` and loader registry, not parallel implementations.
- Staleness is derived from lineage plus version timestamps; it should not
  affect scheduling semantics.
- Asset-aware pruning must respect alias-pinned versions and avoid deleting any
  artifact still referenced by cache or asset metadata.

#### Validation

- A user can inspect asset keys, versions, aliases, and lineage from Python and
  the CLI without reading raw catalog files.
- Moving an alias updates CLI and programmatic resolution immediately without
  mutating historical versions.
- `ginkgo asset status` marks downstream assets stale when upstream versions are
  newer, including transitive staleness.
- Asset-aware pruning removes only unpinned old versions and leaves referenced
  artifacts intact.
- The UI renders staleness and lifecycle state backed by the same catalog data
  as the CLI and programmatic API.

---

<!-- Phase 8 (Workflow Composition, opaque mode) is complete. See
     docs/architecture/execution-model.md §Sub-workflow Composition for
     the shipped design. Inline expansion was explicitly out of scope. -->

<!-- Phase 9 (Remote Input Streaming / FUSE) is complete. See
     docs/architecture/remote-input-access.md for the shipped design. -->

---

<!-- Phase 10 (Static HTML Report Export) is complete. See
     docs/architecture/reporting.md for the shipped design. -->

---

### Phase 11 — Schema Validation and Asset Checks

**Size:** Medium (~2–3 weeks)

**Goal:** Let users declare typed contracts on task inputs, outputs, and
assets, and fail fast with precise diagnostics when a contract is violated.

#### Deliverables

- Declarative schema hooks on task signatures and asset definitions (e.g.
  column names / dtypes for tables, shape / dtype for arrays, pydantic-style
  models for structured values).
- Validation integrated into the task boundary so violations are raised as
  structured task failures with category, location, and offending fields.
- Optional per-asset invariants evaluated on write (e.g. row count bounds,
  nullability, enum domains).
- Validation results surfaced in provenance, CLI diagnostics, and the UI so
  failures are discoverable without reading logs.
- Minimal built-in adapters for common table/array libraries; third-party
  schema libraries pluggable via a thin protocol.

#### Key design points

- Validation is a task-boundary concern, not a scheduling concern. The DAG and
  cache keying should be unaffected beyond including schema identity in the
  task hash.
- Contracts should be expressible inline on the task / asset without forcing a
  separate schema registry.
- Diagnostics should point to the specific input/output and the specific field
  that failed, not just "validation error".
- The design must not couple Ginkgo to any one schema library; the built-in
  adapters should be replaceable.

#### Validation

- A task with a declared input schema fails immediately and cleanly when an
  upstream produces an incompatible value, with the offending field named.
- Asset write-time invariants are enforced before the asset is published, and
  violations prevent alias promotion.
- Failure classification groups schema failures into a dedicated category in
  end-of-run diagnostics.
- Adding or changing a schema invalidates dependent cache entries in a
  predictable, documented way.

---

## Tier 3 — Execution Backends

### Phase 12 — SLURM / HPC Backend

**Size:** Large (~4–6 weeks)

**Goal:** Execute tasks on traditional HPC clusters (SLURM first; LSF / SGE
as follow-ups) so Ginkgo is usable on academic and shared research
infrastructure without requiring Kubernetes or cloud batch services.

#### Deliverables

- A `RemoteExecutor` implementation targeting SLURM via `sbatch` / `srun`,
  integrated with the existing remote worker, code-sync packaging, and
  provenance pathways.
- Resource declaration mapping (CPU, memory, GPU, walltime, partition,
  account, QoS) from the existing per-task resource model to SLURM
  directives.
- Shared-filesystem-aware staging: when the workspace lives on a shared POSIX
  filesystem, avoid redundant code-sync and artifact transfer.
- Job submission, polling, cancellation, and log retrieval integrated with
  the scheduler's remote lifecycle and retry policies.
- Compatibility with the remote-input access layer so FUSE / staged inputs
  continue to work on HPC nodes where permitted, with clear fallbacks when
  FUSE is unavailable.
- Documented environment expectations (module system, container runtime
  availability, Pixi usability on compute nodes).

#### Key design points

- Implement against the existing `RemoteExecutor` protocol; do not fork the
  remote worker.
- HPC environments vary widely — the backend must treat site-specific details
  (partitions, accounts, module loads, container runtimes like Singularity /
  Apptainer) as configuration, not hard-coded behaviour.
- Respect HPC etiquette: bounded polling frequency, job arrays where
  appropriate for large fan-outs, and cooperative cancellation.
- Keep the SLURM implementation separable so LSF / SGE variants can be added
  without a second rewrite of the shared machinery.

#### Validation

- A representative workflow (mixed Python and shell tasks, per-task
  resources, at least one GPU task) runs end-to-end on a SLURM cluster with
  correct provenance.
- Cancellation from the client cleanly terminates submitted jobs.
- Retry policies behave identically to Kubernetes / GCP Batch backends for
  transient failures.
- On a shared filesystem, warm reruns avoid redundant code-sync and artifact
  transfer.
- Large fan-outs are submitted efficiently (job arrays or equivalent) rather
  than one `sbatch` per task where that would overwhelm the scheduler.
