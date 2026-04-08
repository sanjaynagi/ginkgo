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

### Phase 1 — Hardening and UI Polish

**Goal:** Finish the production-readiness and local UI work that remains.

#### Deliverables

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
- Tighten documentation around partial resume, dry-run behavior, and resource
  declarations.

#### Key design points

- This phase is explicitly for remaining gaps in areas that already exist.
- The goal is to reduce ambiguity and operational rough edges before the runtime
  surface area expands further.
- UI work should remain local-first and should build on the current file-backed
  provenance model.

#### Validation

- Re-run `VW-4`, `VW-5`, `VW-6`, and `VW-8` through the polished CLI and UI
  paths and assert the richer retry, cache, and resource behavior is visible in
  both CLI output and persisted provenance.
- Assert the improved diagnostics distinguish common classes of failure such as
  env mismatch, invalid paths, and packaging/importability errors.

---

### Phase 2 — Runtime Profiling and Resource Controls

**Goal:** Add opt-in runtime profiling, explicit per-task thread declarations,
and task-level concurrency limits so workflows can be diagnosed and scheduled
with precision.

**Detailed design:** [`docs/phase20-profiling-and-resource-controls-plan.md`](phase20-profiling-and-resource-controls-plan.md)
(workstreams 1–3; workstream 4 — benchmark comparison table — is shipped)

#### Deliverables

**`--profile` mode (workstream 1):**

- Add an opt-in `--profile` flag to `ginkgo run` that enables wider timing
  coverage across runtime phases (CLI startup, module import, flow construction,
  scheduler loop overhead, event emission, resource monitor lifecycle, final
  reporting).
- Persist the profile in run provenance and surface it in CLI output at run end.
- `ginkgo inspect run` includes the full timing structure.
- Profiling remains coarse-grained and cheap enough for routine diagnostic use.

**First-class per-task thread control (workstream 2):**

- Formalize task CPU footprint as an explicit resource declaration rather than
  the current implicit `threads` argument convention.
- Ensure shell tasks can access the resolved thread count through a standard
  contract (e.g. `GINKGO_THREADS` environment variable).
- Preserve compatibility with existing workflows that use a `threads` parameter.

**Task-level concurrency limits (workstream 3):**

- Add named concurrency groups with integer limits (e.g.
  `model_training: 1`).
- Allow tasks to declare membership in a named concurrency group.
- Enforce concurrency-group limits in scheduler selection alongside existing
  `jobs`, `cores`, and `memory` constraints.
- Ensure the policy applies uniformly to ordinary tasks, `.map()` fan-out, and
  `.product_map()` fan-out.
- Surface concurrency-group metadata in runtime events and provenance.

#### Key design points

- `--profile` should not add measurable overhead or distort the runs it
  explains.
- Thread control must not break existing workflows that already use a `threads`
  parameter — provide a clear migration path or backwards-compatible layering.
- Concurrency groups are a scheduler primitive, not a `.map()` API option.
  Fan-out already produces normal task nodes; the scheduler decides how many run
  at once.
- `threads` remains a declaration of CPU footprint, not a hidden mutex.

#### Validation

- `ginkgo run --profile` produces an attributable timing breakdown that explains
  nearly all non-user-code runtime overhead.
- Shell tasks can consume the scheduled thread budget through a standard
  contract.
- A workflow declaring `model_training` group with limit `1` enforces singleton
  execution across mapped fan-out while unrelated tasks still run concurrently.
- Existing workflows using the implicit `threads` convention continue to work.

---

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

### Phase 4 — DataFrame Assets

**Goal:** Add a DataFrame-aware asset kind so that DataFrame-producing tasks
record schema and shape metadata alongside immutable Parquet artifacts, and
downstream tasks can consume them through the existing asset catalog without
re-hashing the full DataFrame in memory.

**Depends on:** the existing `ArtifactStore` (immutable artifacts) and asset
catalog (asset key resolution).

#### Deliverables

- Add a `kind="dataframe"` asset backend for DataFrame-producing tasks. Each
  successful materialization calls `ArtifactStore.store()` to write an immutable
  Parquet artifact and registers an `AssetVersion` in the catalog with
  tabular-specific metadata:
  - schema summary (column names and dtypes)
  - row count
  - producing run id and task id
- Downstream task cache keys consume `artifact_id` rather than re-hashing the
  full DataFrame in memory, so cache invalidation happens exactly when the
  upstream data changes.
- Asset resolution always returns the latest version for a given asset key.

#### Key design points

- Immutability is inherited from the artifact store: artifacts are read-only
  once written. This phase adds DataFrame-specific metadata; it does not
  re-implement storage.
- No snapshot chains, time-travel, or head-pointer files — the latest version
  is always the one consumers read.
- The asset kind is an implementation detail behind the asset abstraction so
  richer backends (e.g. Delta Lake, Iceberg) can be substituted later if
  versioning becomes a real need.

#### Validation

- A `kind="dataframe"` task materializes a DataFrame as an immutable Parquet
  artifact with schema and row-count metadata in the asset catalog.
- Re-running a consumer task against the same `artifact_id` hits the cache
  without re-hashing the DataFrame.
- Schema summaries and row counts are recorded in both asset metadata and run
  provenance.

---

### Phase 5 — ML Model Training Support

**Goal:** Add model-aware task support so that training progress is observable
in real time, trained models are cataloged and listable, and parameter sweeps
are a first-class fan-out primitive.

**Depends on:** the existing asset catalog and `ArtifactStore`. Benefits from
Phase 4 for upstream dataset lineage.

#### Target DSL

```python
from ginkgo import task, flow, model, file

@task(kind="model")
def train(data: file, *, lr: float, epochs: int):
    clf = fit(load(data), lr=lr, epochs=epochs)
    return model(clf, framework="sklearn")

@flow
def main():
    data = prepare_data(raw=file("data/raw.csv"))
    models = train.sweep(data=data, lr=[0.001, 0.01, 0.1], epochs=[10, 50])
    return models
```

#### Deliverables

**`kind="model"` — model assets with training progress:**

- Add a `ModelResult` sentinel (following the `shell()` / `ShellExpr` pattern)
  returned from `kind="model"` task bodies via a `model()` builder function.
  The sentinel carries the model object, framework name, and optional metrics.
- On task completion, the evaluator serializes the model, registers an immutable
  asset version in the catalog (namespace `"model"`), and auto-captures scalar
  task inputs as `params` in the version metadata.
- Expose real-time training progress through runtime events so the Rich CLI
  renderer can display per-task training status (e.g. epoch, loss) alongside
  the existing task progress output.
- `ginkgo model ls` — list trained model assets with latest metrics summary.

**`.sweep()` — parameter exploration:**

- Add a `.sweep()` method on `TaskDef` / `PartialCall`, parallel to `.map()`.
  It partitions kwargs into fixed (scalar) and swept (list) arguments, computes
  combinations via `itertools.product` (grid) or `zip` (positional), and
  delegates to `.map()` to produce an `ExprList`.
- Support `strategy="grid"` (Cartesian product, default) and `strategy="zip"`
  (positional pairing, equal-length lists required).

#### Key design points

- Model assets are registered in the existing asset catalog — this phase does
  not build a separate model registry. Serialization, versioning, and storage
  go through the existing `ArtifactStore` and `AssetStore`.
- `kind="model"` uses `execution_mode = "driver"` (same as shell) — the task
  body runs on the scheduler, produces a sentinel, and the evaluator handles
  serialization and storage.
- Training progress events are emitted through the existing runtime event bus.
  The Rich renderer displays them; `--agent` mode includes them in JSONL output.
- `.sweep()` is deliberately simple (grid/zip only) — it is not a Bayesian
  optimization framework. Complex HPO should use external tools with Ginkgo
  tasks as the execution substrate.
- Evaluation, model promotion, alias management, and comparison tooling are
  deferred. If needed, they can be added incrementally without changing the
  core model asset contract.

#### Validation

- A `kind="model"` task serializes and registers an immutable model version with
  auto-captured params and metrics.
- Training progress events appear in Rich CLI output during execution.
- `ginkgo model ls` lists trained models with metrics.
- `train.sweep(data=d, lr=[0.01, 0.1], epochs=[10, 50], strategy="grid")`
  produces 4 tasks with correct parameter combinations.
- `strategy="zip"` with equal-length lists produces N tasks; unequal lengths
  raise a clear error.
- Re-running with identical inputs hits the cache; changed inputs create a new
  version.

---

### Phase 6 — Asset Read Paths and Lifecycle Tooling

**Goal:** Add the user-facing read surfaces and maintenance workflows that sit
on top of the asset catalog.

**Depends on:** the existing asset catalog. Benefits from Phase 4 and Phase 5 as
additional asset kinds begin to use the shared catalog.

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

## Tier 3 — Composition and Remote Execution

### Phase 7 — Kubernetes / Batch Executor

<<<<<<< Updated upstream
**Depends on:** Phase 8 (versioned DataFrame assets — profiling hooks into the DataFrame materialization path, drift detection compares version metadata).

#### Deliverables

**Statistical profiling at materialization time:**

- When a `kind="dataframe"` task materializes a snapshot, automatically compute column-level statistics from the in-memory DataFrame before serialization:
  - null count and null rate per column
  - unique count / cardinality
  - min / max / mean / std for numeric columns
  - top-N frequent values for categorical columns
  - approximate quantiles for numeric columns
- Store the profile in `AssetVersion.metadata` under a `profile` key alongside the existing schema and row count fields.
- Profiling adds negligible overhead since the DataFrame is already in memory for serialization.

**Data quality gates:**

- Add a `checks=` parameter to the `dataframe_asset()` builder:

  ```python
  return dataframe_asset(df, checks=[
      check.row_count(min=1000),
      check.no_nulls("user_id"),
      check.unique("user_id"),
      check.range("age", min=0, max=150),
      check.schema_matches({"user_id": "int64", "age": "float64"}),
      check.null_rate("email", max=0.05),
  ])
  ```

- The evaluator runs checks after serialization. The asset version is always written (data is recorded even when quality fails), but tagged `quality: "pass" | "fail"` in metadata with individual check results.
- Downstream tasks consuming the asset can declare `only_if_passing=True` to skip execution when upstream data quality fails, producing a clear skip reason in provenance rather than propagating bad data silently.
- Quality check results are recorded in run provenance alongside the asset metadata.

**Drift detection between versions:**
- Add `ginkgo asset diff <key> <ver1> <ver2>` to compare profiling stats between two versions of the same asset:

  ```
  $ ginkgo asset diff dataframe/features v-a1b2c3 v-d4e5f6

  COLUMN       METRIC       v-a1b2c3    v-d4e5f6    DELTA
  row_count    -            10000       12500       +25.0%
  age          mean         34.2        38.7        +13.2%
  age          null_rate    0.01        0.12        +1100%  ⚠
  category     cardinality  15          23          +53.3%
  ```

- Warning thresholds are configurable per metric. Defaults flag large relative changes in null rates, cardinality, and distribution statistics.
- The UI data preview (from Phase 8) surfaces the profile alongside the data, and the version detail view shows drift indicators when comparing to the parent version.

#### Key design points

- Profiling runs synchronously during materialization while the DataFrame is in memory. This is a one-time cost at write time that avoids needing to re-read the Parquet artifact later.
- Quality gates are advisory by default — the asset is always written. `only_if_passing` on downstream tasks is opt-in. This avoids data loss while still enabling fail-fast pipelines when desired.
- The `check` namespace provides a small, opinionated set of common data quality assertions. This is not Great Expectations — complex validation should use external tools with Ginkgo tasks as the execution substrate.
- Drift detection is pure metadata comparison — it never loads artifact bytes. This means it works for arbitrarily large DataFrames and is fast enough to show in the UI.

#### Validation

- A `kind="dataframe"` task produces a snapshot with a complete statistical profile in version metadata (null rates, cardinality, min/max for all columns).
- A DataFrame asset with `checks=[check.no_nulls("id")]` passes quality when the column has no nulls and fails when it does. Both cases write the asset version; the failing case tags `quality: "fail"` with the check result.
- A downstream task with `only_if_passing=True` skips execution when its upstream DataFrame asset failed quality checks, and runs normally when quality passes.
- `ginkgo asset diff` renders correct deltas between two versions using only stored profile metadata.
- The UI version detail view shows profile statistics and drift indicators relative to the parent version.

---

## Tier 4 — Composition, Publishing, and Remote Execution

### Phase 14 — Kubernetes / Batch Executor

**Goal:** Run tasks on a remote scheduler such as Kubernetes Jobs or cloud batch services while preserving Ginkgo's dynamic DAG and cache semantics.
=======
**Goal:** Run tasks on a remote scheduler such as Kubernetes Jobs or cloud batch
services while preserving Ginkgo's dynamic DAG and cache semantics.
>>>>>>> Stashed changes

**Depends on:** remote-backed artifact storage for managed outputs (hard
prerequisite; remote jobs cannot access local `.ginkgo/cache/`).

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

- The main evaluator can remain the control plane, but it must treat remote jobs
  as asynchronous task futures.
- Dynamic DAG expansion should still happen in the scheduler after parent-task
  results return.
- Remote execution makes remote-backed managed artifact storage mandatory; the
  current remote-input staging layer is necessary but not sufficient on its own.

#### Validation

- Re-run `VW-2`, `VW-3`, `VW-6`, `VW-7`, and `VW-8` through the remote
  executor.
- Assert remote logs, exit codes, and declared resources are reflected in the
  local run manifest.
- Assert cancellation from the CLI propagates to in-flight remote jobs.

---

### Phase 8 — Workflow Composition

**Goal:** Allow Ginkgo workflows to invoke other Ginkgo workflows as first-class
sub-workflows, enabling reuse and composition without duplicating task logic.

#### Deliverables

- Add a `call_workflow` primitive that invokes a named Ginkgo workflow from
  within a parent workflow task.
- Support two composition modes:
  - **Inline expansion**: the sub-workflow's DAG is expanded into the parent DAG
    at plan time, making its tasks visible in the parent's provenance and UI.
  - **Opaque invocation**: the sub-workflow runs as a self-contained execution
    unit and its result is returned as an artifact to the parent.
- Pass parameters, secrets, and resource declarations through the call boundary
  consistently.
- Propagate sub-workflow run ids and provenance back into the parent run
  manifest so lineage is fully traceable.
- Detect and reject circular workflow dependencies at plan time.
- Extend the UI and `ginkgo inspect` to show sub-workflow boundaries and nested
  task graphs.

#### Key design points

- Inline expansion is preferred for small, reusable task groups where joint
  caching and visibility matter.
- Opaque invocation is preferred for independently versioned or cross-team
  workflows where internal structure should be encapsulated.
- Sub-workflow cache semantics must be consistent with top-level workflow
  semantics: the same inputs should hit cache regardless of call depth.
- Recursive or indirect circular dependencies must be caught before any
  execution begins.

#### Validation

<<<<<<< Updated upstream
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

### Phase 16 — UI Performance and Responsiveness

**Goal:** Keep the local web UI fast and predictable as run history, task counts, event volume, and artifact metadata grow.

#### Problem definition

The current UI is local-first and provenance-backed, which keeps the architecture simple, but that model can still become sluggish when a workspace accumulates many runs, large manifests, dense task graphs, or high-frequency live events. Notebook artifacts and richer asset metadata will increase that pressure. UI responsiveness should be treated as an explicit workstream rather than an incidental cleanup item.

#### Deliverables

- Profile the UI server and frontend against larger synthetic and real workspaces to identify the dominant latency and rendering costs.
- Reduce run-list and run-detail load latency by avoiding full manifest reads when summary data is sufficient.
- Add pagination, windowing, or incremental loading for large run collections and task lists.
- Make live updates cheaper by sending targeted diffs and limiting unnecessary client-side recomputation.
- Improve graph rendering performance for large DAGs through layout caching, viewport-aware rendering, or progressive expansion.
- Ensure notebook and asset detail views remain responsive even when runs contain many artifacts.
- Add benchmark-style regression checks for UI server payload construction and selected frontend interactions.

#### Key design points

- Performance work should preserve the current provenance-first architecture; optimisations should improve data access patterns rather than introduce a second source of truth.
- The server should distinguish between summary payloads and detail payloads so the frontend does not pay full-cost reads for overview screens.
- Frontend rendering work should favour incremental rendering and bounded DOM size over cosmetic complexity.
- UI performance should be measured with repeatable fixtures and checked into the repository where practical, so regressions are visible.

#### Risks and tradeoffs

- Aggressive caching can make the UI appear fast while serving stale data if invalidation rules are weak.
- Progressive loading improves responsiveness but can complicate navigation and empty-state handling if the UX is not deliberate.
- Optimising graph rendering may require simplifying some visual affordances for very large workflows.

#### Success criteria

- Large workspaces with many historical runs remain navigable without noticeable stalls in the run list.
- Opening a run with a large task graph or many artifacts does not block the UI for multiple seconds on routine hardware.
- Live runs continue to update smoothly without flooding the browser with redundant state changes.
- Notebook and artifact views remain responsive enough to be practical on real workflow runs rather than only toy examples.

---

### Phase 20 — Task-Level Concurrency Limits

**Goal:** Let workflows limit concurrency for specific classes of tasks, including `.map()` fan-out branches, without abusing `threads` or global `cores`.

#### Problem definition

Ginkgo already expands `.map()` and `.product_map()` into independent task
nodes, and the evaluator already enforces global `jobs`, `cores`, and optional
`memory` limits during dispatch. That is sufficient for broad resource control,
but it does not express policies such as "train only one model at a time" while
still allowing unrelated work to run concurrently. The current workaround is to
inflate a task's `threads` requirement until it monopolizes the global core
budget, but that conflates CPU demand with concurrency intent and can leave the
machine underutilized.

#### Deliverables

- Add first-class task-level concurrency metadata so a task can declare
  membership in a named concurrency group.
- Add run-time configuration for integer limits per concurrency group.
- Enforce concurrency-group limits in scheduler selection alongside existing
  `jobs`, `cores`, and `memory` constraints.
- Ensure the policy applies uniformly to all task dispatch, including ordinary
  tasks, `.map()` fan-out, and `.product_map()` fan-out.
- Surface concurrency-group metadata in runtime events and provenance where it
  improves debuggability.
- Document the recommended authoring pattern and the difference from
  `threads`-based CPU budgeting.

#### Key design points

- This should be implemented as a scheduler primitive, not as a `.map()` API
  option. Fan-out already produces normal task nodes; the scheduler should
  decide how many of those nodes may run at once.
- The initial scope should use named concurrency groups with integer limits,
  for example `model_training: 1`.
- Task metadata should remain explicit and static at definition time unless a
  stronger use case emerges for per-invocation overrides.
- Scheduler selection should account for both currently running group members
  and newly selected ready tasks in the same dispatch cycle.
- `threads` should remain a declaration of CPU footprint, not a hidden mutex or
  singleton mechanism.

#### Risks and tradeoffs

- This adds another axis to scheduler feasibility and increases the complexity
  of dispatch selection.
- Tasks that belong to multiple groups may make contention harder to reason
  about if the API is too flexible too early.
- Exposing both `threads` and concurrency groups requires clear documentation
  so users understand when to use each control.

#### Success criteria

- A workflow can declare that all `train_model` tasks belong to a
  `model_training` group with limit `1` and have that policy enforced across
  mapped fan-out branches.
- Unrelated tasks that do not belong to that group can still run concurrently
  when `jobs`, `cores`, and `memory` permit.
- Existing workflows that rely only on `jobs`, `cores`, and `memory` continue
  to behave as before.
- Scheduler tests cover single-group and mixed-group contention scenarios,
  including mapped workloads.
=======
- Define a parent workflow that calls a sub-workflow in inline mode and assert
  that sub-workflow tasks appear in the parent DAG, share the same run manifest,
  and are individually cached.
- Define a parent workflow that calls a sub-workflow in opaque mode and assert
  that only the sub-workflow's result artifact appears in the parent provenance,
  not its internal tasks.
- Assert that circular workflow references are detected at plan time with a
  clear error message.
- Assert that parameters and secrets passed to a sub-workflow are correctly
  scoped and do not leak into unrelated tasks in the parent workflow.
- Re-run the parent workflow with unchanged inputs and assert that sub-workflow
  tasks are served from cache at the appropriate granularity for each
  composition mode.
>>>>>>> Stashed changes
