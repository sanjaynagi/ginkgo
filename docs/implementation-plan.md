# Ginkgo Implementation Plan

This document describes work that has not yet been implemented. Phases are
grouped into dependency tiers — each tier builds on the foundations established
by the previous one.

Each phase is independently testable and follows the same structure:

- Goal
- Deliverables
- Key design points
- Validation

## Tier 3 — Asset Layer

### Phase 8 — Versioned DataFrame Assets

**Goal:** Give `pandas.DataFrame` assets Iceberg-like snapshot behavior by extending Phase 2's immutable artifact storage with a lineage manifest layer.

**Depends on:** Phase 2 (`ArtifactStore`, immutable artifacts), Phase 7 (asset
catalog for asset key resolution). Benefits from the implemented remote
references and staged-access layer for time-travel reads against remote-backed
storage.

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
- When remote-backed artifact storage is added, snapshot Parquet files should be
  stored remotely via the same `ArtifactStore` interface. Time-travel reads
  should go through `retrieve()` and benefit from the existing local staging
  cache automatically.
- The snapshot manifest is intentionally minimal — it is not Iceberg. The storage contract is immutable blobs plus a lightweight manifest, not a full table format.
- The snapshot store is an implementation detail behind the asset abstraction so larger backends (e.g. Delta Lake, Iceberg) can be substituted later.

#### Validation

- A task materializing a DataFrame asset twice with different inputs produces two distinct snapshots with the correct parent-child relationship and distinct `artifact_id` values.
- A downstream task pinned to an older snapshot id reads the historical data correctly even after a newer snapshot exists, via `ArtifactStore.retrieve()`.
- Re-running a consumer task against the same snapshot id hits the cache without re-hashing the DataFrame.
- Schema summaries and row counts are recorded in both the snapshot manifest and run provenance.
- Assert that with a remote-backed artifact store active, time-travel reads are
  served from the remote store and local staging cache correctly.

---

### Phase 9 — ML Model and Evaluation Support

**Goal:** Add three ML-specific capabilities through the existing `kind=` extension point: versioned model assets (`kind="model"`), structured evaluation records (`kind="eval"`), and parameter sweep fan-out (`.sweep()`). Together these let practitioners train, evaluate, compare, and promote models without manual metric logging or version tracking.

**Depends on:** Phase 7 (asset catalog — provides asset identity, versioning,
alias resolution, and the asset store that this phase registers model and eval
assets into). Phase 2 (`ArtifactStore`, immutable artifacts). Benefits from the
implemented remote references and staged-access layer plus Phase 8 (upstream
dataset snapshot IDs for lineage).

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
- When remote-backed artifact storage is added, model and eval artifacts should
  be stored and retrieved remotely via the same `ArtifactStore` interface, with
  no changes to registry logic.
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

### Phase 10 — Asset Read Paths and Lifecycle Tooling

**Goal:** Add the user-facing read surfaces and maintenance workflows that sit
on top of Phase 7's asset runtime foundation.

**Depends on:** Phase 7 (asset runtime foundation). Benefits from Phase 8 and
Phase 9 as additional asset kinds begin to use the shared catalog.

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

- Phase 10 is read- and lifecycle-oriented. It should not change Phase 7's core
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

### Phase 11 — Data Quality and Profiling

**Goal:** Add automated statistical profiling, data quality gates, and drift detection for DataFrame assets, giving data scientists built-in observability over their data without external tools.

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

**Depends on:** remote-backed artifact storage for managed outputs (hard
prerequisite; remote jobs cannot access local `.ginkgo/cache/`), Phase 13
(Secrets — remote backends require credentials).

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
- Remote execution makes remote-backed managed artifact storage mandatory; the
  current remote-input staging layer is necessary but not sufficient on its own.
p
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

### Phase 12 — Public Documentation

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
