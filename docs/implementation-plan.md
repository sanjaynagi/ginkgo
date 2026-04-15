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

**Goal:** Run tasks on a remote scheduler such as Kubernetes Jobs or cloud batch
services while preserving Ginkgo's dynamic DAG and cache semantics.

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

---

### Phase 9 — Remote Input Streaming (FUSE)

**Goal:** Add a FUSE-style streaming layer for remote inputs so that tasks
running on remote workers can read from object stores without first staging
the full file to local disk.

**Depends on:** Phase 7. The staging contract (`waiting -> staging -> running`)
and the worker-local staging root are the substrate this phase replaces; the
remote executor must already exist so the streaming layer has a real consumer.

#### Deliverables

- Add a worker-local FUSE mount that exposes configured remote prefixes as
  ordinary local paths, so tasks continue to receive plain `file` and
  `folder` arguments.
- Integrate the mount with the existing staging layer as an alternative
  hydration strategy: tasks declare or inherit a streaming policy, and the
  staging phase either downloads (current behavior) or mounts (new behavior)
  on a per-input basis.
- Support read-through caching with bounded local disk so repeated reads do
  not incur repeated network round-trips, while keeping the cache footprint
  predictable on small pod disks.
- Surface streaming metrics (bytes read, cache hit rate, fault count) in
  runtime events and per-task provenance so the cost of streaming is visible
  alongside existing CPU and memory metrics.
- Provide a fallback to staged downloads when streaming is unavailable or
  unsuitable (no FUSE support, tools that mmap or seek pathologically), with
  the choice recorded in provenance.
- Extend `ginkgo doctor` to validate FUSE availability, kernel module
  presence, and required pod security context for the configured executor.

#### Key design points

- Streaming is an input-side optimization. Output publishing continues to go
  through the remote-backed artifact store from Phase 7 — the asymmetry is
  deliberate, since outputs must be immutable and content-addressed.
- The FUSE layer lives behind the existing staging contract rather than
  beside it. Tasks, the scheduler, and the cache should not gain a separate
  "streamed input" code path; the only difference is whether the bytes
  behind a path are present locally or fetched on demand.
- Streaming policy is per-input, not global. Some tools (random-access BAM
  indexing, SQLite) require local files; others (sequential FASTQ scans,
  CSV reads) stream cleanly. The default should be staged for correctness,
  with streaming opted in per task or per input pattern.
- POSIX semantics over object stores are incomplete. The streaming layer
  must document and enforce its supported subset (no atomic rename, no
  random writes, bounded `stat` accuracy) rather than pretending the mount
  is a full filesystem.
- Cache identity is unaffected: streamed inputs hash by the same remote
  reference identity already used for staged inputs.

#### Validation

- A bioinformatics workflow that consumes large FASTQ inputs runs end to end
  on the remote executor with streaming enabled, and the pod's local disk
  high-water mark stays well below the total input size.
- Re-running the same workflow with streaming disabled produces identical
  outputs and identical cache identity, confirming the streaming layer is a
  pure performance optimization.
- A task that requires random-access reads (e.g. indexed BAM) is correctly
  served from the staged-download fallback when declared incompatible with
  streaming, and provenance records the fallback.
- Streaming metrics appear in run provenance and are inspectable via
  `ginkgo inspect run`.
- `ginkgo doctor` reports a clear error on a cluster or host where FUSE is
  unavailable, rather than failing opaquely at task execution time.
