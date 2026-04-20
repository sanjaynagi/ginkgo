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

## Tier 3 — Composition and Remote Execution

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

<!-- Phase 9 (Remote Input Streaming / FUSE) is complete. See
     docs/architecture/remote-input-access.md for the shipped design. -->
