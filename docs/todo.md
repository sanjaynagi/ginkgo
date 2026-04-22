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




ASSET KIND PARAMETER




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

## Tier 3 — Housekeeping

### Keyword-only `*` in skills init template

**Goal:** Clarify or remove the bare `*` separator in function signatures in the
skills init template.

Many functions in the skills init template use `*` as a parameter. This forces
all following parameters to be keyword-only. Review whether this is intentional
and justified, and document the rationale or remove it where it adds no value.

### Strip phase-numbered references from the codebase

**Goal:** Remove references to internal phase numbers (e.g. "Phase 7",
"Phase 9") from code, comments, and docs that ship with the project.

Phase numbers track development order and are not meaningful to external
readers. Replace such references with descriptive names for the feature or
subsystem (e.g. "sub-workflow composition", "remote input streaming"), or
delete them where they add no value. Audit `ginkgo/`, `docs/architecture/`, and
inline comments.
