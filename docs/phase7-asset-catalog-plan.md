# Phase 7 — Asset Runtime Foundation

## Problem Definition

Ginkgo is currently run-centric: task outputs are cached artifacts keyed by
input hashes, but there is no stable asset identity above those cache entries.
Users cannot name an output once and track its successive materializations
across runs, attach aliases for promotion-style workflows, or inspect which
upstream assets contributed to a downstream asset.

Phases 8 and 9 both need this foundation. Phase 8 needs stable asset identity
for DataFrame snapshot histories; Phase 9 needs the same primitives for model
and eval assets. Before adding type-specific behaviors, Ginkgo needs a small,
generic asset layer that works for file outputs and integrates with the
existing artifact store, evaluator, cache, and provenance model.

This phase deliberately focuses on the runtime foundation only. Rich read
surfaces and lifecycle tooling are deferred.

## Proposed Solution

Introduce `asset()` as a return-value wrapper for task outputs. The evaluator
detects this wrapper, materializes the underlying file through the existing
`ArtifactStore`, records immutable asset version metadata in a local
file-backed asset catalog, records lineage when upstream inputs are `AssetRef`
values, and replaces the wrapper with an `AssetRef` in the resolved task
output.

Phase 7 supports file assets only. Programmatic browsing APIs, staleness
reporting, pruning, and UI are explicitly deferred.

## Scope

### In scope

- `asset()` DSL wrapper
- `AssetKey`, `AssetVersion`, `AssetResult`, and `AssetRef`
- local file-backed `AssetStore` under `.ginkgo/assets/`
- evaluator integration for file assets
- `AssetRef` codec and cache integration
- provenance manifest support for produced assets
- tests for version registration, aliasing, lineage, evaluator behavior, and
  cache invalidation

### Out of scope

- programmatic asset API (`ginkgo.assets`)
- staleness reporting
- retention and pruning integration
- UI asset views
- non-file asset kinds beyond the extension points needed for future phases

## Target DSL

```python
from ginkgo import asset, file, flow, shell, task


@task()
def prepare(*, raw: file) -> file:
    cleaned = "cleaned.csv"
    # ... write cleaned.csv ...
    return asset(cleaned, name="cleaned_data")


@task(kind="shell")
def align(*, reads: file, ref: file, output: file) -> file:
    return shell(
        cmd=f"bwa mem {ref} {reads} > {output}",
        output=asset(output, name="aligned_reads"),
    )


@task()
def summarise(*, data):
    path = data.artifact_path
    # data is an AssetRef
    ...


@flow
def main():
    return summarise(data=prepare(raw=file("raw.csv")))
```

## Design

### Part 1 — Core Data Structures

**File:** `ginkgo/core/asset.py`

Add four core types:

- `AssetResult`: sentinel returned by `asset()`
- `AssetKey`: stable logical identity, composed of namespace and name
- `AssetVersion`: immutable metadata for one materialization
- `AssetRef`: resolved reference passed to downstream tasks

Phase 7 uses a single namespace:

- `"file"` for path-like outputs

`AssetRef.load()` for file assets returns the artifact path.

### Part 2 — `asset()` Wrapper

`asset()` wraps a task output and carries:

- the underlying value
- optional explicit `name`
- optional explicit `kind`
- optional metadata dict

Phase 7 accepts only path-like values or explicit `kind="file"`. Other asset
kind auto-detection is deferred.

Default naming:

- If `name=` is supplied, use it.
- Otherwise default to the producing task function name.

### Part 3 — Asset Identity

`AssetKey` and `AssetVersion` separate:

- logical asset identity
- immutable version identity
- physical artifact identity in `ArtifactStore`

`AssetVersion.version_id` should be derived from:

- asset key
- artifact content hash
- producing run id

This keeps versions immutable and stable without conflating them with cache
keys or artifact IDs.

### Part 4 — Asset Store

**File:** `ginkgo/runtime/asset_store.py`

Store metadata under `.ginkgo/assets/`:

```text
.ginkgo/assets/
└── file/
    └── <name>/
        ├── index.yaml
        └── versions/
            └── v-<hash>/
                └── meta.yaml
```

Required operations:

- register a version
- fetch a specific version
- fetch the latest version
- list versions for one asset key
- list all asset keys
- set and resolve aliases
- record and query lineage

Design constraints:

- catalog stores metadata only, never artifact bytes
- version directories are immutable
- `index.yaml` is the single mutable file per asset key
- writes are atomic and idempotent

### Part 5 — Evaluator Integration

At task completion, the evaluator should:

1. detect `AssetResult` values in supported output positions
2. resolve the asset kind as `"file"`
3. store the file through the existing `ArtifactStore`
4. build and register an `AssetVersion`
5. inspect resolved inputs for upstream `AssetRef` values and record lineage
6. record asset metadata in run provenance
7. replace the sentinel with an `AssetRef`

Supported output positions in this phase:

- direct Python task return values
- tuple/list elements in Python task returns
- `shell(..., output=asset(...))`
- notebook/script output declarations that directly contain `asset(...)`

### Part 6 — Cache and Codec Integration

`AssetRef` must:

- round-trip through `ginkgo/runtime/value_codec.py`
- be serializable for worker transport and cached values
- hash by `version_id` when used as a task input

This ensures downstream cache invalidation follows asset version identity
rather than re-hashing artifact contents.

### Part 7 — Provenance Integration

Extend task manifest entries so asset-producing tasks record asset metadata,
including:

- asset key
- version id
- content hash
- artifact id

Tasks producing multiple assets may use an `assets` list rather than a single
`asset` field.

## Implementation Sequence

| Step | Scope | Dependencies |
|------|-------|-------------|
| 1 | Add `AssetKey`, `AssetVersion`, `AssetResult`, and `AssetRef` | None |
| 2 | Add `asset()` wrapper with file-only validation | Step 1 |
| 3 | Implement `AssetStore` version and alias operations | Step 1 |
| 4 | Implement `AssetStore` lineage operations | Step 3 |
| 5 | Add file asset serializer/loader registry entry | Step 1 |
| 6 | Integrate evaluator asset materialization and `AssetRef` replacement | Steps 2-5 |
| 7 | Add `AssetRef` codec and cache hashing support | Steps 1, 6 |
| 8 | Record asset metadata in provenance | Step 6 |
| 9 | Add validation tests across store, evaluator, cache, and provenance | Steps 3-8 |

## Risks and Tradeoffs

| Risk | Mitigation |
|------|-----------|
| Asset detection across task kinds becomes too broad or implicit | Restrict support to direct, known output positions in this phase. |
| Users expect rich non-file asset behavior immediately | Make file-only support explicit in docs and keep extension points narrow but real. |
| Lineage accumulates historical edges that are no longer active | Accept additive lineage for now; it is informational and sufficient for audit/debug use. |
| Multiple public surfaces would lock in premature abstractions | Defer programmatic API, advanced CLI, and UI until the runtime model settles. |

## Deferred to Phase 10

The following are intentionally not part of Phase 7:

- programmatic asset API
- advanced CLI read paths beyond minimal inspection
- staleness reporting
- retention and pruning integration
- UI asset views and lineage dashboards

## Validation

1. A Python task returning `asset("output.csv")` registers a file asset with
   the expected asset key, version id, content hash, and artifact id.
2. A shell task returning `shell(..., output=asset(path))` registers the file
   asset correctly.
3. A task returning `asset(path, name="processed/sample_1")` creates a
   hierarchical asset key with the explicit name.
4. Register two versions of the same asset and assert `get_latest()` returns
   the newer one.
5. Set an alias, resolve it, update it, and assert resolution changes.
6. A downstream task receiving an `AssetRef` hashes `version_id` for cache
   invalidation.
7. Lineage edges are recorded when an asset-producing task consumes upstream
   `AssetRef` values.
8. Provenance records asset metadata for producing tasks.
9. `AssetRef.load()` for file assets returns a usable path.

## Success Criteria

- A task can return `asset(path, name=...)` and produce a durable file asset.
- Ginkgo records immutable asset versions in a local metadata catalog while
  continuing to store bytes only in `ArtifactStore`.
- Downstream tasks receive `AssetRef` values automatically.
- Cache invalidation follows `AssetRef.version_id`.
- Aliases can be assigned and resolved.
- Lineage is recorded between produced assets.
- Provenance manifests include asset metadata for producing tasks.
- Phase 8 and Phase 9 can add new asset kinds without changing the Phase 7
  storage model.
