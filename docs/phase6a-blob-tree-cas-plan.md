# Phase 6A Plan: Refactor Local Artifact Store onto Unified Blob/Tree CAS

## Problem Definition

The current `LocalArtifactStore` uses a flat content-addressed layout:

- Files: `.ginkgo/artifacts/<blake3>.<ext>`
- Directories: `.ginkgo/artifacts/<blake3>/` (full recursive copy)

This works for local execution but has three limitations that block later
Phase 6 work:

1. **No artifact metadata**: artifact IDs are bare filesystem names with no
   associated metadata (digest algorithm, kind, size, provenance origin).
   Remote backends need richer identity records.

2. **Opaque directory storage**: directories are stored as monolithic copies.
   There is no manifest describing their internal structure, which prevents
   lazy access, sparse hydration, and deduplication of shared files across
   directories.

3. **Extension-in-ID coupling**: the `<hash>.<ext>` naming convention bakes
   the file extension into the artifact identity. A storage-layout-neutral
   model should separate content identity from presentation metadata.

Phase 6A refactors the local store onto a unified blob/tree CAS model that
resolves these issues. Breaking changes to internal APIs and storage layout
are acceptable.

## Proposed Solution

### New Storage Layout

```text
.ginkgo/artifacts/
  blobs/
    <digest>                # raw file bytes, read-only
  trees/
    <tree_digest>.json      # directory manifest
  refs/
    <artifact_id>.json      # artifact metadata record
```

- **Blobs** are raw content bytes, named by digest alone (no extension).
- **Tree manifests** map `{relative_path: blob_digest, size, mode}` entries.
- **Artifact refs** are metadata records that tie together identity, kind,
  digest, original extension, size, and storage backend.

### New Data Model

```python
@dataclass(frozen=True, kw_only=True)
class BlobRef:
    """Reference to a single content-addressed blob."""
    digest_algorithm: str   # "blake3"
    digest_hex: str         # hex digest
    size: int               # byte count
    extension: str          # original file extension (e.g. ".csv")

@dataclass(frozen=True, kw_only=True)
class TreeEntry:
    """One entry in a tree manifest."""
    relative_path: str
    blob_digest: str
    size: int
    mode: int               # original file mode (e.g. 0o644)

@dataclass(frozen=True, kw_only=True)
class TreeRef:
    """Reference to a directory manifest."""
    digest_algorithm: str
    digest_hex: str         # digest of the manifest content
    entries: tuple[TreeEntry, ...]

@dataclass(frozen=True, kw_only=True)
class ArtifactRecord:
    """Metadata record persisted alongside stored content."""
    artifact_id: str        # unique ID (= content digest for managed artifacts)
    kind: str               # "blob" | "tree"
    digest_algorithm: str
    digest_hex: str
    extension: str           # original extension for blobs, empty for trees
    size: int                # total bytes
    created_at: str          # ISO timestamp
    storage_backend: str     # "local" for now, "s3" / "oci" later
```

### Refactored ArtifactStore Contract

The `ArtifactStore` protocol changes to return richer types. All callers
are updated directly -- no backward-compat shims.

| Current method              | New method                          | Change summary                           |
|-----------------------------|-------------------------------------|------------------------------------------|
| `store(src_path) -> str`    | `store(src_path) -> ArtifactRecord` | Returns record instead of bare ID string |
| `retrieve(id, dest_path)`   | `retrieve(id, dest_path)`           | Unchanged caller contract                |
| `exists(id) -> bool`        | `exists(id) -> bool`                | Unchanged                                |
| `delete(id)`                | `delete(id)`                        | Cleans up blobs + tree + ref             |
| `artifact_path(id) -> Path` | `artifact_path(id) -> Path`         | Returns blob path or reconstructed dir   |
| `store_bytes(data, ext)`    | `store_bytes(data, ext)`            | Returns `ArtifactRecord`                 |
| `read_bytes(id) -> bytes`   | `read_bytes(id) -> bytes`           | Unchanged                                |

### Directory Storage Change

Currently directories are copied wholesale. Under the new model:

1. Walk the directory tree.
2. Store each file as an individual blob.
3. Build a `TreeRef` manifest from the entries.
4. Serialize and store the manifest as `trees/<tree_digest>.json`.
5. Write an `ArtifactRecord` with `kind="tree"`.

On retrieval:

1. Load the tree manifest.
2. Reconstruct the directory by creating symlinks (or copies) from blobs.

This decomposition enables later phases to do sparse hydration, lazy reads,
and cross-directory blob deduplication.

## Implementation Stages

### Stage 1: Introduce data model types (pure addition)

- Add `ginkgo/runtime/artifact_model.py` with `BlobRef`, `TreeEntry`,
  `TreeRef`, `ArtifactRecord`.
- Unit tests for serialization/deserialization of these types.
- No behavioral changes to existing code.

### Stage 2: Refactor LocalArtifactStore for blob storage

- Change file storage from `<hash>.<ext>` to `blobs/<digest>`.
- Add `refs/<artifact_id>.json` metadata writing.
- Change `store()` return type to `ArtifactRecord`.
- Update all callers directly.
- Update `retrieve()`, `exists()`, `delete()`, `artifact_path()`.
- Update `store_bytes()` and `read_bytes()`.

### Stage 3: Add tree manifest support for directories

- Decompose directory storage into per-file blobs + tree manifest.
- Store manifests under `trees/<tree_digest>.json`.
- Update `retrieve()` to reconstruct directories from tree manifests.
- Ensure symlink-based retrieval still works for directory outputs.

### Stage 4: Update cache and evaluator integration

- Update `CacheStore` to work with `ArtifactRecord` instead of bare string
  IDs in `meta.json`.
- Update `_store_output_artifacts()` and `_symlink_output_artifacts()`.
- Update `validate_cached_outputs()` for new blob paths.
- No backward compatibility with old cache entries -- users run
  `ginkgo cache clear` after upgrading.

### Stage 5: Update tests

- Update `test_artifact_store.py` for new layout and return types.
- Update `test_cache_integrity.py` for new symlink targets.
- Run full integration suite (`test_examples.py`) to confirm no regressions.

## Risks and Tradeoffs

- **Directory decomposition increases I/O**: storing N individual blobs
  instead of one `shutil.copytree` is more filesystem operations. For the
  local case this is acceptable; it becomes an advantage for remote backends.
- **Breaking change**: existing `.ginkgo/artifacts/` and `.ginkgo/cache/`
  directories become invalid after the refactor. A `ginkgo cache clear`
  is required after upgrading.

## Success Criteria

- All existing tests pass with the new storage layout.
- `LocalArtifactStore` uses `blobs/<digest>` for file storage.
- Directory artifacts are decomposed into individual blobs plus a tree
  manifest.
- Artifact metadata records are persisted as JSON alongside stored content.
- Cache hit/miss behavior is correct on fresh runs.
- Integration examples run and cache correctly.
