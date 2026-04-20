# Caching

The cache lives under `.ginkgo/cache/` and is keyed by:

- task identity
- task version
- task source hash
- resolved input hashes
- environment lock hash when `env=` is used
- source file hash for driver tasks (notebook and script) folded at evaluation time

Implemented cache hashing includes:

- BLAKE3 as the canonical digest algorithm for cache keys, artifact IDs, input hashing, and source hashing
- scalar hashing via stable value hashing
- file-content hashing
- recursive folder-content hashing
- Pixi lock hashing for local environments
- container image digest hashing for container environments
- codec-based hashing for arrays, DataFrames, and other supported Python values

Cache entries are written atomically and reused across reruns when inputs are unchanged.

The runtime hashes the top-level task function source during task registration
and stores that `source_hash` in both the cache key payload and `meta.json`, so
task-body changes invalidate prior cache entries without requiring a manual
`version=` bump. If source extraction fails for a task definition, registration
fails explicitly instead of silently weakening cache correctness.

File and folder outputs flow through a formal `ArtifactStore` contract,
implemented locally by `LocalArtifactStore` in
`ginkgo/runtime/artifacts/artifact_store.py`. Artifact identity is content-addressed:
files use the blob digest and directories use a manifest digest. That identity
is recorded in cache metadata as `artifact_ids`, which gives later roadmap
phases a stable contract for remote storage and lineage features.

The artifact store is the canonical immutable source of truth for managed path
outputs, while the working tree is a writable materialized view. When a task
produces a `file` or `folder`, Ginkgo copies the bytes into
`.ginkgo/artifacts/` as a read-only artifact but leaves the working-tree output
in place as an ordinary writable file or directory. On cache hit, Ginkgo
compares each managed output path against the cached artifact content and
restores only paths that are missing, type-mismatched, or have diverged. If a
working-tree output already matches the cached artifact, it is left untouched.

`ginkgo cache prune` and related cache cleanup paths are artifact-aware:
read-only artifacts have permissions restored before deletion so cache
maintenance can safely remove unreferenced stored outputs.

`ginkgo cache prune` supports three orthogonal policies, which may be
combined:

- `--older-than <duration>` — remove every entry older than the cutoff
  (`45m`, `12h`, `30d`).
- `--max-size <size>` — remove oldest entries until total cache size is at
  or below the target (`500MB`, `2GB`, `10GB`).
- `--max-entries <N>` — remove oldest entries until the total entry count
  is at or below the target.

At least one policy is required. When multiple are given, they are applied
together: `--older-than` selects unconditionally, and `--max-size` /
`--max-entries` then pick additional oldest-first entries until their
budgets are met. Orphan artifacts are garbage-collected once at the end.
