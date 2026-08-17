# Caching

The cache lives under `.ginkgo/cache/` and is keyed by:

- task identity
- task version
- task source hash
- resolved input hashes
- environment identity when `env=` is used
- source file hash for driver tasks (notebook and script) folded at evaluation time

Implemented cache hashing includes:

- BLAKE3 as the canonical digest algorithm for cache keys, artifact IDs, input hashing, and source hashing
- scalar hashing via stable value hashing
- file-content hashing
- recursive folder-content hashing
- Pixi manifest hashing for local environments
- declared image reference for container environments
- codec-based hashing for arrays, DataFrames, and other supported Python values

Cache entries are written atomically and reused across reruns when inputs are unchanged.

## Environment identity

`ExecutionEnvironment.env_identity` returns the identity of the *declared*
environment: the digest of the Pixi manifest, or the container image reference.
`CacheStore._env_hash` folds it into the key, and refuses (`UnresolvedEnvIdentityError`)
to build a key from a backend that returns nothing for a declared `env=`.

The identity has to be knowable before the environment is materialised, because
`_prepare_node` builds the cache key before `_prepare_task_environment` runs —
deliberately, so that a task about to hit the cache does not pay for a
`pixi install` or an image pull. Identity previously came from local
materialisation state (the `pixi.lock` that `pixi install` writes, the image ID
of a pulled image), which meant the key on the run that materialised an
environment differed from the key on every run afterwards, and every env-backed
task re-ran exactly once — issue #194, visible on the stock `ginkgo init`
scaffold, which ships manifests and no locks.

The state of the environment as materialised on this machine is still recorded,
by `materialized_digest`, but only for provenance: the lock file is copied into
the run directory, and a container task's manifest entry carries
`container_image_digest`. The tradeoff is that changes not visible in the
declaration — `pixi update` re-solving a lock, a mutable tag repointed upstream
— no longer invalidate cache entries. Pin `image@sha256:...`, or bump `version=`,
when that matters. Hashing the declaration also makes keys portable: a lock file
is solved per machine and per moment, so lock-derived keys never transferred
between machines in the first place.

## Untracked path boundaries

Content hashing is dispatched on the declared annotation:
`is_path_shaped_annotation` (`ginkgo/core/types.py`) decides whether a parameter
or return carries a `file` / `folder` marker. A path that crosses a task boundary
annotated only `str` or `Path` falls through to the scalar branch, so the
downstream cache key records the *path string* and nothing about the bytes at
that path. The producer can rewrite the file and the consumer still reports a
cache hit — the stale-result failure in issue #121.

Ginkgo cannot silently promote those boundaries to content hashing: `str` is
also the legitimate way to pass a path deliberately left untracked (a log, a
scratch location, an append-only sink). Instead the evaluator warns.
`ConcurrentEvaluator._warn_on_untracked_path_inputs` runs in `_prepare_node`,
before the cache-hit branch, and emits a `TaskNotice` when all of the following
hold:

- the argument was resolved from an upstream `Expr` / `OutputIndex` / `ExprList`
  in this graph, so a producer in the same run can rewrite it;
- the resolved value is path-like (`is_path_like`) but not a `file` / `folder`
  marker instance;
- the parameter annotation is not path-shaped;
- the value names something that exists on disk
  (`is_untracked_path_value` in `ginkgo/runtime/task_validation.py`, built from
  the `core/types.py` predicates rather than repeating them).

An `AssetRef` never trips this: it is not path-like, and the cache keys it by
version id, so a content change does invalidate the consumer.

Arguments are walked in step with their resolved values, so a path reaching the
task inside a list, tuple, dict, or fan-out `ExprList` — the ordinary fan-in
shape, `inputs=[a, b]` — is checked exactly as one passed directly. The
container annotation is carried down unchanged, since `annotation_includes`
looks inside `list[file]` for it. A dict key that is itself an expression
resolves to a different key, so its value is skipped rather than mispaired.

Warnings are deduplicated per producer/consumer/parameter, so a fan-out reports
once. Literal path arguments never trigger it — the signal is the graph edge,
not the shape of the string. Detection is inherently runtime: it needs a
resolved value, so `ginkgo doctor` does not report it.

The runtime hashes the top-level task function source and the statically
imported closure of already-loaded local Python modules during task
registration. The resulting `source_hash` is stored in both the cache key
payload and `meta.json`, so task-body and local-helper changes invalidate prior
cache entries without requiring a manual `version=` bump. This is deliberately
conservative: an edit anywhere in a reachable local module invalidates tasks
that import it. Dynamic imports and external runtime dependencies are outside
this static boundary; use `version=` to invalidate cache entries for them. If
source extraction fails for a task definition, or a module in the local import
closure cannot be read or parsed, registration fails explicitly instead of
silently weakening cache correctness.

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
