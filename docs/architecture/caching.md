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

## The cache index

The database at `.ginkgo/ginkgo.db` is the only cache index. An entry is a
`cache_entries` row plus the bytes at `cache/<key>/output.json`; its key
components are `cache_key_components` rows, its outputs are `cache_artifacts`
rows, and the artifact store's own contents are `artifacts` rows. There is no
`meta.json`, no `refs/<id>.json`, no `stat_index.json` and no
`materializations.json` — each was a file index of the same facts, and each is
gone.

`CacheIndex` (`runtime/caching/index.py`) is the only reader and writer of those
rows. It holds its own write connection rather than sharing the recorder's: the
recorder's belongs to its writer thread and carries events, while a cache save
has to be visible to the `load` that may follow it immediately. Its own lock
serialises the evaluator's threads over that one connection. Every write is an
`INSERT OR IGNORE` on a content-addressed key or an idempotent upsert, which is
what makes two runs saving the same entry at once agree rather than conflict.

Losing the database means a cold cache. The bytes survive, but nothing can find
them: the key that would is only in the row that is gone. `ginkgo db check`
reports those directories and `ginkgo cache clear --orphans` removes them; prune
never does so implicitly. Back the database up as you would `.git`.

`ginkgo cache stats` reports what the index holds: entry count, total bytes, the
hit-count histogram, how much is taken by entries nobody has ever used, and the
ten functions holding the most bytes. Hit counting happens in the projector, on
`TaskCacheHit`, because the cache itself never learns that a value it handed out
was used.

## Remembering digests between runs

`HashMemo` caches content digests by stat fingerprint — device, inode, size,
mtime for a file; the same over the children for a directory. Since #245 the
memo is two tiers: a dict for this process, and the `digest_memo` table behind
it. A second run over the same inputs therefore reads no bytes at all, which is
the difference between a warm run that stats a folder and one that re-hashes
it. A file whose size or mtime moved misses in both tiers and is re-read.

`DigestRegistry` stays in memory: it maps this run's output paths to the digests
this run computed, so a downstream task skips re-hashing what an upstream task
just produced. Those paths are being written as the run goes, so nothing about
them is true past the end of it.

## Task source hash

The task source hash covers the task body and the local helper modules it
statically imports, so editing a helper invalidates the tasks that use it. The
closure stops at the project's own source: modules under the interpreter's
prefix or its installed-package directories are excluded, because dependencies
are pinned by environment identity instead. Without that boundary a project
that keeps its environment in its own tree (`.pixi/envs/`, `.venv/`) would walk
the whole interpreter for every task.

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

Hashing the declaration also makes keys portable, which lock-derived keys never
were: a lock is solved per machine and per moment.

### Drift is caught on the entry, not in the key

Keying on the declaration leaves drift the declaration does not record —
`pixi update` re-solving a lock while `pixi.toml` still says `numpy = ">=2.0"`,
or a mutable tag repointed upstream. Those changes must not serve stale results,
so they are caught where the evidence exists rather than in the key.

`ExecutionEnvironment.materialized_digest` reports the environment as
materialised on *this* machine — the lock file's digest, the pulled image's ID.
`CacheStore.save` records it on the entry's row as
`env_materialized_digest`, and `CacheStore._env_materialization_matches` checks a
candidate hit against it inside `load` and `has_entry`, so every lookup path —
content-addressed, the `--trust-mtimes` stat index, and the `--dry-run` preview —
gets the same answer:

- materialised here and different: the entry was produced against other
  dependencies, so it is a miss and the task re-runs;
- materialised here and the same: a genuine hit;
- not materialised here: no local evidence either way, and the entry stands.
  Establishing evidence would mean installing or pulling an environment to serve
  a cache hit, which is the cost the deferred materialisation exists to avoid.
  This is the case a shared cache lands in on a machine that has never built the
  environment.

Neither read costs anything next to executing the task, both are memoised per
environment per run, and neither happens on a miss. Because a lookup can ask for
the digest before `prepare` has run, neither backend memoises a *negative*
answer: an absent lock file or an unpulled image has to read as materialised once
it is there.

An entry that recorded no digest has nothing to compare and stands.

The digest is also recorded for provenance, where it always has been: the lock
file is copied into the run directory, and a container task's manifest entry
carries `container_image_digest`.

## Explaining a re-run

`ginkgo cache explain <run_id>` answers "why did this task run again?" by
naming the component of the key that moved, not the fact that the key did
(issue #223). `explain_run_cache` (`cli/commands/cache.py`) diffs two sets of
`cache_key_components` rows: the entry the run wrote, and the entry this node
used before. `key_components` (`runtime/caching/cache.py`, next to the
`build_cache_key` payload it mirrors, so the two cannot drift) is what wrote
both sets, so the labels are the same on either side by construction:

- `task`, `version`, `source_hash`, `extra_source_hash` (the notebook or script
  hash folded in for driver tasks), `env`, `env_hash.pixi_lock`;
- one component per input parameter, `inputs.<parameter>`, so a moved input is
  named rather than lumped into "the inputs changed".

Each reported component carries a `status` — `changed` with both values, or
`added` / `removed` for a parameter that appeared or went away. The coarse codes
(`source_hash_changed`,
`version_bump`, `env_changed`, `input_changed`, or `cache_key_changed` when no
component is conclusive or the one that moved has no code of its own) stay as
the summary `reason` / `details`, with the components listed beneath them. A
task whose key has no entry in the cache at all — a task that failed, or whose
entry has been pruned — reports `no_entry_for_key` and no components, and one
with no sibling to compare against reports `no_prior_entry`.

What to compare against is the whole of the question. `Query.previous_cache_key`
looks for the *same node* — the same `display_label`, or the task name where a
node has none — in the most recent earlier run of the same workflow, and that
entry is what this run superseded. Only when no such node exists (the node is
new, or its label changed) does it fall back to the newest earlier entry for the
same function. `compared_with` names both the key and which of the two
strategies found it, so a `strategy` of `newest_by_function` is the signal to
read the components sceptically.

That join is what fixed the fan-out mis-pairing: matching on function alone,
the nearest earlier entry for a task fanned out over many inputs was usually a
different element of the fan-out rather than the same element from the previous
run, and the components it named were real differences between two real entries
that had nothing to do with why this element re-ran.

The re-run reason is deliberately not carried on the run report's task rows.
It is computable only from the cache index, which the report is meant to
outlive — a report exported after a `cache prune` would show reasons for some
rows and nothing for others. `cache explain` asks the cache at the moment the
question is asked, which is when the answer is trustworthy.

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
payload and on the entry's row, so task-body and local-helper changes invalidate prior
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
files use the blob digest and directories use a manifest digest. The store keeps
bytes — `blobs/`, `trees/` — and records what it holds in the `artifacts` table;
which entry owns which output is a `cache_artifacts` row. A remote artifact
still carries a JSON record beside its bytes in the object store, because a
machine that downloads it cannot read this workspace's database.

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
`--max-entries` then pick additional entries until their budgets are met.
`--least-recently-hit` changes which entries those are: least recently used
first rather than oldest first, with a never-hit entry counting as never used
and age breaking ties. Age is not use — an old entry that hits on every run is
worth more than a young one nothing has touched. Orphan artifacts are
garbage-collected once at the end, against the union of what the cache and the
asset catalog still reference.
