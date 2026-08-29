# Caching And Provenance

Ginkgo caches task results so repeated runs reuse prior work, and records
provenance so you can inspect what happened in any run.

## Cache Identity

The cache lives under `.ginkgo/cache/` and is content-addressed. At a high
level, Ginkgo hashes:

- task identity
- task version
- task source and its statically imported local Python modules
- notebook source for notebook tasks
- resolved input values
- environment identity for foreign execution

Inputs annotated `file` or `folder` are hashed by content; everything else is
hashed from its `repr`. See [Cache Correctness](#cache-correctness) for why that
distinction decides whether the cache stays correct.

Local-import tracking is conservative: changing a reachable helper module
invalidates tasks that import it, even when the changed symbol is not called.
Dynamic imports and other runtime dependencies cannot be tracked this way; set
or increment `version=` on the task when those dependencies change.

The conservative closure has a practical consequence for fan-out. If tasks read
their parameters from a shared module-level structure, every consumer's cache
identity is coupled to every other consumer's parameters — editing one model's
entry in a shared `MODEL_HYPERPARAMS` dict invalidates every task that imports
the module, so the whole fan-out re-runs rather than the affected branch. Pass
such parameters as task arguments instead; arguments are hashed per call, so
only the branches whose values changed are invalidated.

(cache-correctness)=
## Cache Correctness

### Annotate Path Boundaries `file` Or `folder`, Not `str`

A path that flows between tasks must be annotated `file` (or `folder`) at both
ends — the producer's return and the consumer's parameter. Content hashing is
dispatched on that annotation.

**A `str`-annotated path boundary makes the cache key path-identity only.** The
key incorporates the path string, not the file's contents, so if an upstream
task rewrites the file at the same path the downstream task still matches its
old key: it reports `↺ cached` and serves a stale result as current. Nothing
warns, because from the cache's point of view nothing changed.

```python
from ginkgo import file, task

# WRONG — coords is keyed on the path string, so a rewritten file still hits
@task()
def analyze(coords: str, output_path: str) -> str: ...

# CORRECT — coords is keyed on the file's contents
@task()
def analyze(coords: file, output_path: str) -> file: ...
```

The producer's annotation matters as much as the consumer's: a task declared
`-> str` returns a plain `str` at runtime, which is hashed by `repr` even if the
consumer asks for `file`. Note that `file` and `folder` are `str` subclasses, so
a `str` annotation is indistinguishable from the correct one at the type level
while behaving oppositely at the cache level — no type checker will catch this.

Output paths stay `str`. The file does not exist when the key is computed, so
there is nothing to hash; annotate the return `file` when the produced path
should be content-tracked and stored as an artifact.

`Path` and `pathlib.Path` annotations are **not** content-hashed either. Use
`file` and `folder`.

## Artifact Storage

For file and folder outputs, Ginkgo stores content-addressed artifacts under
`.ginkgo/artifacts/` and uses those as the durable backing store for cached path
outputs.

A task's declared output path is not the source of truth — the artifact store
is.

## Where Provenance Lives

What happened goes into one SQLite database per workspace, at
`.ginkgo/ginkgo.db`: an append-only event log, plus the tables `ginkgo inspect
run`, `ginkgo debug` and `ginkgo report` read. All three work on a run that is
still going.

Each run also gets a directory under `.ginkgo/runs/<run_id>/` holding the bytes
— per-task logs, notebook artifacts, copies of the environment lock files, and
a `manifest.yaml` snapshot of everything the database recorded for the run.

Together, the cache and the ledger answer different questions:

- cache: can this work be reused safely?
- provenance: what happened in this specific run?

## Maintaining The Provenance Database

```bash
ginkgo db path                # where the database is
ginkgo db check               # schema version, integrity, rows against bytes
ginkgo db migrate             # create or upgrade it
ginkgo db prune --events-older-than 90d --dry-run
ginkgo db prune --staging-older-than 30d    # staged remote inputs, and their bytes
ginkgo db vacuum              # give the freed space back
```

`ginkgo db check` asks every index whether its rows and the files they name
still agree — the cache, the artifact store, the run directories, the staged
remote inputs — and reports both directions: a row whose bytes are gone, and
bytes no row can find. It never repairs anything.

`ginkgo db prune --events-older-than 90d` deletes the raw event stream of runs
that finished more than 90 days ago. Everything `ginkgo runs show`, the report
and `ginkgo history` read is left alone; what goes is the per-event detail
`ginkgo export events` prints. Add `--digest-memo-older-than` to drop memoised
file digests, which cost only a re-hash to lose, `--staging-older-than` to
evict downloaded remote inputs that nothing has read for a while — bytes and
row together, and the only eviction the staging cache has — and `--dry-run` to
see the counts first. Deleting rows does not shrink the database file; `ginkgo
db vacuum` does.

`ginkgo db check` reads; it never creates. In a directory nobody has run a
workflow in it says so and succeeds.

### Upgrading from a pre-ledger workspace

Workspaces recorded before `.ginkgo/ginkgo.db` existed are **not** migrated.
Their runs, cache entries and asset catalog lived in files ginkgo no longer
reads, so they are invisible to every command. If you have one, delete
`.ginkgo/` and run the workflow again; there is no import path, and nothing in
the old layout is read by mistake.

`ginkgo.db` is the record of your runs and of your cache; back it up as you
would `.git`. If it is lost, the run history goes with it and the cache goes
cold: the cached bytes are still under `.ginkgo/cache/`, but the keys that find
them were rows in the database. `ginkgo db check` lists those stranded
directories and `ginkgo cache clear --orphans` removes them.

Each run directory still holds a `manifest.yaml` of what that run did, which is
there to be read rather than re-imported: ginkgo does not load it back.

`GINKGO_DB=<path>` relocates the database. Do that if `.ginkgo` is on a network
filesystem: SQLite locking is unreliable over NFS, Lustre, SMB and FUSE, and
ginkgo prints one warning when it notices.

Two `ginkgo run` processes can share a workspace; the ledger is built for it.

## Inspecting Cache State

Use the cache subcommands to inspect or clean cache state:

```bash
ginkgo cache ls
ginkgo cache stats
ginkgo cache clear <cache-key>
ginkgo cache prune --older-than 30d --dry-run
```

These commands report reuse behavior without navigating the hidden cache
directory by hand. `ginkgo cache stats` adds the aggregate picture: how many
entries there are, how much they take, how often they are hit, and how much is
held by entries nothing has ever reused. They read the database read-only, so
they answer while a run is in progress.

## Bounding Cache Size

`ginkgo cache prune` supports three eviction policies, which can be combined
in one invocation:

```bash
# Time-based: remove anything older than 30 days
ginkgo cache prune --older-than 30d

# Size-based: bring total cache size down to 5 GB
ginkgo cache prune --max-size 5GB

# Count-based: keep only the newest 500 entries
ginkgo cache prune --max-entries 500

# Combined: also remove anything older than 90 days
ginkgo cache prune --older-than 90d --max-size 5GB

# Give up what nobody has used lately, rather than what is oldest
ginkgo cache prune --max-size 5GB --least-recently-hit
```

Eviction is oldest-first unless you pass `--least-recently-hit`, which gives up
the entries with the oldest last hit first — an old entry that hits on every run
is worth more than a young one nothing has touched. Orphaned artifacts are
garbage-collected at the end of the operation. Use `--dry-run` to preview what
would be removed.

## Partial Resume

When a run fails partway through, Ginkgo preserves every successfully cached
task. Rerunning the same workflow picks up where the previous run left off:
tasks whose inputs are unchanged serve from cache, and only the tasks that
failed or were never reached are re-executed. The `cache_key` column in
`ginkgo cache ls` and the cache-hit markers in `ginkgo run` output make this
reuse visible. There is no separate resume command — the cache itself is the
resume mechanism.

## Dry-Run Mode

`ginkgo run flow.py --dry-run` validates the workflow without executing
any task body. Ginkgo resolves the expression tree, checks environments and
secrets, computes cache keys for every task, and reports which tasks would
run, which would serve from cache, and which resources they declare. Dry-run
is the fastest way to confirm that a workflow is correctly wired, that every
declared environment exists, and that planned caching aligns with intent
before committing to a real run.
