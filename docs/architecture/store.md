# Provenance Store

Ginkgo keeps one SQLite database per workspace, at `.ginkgo/ginkgo.db`
(`WorkspaceLayout.db`, relocated by `GINKGO_DB=<path>`). It holds the index:
what ran, what it consumed and produced, which cache entries and asset
versions exist. Bytes — CAS blobs, cached `output.json`, logs, lockfiles,
rendered notebooks — stay on disk.

The package is `ginkgo/store/`. `core/` must not import it; `runtime/` and
`cli/` may.

## Ledger and projections

The store has two halves.

- **The ledger.** One append-only `events` table. Each row is a runtime event
  from `runtime/events.py`: `run_id`, `ts`, `type`, `v`, the columns readers
  filter and join on (`task_id`, `attempt`, `cache_key`, `asset_key`), and the
  whole payload as JSON. `seq` is assigned by SQLite, so insertion order is the
  order things happened. Because the payload is JSON, a new event type needs no
  migration.
- **The projections.** `runs`, `tasks`, `attempts`, `task_inputs`,
  `task_outputs`, `edges`. Every one of them is derived from the ledger and
  updated in the same transaction as the event that caused it. A projection is
  a cache of the ledger, never a second source of truth: if the two disagree,
  the ledger wins.

The cache, asset and staging tables — `cache_entries`, `cache_artifacts`,
`artifacts`, `stat_index`, `materializations`, `digest_memo`,
`env_materializations`, `asset_versions`, `asset_aliases`, `staging_entries` —
are neither. They are **direct-write indexes** (`store/direct_index.py`), each
owned entirely by one class that writes it synchronously: `CacheIndex`
(`runtime/caching/index.py`) for the cache half, `AssetStore`
(`runtime/artifacts/asset_store.py`) for the catalog, and `StagingIndex`
(`remote/staging.py`) for downloaded remote inputs. That is deliberate. A cache
save must be visible to the `load` that may follow it microseconds later, which
an event queued for the writer thread cannot promise. Registering an asset
version must read the parents registered moments earlier — possibly by a
sibling task on another thread in the same run — to derive the child's
`data_version`, and the projector is a pure function over one event that
cannot query. In every case the facts held are not events that
happened to a run; they are what the cache, the catalog and the staging cache
currently hold.

Nothing else writes them, including the projector. `TaskCacheHit` updates the
task's own row and leaves `hit_count` to the index, which counts the hit as it
serves it. `AssetMaterialized` is appended to the ledger as the history of a
version coming into being — and projected nowhere, because `AssetStore` has
already written the row it would have written. `asset_versions` carries
`code_version` and `data_version`; both are recorded and neither is yet read.
See [Assets](assets.md).

There is no `asset_keys` table. An asset key is the set of versions carrying
it, so its latest version and its version count are questions about
`asset_versions` and a summary row could only disagree with them.

`env_materializations` records, per `(env_hash, host)`, the digest a *declared*
environment actually installed as, whenever `CacheStore` observes one. A cache
key names the declaration, so drift the declaration does not record — `pixi
update` re-solving a lock, a mutable image tag repointed upstream — leaves the
key unchanged; the row is how `db check` can say that one declaration
materialized two different ways across two machines. The read path that decides
whether a candidate hit is genuine is unchanged: it compares the entry's own
`env_materialized_digest` against the local environment.

`staging_entries` holds one row per staged remote URI — its digest, the
provider ETag and version id at download time, and where under
`.ginkgo/staging/` the bytes went. The bytes are content-addressed and carry no
identity of their own, so without the row a second run could not tell a stale
download from a fresh one.

Content-addressed identifiers — `cache_key`, `artifact_id`, `version_id`,
`source_hash`, `env_hash` — are the join columns throughout. Nothing is
renumbered, so a row means the same thing in every table it appears in.

`edges` is the one graph table: `(run_id, src_kind, src_id, dst_kind, dst_id,
edge)` over kinds `task | artifact | asset_version | run` and edges
`depends_on | dynamic_depends_on | produced | consumed | derived_from |
child_of`. Task dependencies, asset lineage and sub-workflow links are the same
shape, so one query walks all of them.

## Connections and pragmas

`open_store(path)` returns a `SqliteStore`. A write-mode open creates the
parent directory, applies the pragmas, and migrates. A read-only open
(`readonly=True`) goes through a `file:…?mode=ro` URI, never migrates, and
raises `SchemaVersionError` naming `ginkgo db migrate` if the schema is behind
— so a CLI read path can never upgrade a database out from under a running
workflow.

| Pragma | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | Readers do not block the writer, so `cache ls` works mid-run. Set on write opens only: it is a property of the file, not the connection. |
| `synchronous` | `NORMAL` | Durable across process crashes, which is the failure the ledger has to survive. |
| `busy_timeout` | `5000` ms | Two `ginkgo run`s in one workspace are supported; the loser of a lock race waits rather than failing. |
| `foreign_keys` | `ON` | |
| `temp_store` | `MEMORY` | Sorting and grouping stay off a possibly network-mounted disk. |
| `query_only` | `ON` (read opens) | `ginkgo query` runs user-written SQL on a read connection. `mode=ro` is a property of the URI alone; this refuses a write inside the engine as well, so neither guard is the only one. `SqliteStore.restrict_to_reads()` applies it after the fact to the in-memory ledger a reader opens for a workspace with no database, which has to be created write-mode before anything can be selected from it. |

Transaction control is explicit: the driver's implicit `BEGIN` is turned off,
and `store.transaction()` wraps a `BEGIN IMMEDIATE` … `COMMIT`, taking the
write lock up front so a competing writer is reported at the start of the
transaction rather than half-way through it. Transactions do not nest —
SQLite has no nested commit, so an inner block that "committed" could still be
rolled back by the outer one — and a failed commit rolls back rather than
leaving the connection inside a transaction it can never finish.

Network filesystems are detected at the first write-mode open of a process
(`store/fs.py`, reading `/proc/self/mounts` or `mount`). NFS, Lustre, SMB,
FUSE, 9p, AFS and GPFS mounts get one warning on stderr, naming the database
path and `GINKGO_DB`. Ginkgo never refuses to run there.

## Migrations

`schema.py` holds `MIGRATIONS: list[tuple[int, str | Callable[[Connection],
None]]]` and `migrate(conn)`, which applies the missing steps inside one
transaction and records each in `schema_version`. Version 1 creates every
table, including the ones later phases populate: one migration is easier to
reason about than eight, and an empty table costs nothing. A shipped step is
never edited — a schema change is another step. Version 2 is the first of
those: it gives `artifacts` the `digest_hex` its records carry, and removes
`cache_key_components` and the write-only columns of `digest_memo`, which held
facts their own tables already had.

Nothing in the phase that retired the file indexes needed a step: version 1
already created `staging_entries` and `env_materializations`, so filling them
was a matter of writing rows, not of changing the schema.

`tests/store/fixtures/schema_v2.txt` is a snapshot of `sqlite_master` after the
last migration. It is regenerated deliberately, as part of adding one, and a
test migrates a version 1 database forward so the steps are exercised on a
database that already exists rather than only on a fresh one.

## Errors

`StoreError`, `SchemaVersionError` and `StoreLockedError` are `GinkgoError`
subclasses, so the CLI prints the message and nothing else; each message names
the database and the way out. A run whose ledger cannot be written fails —
provenance never degrades silently.

## `ginkgo db`

- `ginkgo db migrate` — create or upgrade the database.
- `ginkgo db check` — schema version, `PRAGMA integrity_check`, and every way
  an index and the bytes it names can disagree. A read path: it opens the
  database read-only and never creates one, so a workspace nobody has run
  anything in reports that and succeeds. Each owner answers for its own half,
  in both directions:
  - the cache — an entry row whose `output.json` is gone, an entry directory
    with no row, a `cache_artifacts` row whose blob the artifact store lost;
  - the artifact store — a row whose blob or tree manifest is missing, and a
    file in `blobs/` or `trees/` that no row (and no tree manifest) names;
  - the cache and the asset catalog together — a cache entry whose stored
    output would replay an `AssetRef` naming a version `asset_versions` has no
    row for, and an entry whose `output.json` cannot be read at all
    (`CacheStore.asset_reference_problems`). It walks the encoded form rather
    than decoding it, so a ref inside a pickled `asset_result` payload is
    invisible to it — a read-only check does not unpickle what it is
    inspecting, and under-reports instead;
  - runs — a `runs` row with no run directory, and a run directory with no row;
  - the staging cache — a staged URI whose bytes are gone;
  - environments — a declared environment recorded as materializing two
    different ways across hosts (`CacheIndex.env_drift_problems`).

  The run check is `rundir.run_directory_problems`, beside everything else
  about a run directory; the rest are `integrity_problems()` on the class that
  owns the bytes.

  It reports; it never repairs. Exit status is 1 if anything was reported.
- `ginkgo db prune --events-older-than <30d|12h|45m> [--dry-run]` — delete the
  raw `events` of runs that *finished* before the cutoff. Projections are never
  touched, so `runs show` and the report are unchanged; what is lost is the
  per-event detail `ginkgo export events` reads. A run still in flight keeps its
  events whatever its start time. `--digest-memo-older-than` prunes
  `digest_memo` on `last_seen`, where losing a row costs one re-hash.
  `--staging-older-than` prunes `staging_entries` on `last_used_at` **and the
  bytes beside them** — staged downloads are the largest thing under
  `.ginkgo/` and this is their only eviction; a blob two URIs share is kept
  until the last row naming it goes. At least one cutoff is required.
- `ginkgo db vacuum` — rebuild the file, returning the pages a prune freed to
  the filesystem, and report the size either side. SQLite cannot rebuild while
  another connection holds the database and says nothing when it declines, so
  an unchanged size is reported as "no space reclaimed" rather than as a win.
- `ginkgo db path` — where the database is, after `GINKGO_DB`.

There is no `db rebuild`. The database is the record, not a cache of the run
directories: back it up as you would `.git`. Losing it loses the run history —
the `events` ledger has no on-disk counterpart at all — and colds the cache,
because since the cache index moved into the database the key that finds an
entry exists nowhere else. The bytes stay behind as orphans; `db check` lists
them and `ginkgo cache clear --orphans` removes them. Each run directory keeps a
`manifest.yaml` of what that run did, to be read rather than re-imported.

## Layering

`core/` must not import `store/`. `runtime/` and `cli/` may — which means
`store/` must not import either of them, and it does not:
`grep -rn "from ginkgo.runtime" src/ginkgo/store` is empty, and a test would be
the better guard if this ever drifts.

The consequence worth knowing is that `store/` deals in rows, not in events. A
`GinkgoEvent` becomes a `StoredEvent` in `runtime/store_recorder.py`, and the
projector is handed the row. Anything that has to know the shape of an event —
the translation, the rendering of user values — lives in `runtime/`.

## Writing and reading

The ledger is written through `store/writer.py`'s `StoreWriter`: a queue and one
background thread owning its write-mode connection, batching rows into
transactions. `runtime/store_recorder.py` subscribes it to the event bus and
writes the run's manifest when it completes. `store/projector.py` is the whole
of the event-to-rows mapping, one pure function per event type. See
[Provenance and Run State](provenance.md) for the shape of that path.

The direct indexes are the other writer, and they hold their own connection. A
cache save is synchronous — the `load` that follows it must see the row — while
the writer's queue is asynchronous and its connection belongs to its thread;
routing cache rows through it would mean either blocking on the queue or
inventing a second protocol for it to carry. Two connections over one WAL
database is what WAL is for, and every direct write is an `INSERT OR IGNORE` on
a content-addressed key or an idempotent upsert, so the two writers cannot
produce a row that disagrees with itself.

The cache index and the asset catalog are two sets of tables, not two
databases: inside a run the catalog is `AssetStore.attached_to(index)`, sharing
the cache index's connection and its lock, so a save in one never waits on
SQLite's write lock for a save in the other. Read paths do not construct either
one: they open a `Query` and take `reader.catalog`, which is one catalog over
the reader's own connection. A workspace with no database is opened with
`query.open(missing_ok=True)` and reads an empty in-memory ledger rather than
creating a file, so `asset ls`, `lineage`, `notebooks` and a report all answer
an empty workspace with their own empty result instead of a missing-file
error.

Readers go through `ginkgo.query`, which opens read-only — including the cache
readers `cache ls`, `cache explain` and `cache stats`, the asset readers
`asset ls/versions/show/inspect`, `models`, the report, and `lineage`, and the
run readers `runs ls/show`, `history`, `query` and `export`. No read path ever
opens a write connection, so listings work while a run is writing and can never
migrate a database out from under one.

`SqliteStore.query` serves ginkgo's own SQL, which knows its columns and its
row count. `SqliteStore.select_with_columns` serves SQL ginkgo did not write — `Query.sql`,
behind `ginkgo query` — and answers the two further questions that raises: an
empty result still has to name its columns, so a CSV export has a header, and a
statement nobody reviewed has to be stopped at a row limit. The limit is applied
while fetching from the cursor rather than by wrapping the statement, so the SQL
that runs is the SQL the user wrote.

The tables here are versioned but not stable. `Query.sql` hands them out
directly, and they change between releases without a deprecation period; the
methods on `Query` are the surface that is kept working. This is stated in the
`ginkgo.query` module docstring and on the Querying Provenance guide page, so a
user meets it before writing SQL rather than after an upgrade.

Two `ginkgo run` processes in one workspace are supported and tested
(`tests/store/test_concurrent_runs.py`). WAL keeps readers off the writer's
back; `busy_timeout` covers lock contention; and the very first open of an empty
workspace retries the switch into WAL, which is the one lock SQLite's own busy
handler does not cover.

## What is still a file, and why

Everything ginkgo *knows* is a row. What is left on disk is bytes, a log, or
something a third-party tool opens for itself:

| File | Why it is not a table |
|---|---|
| `cache/<key>/output.json` | The cached value itself. Bytes. |
| `artifacts/blobs/*`, `artifacts/trees/<digest>.json` | The content-addressed store and its tree manifests. Bytes, named by their own digest — a recorded blob keeps its file extension after the digest. |
| `staging/blobs/*`, `staging/folders/*` | Downloaded remote inputs. Bytes; `staging_entries` is their index. |
| `runs/<id>/logs/*` | Task stdout and stderr, appended while a task runs and read as text. A log. |
| `runs/<id>/notebooks/*.ipynb`, `*.html` | The executed notebook and its rendered page — the task's output, opened by a browser or Jupyter. Bytes. |
| `runs/<id>/envs/*.lock` | A copy of the lockfile a run resolved, kept so the environment can be rebuilt from the run directory alone. Bytes. |
| `runs/<id>/manifest.yaml` | The `RunSummary` the ledger already holds, exported once at finalize for a person to read. Derived, and stated as such. |
| `<task>.params.yaml` beside an executed notebook | Papermill's parameter file. Written for a third-party tool to read. |
| `jupyter/` under the notebook runtime root | A Jupyter data directory: `share/jupyter/kernels/ginkgo-<digest>/kernel.json` and what belongs beside it. Every Jupyter subprocess discovers kernels by walking `jupyter_path()` on disk, so the directory layout *is* the interface; there is no way to hand a kernelspec over in memory. |
| `refs/<artifact_id>.json` in a remote object store | The cross-machine wire format. A worker with no access to this workspace's database learns an artifact's shape from it. |
| `.ginkgo-report.json` at the root of an exported report | The marker that says a directory is a ginkgo report, and so may have its contents replaced. It guards *deleting the user's files*, and an exported bundle is portable — copied to a webserver, moved, shared — so the evidence has to travel with the bytes it guards rather than living in a workspace database the bundle may no longer be anywhere near. A row keyed by path would go stale in the dangerous direction. |
