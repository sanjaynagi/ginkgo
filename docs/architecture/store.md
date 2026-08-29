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

The cache and asset tables — `cache_entries`, `cache_artifacts`, `artifacts`,
`stat_index`, `materializations`, `digest_memo`, `asset_versions`,
`asset_aliases` — are neither. They are **direct-write indexes**
(`store/direct_index.py`), each owned entirely by one class that writes it
synchronously: `CacheIndex` (`runtime/caching/index.py`) for the cache half and
`AssetStore` (`runtime/artifacts/asset_store.py`) for the catalog. That is
deliberate on both sides. A cache save must be visible to the `load` that may
follow it microseconds later, which an event queued for the writer thread
cannot promise. Registering an asset version must read the parents registered
moments earlier — possibly by a sibling task on another thread in the same run —
to derive the child's `data_version`, and the projector is a pure function over
one event that cannot query. In both cases the facts held are not events that
happened to a run; they are what the cache and the catalog currently hold.

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
- `ginkgo db check` — schema version, `PRAGMA integrity_check`, and the ways
  the cache index and the bytes on disk can disagree: an entry row whose
  `output.json` is gone, an entry directory with no row, and a
  `cache_artifacts` row whose blob the artifact store has lost.
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
row count. `SqliteStore.select` serves SQL ginkgo did not write — `Query.sql`,
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
