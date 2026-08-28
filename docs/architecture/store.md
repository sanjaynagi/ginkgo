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
  `task_outputs`, `edges`, the cache index, the artifact and asset tables.
  Every one of them is derived from the ledger and updated in the same
  transaction as the event that caused it. A projection is a cache of the
  ledger, never a second source of truth: if the two disagree, the ledger wins.

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
never edited — a schema change is another step.

`tests/store/fixtures/schema_v1.txt` is a snapshot of `sqlite_master` after
version 1. It is regenerated deliberately, as part of adding a migration.

## Errors

`StoreError`, `SchemaVersionError` and `StoreLockedError` are `GinkgoError`
subclasses, so the CLI prints the message and nothing else; each message names
the database and the way out. A run whose ledger cannot be written fails —
provenance never degrades silently.

## `ginkgo db`

- `ginkgo db migrate` — create or upgrade the database.
- `ginkgo db check` — schema version and `PRAGMA integrity_check`.
- `ginkgo db path` — where the database is, after `GINKGO_DB`.
There is no `db rebuild`. The database is the record, not a cache of the run
directories: back it up as you would `.git`. Losing it loses the run history —
the `events` ledger has no on-disk counterpart at all — and does not touch the
cache, whose entries are found by key on disk. Each run directory keeps a
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

One process writes, through `store/writer.py`'s `StoreWriter`: a queue and one
background thread owning the only write-mode connection, batching rows into
transactions. `runtime/store_recorder.py` subscribes it to the event bus and
writes the run's manifest when it completes. `store/projector.py` is the whole
of the event-to-rows mapping, one pure function per event type. See
[Provenance and Run State](provenance.md) for the shape of that path.

Readers go through `ginkgo.query`, which opens read-only. No read path ever
opens a write connection, so listings work while a run is writing and can never
migrate a database out from under one.

Two `ginkgo run` processes in one workspace are supported and tested
(`tests/store/test_concurrent_runs.py`). WAL keeps readers off the writer's
back; `busy_timeout` covers lock contention; and the very first open of an empty
workspace retries the switch into WAL, which is the one lock SQLite's own busy
handler does not cover.
