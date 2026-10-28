---
orphan: true
---

# Maintaining The Database And Cache

Ginkgo keeps what it records under `.ginkgo/` in the project root:

- `ginkgo.db` is the provenance database, Ginkgo's record of every run and
  every cache entry. It is one SQLite file.
- `cache/` holds the stored results of cached tasks.
- `artifacts/` holds stored copies of the files and folders that tasks wrote.
- `runs/<run_id>/` holds each run's logs, rendered pages and manifest.

Ginkgo deletes nothing from these unless you ask. This page covers the
commands that inspect, check and shrink them.

## Inspecting The Cache

```bash
ginkgo cache ls                       # one line per entry, with its full cache key
ginkgo cache ls --task align --json   # one task's entries, as JSON
ginkgo cache stats                    # totals: entries, size, hits, entries never reused
```

`cache ls` prints each full cache key on one line, so you can copy it into
`cache clear`. `--task` takes a task name or a branch label, as described in
[Task Names](cli.md#task-names).

`cache stats` shows how many entries there are, how much space they take and
how often they are hit. It also shows how much space is held by entries that
were never reused. Both commands only read the database, so they work while a
run is in progress.

## Removing Cache Entries

```bash
ginkgo cache clear <cache_key>     # one entry
ginkgo cache clear --task align    # every entry one task wrote
ginkgo cache clear --all           # every entry
ginkgo cache clear --orphans       # cache directories the database has no row for
```

`cache clear` takes exactly one of a key, `--task`, `--all` or `--orphans`.
It removes what it names at once, with no prompt. Run `cache ls --task` first
to see what `--task` would remove. `--all` also removes what `--orphans` would.

## Bounding Cache Size

`ginkgo cache prune` removes entries by age, total size or count. Pass one or
more of these limits:

```bash
# Remove entries older than 30 days
ginkgo cache prune --older-than 30d

# Bring the cache down to 5 GB (5GB is 5,000,000,000 bytes, 5GiB is 5 × 1024³)
ginkgo cache prune --max-size 5GB

# Keep only the newest 500 entries
ginkgo cache prune --max-entries 500

# Combine limits
ginkgo cache prune --older-than 90d --max-size 5GB

# Preview what would be removed
ginkgo cache prune --max-size 5GB --dry-run
```

A duration is a positive whole number of minutes, hours or days, such as
`45m`, `12h` or `30d`.

Prune removes the oldest entries first. Add `--least-recently-hit` to remove
first the entries whose last hit is oldest. An old entry that is hit on every
run is worth more than a new one that is never used.

`--max-size` counts each entry's stored return value and the files and folders
it restores. After removing entries, `clear` and `prune` delete any artifact
that no remaining entry uses. A prune also removes cache directories the
database has no row for.

## How Sizes Are Counted

The cache commands report three sizes:

- **Entry size** (`cache ls`, and the total in `cache stats`) is the entry's
  stored return value plus the files and folders it restores. Two entries can
  share an artifact, which then counts toward both. So the entry sizes can add
  up to more than the artifact store holds.
- **Artifact store** (`cache stats`, or `artifact_bytes` in JSON) is the size
  of `.ginkgo/artifacts/`, with each byte counted once. The store can hold
  bytes that no entry uses, so pruning entries does not bound it.
- **On disk** (`cache stats`, or `disk_bytes` in JSON) is the space the cache
  and the artifact store take on disk, as `du` counts it. For many small
  entries, this is far larger than the other two.

The first two count the bytes the files hold. The space that `clear` and
`prune` report as freed is measured on disk, like the third. Sizes are printed
in binary units (KiB, MiB, GiB).

## Checking The Database

```bash
ginkgo db path               # where the database is
ginkgo db check              # schema version, integrity, rows against files
ginkgo db check --verify     # also re-hash every stored artifact
ginkgo db check --fix        # first remove what nothing can use
```

`ginkgo db check` compares the database with the files it points to: the
cache, the artifact store, the run directories and the staged remote inputs.
It reports a row whose files are gone, and files that no row points to. On its
own, it changes nothing.

`--fix` first removes what nothing can use, then reports what is left. It
removes:

- run directories and cache directories that have no row in the database
- cache entries whose files are missing or unreadable, since each would only
  miss
- artifacts that no cache entry uses
- files SQLite left behind for a database that was moved or deleted

`--fix` keeps a run's row when its directory is gone, because the row is the
only record of that run.

`db check` compares rows with file names, so it cannot see a stored file whose
bytes changed. `--verify` re-hashes every stored file against its digest. It
reports each mismatch and exits non-zero. It reads the whole store, so it takes
longer the larger the store is.

A cache hit also checks the bytes it restores as it copies them. If a copy does
not match its digest, Ginkgo warns and deletes the copy and the stored bytes.
The task then runs again.

`ginkgo doctor` reports the same problems. A database that SQLite cannot read
is an error. Cache files that don't match the database are a warning that
points at `db check --fix`.

`db check` never creates a database. In a project with no runs, it says so and
exits 0. If the database is missing but `.ginkgo/runs/` holds run
directories, it warns and exits 0, and so do `ginkgo runs ls` and
`ginkgo doctor`. Restore the database to read those runs again. If you don't
need them, `ginkgo db check --fix` removes the run directories.

## Keeping The Database Small

Every run adds to the database, a fully cached run too. Expect about 4 KiB per
task, and more for a task with many inputs. `ginkgo doctor` warns once the
database passes 1 GiB.

```bash
ginkgo db prune --events-older-than 90d --dry-run
ginkgo db prune --events-older-than 90d
ginkgo db vacuum
```

`ginkgo db prune --events-older-than 90d` deletes the events of runs that
finished more than 90 days ago. These are what `ginkgo export events` prints.
`ginkgo runs show`, `ginkgo history` and the report don't read them, so those
still work for the older runs. Add `--dry-run` to see the counts first.

Two more options free other rows:

- `--digest-memo-older-than` drops saved file hashes. Losing one costs only a
  re-hash of that file.
- `--staging-older-than` removes downloaded remote inputs that nothing has read
  for that long, with their files. It is the only way to clear the staging
  cache.

Deleting rows does not shrink the database file. Run `ginkgo db vacuum` to give
the space back to the disk. `cache clear` and `cache prune` also free rows
without shrinking the file.

To prune events as each run finishes, set this in `ginkgo.toml`:

```toml
[ledger]
keep_events_for = "90d"   # each run drops the events of runs finished over 90 days ago
```

Each run then does what `ginkgo db prune --events-older-than 90d` does. The
runs themselves stay listed. Run `ginkgo db vacuum` to give the space back.

## Backing Up The Database

Back up `.ginkgo/ginkgo.db` as you would `.git`. It holds the record of every
run and the keys of every cache entry.

If you lose it, the run history goes with it, and the cache goes cold. The
stored results are still under `.ginkgo/cache/`, but no key leads to them.
`ginkgo db check` lists those stranded cache and run directories.
`ginkgo db check --fix` removes both, and `ginkgo cache clear --orphans`
removes the cache ones.

Each run directory also holds a `manifest.yaml` of what that run did. It is
there for you to read. Ginkgo does not load it back.

## Upgrading Ginkgo

Before version 1.0, Ginkgo does not migrate the database between schema
versions. If a new release uses a different schema, it refuses the old
database with an error that names both versions. Delete `.ginkgo/` and run the
workflow again. This loses the run history and the cache.

## Network Filesystems

Several `ginkgo run` processes can share one project and its database.

SQLite locking is unreliable on a network filesystem such as NFS, Lustre or
SMB, and on FUSE. If `.ginkgo/` is on one of these, set `GINKGO_DB=<path>` to
put the database on a local disk. Ginkgo prints a warning when it notices the
problem.
