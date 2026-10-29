# Maintaining The Database And Cache

Ginkgo keeps what it records under `.ginkgo/` in the project root:

- `ginkgo.db` is the [provenance](concepts.md#glossary) database, Ginkgo's record of every run and
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
first the entries whose last hit is oldest.

After removing entries, `clear` and `prune` delete any artifact that no
remaining entry uses. A prune also removes cache directories the database has
no row for.

## How Sizes Are Counted

The cache commands report three sizes:

- **Entry size** (`cache ls`, and the total in `cache stats`) is the entry's
  stored return value plus the files and folders it restores. `--max-size`
  counts this. Entries can share an artifact, so the sizes can add up to more
  than the artifact store holds.
- **Artifact store** (`cache stats`) is the size of `.ginkgo/artifacts/`, with
  each byte counted once.
- **On disk** (`cache stats`) is the space the cache and the artifact store
  take on disk, as `du` counts it. For many small entries, this is far larger
  than the other two. The space `clear` and `prune` report as freed is
  measured this way.

## Checking The Database

```bash
ginkgo db path               # where the database is
ginkgo db check              # schema version, integrity, rows against files
ginkgo db check --verify     # also re-hash every stored artifact
ginkgo db check --fix        # first remove what nothing can use
```

`ginkgo db check` compares the database with the files it points to: the
cache, the artifact store, the run directories and the staged remote inputs.
It reports rows whose files are gone, and files that no row points to. On its
own, it changes nothing. `ginkgo doctor` reports the same problems.

`--fix` removes what nothing can use, then reports what is left:

- run and cache directories that have no row in the database
- cache entries whose files are missing or unreadable
- artifacts that no cache entry uses
- files SQLite left behind for a database that was moved or deleted

It keeps a run's row when its directory is gone, because the row is the only
record of that run.

`--verify` also re-hashes every stored file, to catch bytes that changed. It
reports each mismatch and exits non-zero. It reads the whole store, so it is
slow on a large one.

If the database is missing but `.ginkgo/runs/` holds run directories,
`db check` warns. Restore the database to read those runs again, or run
`ginkgo db check --fix` to remove them.

## Keeping The Database Small

Every run adds to the database, a fully cached run too: about 4 KiB per task,
and more for a task with many inputs. `ginkgo doctor` warns once the database
passes 1 GiB.

```bash
ginkgo db prune --events-older-than 90d --dry-run   # see the counts first
ginkgo db prune --events-older-than 90d
ginkgo db vacuum
```

`db prune --events-older-than` deletes the events of runs that finished
longer ago. These are what `ginkgo export events` prints. `ginkgo runs show`,
`ginkgo history` and the report still work for those runs. Two more options
free other rows:

- `--digest-memo-older-than` drops saved file hashes, which costs only a
  re-hash.
- `--staging-older-than` removes downloaded remote inputs that nothing has read
  for that long. It is the only way to clear the staging cache.

Deleting rows, with `db prune`, `cache clear` or `cache prune`, does not
shrink the database file. Run `ginkgo db vacuum` to give the space back.

To prune events as each run finishes, set this in `ginkgo.toml`:

```toml
[ledger]
keep_events_for = "90d"   # each run drops the events of runs finished over 90 days ago
```

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
