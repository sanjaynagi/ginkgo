# Querying Provenance

Every run ginkgo executes is recorded in one SQLite database, `.ginkgo/ginkgo.db`.
This page is about reading it: from the command line, from Python, and — when
nothing else fits — with SQL.

Every read here opens the database read-only. You can query a workspace while a
run is writing to it, and querying a workspace nobody has run anything in yet is
an empty answer rather than an error.

## From the command line

### What has been run

```bash
ginkgo runs ls                          # the twenty most recent runs
ginkgo runs ls --workflow flow.py       # only runs of one workflow
ginkgo runs ls --status failed --limit 5
ginkgo runs ls --since 2026-08-01 --json
```

`ginkgo runs show <run_id>` prints one run: its workflow, status, duration, and
a table of its tasks. `--json` prints the full run manifest — the same document
`.ginkgo/runs/<run_id>/manifest.yaml` holds. The run id is optional; without one
you get the most recent run.

### How one task has fared

```bash
ginkgo history prepare_data
ginkgo history prepare_data --limit 50 --json
```

One row per run of that task, newest first: when it started, how long it took,
whether it was served from the cache, and which cache key it used. Give it the
task's name, its fully qualified name, or the display label of one branch of a
fan-out.

### Exporting a run

```bash
ginkgo export events <run_id>              # JSONL, to stdout
ginkgo export events <run_id> --out run.jsonl
ginkgo export manifest <run_id> --out manifest.yaml
```

`export events` writes the run's ledger in exactly the shape
`ginkgo run --agent-output` prints while a run is live, so a tool that consumes
one consumes the other. `export manifest` re-exports the manifest through the
same code that wrote the run's own, so the two are byte-identical.

### Raw SQL

```bash
ginkgo query "SELECT name, status, count(*) AS n FROM tasks GROUP BY name, status"
ginkgo query "SELECT cache_key, function, size_bytes FROM cache_entries" --csv
ginkgo query "SELECT * FROM runs" --json --limit 5
```

One `SELECT` at a time, against a read-only connection. A statement that would
change anything is refused before it reaches SQLite, and so is a second
statement after a semicolon. At most 1000 rows come back unless `--limit` says
otherwise.

## From Python

`ginkgo.query` is the module the CLI is built on, and it is a supported API.

```python
from ginkgo import query

with query.open() as reader:
    for run in reader.runs(status="failed", limit=10):
        print(run.run_id, run.workflow, run.duration_s)
```

`query.open()` raises if the workspace has no database; pass `missing_ok=True`
to read an empty ledger instead, which is what every CLI listing does.

### Into pandas

`Query.sql` returns column names alongside the rows, which is all a
`DataFrame` needs:

```python
import pandas as pd
from ginkgo import query

with query.open() as reader:
    result = reader.sql(
        """
        SELECT t.name, t.status, t.cached,
               julianday(t.finished_at) - julianday(t.started_at) AS days
        FROM tasks t JOIN runs r ON r.run_id = t.run_id
        WHERE r.workflow LIKE ?
        """,
        ("%flow.py",),
        limit=10_000,
    )

frame = pd.DataFrame(result.rows, columns=result.columns)
frame["seconds"] = frame.pop("days") * 86_400
print(frame.groupby("name")["seconds"].describe())
```

### Walking lineage

```python
with query.open() as reader:
    graph = reader.lineage("table:features", direction="upstream")
    for version_id in graph.neighbours(graph.root.version_id):
        parent = graph.versions[version_id]
        print(parent.key, parent.version_id, parent.producer_task)
```

Nodes are `AssetVersion` objects — the same model the asset catalog uses.
`direction="downstream"` walks the other way, and `depth=N` stops the walk.
`reader.why(path_or_artifact_id)` answers the related question for a file:
which run and task produced these bytes, and what that task consumed.

### Cache statistics

```python
with query.open() as reader:
    stats = reader.cache_stats()

print(f"{stats.entries} entries, {stats.total_bytes / 1e9:.1f} GB")
print(f"{stats.never_hit} never hit ({stats.never_hit_bytes / 1e9:.1f} GB reclaimable)")
for function, entries, size in stats.top_functions:
    print(f"  {function}: {entries} entries, {size / 1e6:.0f} MB")
```

### Task history

```python
with query.open() as reader:
    for row in reader.task_history("prepare_data", limit=20):
        print(row.run_id, row.status, row.duration_s, row.cached)
```

## The schema is not stable

`Query.sql` and `ginkgo query` hand out the database's tables directly. Those
tables are versioned — `ginkgo db path` and `ginkgo db check` will tell you
which version a workspace is at — but they are **not** a stable interface. They
change between ginkgo releases without a deprecation period, and a query written
against them may need rewriting after an upgrade.

The methods on `ginkgo.query.Query` are the surface that is kept working. Reach
for SQL when they cannot answer your question, and expect to revisit it.

The tables themselves are described in `docs/architecture/store.md`. The ones
you will want most often:

| Table | One row per |
|---|---|
| `runs` | run — workflow, status, timings, params |
| `tasks` | task within a run — status, cache key, timings, attempts |
| `events` | ledger event, with the full payload as JSON |
| `cache_entries` | cache entry — function, size, hit count |
| `asset_versions` | version of a catalogued asset |
| `edges` | provenance edge between two nodes |
