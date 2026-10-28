# Querying Provenance

Ginkgo records every run in the [provenance](concepts.md#glossary) database. It records which tasks
ran, their inputs, how long they took, whether they came from the cache, and
the files they wrote. Use the record to answer questions such as these:

- Which runs failed?
- What produced this file?
- How long does each task take?
- How much of my cache is never reused?

This page shows how to read the record: from the command line, from Python,
and with SQL when nothing else fits.

## Where Provenance Lives

The provenance database is one SQLite file per project, at
`.ginkgo/ginkgo.db`. Each run also gets a directory, `.ginkgo/runs/<run_id>/`,
that holds each task's logs, rendered notebook pages, copies of the
environment lock files, and a `manifest.yaml` of what the database recorded
for the run.

Every command on this page only reads the database, so you can query a project
while a run is writing to it.

## From The Command Line

### What Has Been Run

```bash
ginkgo runs ls                          # the 20 most recent runs
ginkgo runs ls --workflow flow.py       # only runs of one workflow
ginkgo runs ls --status failed --limit 5
ginkgo runs ls --since 2026-08-01 --json
ginkgo runs show                        # the most recent run
ginkgo runs show <run_id> --all         # one run, with every task
```

`--status` takes `running`, `succeeded`, `failed` or `cancelled`.

`ginkgo runs show` prints one run: its workflow, status, duration, and a table
of its tasks. For a run with more than 50 tasks, the table is cut short but
keeps the tasks that did not succeed. Pass `--all` to list every task.

`--json` prints the run manifest, the same document as
`.ginkgo/runs/<run_id>/manifest.yaml`. See [Task Names](cli.md#task-names)
for how it names tasks.

### How One Task Has Fared

```bash
ginkgo history prepare_data
ginkgo history prepare_data --limit 50 --json
ginkgo history prepare_data --include-unstarted
```

`ginkgo history` prints one row per run of that task, newest first. Each row
shows when it started, how long it took, whether it came from the cache, and
which cache key it used. Give it the task's name, its full module path such as
`workflow.modules.prep.prepare_data`, or the label of one branch of a fan-out.

`--limit` counts runs, so a fan-out can give more rows than that. A run that
skipped the task, for example after an upstream failure, is left out. Add
`--include-unstarted` to list those runs too.

### What Produced A File

```bash
ginkgo why results/features.parquet
ginkgo why results/features.parquet --json
```

`ginkgo why` takes a file that a task wrote or restored, or an artifact id. It
prints the run and task that produced those bytes, the cache entry that holds
them, and the inputs that task recorded. A path inside an output folder is
answered for that folder.

If `why` finds nothing, the task may have returned the path as a plain `str`.
Annotate the return `-> file` (or `-> folder`), or write the file through an
`Out[file]` parameter, so that its runs record it.

### Exporting A Run

```bash
ginkgo export events <run_id>              # JSON Lines, to stdout
ginkgo export events <run_id> --out run.jsonl
ginkgo export manifest <run_id> --out manifest.yaml
```

`export events` writes the run's events as JSONL, in the same shape that
`ginkgo run --agent-output` prints. `export manifest` writes the same file as
`.ginkgo/runs/<run_id>/manifest.yaml`.

## From Python

`ginkgo.query` is the module the command line uses, and it is a supported API.
`query.open()` returns a `Query` object. The examples call it `reader`:

```python
from ginkgo import query

with query.open() as reader:
    for run in reader.runs(status="failed", limit=10):
        print(run.run_id, run.workflow, run.duration_s)
```

`query.open()` raises `FileNotFoundError` in a project with no runs. Pass
`missing_ok=True` to read an empty record instead.

### What Produced A File, From Python

```python
with query.open() as reader:
    provenance = reader.why("results/features.parquet")

print(provenance.task_name, provenance.run_id, provenance.cache_key)
for entry in provenance.inputs:
    print(entry["param"], entry.get("digest") or entry.get("value_summary"))
```

`reader.why(path_or_artifact_id)` returns a `Provenance`, with the same
information `ginkgo why` prints. `inputs` has one mapping per parameter. When a
task returns other tasks, each output is credited to the returned task that
wrote it.

### Cache Statistics

```python
with query.open() as reader:
    stats = reader.cache_stats()

print(f"{stats.entries} entries, {stats.total_bytes / 1e9:.1f} GB")
print(f"{stats.never_hit} never hit ({stats.never_hit_bytes / 1e9:.1f} GB reclaimable)")
for qualified_name, entries, size in stats.top_functions:
    print(f"  {qualified_name}: {entries} entries, {size / 1e6:.0f} MB")
```

`top_functions` lists the tasks whose entries take the most space, by full
module path.

### Task History

```python
with query.open() as reader:
    for row in reader.task_history("prepare_data", limit=20):
        print(row.run_id, row.status, row.duration_s, row.cached)
```

`limit` counts runs, as `ginkgo history --limit` does. `limit=None` returns
the task's whole history.

## Raw SQL

Use the methods on `Query` when they can answer your question. They are the
supported interface.

```{warning}
The database tables are not a stable interface. They change between Ginkgo
releases with no deprecation period, so a query written against them may need
rewriting after an upgrade. Before version 1.0, a new release can also refuse
your existing database: see
[Upgrading Ginkgo](maintenance.md#upgrading-ginkgo).
```

`ginkgo db path` and `ginkgo db check` show which schema version a project's
database is at.

### The Main Tables

`ginkgo query --schema` prints every table and its columns as they stand in
your project. The ones you will want most often:

| Table | One row per |
|---|---|
| `runs` | run: workflow, status, timings, parameters |
| `tasks` | task within a run: status, cache key, timings, attempts |
| `events` | event, with the full payload as JSON |
| `cache_entries` | cache entry: task, size, hit count |
| `edges` | provenance edge between two nodes (for `depends_on`, `src_id` is the upstream task and `dst_id` the task that depends on it) |

In `tasks`, the `name` column holds the task's full module path. In
`cache_entries`, the task is in the `function` column.

### Running SQL With `ginkgo query`

```bash
ginkgo query --schema                   # every table and its columns
ginkgo query "SELECT name, status, count(*) AS n FROM tasks GROUP BY name, status"
ginkgo query "SELECT cache_key, function, size_bytes FROM cache_entries" --csv
ginkgo query "SELECT * FROM runs" --json --limit 5
```

A statement that names a table or column that does not exist gets an error
that lists the ones that do:

```
$ ginkgo query "SELECT task_name FROM tasks LIMIT 10"
✖ SQLite rejected the query: no such column: task_name. tasks has run_id,
task_id, node_id, name, display_label, kind, execution_mode, env, status, …
`ginkgo query --schema` lists every table.
```

`ginkgo query` runs one statement, and refuses one that would write. At most
1000 rows come back unless you pass `--limit`, and a cut-short result says so.

### Into Pandas

`reader.sql(...)` returns the column names with the rows, which is all a
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
