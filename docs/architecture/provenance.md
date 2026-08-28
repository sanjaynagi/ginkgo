# Provenance and Run State

What happened in a run is recorded in the ledger at `.ginkgo/ginkgo.db`; see
[Provenance Store](store.md) for the schema and the pragmas. This page covers
how a run gets there and what it leaves on disk.

## The write path

Everything a run knows, it says on the `EventBus` as a typed event from
`runtime/events.py`. One subscriber persists them: `store/recorder.py`'s
`StoreRecorder`, constructed once in `cli/commands/run.py` and subscribed
before any other handler that reads the store back.

```text
evaluator ─emit─▶ EventBus ─▶ StoreRecorder ─▶ StoreWriter ─▶ events + projections
                          └─▶ notifications, Rich / JSONL renderers
```

- `StoreWriter` (`store/writer.py`) queues events and applies them from one
  background thread, which owns the process's only write-mode connection. A
  batch becomes one transaction, bounded three ways: a terminal event
  (`TaskCompleted`, `TaskFailed`, `RunCompleted`) commits at once, and
  otherwise a batch closes at 256 events or 50 ms. What the writing cost is
  recorded as the run's `provenance_write_seconds`.
- `store/projector.py` turns each event into the rows it implies — one pure
  function per event type, returning SQL. The same functions serve the live
  write path and `db rebuild`.
- A write that fails is not survivable: the exception is re-raised on the next
  `put`, `flush` or `close`, so it fails the run with a message naming the
  database. Provenance never degrades silently.
- `TaskLog` events are the one exception to "everything is recorded": log
  chunks are bytes, and the log files already hold them.

The evaluator itself holds no recorder. It emits events, and asks
`runtime/rundir.py`'s `RunDir` for the two filesystem things it needs: where a
task's logs go, and a copy of each environment lockfile.

## The read path

`RunSummary` (`runtime/run_summary.py`) is the single read model (issue #79),
built from the `runs`, `tasks`, `attempts`, `task_inputs` and `edges` rows.
Every presenter — `inspect run`, `debug`, `report`, `models`, notifications,
the end-of-run console summary — formats that and nothing else. Readers reach
it through `ginkgo.query`, which opens the database read-only, so a listing
works while a run is writing.

A run is visible as soon as it has rows: `inspect run` on a live run shows its
tasks as they reach each state, rather than waiting for a finalize step.

## On disk

```text
.ginkgo/runs/<run_id>/
├── manifest.yaml   the snapshot, exported once when the run completes
├── logs/           per-task stdout and stderr
├── envs/           a copy of each Pixi lockfile the run resolved
└── notebooks/      executed notebooks and their rendered HTML
```

`manifest.yaml` is an **export**, not a source of truth. It is written once,
from the projections, and ginkgo never reads it again except through
`ginkgo db rebuild`. Its shape is the projection tables serialised — one list
per table — which is what makes rebuild a re-insert rather than a parse:

```yaml
ginkgo_snapshot: 1
runs:        [{run_id, workflow, status, started_at, finished_at, error, jobs,
               cores, memory, params, param_sources, resources, timings,
               parent_run_id, parent_task_id, ginkgo_version}]
tasks:       [{run_id, task_id, node_id, name, display_label, kind, status,
               cached, cache_key, source_hash, attempts, exit_code, failure,
               output_summary, resource_usage, timings, extra, …}]
attempts:    [{run_id, task_id, attempt, started_at, finished_at, status, …}]
task_inputs: [{run_id, task_id, param, value_type, value_summary, digest, …}]
task_outputs:[{run_id, task_id, position, name, value_type, path, …}]
edges:       [{run_id, src_kind, src_id, dst_kind, dst_id, edge}]
```

`ginkgo db rebuild` reads these and re-inserts the rows. A run directory
holding anything else is skipped with one warning; no older format is read,
and nothing is auto-ingested. The permanent guard on all of this is the
round-trip test in `tests/store/test_rebuild.py`: run → export → delete the
database → rebuild → export → equal.

Notes on the fields:

- `extra` holds the open-ended annotations a task collects — the execution
  backend, a container image digest, remote input-access statistics, notebook
  artefact pointers, a sub-run id — merged from `TaskAnnotated` events. Facts a
  reader filters or joins on get a column instead.
- `task_inputs.value_summary` is the JSON-encoded rendered argument, with
  secrets redacted, so `debug` can show what a failed task was given.
- Input digests are spelled `digest`. They are BLAKE3; only the cache key's own
  payload still says `sha256`, and renaming it there would invalidate every
  entry on disk for no gain.

## Sub-workflows

A parent passes `GINKGO_PARENT_RUN_ID` and `GINKGO_PARENT_TASK_ID` to the child
`ginkgo run`. The child records both on its own `RunStarted`, which becomes
`runs.parent_run_id` / `runs.parent_task_id` and one `child_of` edge in the
parent's graph. The parent then reads the child's run id back out of the store
once the subprocess exits.

## What is not here

There is no `events.jsonl` and no `params.yaml`. The ledger is the event log —
`ginkgo export events` (Phase 4) writes it back out in the `--agent-output`
shape — and resolved parameters are `runs.params`, with `runs.param_sources`
beside them. `--agent-output` continues to render the bus directly and does not
read the database.
