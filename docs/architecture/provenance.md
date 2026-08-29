# Provenance and Run State

What happened in a run is recorded in the ledger at `.ginkgo/ginkgo.db`; see
[Provenance Store](store.md) for the schema and the pragmas. This page covers
how a run gets there and what it leaves on disk.

## The write path

Everything a run knows, it says on the `EventBus` as a typed event from
`runtime/events.py`. One subscriber persists them:
`runtime/store_recorder.py`'s `StoreRecorder`, constructed once in
`cli/commands/run.py`.

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
  function per event type, returning SQL. It works on stored rows rather than
  on `GinkgoEvent` objects: `store/` sits below `runtime/`, so the translation
  from one to the other (`stored_event`) lives in `runtime/store_recorder.py`.
- A write that fails is not survivable: the exception is kept and re-raised on
  every later `put`, `flush` and `close`, so it fails the run with a message
  naming the database wherever the failure is noticed. Provenance never
  degrades silently.
- Anything that reads the run back while it is going — the notification service
  asks which tasks failed — registers with `StoreRecorder.on_committed` rather
  than on the bus. A bus subscriber can be called before the recorder has
  committed the event it is reacting to; a committed handler cannot.
- `TaskLog` events are the one exception to "everything is recorded": log
  chunks are bytes, and the log files already hold them.

The evaluator itself holds no recorder. It emits events, and asks
`runtime/rundir.py`'s `RunDir` for the two filesystem things it needs: where a
task's logs go, and a copy of each environment lockfile.

## The read path

`RunSummary` (`runtime/run_summary.py`) is the single read model (issue #79),
built from the `runs`, `tasks`, `attempts`, `task_inputs` and `edges` rows.
Every presenter — `runs show`, `debug`, `report`, `models`, notifications,
the end-of-run console summary — formats that and nothing else. Readers reach
it through `ginkgo.query`, which opens the database read-only, so a listing
works while a run is writing.

A run is visible as soon as it has rows: `runs show` on a live run shows its
tasks as they reach each state, rather than waiting for a finalize step.

## On disk

```text
.ginkgo/runs/<run_id>/
├── manifest.yaml   what the run did, written once when it finished
├── logs/           per-task stdout and stderr
├── envs/           a copy of each Pixi lockfile the run resolved
└── notebooks/      executed notebooks and their rendered HTML
```

`manifest.yaml` is an **export**, and ginkgo never reads it back. It is exactly
what `ginkgo runs show --json` prints, serialised as YAML — the same
`RunSummary.to_payload()` on both sides, so the file and the command cannot
disagree about what a run was. Written through a temporary file and renamed
over its destination, so an interrupted export leaves the previous one intact.

`.ginkgo/ginkgo.db` is the record. Back it up as you would `.git`: there is no
import path back from the manifests, and the `events` ledger in particular has
no on-disk counterpart at all. Losing the database loses the run history; it
does not touch the cache, whose entries are found by key on disk.

Notes on the fields:

- `extra` holds the open-ended annotations a task collects — the execution
  backend, a container image digest, remote input-access statistics, notebook
  artefact pointers, a sub-run id — merged from `TaskAnnotated` events. Facts a
  reader filters or joins on get a column instead.
- `task_inputs.value_summary` is the JSON-encoded rendered argument. Both a
  task's arguments and a run's parameters pass through
  `runtime/event_values.py:render_value` before they reach the bus, which
  redacts secrets and reduces anything with no JSON form to a description of
  itself. That happens at emit time rather than on the way into SQLite because
  `--agent-output` renders the same events straight to stdout.
- Input digests are spelled `digest`. They are BLAKE3; only the cache key's own
  payload still says `sha256`, and renaming it there would invalidate every
  entry on disk for no gain.

## Sub-workflows

A parent passes `GINKGO_PARENT_RUN_ID` and `GINKGO_PARENT_TASK_ID` to the child
`ginkgo run`. The child records both on its own `RunStarted`, which becomes
`runs.parent_run_id` / `runs.parent_task_id` and one `child_of` edge in the
parent's graph. The parent then reads the child's run id back out of the store
once the subprocess exits. The child's run id is the whole of the handle:
`SubWorkflowResult` carries `run_id` and `status`, and `ginkgo runs show
<child_run_id>` is how you read what it did.

## What is not here

There is no `events.jsonl` and no `params.yaml`. The ledger is the event log —
`ginkgo export events` (Phase 4) writes it back out in the `--agent-output`
shape — and resolved parameters are `runs.params`, with `runs.param_sources`
beside them. `--agent-output` continues to render the bus directly and does not
read the database.
