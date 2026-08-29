# CLI

The current CLI supports:

- `ginkgo run`
- `ginkgo debug`
- `ginkgo doctor`
- `ginkgo inspect`
- `ginkgo runs ls`
- `ginkgo runs show`
- `ginkgo history`
- `ginkgo query`
- `ginkgo export events`
- `ginkgo export manifest`
- `ginkgo secrets`
- `ginkgo init`
- `ginkgo asset ls`
- `ginkgo asset versions`
- `ginkgo asset show`
- `ginkgo asset inspect`
- `ginkgo models`
- `ginkgo lineage`
- `ginkgo cache ls`
- `ginkgo cache stats`
- `ginkgo cache explain`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls`
- `ginkgo env clear`
- `ginkgo db migrate`
- `ginkgo db check`
- `ginkgo db prune`
- `ginkgo db vacuum`
- `ginkgo db path`
- `ginkgo report`
- `ginkgo notebooks`

Implemented CLI features include the dry-run execution-plan preview, merged
config overrides, human-readable run summaries, structured inspection and
diagnostics, secret discovery and validation, cache inspection and eviction,
failed-task debugging, and asset catalog inspection for local workspaces.

`asset versions`, `asset show`, and `asset inspect` resolve their key argument
against the catalog rather than parsing it in isolation
(`resolve_asset_key` in `cli/commands/asset.py`). A `<kind>:<name>` key is
looked up directly; a bare `<name>` is searched across kinds, resolving when
exactly one kind holds it and reporting the candidate keys when several do. An
unknown key reports near matches from the catalog, so no lookup ever invents a
kind the user did not use.

`ginkgo lineage <asset-key[@version]>` walks the lineage edges around one asset
version and renders them as a tree — `--downstream` for what came of it,
`--depth N` to stop the walk, `--json` for the graph as data. Given a
materialized file path or an artifact id in place of an asset key, it answers a
different question with the same verb: which run and task produced those bytes,
through which cache entry, and what that task consumed. Both readings are
`ginkgo.query.Query.lineage` and `.why` underneath, and both open the database
read-only.

`ginkgo runs` is where a recorded run is read. `runs ls` lists the run index
with `--workflow`, `--status`, `--since` and `--limit` filters; `runs show`
prints one run's header and task table, or its full manifest under `--json`.
That JSON is what `ginkgo inspect run` used to print: a run belongs to the
`runs` group rather than to `inspect`, so it has one home there and `inspect`
is now only about a workflow's static graph.

`ginkgo history <task-name>` crosses runs instead of staying inside one — one
row per execution of that task, with status, duration, cache key and attempts,
resolved through `Query.task_history`. The task is matched on its name, its
fully qualified name, or the display label of one fan-out branch. Rows are
ordered by the *run's* start time: a cached task never started, so its own
timestamp is null and would sort out of the history it belongs to.

`ginkgo query "<sql>"` runs one statement through `Query.sql` and prints a
table, `--json`, or `--csv`. Three things are refused: a statement that is not a
read; more than one statement; and more rows than `--limit` (1000 by default).
The row cap is applied while fetching from the cursor rather than by wrapping
the statement in a `LIMIT`, so the SQL that runs is the SQL the user wrote and a
syntax error names their text.

"Not a read" is decided by `_refusal`, over a scanner (`_top_level_words`) that
yields a statement's bare words outside parentheses, skipping string literals,
quoted identifiers and comments. The leading word must be `SELECT`, `WITH`,
`VALUES` or `EXPLAIN`; a `WITH` is then followed to the verb its clause ends in,
because `WITH t AS (SELECT 1) DELETE FROM runs` leads with a word this allows.
Scanning rather than splitting on whitespace is also what makes `VALUES(1),(2)`
a read: the verb ends at the first character that cannot continue an identifier.

The check is what produces a readable message; it is not the enforcement. Every
read connection is `mode=ro` with `PRAGMA query_only=ON`, and the in-memory
ledger `query.open(missing_ok=True)` returns is created write-mode and then
closed to writes with `SqliteStore.restrict_to_reads()` — so an empty workspace
refuses exactly what a populated one does, and a write the scanner failed to
recognise still fails inside the engine.

`Query.sql` returns a `SqlResult` — columns, rows, the `limit` applied, and
whether it `truncated`. Every output mode reports truncation: `--json` in the
envelope, `--csv` on stderr so stdout stays openable, the table in a footer.

`ginkgo export events <run_id>` replays a finished run's ledger as JSONL in the
`--agent-output` wire shape, and `ginkgo export manifest <run_id>` re-exports
the run's manifest as YAML. Both print to stdout unless `--out` names a file.
Neither has a format of its own: events go through `cli/renderers/jsonl.py`'s
`event_line`, which is also what the live agent renderer writes, and the
manifest through `runtime/rundir.py`'s `manifest_text` / `write_manifest`, which
is what the run itself wrote at finalize.

`ginkgo run --dry-run` validates the workflow and prints a static execution
plan instead of running it: tasks grouped into dependency waves, each
annotated `[cached]`, `[will run]`, or `[unknown]`, with static `.map()`
fan-out fully expanded and a peak-resource summary. Cache status is resolved
by a leaf-anchored cascade — a task is checkable only while every upstream
dependency is a confirmed cache hit — so a fully warm rerun previews as all
`[cached]`. The plan builder (`runtime/dry_run.py`) is read-only: no task
runs, no environment is prepared, and no cached output is materialised. Large
fan-out groups collapse unless `--verbose` is passed.

There is no separate command for a project's validation workflows. `ginkgo test`
used to discover every `*.py` under `tests/workflows/` (or a legacy `.tests/`)
and run each one, which duplicated `ginkgo run --dry-run` closely enough that
user testing found the two read as interchangeable. A validation workflow is a
workflow, so it is run by path, and `ginkgo run --dry-run` is the one wiring
check — it previews the plan for the entrypoint that will actually run.

Task labels have one source, `Expr.display_label` (`core/expr.py`): the task's
base name, with its fan-out values in brackets when the graph fixed them —
which `.map()` and `.product_map()` do at graph-build time, `per_branch()`
arguments excluded as values derived from a branch rather than naming it.
`display_labels()` assigns those labels across a graph, giving an ordinal to
repeats that nothing else tells apart. Both the dry-run plan and the live run
table label from it (`cli/commands/run.py:planned_task_rows`), so a branch
still waiting to be dispatched reads the same in both. A mapped task whose
fan-out left no label parts is the one case the graph cannot label; the
evaluator supplies a label from its resolved arguments when it prepares the
node, and the renderer adopts it on the node's first event.

Commands that import a workflow — `run`, `doctor`, `secrets`, and
`inspect workflow` — accept flags for the parameters that workflow declares with
`ginkgo.param(...)`, resolved CLI-first, then the config `[params]` table, then
the declared default. `ginkgo run <workflow> --help` renders the run flags and
then imports the workflow to list its declared parameters; an unrecognised flag
is reported together with the parameters the workflow does declare. See
[Configuration, Parameters, and Secrets](config-secrets.md).

`ginkgo cache prune` accepts `--older-than <duration>`, `--max-size <size>`,
and `--max-entries <N>`. At least one of the three is required; multiple
may be combined, and eviction proceeds oldest-first — or least-recently-hit
first under `--least-recently-hit` — with orphan artifact garbage collection at
the end. `--dry-run` previews what would be removed without touching disk.

`ginkgo cache clear <key>` removes one entry. `ginkgo cache clear --orphans`
removes every entry directory the database has no row for, which is what a lost
database leaves behind; `ginkgo db check` lists them first. `ginkgo cache stats`
summarises the index — entries, bytes, hit histogram, never-hit bytes, and the
functions holding the most — as a table or `--json`.

`ginkgo db` maintains the ledger itself. `db check` reports every way an index
and the bytes it names disagree — the cache, the artifact store in both
directions, runs against run directories, the staging cache, and an environment
recorded as materializing two different ways across hosts — and exits 1 if it
found anything. `db prune --events-older-than <duration>` deletes the raw events
of runs that finished before the cutoff, leaving every projection intact, and
`--digest-memo-older-than` prunes the digest memo on `last_seen`; `--dry-run`
counts without deleting. `db vacuum` then returns the freed pages to the
filesystem. Durations are the same `30d` / `12h` / `45m` shape `cache prune
--older-than` takes, parsed by `formatting.parse_duration`.

Every command that prints a table builds it through `cli/common.py`'s
`stdout_console()` and `new_table()`, so column style and the terminal-versus-pipe
width rule are written down once rather than in each command module.

## Error reporting

Two kinds of failure reach the CLI's top-level handler, and they are reported
differently (`cli/errors.py`, on the taxonomy in `ginkgo/errors.py`):

- A `GinkgoError` — the base class of every named ginkgo exception, from
  `ParamError` to `PixiEnvNotFoundError` — is a mistake ginkgo detected and can
  explain. It prints as a single `✖ <message>` line. Any other failure raised
  with no user code on the stack is reported the same way: nothing but ginkgo's
  own frames were involved, so its message is the whole report.
- Anything else is a bug in the workflow or in ginkgo. The message is followed
  by the location of the innermost frame in code the user wrote —
  `<Type> at <file>:<line> in <function>` — so a mistake in a flow body is
  always locatable without re-running. `GINKGO_TRACEBACK=1` or `--verbose` adds
  the full rich traceback beneath it.

`GINKGO_TRACEBACK=1` prints a traceback for **every** failure, including the
ones whose default report is a bare message — a `GinkgoError`, or an internal
crash after the flow body returned with no user frame left on the stack. Only
the hint that advertises the variable is withheld there, so that ginkgo's
one-line messages stay one line; the escape hatch itself always works.

`KeyboardInterrupt` prints `⨯ Interrupted` and exits 130; `SystemExit`
propagates untouched, so argparse and `--version` keep the status they chose.
`ginkgo doctor` reports the same user-code location under each diagnostic.

Run-time failure diagnostics classify each task failure into one of a small
set of categories — `env_mismatch`, `import_error`, `invalid_path`,
`missing_input`, `shell_command_error`, `serialization_error`,
`user_code_error`, `output_validation_error`, `cache_error`,
`cycle_detected`, and `scheduler_error` — and the end-of-run renderer
groups failures by category so that common root causes stand out without
digging through individual panels.
