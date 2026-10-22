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
- `ginkgo why`
- `ginkgo plugins`
- `ginkgo cache ls`
- `ginkgo cache stats`
- `ginkgo cache explain`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls` / `ginkgo env clear` (from the `ginkgo-pixi` plugin)
- `ginkgo db check`
- `ginkgo db prune`
- `ginkgo db vacuum`
- `ginkgo db path`
- `ginkgo report` (from the `ginkgo-report` plugin)

Implemented CLI features include the dry-run execution-plan preview, merged
config overrides, human-readable run summaries, structured inspection and
diagnostics, secret discovery and validation, cache inspection and eviction,
failed-task debugging, and file provenance (`why`) for local workspaces.

`ginkgo why <path-or-artifact-id>` answers which run and task produced a
materialized file's bytes, through which cache entry, and what that task
recorded as its inputs — one line per parameter, with its digest or value
summary. `--json` prints the same as data. It is `ginkgo.query.Query.why`
underneath (`cli/commands/why.py`) and opens the database read-only. A path is
resolved through `materializations`, then through the `task_outputs` row that
carries the artifact id.

`ginkgo runs` is where a recorded run is read. `runs ls` lists the run index
with `--workflow`, `--status`, `--since` and `--limit` filters; `runs show`
prints one run's header and task table, or its full manifest under `--json`.
That JSON is what `ginkgo inspect run` used to print: a run belongs to the
`runs` group rather than to `inspect`, so it has one home there and `inspect`
is now only about a workflow's static graph.

`ginkgo history <task-name>` crosses runs instead of staying inside one — one
row per execution of that task, with status, duration, cache key and attempts,
resolved through `Query.task_history`. The task is matched on its name, its
qualified name (`TaskDef.name`: `<workflow>.align` for a single-file workflow),
or the display label of one fan-out branch. `--limit` counts runs. Rows are
ordered by the *run's* start time: a cached task never started, so its own
timestamp is null and would sort out of the history it belongs to. A run in
which the task never started, such as one that skipped it after an upstream
failure, is left out, because it holds no history of the task; a task waiting
to retry has started and stays. `--include-unstarted` lists those runs too,
and `--json` applies the same filter.

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

The schema is discoverable from the terminal, because the terminal is where the
user is standing. `ginkgo query --schema` prints every table with its columns,
read out of the database by `Query.schema` (`sqlite_master` joined to
`pragma_table_info`) rather than out of prose that can fall behind it, and takes
`--json` and `--csv` like a result does. A statement SQLite rejects is answered
the same way: `Query._schema_hint` names the columns of whichever table the
statement mentioned, or lists the tables when it mentioned none — a table typo
or a syntax error. An error that points at documentation instead is a bug
(#290): the architecture docs it named are not published, and the user cannot
reach them from where the error found them.

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
`[cached]`. Probing a checkable task also validates its resolved arguments
through the run's own `TaskValidator.validate_inputs` — cached upstream
outputs included, so a kind/path mismatch such as a DataFrame bound to a
`file` parameter is provable before anything runs (#232). Each refusal is
collected as a `PlanDiagnostic`, rendered under a red **Problems** section,
and makes the command exit non-zero — in `--agent-output` mode each
diagnostic is a `task_notice` event and the synthetic `run_completed` says
`failed`. A cold cache cannot resolve those arguments and so reports
nothing. Probe errors the live run would repair before validating (a
deleted output the artifact store can restore) degrade the node to
`[unknown]` rather than aborting the preview: the probe materialises
nothing, so it makes no claim it cannot stand behind. Those honest unknowns
are a narrow set — `FileNotFoundError` and `ValueError` out of argument
resolution or cache-key hashing, the same pair `NodeCache` treats as "not
really cached". Any other exception is a probe defect, not an answer: the node
still degrades to `[unknown]`, because a dry run must never crash on a
workflow that would run fine, but the error is recorded as a `ProbeFailure`
naming the task, the probe step, and the exception (#294). The renderer counts
them under a separate yellow heading and prints each one under `--verbose`,
apart from **Problems** and without failing the command — "Ginkgo could
not work this out" is a different claim from "your workflow is broken". The
plan builder (`runtime/dry_run.py`) is
read-only: no task runs, no environment is prepared, and no cached output is
materialised. Large fan-out groups collapse unless `--verbose` is passed.

Three more things keep the preview in step with the run. A kind with
`eager_source_hash` (script, notebook, subworkflow) folds its directive's
`source_hash` into the key, and the run gets it by calling the body; the probe
instead evaluates a body that is a single `return constructor(...)` over a
whitelisted expression subset (`runtime/static_directive.py`), calls the real
constructor, which only hashes files, and leaves anything else `[unknown]`.
An upstream folder output the run would restore before a consumer hashes it
is swapped, when the working copy has drifted from its artifact, for a
symlinked copy of the stored tree in a temporary directory, since a folder's
hash covers contents and relative paths only. And a missed key carries
`CacheStore.miss_reasons` — the `cache explain` reason codes from diffing its
components against the task's newest entry — rendered as
`[will run: source changed]`. Separately from the probe, every task's
declared resources go through `ConcurrentEvaluator.check_resource_budget`,
the run's own check, so a task the `--cores`/`--memory`/`--resource` budgets
can never fit is a `PlanDiagnostic` too.

There is no separate command for a project's validation workflows. `ginkgo test`
used to discover every `*.py` under `tests/workflows/` (or a legacy `.tests/`)
and run each one, which duplicated `ginkgo run --dry-run` closely enough that
user testing found the two read as interchangeable. A validation workflow is a
workflow, so it is run by path, and `ginkgo run --dry-run` is the one wiring
check — it previews the plan for the entrypoint that will actually run.

Each task node is labelled once, when the evaluator registers it
(`ConcurrentEvaluator._next_display_label`). The label starts from
`Expr.display_label` (`core/expr.py`): the task's base name, with its fan-out
values in brackets when the graph fixed them — which `.map()` and
`.product_map()` do at graph-build time, `per_branch()` arguments excluded as
values derived from a branch rather than naming it. A repeat that nothing else
tells apart takes an ordinal, counted across the static graph and every dynamic
expansion. The label is recorded on `GraphNodeRegistered` and projected onto the
task's row, and every surface — the dry-run plan, the live run table,
`runs show`, `debug`, `history`, `cache ls` and the HTML report — reads that one
label, so a branch still waiting to be dispatched reads the same everywhere. A
mapped task whose fan-out left no label parts is the one case registration
cannot label; the evaluator supplies a label from its resolved arguments when it
prepares the node and records it on `TaskPlanned`.

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
database leaves behind; `ginkgo db check` lists them first, and `cache prune` and
`db check --fix` remove them too. `ginkgo cache stats`
summarises the index — entries, bytes, hit histogram, never-hit bytes, and the
functions holding the most — as a table or `--json`.

`ginkgo db` maintains the ledger itself. `db check` reports every way an index
and the bytes it names disagree — the cache, the artifact store in both
directions, runs against run directories, the staging cache, and an environment
recorded as materializing two different ways across hosts — and exits 1 if it
found anything. Like every other read path it opens the database read-only and
never creates one; an empty workspace reports that and exits 0. A workspace
with run directories but no database is warned about rather than called empty,
still at exit 0. Creating the database is `ginkgo run`'s job.

`runs ls` and `doctor` report that same condition, from the one place that
computes it (`rundir.runs_without_ledger_warning`), which also covers run
directories older than a database recreated since (the next `ginkgo run` after
a deletion starts a new one, and warns that it does): a run listing adds the
warning when `.ginkgo/runs/` holds directories it has no row for; `doctor`
reports it as a `runs_without_ledger` warning diagnostic, and recorded runs
whose directories are gone (`rundir.runs_without_directory_warning`) as
`runs_without_directory`. Both leave its exit status at 0. Everything else
`db check` fails on reaches doctor through `db.ledger_problems`: a ledger SQLite
cannot open, or whose integrity check fails, is a `corrupt_ledger` error, and
cache drift a `ledger_drift` warning that points at `db check --fix`.

`db prune` takes three cutoffs, at least one required.
`--events-older-than <duration>` deletes the raw events of runs that finished
before it, leaving every projection intact; `--digest-memo-older-than` prunes
the digest memo on `last_seen`; `--staging-older-than` prunes staged remote
inputs on `last_used_at` and deletes their bytes, which is the only eviction
the staging cache has. `--dry-run` counts without deleting. `db vacuum` then
returns the freed pages to the filesystem, and says so plainly when it could
not. Durations are the same `30d` / `12h` / `45m` shape `cache prune
--older-than` takes, parsed by `formatting.parse_duration`.

Every command that prints a table builds it through `cli/common.py`'s
`console()` and `new_table()`, so column style and the terminal-versus-pipe
width rule are written down once rather than in each command module.

Three rules keep the live run table from flickering, all in
`renderers/run.py`. Rich repaints by erasing the block line by line and
writing it back, so what the terminal shows mid-repaint is a half-erased
screen — the flicker — and the fix is to make that happen less often, more
cheaply, and invisibly.

- **One cadence.** `_LIVE_REFRESH_PER_SECOND`, handed to Rich's `Live`, is the
  only thing that paints. Events update `_RunEventState` and stop; the next
  frame picks the change up. A task finishing does not paint a frame of its
  own, which is what keeps a burst of concurrent completions from becoming a
  burst of repaints.
- **The block fits the screen.** A live block as tall as the terminal cannot be
  overwritten in place — the terminal scrolls to fit the last line, so the next
  repaint redraws everything. `row_budget` sizes the task rows from the console
  height less `chrome_lines`, leaving a spare line, and `visible_items` keeps a
  contiguous window over the rows that follows the frontier, so the tasks in
  flight stay on screen while finished ones scroll off the top. `chrome_lines`
  measures the resource line and the notices rather than counting them: both
  are prose at the console's width, and a long notice wraps. The window is a
  repaint concern only, asked of the display itself — `_repainting_in_place`
  reads Rich's `is_started`, which `stop` clears before painting the final
  frame, so the frame that stays in the scrollback shows every row, and a run
  whose output is a pipe is never windowed at all. Windowing is also what keeps
  a frame cheap: rendering every row of a 5000-task run takes ten times the
  refresh period, so an unwindowed live display could not keep up with itself.
- **Each repaint is one update.** `_SynchronisedLive` brackets a repaint in DEC
  private mode 2026, which asks the terminal to hold its screen until the frame
  is complete. Terminals that do not know the mode ignore it.

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
An interrupt during `ginkgo run` closes the run in the ledger under the
terminal status `cancelled` — distinct from `failed`, because stopping a run
on purpose is not the workflow going wrong — carrying the task counts as they
stood at the interrupt. `_close_unfinished_run` records the same status for an
interrupt arriving outside the scheduler loop.

Exit statuses, all of them:

| Status | Meaning |
| --- | --- |
| 0 | The command succeeded. |
| 1 | A failure reached the top level, and the run stopped at it. |
| 2 | Argparse rejected the command line, or no command was given. |
| 3 | The run drained under a non-fatal failure policy — `@task(on_failure="ignore")` or `ginkgo run --keep-going` — with failures in it. The run is recorded `failed`; the tasks that could run, ran. In phase 1 such a run ends without a result, because the workflow's own return value is downstream of everything: the code path for a run whose root survives its ignored failures exists, and phase 2's partial fan-in is what reaches it. |
| 130 | Interrupted (SIGINT). The run is recorded `cancelled`. |

The named constants are `IGNORED_FAILURES_EXIT_CODE` and
`INTERRUPT_EXIT_CODE` in `cli/errors.py`.

`ginkgo run --keep-going` treats every task as `on_failure="ignore"`: a task
failure stops nothing but the branch below it. Tasks left resultless by a
failure are reported `skipped`, each naming the failure that blocked it — one
line each while that stays readable, counted per blocking failure beyond that,
so a handful of failures at the head of a wide fan-out cannot print thousands
of lines. Failures the policy let pass are diagnosed under their own heading,
apart from the one that would otherwise have stopped the run, and are panelled
up to a cap of ten with the rest counted — except the failure carrying the
environment hint, which keeps its panel wherever it falls in that list. A run
that ends this way still lists the pages its surviving branches
rendered: only the summary line differs from a success.
`ginkgo doctor` reports the same user-code location under each diagnostic.

Run-time failure diagnostics classify each task failure into one of a small
set of categories — `env_mismatch`, `import_error`, `invalid_path`,
`missing_input`, `shell_command_error`, `serialization_error`,
`user_code_error`, `output_validation_error`, `cache_error`,
`cycle_detected`, `scheduler_error`, and `cancelled` — and the end-of-run
renderer groups failures by category so that common root causes stand out
without digging through individual panels. `cancelled` marks a task the run
had started when something else stopped it, so the record closes it rather
than leaving it running. A failure while a task is being prepared (resolving
and validating its inputs, running a notebook or script body to hash its
source, preparing its environment) is that task's failure, and is not retried.
