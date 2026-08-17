# CLI

The current CLI supports:

- `ginkgo run`
- `ginkgo test`
- `ginkgo debug`
- `ginkgo doctor`
- `ginkgo inspect`
- `ginkgo secrets`
- `ginkgo init`
- `ginkgo asset ls`
- `ginkgo asset versions`
- `ginkgo asset inspect`
- `ginkgo models`
- `ginkgo cache ls`
- `ginkgo cache explain`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls`
- `ginkgo env clear`

Implemented CLI features include the dry-run execution-plan preview, merged
config overrides, human-readable run summaries, structured inspection and
diagnostics, secret discovery and validation, cache inspection and eviction,
failed-task debugging, and asset catalog inspection for local workspaces.

`ginkgo run --dry-run` validates the workflow and prints a static execution
plan instead of running it: tasks grouped into dependency waves, each
annotated `[cached]`, `[will run]`, or `[unknown]`, with static `.map()`
fan-out fully expanded and a peak-resource summary. Cache status is resolved
by a leaf-anchored cascade — a task is checkable only while every upstream
dependency is a confirmed cache hit — so a fully warm rerun previews as all
`[cached]`. The plan builder (`runtime/dry_run.py`) is read-only: no task
runs, no environment is prepared, and no cached output is materialised. Large
fan-out groups collapse unless `--verbose` is passed. `ginkgo test --dry-run`
keeps its terse per-workflow validation line rather than printing a full plan
for each discovered workflow.

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
may be combined, and eviction always proceeds oldest-first with orphan
artifact garbage collection at the end. `--dry-run` previews what would be
removed without touching disk.

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
