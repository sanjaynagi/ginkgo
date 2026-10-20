# CLI

The `ginkgo` CLI is the main operator surface for authoring, validating,
running, and inspecting workflows. Every command operates on the project rooted
at the nearest `ginkgo.toml`.

## Command Overview

`ginkgo init`
: Scaffold a new project &mdash; `ginkgo.toml`, a starter `workflow/flow.py`, the
  canonical layout, and a `skills/` directory for coding agents. See
  [Working with Coding Agents](coding-agents.md).

`ginkgo run`
: Build the expression tree, validate the workflow, evaluate ready tasks, and
  record the run. The command you reach for most.

`ginkgo inspect`
: Inspect the resolved task graph of a workflow without running it
  (`inspect workflow`).

`ginkgo runs`
: List recorded runs (`runs ls`) and show one of them (`runs show <run_id>`),
  including the pages its tasks rendered, such as marimo notebooks.
  See [Querying Provenance](querying-provenance.md).

`ginkgo history`
: Show every run of one task, with its status, duration, and cache key. Runs in
  which the task never started, such as one that skipped it after an upstream
  failure, are left out; `--include-unstarted` lists them too. `--limit` counts
  runs, so every branch of a fan-out in a listed run is shown.

`ginkgo query`
: Run one read-only SQL statement against the provenance database.

`ginkgo export`
: Write a run's ledger events as JSONL (`export events`) or its manifest as
  YAML (`export manifest`).

`ginkgo debug`
: Inspect a finished run &mdash; task status, timing, logs, and cache decisions
  &mdash; from its recorded run directory.

`ginkgo doctor`
: Check a workflow and its environment for problems: missing environments,
  unresolved secrets, malformed config.

`ginkgo report`
: Provided by the `ginkgo-report` plugin. Render a finished run as an HTML report. By default this produces a
  directory bundle (`index.html` plus an `assets/` folder); pass
  `--single-file` for a single self-contained HTML file. See
  [Reports](reports.md).

`ginkgo cache`
: List, clear, and prune cached task results. See
  [Caching and Provenance](caching-and-provenance.md).

`ginkgo why`
: Name the task and run that produced a materialized file (or an artifact id),
  its cache key, and the inputs that task recorded. Add `--json` for a
  structured payload. See [Querying Provenance](querying-provenance.md).

`ginkgo plugins`
: List the installed plugins and what each contributes: executor types,
  object stores, FUSE drivers, secret backends, run notifications, task kinds,
  environments and CLI commands (the bare-name environment shows as
  `(bare name)`). The Kubernetes and GCP Batch executors, Slack notifications
  and `ginkgo report` appear only once their plugins are installed. Add `--json` for a structured payload.

`ginkgo env`
: Provided by the `ginkgo-pixi` plugin. List and reset the Pixi environments
  backing shell tasks. See [Environments](environments.md).

`ginkgo secrets`
: List and validate the secret references a workflow resolves at run time.

`ginkgo db`
: Maintain the provenance database. `db path` prints where it is, `db migrate`
  creates or upgrades it, `db check` reports anything the database and the files
  beside it disagree about, `db prune` deletes history you no longer need, and
  `db vacuum` gives the freed space back. See
  [Caching and Provenance](caching-and-provenance.md).

Run `ginkgo <command> --help` for the full flag set of any command.

## Running Workflows

```bash
ginkgo run flow.py
ginkgo run flow.py --jobs 8 --cores 32 --memory 64
ginkgo run flow.py --dry-run
```

`ginkgo run` builds the expression tree, validates the workflow, evaluates ready
tasks subject to the `--jobs`, `--cores`, `--memory`, and `--gpus` budgets,
and writes run history under `.ginkgo/runs/`. Run it from a project root with
no path argument and Ginkgo discovers the canonical `workflow/flow.py`
entrypoint. Output then names the workflow by its discovered path, marked
`(auto-discovered)`, so you can see which file is running.

Commands may be run from any directory inside the project. Ginkgo locates the
root — the nearest enclosing `ginkgo.toml` — and works from there, so
`.ginkgo/`, config files, and task environments resolve identically wherever
you invoke from. Paths you pass on the command line are relative to where you
stand; a task's own relative output paths are relative to the project root.
`ginkgo.toml` is optional: without one, the directory you run from is the
project root, so run from the top of the project.

A workflow file imports its own modules from three import roots: the file's
own directory, the parent of the package it sits in (when its directory has
an `__init__.py`), and the project root. So `exp/flow.py` can import
`exp/tasks.py` either as `tasks` or as `exp.tasks`, and a test workflow under
`tests/workflows/` can import the project's own package.

Repeated `--resource name=value` flags budget any custom resource dimensions
tasks declare (e.g. `--resource api_calls=10`) — see
[Custom Resource Dimensions](resources.md#custom-resource-dimensions).

`--dry-run` resolves the graph and computes cache keys without executing any
task body &mdash; the fastest way to confirm a workflow is wired correctly.
On a warm cache it goes further: with cached upstream outputs in hand, each
downstream task's arguments run through the same input validation the real
run applies, so a kind/path mismatch (say, a DataFrame wired into a
`file` parameter) is reported under **Problems** before anything runs &mdash;
and the command exits non-zero, so a scripted preflight fails loudly. A
first, cold dry run cannot resolve those arguments and so makes no claim.
**Problems** also lists, cold or warm, any task whose `threads` or `memory`
exceeds the run's `--cores` or `--memory` (or a custom resource its
`--resource` budget), with the message the run would fail with.
The plan covers only the graph the flow builds before anything runs: tasks a
task returns at runtime are not shown, so for a dynamic workflow it is a lower
bound on what the real run executes.

`--keep-going` stops a task failure from ending the run: every task is treated
as `on_failure="ignore"`, so each branch that does not depend on the failure
still runs, and the tasks that do depend on it are reported `skipped`. The run
is still recorded as failed &mdash; see
[When a Failure Should Not Stop the Run](resources.md#when-a-failure-should-not-stop-the-run).

`--agent-output` swaps the live terminal UI for a stream of newline-delimited JSON
events, for programmatic use by AI coding agents &mdash; see
[Working with Coding Agents](coding-agents.md).

The run header names the workflow by its path from where you stand, and
carries the run id only once the run is about to be created: a run rejected
while loading or validating (a missing file, `--cores 0`) never had an id, so
none is printed.

### Exit Statuses

| Status | Meaning |
| --- | --- |
| 0 | Success. |
| 1 | A failure ended the run, or the workflow could not be started: an unknown or invalid workflow parameter, a missing workflow file, or an invalid budget such as `--cores 0` or `--jobs 0`. |
| 2 | `argparse` rejected the command line (an unknown subcommand, a malformed option value such as `--jobs abc` or `--older-than 0d`, an unknown `runs ls --status`, no command at all), or a command rejected its arguments in its own check, for example `cache clear` with no key, `cache prune` or `db prune` with no limit, `query` with no statement. The message goes to stderr. |
| 3 | The run finished under a non-fatal failure policy &mdash; `on_failure="ignore"` or `--keep-going` &mdash; with failures in it. |
| 130 | Interrupted with Ctrl-C. |

### Task Names

Output names a task the way its code does: by its function name, such as
`align`, or by the display label of one branch of a fan-out, such as
`align[sample_a]` or `align[2]`. Failure messages, `runs show`, `debug`,
`history`, `why` and the `cache` commands all use these names, and `history`
and `cache ls --task` accept them.

JSON output carries the function name as `task_name` and the branch label as
`display_label`. Where two modules could define a same-named task, it also
carries the qualified name as `qualified_name` (`history --json`, `cache ls
--json`, `cache stats --json`): the module path for a task in a package, such
as `workflow.modules.align.align`, and `<workflow>.align` for a task defined
in a single-file workflow, whatever that file is called or wherever it lives.
Events and the ledger's `tasks.name` column record that same qualified name.

## Workflow Parameters

A workflow declares the inputs it accepts with `ginkgo.param(...)`, and each one
becomes a command-line flag:

```python
import ginkgo

n_replicates = ginkgo.param("n_replicates", type=int, default=12, help="Replicates per item")
region = ginkgo.param("region", help="Genome region")   # no default: required
```

```bash
ginkgo run flow.py --n-replicates 24 --region 2L:1-100000
```

The flag is the dashed form of the name. A value resolves from the command line
first, then the `[params]` table of `ginkgo.toml`, then the declared default:

```toml
[params]
n_replicates = 24
region = "2L:1-100000"
```

`ginkgo run flow.py --help` lists the parameters that workflow declares,
with their types and defaults. A parameter set in `[params]` shows that value
as its default and the file it came from, such as `(default: 24, from
ginkgo.toml)`, since that is what a run without the flag would use. With no
path, `ginkgo run --help` lists the parameters of the auto-discovered workflow
and says so. A flag the workflow does not declare is rejected
before anything runs, and the error names the parameters it does declare. A
required parameter that is not supplied fails the same way.

`type` follows `argparse`'s convention, so `type=int`, `type=float`, and
`type=Path` all work. Without `type`, a `bool`, `int` or `float` default sets
it, so `ginkgo.param("threshold", default=40.0)` reads `--threshold 2.5` as a
float; any other default leaves values as strings. Booleans accept a bare `--flag` or an explicit
`--flag false`, and `multiple=True` makes a flag repeatable:

```bash
ginkgo run flow.py --item alpha --item beta --verbose
```

Resolved values are recorded with the run, along with where each came from
&mdash; the CLI, config, or the default. `ginkgo runs show --json` shows both.

The `[params]` table layers across config files, so `--config extra.toml` setting
one parameter leaves the others in `ginkgo.toml` alone.

```{important}
**Pass a parameter into a task as an argument.** Cache keys hash task arguments,
so a parameter passed as one correctly re-runs the tasks that used it. A
parameter read from a module global inside a task body is not part of the key, so
changing it would silently reuse the previous result. Both `ginkgo run` and
`ginkgo doctor` warn when they spot this.
```

```{important}
**Pass a data file into a task as a `file` argument.** A file opened by a literal
path inside a task body, such as `open("data/samples.tsv")`, is invisible to the
cache: editing it does not re-run the task, and Ginkgo does not warn. Declare it
as a `file` parameter instead (`def summarise(samples: file)`) and pass the path
at the call site, so its bytes are hashed into the key and, when another task
produces it, the dependency edge is created. `version=` on the task is the
stopgap when the path cannot be passed in.
```

## Validation And Diagnostics

Use these commands to inspect a workflow without committing to the full
workload:

```bash
ginkgo run --dry-run
ginkgo doctor flow.py
ginkgo debug <run_id>
```

`ginkgo run --dry-run` previews the plan for the entrypoint you actually run,
reporting the waves tasks fall into, which would run and which would serve from
cache, and the resources they declare. It is a quick way to confirm the wiring
you *expressed* — one task's return value passed as another's argument — is
correct, but it can only show the dependency edges the graph actually has: if
two tasks are wired by repeating the same literal path instead of passing the
producer's return value, dry-run sees no edge between them and cannot flag that
the consumer might run before or alongside the producer.

A task that would run says why, in the terms `ginkgo cache explain` uses
after the fact: `[will run: no prior entry]` for a task never run before, or
`source changed`, `input changed`, `version changed`, `environment changed`
when its newest cache entry differs in that part of the key. A collapsed
fan-out counts its branches by reason, as in
`align × 96   90 cached, 6 will run (6 input changed)`.

A task marked `? [unknown]` has a cache status dry-run cannot work out without
running anything, most often because it sits downstream of a task that would
run; the plan prints a one-line legend saying so. Dry-run never calls a task
body, but a script, notebook or subworkflow task's key includes the file its
body names, so dry-run reads the body instead. When the body is a single
`return script(...)` (or `notebook(...)`, `subworkflow(...)`) whose arguments
are literals, the task's parameters, module-level names, f-strings, `+`, `/`,
`Path(...)`, `str(...)` and pure path operations (`.parent`, `.name`,
`.stem`, `.suffix`, `.resolve()`, `.with_suffix()`, `.with_name()`,
`.joinpath()`), dry-run evaluates it and predicts the task like any other.
Any other body, such as one that computes the path in an earlier statement or
calls a helper function, leaves the task, and everything downstream of it,
`unknown`. Where the cache check itself fails, dry-run says so and counts the
tasks affected; `--verbose` names each one and the error behind it.

Both `ginkgo doctor` and `ginkgo run --dry-run` also flag, statically and
without touching the filesystem, a parameter whose name looks like a path
(`path`, `output_dir`, `report_files`, ...) but is annotated a bare `str`
shape rather than `file`/`folder` — a `path_like_str_param` warning. Such a
parameter is content-tracked only when its value names an existing regular
file and reads as a path (it contains a separator or has a file extension); a
directory, a bare word, or any other non-file path is tracked by its string
alone, and nothing checks that it exists. A path the task writes belongs in
`Out[...]` (see the FAQ entry on `file`/`folder`). A parameter that every call
fills from another task's result is not flagged: the producing task's return
annotation decides how that value is tracked.

Every diagnostic code is lower snake_case, such as `missing_secret` or
`invalid_env_kind`. An error doctor does not recognise is reported under its
exception's name, as in `file_not_found_error`.

They also warn (`missing_directive_return`) about a driver task, such as
`@task("shell")`, whose body can never return its directive: every `return` in
it yields a literal, an f-string, or arithmetic over names, as in
`return f"wc -l {path}"` where `return shell(cmd=f"wc -l {path}")` was meant.
Without the warning the task fails only when the run reaches it. The check is
best-effort: a `return` of a call or a variable is never flagged.

A mistyped flag is refused with the closest flag the command or the workflow's
declared parameters accept, as in `--jbos: did you mean --jobs?`.

### Validation workflows

A project may keep workflow files under `tests/workflows/` that exercise its own
flow. There is no separate command for them: a validation workflow is a workflow,
so run it by path like any other.

```bash
ginkgo run tests/workflows/smoke.py --dry-run  # validate
ginkgo run tests/workflows/smoke.py            # execute
```

Each such file must expose a flow, usually by re-exporting the project's own.
`ginkgo init` scaffolds one `tests/workflows/smoke.py` that re-exports `main`
from `workflow/flow.py`.

`ginkgo doctor` catches environment and configuration problems before a run.
Pass `--json` for structured output suitable for programmatic use:

```bash
ginkgo doctor flow.py --json
```

Doctor checks the imports that run when the workflow module loads. An import
inside a task body runs only when the task does, so a missing package
imported there is not caught until the run reaches that task. Import at
module level to have doctor check it.

`ginkgo debug` is most useful after the fact: once a run directory exists, it
surfaces recorded task status, logs, and cache behavior without manually
navigating `.ginkgo/runs/`. A retried task's attempts share one log file, so
each retry starts with a `--- attempt N/M ---` line.

`ginkgo cache` has several subcommands beyond listing:

```bash
ginkgo cache ls                          # list cached task results
ginkgo cache ls --task align --json      # one task's entries, as JSON
ginkgo cache stats                       # size, artifact-store bytes, hit counts, biggest tasks
ginkgo cache explain [run_id]            # explain cache decisions for a run (default: latest)
ginkgo cache explain [run_id] --json     # same, as JSON
ginkgo cache prune --older-than 7d       # remove entries older than a duration
ginkgo cache prune --max-size 10GB       # remove entries to stay under a size limit
ginkgo cache prune --max-entries 500     # remove entries to stay under an entry count
ginkgo cache clear <cache_key>           # remove a specific cache entry
ginkgo cache clear --orphans             # remove entries the database has lost
```

`cache ls` prints each full cache key on one line, so it can be copied into
`cache clear`; `--task` narrows the list to one task's entries, named the way
`history` takes it, and `--json` prints the entries as JSON. `cache prune`
requires at least one of `--older-than`, `--max-size`, or `--max-entries`. A
duration is a positive whole number of minutes, hours or days (`45m`, `12h`,
`30d`). Add `--least-recently-hit` to give up unused entries before old
ones, and `--dry-run` to preview what would be removed. `cache stats` takes
`--json`. Its artifact-store figure (`artifact_bytes` in JSON) is separate from
the entries' total size: the store also holds bytes no entry names, so pruning
entries does not bound it.

### Why Did This Task Run Again?

`ginkgo cache explain <run_id>` prints formatted text by default, one entry per
task, naming each input's cache-tracking label (`content`, `version`, `path`,
`value`, `output`, `untracked` — see
[Caching and Provenance](caching-and-provenance.md#how-is-each-input-tracked)) alongside
it, with a `path` label highlighted: since #307 phase 2 a `str` naming an
existing *file* is content-tracked by default, so what remains labelled
`path` is a directory (annotate `folder` to track its contents) or a
deliberately `untracked` parameter. Pass `--json` for the same data as JSON.

For a task that re-ran, it also names the components of the cache key that
moved, so the answer is a specific fact rather than "the key changed". The
JSON shape (`--json`):

```json
{
  "task_name": "produce",
  "cache_key": "fddb71a9…",
  "input_labels": {"samples": "path", "threads": "value"},
  "compared_with": {"cache_key": "4388619b…", "strategy": "same_node"},
  "reason": "input_changed",
  "details": ["input_changed"],
  "components": [
    {
      "component": "inputs.samples",
      "status": "changed",
      "current": {"type": "str", "sha256": "2f64dc9d…"},
      "prior": {"type": "str", "sha256": "f7a9858a…"}
    }
  ]
}
```

`reason` and `details` are the summary; `components` lists what actually
differs. Component names follow the cache key: `task`, `version`, `source_hash`,
`extra_source_hash` (the notebook or script a driver task runs), `env`,
`env_hash.pixi_lock` (the identity of the declared environment), and one
`inputs.<parameter>` per task argument.

`compared_with` says what the entry was compared against, and how it was found.
`"strategy": "same_node"` means the key this same node used in the most recent
earlier run of the workflow — the entry this run superseded, which is the
comparison you want. `"strategy": "most_similar_by_function"` is the fallback
used when the node is new or its label changed: of the task's recent earlier
entries, the one that differs from this node least, which may be a different
branch of a fan-out. Nothing of the
node's own changed then, so its `reason` is `no_prior_entry`; `details` and
`components` describe how it differs from that sibling, as context.

When more than one thing moved, `details` lists every reason in a fixed order —
`source_hash_changed`, `version_bump`, `env_changed`, `input_changed` — and
`reason` is the first of them. A moved component none of those covers reports
`cache_key_changed`.

A component's `status` is `changed`, or `added` / `removed` for a parameter that
appeared or went away.

Some tasks get a summary `reason` on its own, with no `components`: a task that
did not re-run reports `all_inputs_match`, one with no earlier entry of any kind
to compare against reports `no_prior_entry`, and one whose key has no cache entry at all —
because it failed, or because the entry was pruned — reports `no_entry_for_key`.

## A Typical Loop

For local development, a practical cycle looks like this:

1. author and adjust tasks in code
2. check the wiring with `ginkgo run --dry-run`
3. run with `ginkgo run`
4. inspect failures or cache reuse with `ginkgo debug`

Because Ginkgo caches completed tasks, iterating on a later stage of a workflow
re-executes only that stage &mdash; earlier tasks serve straight from cache.
