# CLI

The `ginkgo` command is how you create, check, run and inspect workflows. Each
command works on one project. The project root is the nearest directory, at or
above the one you run from, that holds a `ginkgo.toml`. Without one, the
directory you run from is the root, so run from the top of the project.

## Command Overview

Run `ginkgo <command> --help` for every flag of a command.

### Create And Run

`ginkgo init`
: Create a new project: a `ginkgo.toml`, a starter `workflow/flow.py`, and a
  `skills/` folder for coding agents. See
  [Working With Coding Agents](coding-agents.md).

`ginkgo run`
: Check a workflow, run its tasks, and record the run.

### Check

`ginkgo doctor`
: Check a workflow and its setup for problems, such as missing environments,
  unresolved secrets or bad config.

`ginkgo inspect workflow`
: Show a workflow's task graph without running it.

`ginkgo secrets`
: List and check the secret references a workflow uses.

### Inspect Runs

`ginkgo runs`
: List recorded runs (`runs ls`) and show one of them (`runs show`),
  including the pages its tasks rendered. See
  [Querying Provenance](querying-provenance.md). `runs status` and
  `runs wait` follow a run started with `--detach`. See
  [Running In The Background](#running-in-the-background).

`ginkgo history`
: Show every run of one task, with its status, duration and cache key.

`ginkgo debug`
: Show a finished run's task status, timing, logs and cache decisions.

`ginkgo why`
: Name the task and run that produced a file, and the inputs that task
  recorded.

`ginkgo query`
: Run one read-only SQL statement against the [provenance](concepts.md#glossary) database.

`ginkgo export`
: Write a run's events as JSON Lines (JSONL), or its manifest as YAML.

`ginkgo report` (plugin)
: Render a finished run as an HTML report. See [Reports](reports.md).

### Maintain

`ginkgo cache`
: List, explain, clear and prune cached task results.

`ginkgo db`
: Check and shrink the provenance database. See
  [Maintaining The Database And Cache](maintenance.md).

### Configure

`ginkgo plugins`
: List the installed plugins and what each one adds.

`ginkgo env` (plugin)
: List and reset the Pixi environments behind shell tasks. See
  [Environments](environments.md).

A command marked (plugin) appears once its [plugin](concepts.md#glossary) is installed: `ginkgo report`
comes from `ginkgo-report`, and `ginkgo env` from `ginkgo-pixi`. See
[Plugins](../getting-started/installation.md#plugins).

## A Typical Loop

1. Write or change tasks in code.
2. Check the workflow with `ginkgo doctor`.
3. Check the plan with `ginkgo run --dry-run`.
4. Run it with `ginkgo run`.
5. Look into failures with `ginkgo debug`, and into cache reuse with
   `ginkgo cache explain`.

Ginkgo caches finished tasks. When you change a later stage of a workflow,
only the tasks it affects run again. Earlier tasks come from the cache.

## Running Workflows

```bash
ginkgo run                                          # runs workflow/flow.py
ginkgo run workflow/qc_flow.py
ginkgo run workflow/flow.py --jobs 8 --cores 32 --memory 64
```

`ginkgo run` builds the workflow's task graph, checks it, runs the tasks, and
records the run in `.ginkgo/`. With no path, it runs `workflow/flow.py` in the
project root.

You can run a command from any directory inside the project. A path you pass
on the command line is relative to the directory you run from. A relative
output path in a task's arguments is relative to the project root.

A workflow file can import modules from its own directory and from the project
root.

### Budgets

- `--jobs` is the most tasks that run at once.
- `--cores` caps the total threads that running tasks declare.
- `--memory` caps the total memory that running tasks declare. It takes whole
  GiB, such as `64`, or Kubernetes notation, such as `80Gi` or `512Mi`.
- `--gpus` caps the total GPUs that running tasks declare.
- `--resource name=value` sets a budget for a custom resource that tasks
  declare, such as `--resource api_calls=10`. Repeat it for each resource. See
  [Custom Resource Dimensions](resources.md#custom-resource-dimensions).

A task that declares more than a budget can never run. A dry run lists it as a
problem before anything runs.

### Task Names

Output names a task by its function name, such as `align`. One branch of a
[fan-out](concepts.md#glossary) is named by its label, such as
`align[sample_a]`. See
[How Branches Are Labelled](tasks-and-flows.md#how-branches-are-labelled).
`history`, `cache ls --task`, `cache clear --task` and `--rerun` accept either
form.

JSON output gives the function name as `task_name` and the branch label as
`display_label`. Some commands also give a `qualified_name`, the task's full
module path, such as `workflow.modules.align.align`.

### Running A Task Again

`--rerun TASK` runs a task again instead of using its cached result. You don't
need to clear the cache first.

- Repeat the flag to rerun several tasks.
- A function name reruns every branch of a fan-out, as in `--rerun align`.
- A branch label reruns one branch, as in `--rerun 'align[sample_a]'`.

Tasks downstream run again only if the new result differs from the cached
one. A name that no task has, or a label that matches no branch, is a usage
error (exit 2) before anything runs.

### Continuing After A Failure

`--keep-going` lets a run continue after a task fails. Tasks that don't depend
on the failed task still run. Tasks that do depend on it are skipped. The run
is recorded as failed, and `ginkgo run` exits with code 3.

`--keep-going` treats every task as `on_failure="ignore"`. See
[When A Failure Should Not Stop The Run](resources.md#when-a-failure-should-not-stop-the-run).

### Output For Agents

`--agent-output` replaces the live terminal display with a stream of JSONL
events. It is for AI coding agents and other programs. See [Working With Coding Agents](coding-agents.md).

### Running In The Background

`--detach` starts the run in the background and prints its run ID and log
path. It returns once the run is recorded, or after 30 seconds if loading the
workflow takes longer. A workflow that fails its checks before then is
reported straight away. Ctrl-C while it waits leaves the run going.
`--detach` can't be combined with `--dry-run`. Follow the run with one of
these:

- `ginkgo runs status <run_id>` shows the run once: its status, task counts,
  failures and outputs.
- `ginkgo runs wait <run_id> [--timeout SECONDS]` waits for the run to finish.
  It exits as `ginkgo run` would have (see [Exit Statuses](#exit-statuses)),
  124 at the timeout, or 2 for an unknown run ID. Ctrl-C stops the wait, not
  the run.

Both commands take `--json`. See [Long Runs](coding-agents.md#long-runs) for
how an agent uses them.

### Exit Statuses

| Status | Meaning |
| --- | --- |
| 0 | Success. |
| 1 | The run failed, or could not start. For example, a missing workflow file, a flag the workflow does not declare, a bad parameter value, or a budget such as `--cores 0`. |
| 2 | The command line is wrong. For example, an unknown subcommand, a bad option value such as `--jobs abc`, `cache clear` with no key, or `--rerun` naming no task in the workflow. The message is on stderr. |
| 3 | The run finished, with failures that `--keep-going` or `on_failure="ignore"` let pass. The run is recorded as failed. |
| 130 | Interrupted with Ctrl-C. The run is recorded as `cancelled`. Run `ginkgo run` again to resume: the tasks that finished come from the cache (see [Partial Resume](caching-and-provenance.md#partial-resume)). |

## Workflow Parameters

A workflow declares the inputs it accepts with `ginkgo.param(...)`. Each one
becomes a command-line flag, with dashes for underscores:

```python
import ginkgo

n_replicates = ginkgo.param("n_replicates", type=int, default=12, help="Replicates per item")
region = ginkgo.param("region", help="Genome region")   # no default: required
```

```bash
ginkgo run flow.py --n-replicates 24 --region 2L:1-100000
```

Ginkgo takes a value from the command line first, then from the `[params]`
table of `ginkgo.toml`, then from the declared default:

```toml
[params]
n_replicates = 24
region = "2L:1-100000"
```

- `ginkgo run flow.py --help` lists the workflow's parameters, with their
  types and defaults.
- A flag the workflow does not declare, or a missing required parameter, is an
  error before anything runs.
- `--config extra.toml` adds a config file whose `[params]` override the same
  parameters in `ginkgo.toml`.
- Ginkgo records each value with the run, and where it came from.
  `ginkgo runs show --json` shows both.

Parameter types:

- `type=int`, `type=float` and `type=Path` convert the value. If you leave out
  `type`, a `bool`, `int` or `float` default sets it. Any other default leaves
  values as strings.
- A boolean takes a bare `--flag`, or `--flag false` to turn off one whose
  default is `True`.
- `multiple=True` makes the flag repeatable, as in `--item alpha --item beta`.
  The value is always a tuple.

Pass a parameter into a task as an argument, so the cache can see it. A task
body that reads it from a module global hides it from the cache. See
[A Task Sees Only Its Arguments](caching-and-provenance.md#a-task-sees-only-its-arguments).

## Checking A Workflow

```bash
ginkgo doctor
ginkgo doctor workflow/qc_flow.py --json
ginkgo run --dry-run
```

### Doctor

`ginkgo doctor` checks for environment and configuration problems before a
run. Pass `--json` for output a program can read.

Doctor checks the imports that run when the workflow module loads. An import
inside a task body runs only when the task does, so doctor can't check it. A
missing package imported there is not caught until the run reaches that task.
Import at module level to have doctor check it.

### Dry Run

`ginkgo run --dry-run` checks the workflow and works out each task's cache key
without running any task. It prints:

- the waves the tasks fall into, where a wave is a group of tasks that can run
  at the same time
- which tasks would run, and why (see
  [Why Did A Task Run Again?](caching-and-provenance.md#why-did-a-task-run-again))
- which tasks would come from the cache
- the resources the tasks declare, and the most work running at once
- any problems, under **Problems**

A fan-out is shown on one line, with its branches counted by reason:

```
align × 96   90 cached, 6 will run (6 input changed)
```

A task marked `? [unknown]` has a cache status that dry run can't work out
without running anything. Most often it is downstream of a task that will run.

**Problems** lists any task that declares more than a budget, such as
`--cores` or `--memory`, with the message the run would fail with. If there
are problems, the dry run exits with code 1.

Dry run can't see everything a run will do:

- It misses a dependency on a path that a task writes without declaring it
  `Out[...]`, or on a path built at run time. Pass the producer's return value
  to the reader instead.
- It leaves out the tasks that a task will return, if that task will run.
- A script, notebook or subworkflow task shows `? [unknown]`, with every task
  after it, unless its body is a single `return script(...)`,
  `return notebook(...)` or `return subworkflow(...)` with a simple path, such
  as a literal, a task parameter or an f-string.

### Warnings

`ginkgo doctor` and `ginkgo run --dry-run` also warn about these:

- `path_like_str_param`: a parameter whose name looks like a path, such as
  `output_dir`, is annotated `str`. Annotate it `file`, `folder` or `Out[...]`
  (see [Declaring Outputs](outputs.md#reads-and-writes)).
- `spaced_command_path`: a shell task reads another task's `Out[...]` path
  that contains a space, which the shell would split.
- `missing_directive_return`: a task whose kind needs its body to return a
  call never does, as in a shell task that returns `f"wc -l {path}"` instead
  of `shell(cmd=f"wc -l {path}")`.

These two are errors. `ginkgo doctor` reports them, and `ginkgo run` refuses
them before any task runs:

- `positional_task_call`: a task called with positional arguments. Task calls
  are keyword-only.
- `inferred_return_task_call`: a task with `Out[...]` parameters and no return
  annotation returns task calls. Add a return annotation.

These checks are best-effort, so they can miss a case.

## Debugging A Run

```bash
ginkgo debug <run_id>
```

`ginkgo debug` shows a finished run's task status, logs and cache decisions,
from its run directory under `.ginkgo/runs/`. A retried task's attempts share
one log file, and each retry starts with a `--- attempt N/M ---` line.

## Projects With Several Workflows

A file holds exactly one `@flow`. So a project with several workflows keeps one
entry file per workflow, each importing tasks from the shared modules:

```text
workflow/
├── flow.py          # the default: `ginkgo run` with no path runs this one
├── qc_flow.py       # ginkgo run workflow/qc_flow.py
├── report_flow.py   # ginkgo run workflow/report_flow.py
└── modules/         # tasks the flows share
```

`ginkgo run` with no path only ever picks `flow.py`, so run the others by path.
They all share the project's `.ginkgo/`, with one provenance database and one
cache. A cache key depends on the task and its inputs, not on which flow called
it. So a task that two flows call with the same arguments runs once, and the
other flow gets a cache hit.

Each run records the entry file it ran. `ginkgo runs ls --workflow
workflow/qc_flow.py` lists one workflow's runs. `ginkgo doctor <path>` and
`ginkgo run <path> --dry-run` check one entry file at a time.

### Validation Workflows

A project can keep workflow files under `tests/workflows/` that test its own
flow. A validation workflow is a workflow, so run it by path like any other:

```bash
ginkgo run tests/workflows/smoke.py --dry-run  # check
ginkgo run tests/workflows/smoke.py            # run
```

Each such file must expose a flow, usually by re-exporting the project's own.
`ginkgo init` creates one, `tests/workflows/smoke.py`, which re-exports `main`
from `workflow/flow.py`.

## Cache Commands

```bash
ginkgo cache ls                          # list cached task results
ginkgo cache ls --task align --json      # one task's entries, as JSON
ginkgo cache stats                       # size, hit counts, biggest tasks
ginkgo cache explain [run_id]            # why each task ran or was cached (default: latest run)
ginkgo cache prune --older-than 7d       # remove entries older than a duration
ginkgo cache prune --max-size 10GB       # remove entries to stay under a size
ginkgo cache prune --max-entries 500     # remove entries to stay under a count
ginkgo cache clear <cache_key>           # remove one entry
ginkgo cache clear --task align          # remove every entry one task wrote
ginkgo cache clear --all                 # remove every entry
ginkgo cache clear --orphans             # remove entries the database has lost
```

`cache clear` and `cache prune` remove what they name at once, with no prompt.
Pass `--dry-run` to `cache prune` to preview it. See
[Maintaining The Database And Cache](maintenance.md) for what each command
removes and how sizes are counted. See
[Why Did A Task Run Again?](caching-and-provenance.md#why-did-a-task-run-again)
for `cache explain`.
