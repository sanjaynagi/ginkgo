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
  [Querying Provenance](querying-provenance.md).

`ginkgo history`
: Show every run of one task, with its status, duration and cache key.

`ginkgo debug`
: Show a finished run's task status, timing, logs and cache decisions.

`ginkgo why`
: Name the task and run that produced a file, and the inputs that task
  recorded.

`ginkgo query`
: Run one read-only SQL statement against the provenance database.

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

A command marked (plugin) appears once its plugin is installed: `ginkgo report`
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
project root. The output then names that file and marks it
`(auto-discovered)`.

You can run a command from any directory inside the project. Ginkgo finds the
project root and works from there, so `.ginkgo/`, config files and task
environments are the same wherever you run from. A path you pass on the
command line is relative to the directory you run from. A relative output path
in a task's arguments is relative to the project root.

A workflow file can import modules from its own directory and from the project
root. If its directory is a package (it has an `__init__.py`), it can also
import through the package's parent. So `exp/flow.py` can import `exp/tasks.py`
as `tasks` or as `exp.tasks`. A test workflow under `tests/workflows/` can
import the project's own package.

A mistyped flag is refused with the closest flag that the command or the
workflow accepts, as in `--jbos: did you mean --jobs?`.

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
fan-out is named by its label, such as `align[sample_a]`. The part in brackets
comes from the values that tell the branches apart. When there are none, it is
a number, as in `align[2]`. See
[How Branches Are Labelled](tasks-and-flows.md#how-branches-are-labelled).

Failure messages, `runs show`, `debug`, `history`, `why` and the `cache`
commands all use these names. `history`, `cache ls --task`,
`cache clear --task` and `--rerun` accept them.

JSON output gives the function name as `task_name` and the branch label as
`display_label`. `history --json`, `cache ls --json` and `cache stats --json`
also give a `qualified_name`. It is the task's full module path, such as
`workflow.modules.align.align`, and tells apart two tasks with the same name
in different modules.

### Running A Task Again

`--rerun TASK` runs a task again instead of using its cached result. You don't
need to clear the cache first.

- Repeat the flag to rerun several tasks.
- A function name reruns every branch of a fan-out, as in `--rerun align`.
- A branch label reruns one branch, as in `--rerun 'align[sample_a]'`.
- A task that another task returns is still looked up in the cache on its own.
  You can name it by its label too.

The rerun saves its result as usual. Tasks downstream run again only if the
new result differs from the cached one. A dry run shows a rerun task as
`will_run`.

`--rerun` stops with a usage error (exit 2) before anything runs in these
cases:

- No task in the workflow's modules has that name.
- The label names a task the flow calls, but matches none of its branches, as
  in `align[sample_z]`. The error lists the labels that exist.

A task can be defined but not called by the flow, because another task returns
it at run time. Then `--rerun` accepts its name, and the run warns at the end
if nothing matched it.

### Continuing After A Failure

`--keep-going` lets a run continue after a task fails. Tasks that don't depend
on the failed task still run. Tasks that do depend on it are skipped. The run
is recorded as failed, and `ginkgo run` exits with code 3.

`--keep-going` treats every task as `on_failure="ignore"`. See
[When A Failure Should Not Stop The Run](resources.md#when-a-failure-should-not-stop-the-run).

### Output For Agents

`--agent-output` replaces the live terminal display with a stream of JSONL
events. It is for AI coding agents and other programs. See [Working With Coding Agents](coding-agents.md).

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
becomes a command-line flag:

```python
import ginkgo

n_replicates = ginkgo.param("n_replicates", type=int, default=12, help="Replicates per item")
region = ginkgo.param("region", help="Genome region")   # no default: required
```

```bash
ginkgo run flow.py --n-replicates 24 --region 2L:1-100000
```

The flag is the name with dashes for underscores. Ginkgo takes a value from
the command line first, then from the `[params]` table of `ginkgo.toml`, then
from the declared default:

```toml
[params]
n_replicates = 24
region = "2L:1-100000"
```

`ginkgo run flow.py --help` lists the parameters that workflow declares, with
their types and defaults. A parameter set in `[params]` shows that value as its
default, and the file it came from, such as `(default: 24, from ginkgo.toml)`.
With no path, `ginkgo run --help` lists the parameters of the auto-discovered
workflow, and says so.

A flag the workflow does not declare is an error before anything runs. The
error names the parameters the workflow does declare. A required parameter
that is not given fails the same way.

Parameter types work like this:

- `type=int`, `type=float` and `type=Path` convert the value, as Python's
  `argparse` does.
- If you leave out `type`, a `bool`, `int` or `float` default sets it. So
  `ginkgo.param("threshold", default=40.0)` reads `--threshold 2.5` as a
  float. Any other default leaves values as strings.
- A boolean takes a bare `--flag`, or `--flag false` to turn off one whose
  default is `True`.
- `multiple=True` makes the flag repeatable. The value is then always a tuple,
  such as `("alpha", "beta")`. That holds when it comes from a list in
  `[params]`, or from a single value.

```bash
ginkgo run flow.py --item alpha --item beta --verbose
```

Ginkgo records each value with the run, and where it came from: the command
line, config, or the default. `ginkgo runs show --json` shows both.

`--config extra.toml` adds another config file on top of `ginkgo.toml`. A
parameter it sets in `[params]` overrides the same parameter in `ginkgo.toml`,
and leaves the others alone.

Pass a parameter into a task as an argument, so the cache can see it. A task
body that reads a parameter from a module global hides it from the cache, and
`ginkgo run` and `ginkgo doctor` warn when they spot this. The same holds for
data files. See
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
without running any task. It prints the plan:

- the waves the tasks fall into, where a wave is a group of tasks that can run
  at the same time
- which tasks would run, and why
- which tasks would come from the cache
- the resources the tasks declare
- any problems, under **Problems**

A dry run uses the same task graph as a real run. That includes the
dependencies Ginkgo finds from matching literal paths (see
[Dependencies From Matching Paths](outputs.md#dependencies-from-matching-paths)).
It can't see a dependency in two cases: a path that a task writes without
declaring it `Out[...]`, and a path built at run time. Pass the producer's
return value to the reader instead.

A task that would run says why, using the reasons `ginkgo cache explain`
gives, in shorter words. A task never run before shows
`[will run: no prior entry]`. See
[Why Did A Task Run Again?](caching-and-provenance.md#why-did-a-task-run-again)
for every reason. A fan-out is shown on one line, with its branches counted by
reason:

```
align × 96   90 cached, 6 will run (6 input changed)
```

A task marked `? [unknown]` has a cache status that dry run can't work out
without running anything. Most often the task is downstream of a task that
will run. The plan prints a one-line legend saying so.

**Problems** lists any task that declares more `threads` or `memory` than the
run's `--cores` or `--memory`, or more of a custom resource than its
`--resource` budget. It shows the message the run would fail with. When the
cache holds the outputs of earlier tasks, dry run also checks each later
task's inputs against them, as the real run would. For example, it reports a
DataFrame passed to a `file` parameter. The first dry run, with an empty
cache, can't make this check. If there are problems, the dry run exits with
code 1, so a script that runs it first stops.

A task that returns other tasks adds them only when it runs. If its own cache
entry is a hit, dry run expands the tasks it returned last time and shows each
one's status. So a returned task whose code or inputs changed shows
`will_run` under a `cached` parent. If the task itself will run, the tasks it
returns are left out. The plan then shows fewer tasks than the real run will
execute.

Dry run never runs a task's body. But a script, notebook or subworkflow task's
key includes the file its body names, so dry run reads the body to find that
file. Keep the body a single `return script(...)`, `return notebook(...)` or
`return subworkflow(...)` with a simple path, such as a literal, a task
parameter or an f-string. Otherwise the task, and every task after it, shows
`? [unknown]`.

The last line shows the most work running at once among the tasks that will
run, such as `up to 3 tasks at once, one thread each` or
`12 threads requested at peak`. Tasks that come from the cache are not
counted.

If the cache check itself fails for a task, dry run says so and counts the
tasks affected. `--verbose` names each one and the error.

### Warnings

`ginkgo doctor` and `ginkgo run --dry-run` also warn about these:

- `path_like_str_param`: a parameter whose name looks like a path, such as
  `output_dir`, but is annotated `str`. Annotate it `file`, `folder` or
  `Out[...]` (see
  [Reads vs. Writes](caching-and-provenance.md#reads-vs-writes-file-vs-outfile)).
  A parameter that every call fills from another task's result is not
  flagged, because that task's return annotation decides how it is tracked.
- `spaced_command_path`: a shell task reads another task's literal
  `Out[...]` path, and the path contains a space. The shell would split the
  path, so the run would fail at the shell task, after the work before it.
- `missing_directive_return`: a task whose kind needs its body to return a
  call, but whose body never does. For example, a `@task("shell")` body that
  ends `return f"wc -l {path}"` where `return shell(cmd=f"wc -l {path}")` was
  meant. The check is best-effort. It never flags a `return` of a call or a
  variable.

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
