---
title: "FAQ"
---

Short answers to common questions about Ginkgo. Each answer links to the guide
page that covers its topic in full. If you are new, start with
[Why Ginkgo](/ginkgo/motivation/) and the [Quickstart](/ginkgo/getting-started/quickstart/).

## Getting Started

### What problem does Ginkgo solve?

Ginkgo runs scientific workflows written as plain Python functions. It runs
tasks in parallel, caches their results, records what each run did, and lets
the graph grow as tasks discover more work. See [Why Ginkgo](/ginkgo/motivation/).

### How does Ginkgo compare to Snakemake, Nextflow, Prefect, and Dagster?

Snakemake builds its graph from rules and file-name patterns, and Nextflow uses
its own Groovy-based language. Prefect and Dagster are Python, but running
tools in their own environments takes extra work. In Ginkgo you write plain
Python, and each task can name a Pixi environment or container with `env=`.

### What does the usual project layout look like, and how does Ginkgo find my flow?

`ginkgo init` creates a `workflow/` package with `flow.py`, task modules and
environments, shown in [Keep Flows Thin](/ginkgo/guide/tasks-and-flows/#keep-flows-thin).
With no path, `ginkgo run` looks for a `flow.py` at the project root or one
level down, and prefers `workflow/flow.py` if it finds several. An explicit
path can name any file. See [CLI](/ginkgo/guide/cli/).

### What's the smallest possible workflow?

A single task, and a flow that returns a call to it:

```python
from ginkgo import Out, file, flow, task


@task()
def write_text(message: str, output_path: Out[file]):
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(message)


@flow
def main():
    return write_text(message="hello from ginkgo", output_path="hello.txt")
```

```bash
ginkgo run flow.py
```

## Writing Workflows

### How do `@flow` and `@task` fit together, and when does my code run?

Calling a task inside a `@flow` doesn't run it. It returns an
[expression](/ginkgo/guide/concepts/#glossary) that stands for the call, and Ginkgo
builds the graph from these before it runs any task body. See
[Core Concepts](/ginkgo/guide/concepts/).

### How do I pass the output of one task as the input to another?

Pass the expression one task call returns as an argument to another. The second
task then waits for the first and receives its result:

```python
@flow
def main():
    raw = download(url="https://example.com/data.csv")
    return summarize(data=raw)
```

See [Tasks and Flows](/ginkgo/guide/tasks-and-flows/).

### How do I fan out over a list of inputs and combine the results?

Call `.map()` with a list for each argument that varies, and pass the result to
one downstream task, which waits for every branch:

```python
@flow
def main():
    per_sample = analyze().map(sample=["a", "b", "c"])
    return combine(results=per_sample)
```

`.product_map()` runs every combination instead. See
[Fan-Out](/ginkgo/guide/tasks-and-flows/#fan-out-with-map).

### How can the graph change while a workflow runs?

A task can return new task calls instead of a plain value. Ginkgo adds them to
the graph, runs them, and finishes the task with their results. See
[Dynamic Graphs](/ginkgo/guide/tasks-and-flows/#dynamic-graphs-returning-tasks-from-a-task).

## Task Kinds

### What task kinds exist, and when do I use each?

Python (the default), `shell` for a command line, `script` for a Python or R
script, `notebook` for a marimo notebook, and `subworkflow` for another
workflow. Set the kind on the decorator, as in `@task("shell")`. See
[Task Kinds](/ginkgo/guide/tasks-and-flows/#task-kinds).

### Are there path-oriented input/output types?

Yes. Annotate every path a task reads as `file` or `folder`, and every path it
writes as `Out[file]` or `Out[folder]`. `tmp_dir` gives a fresh scratch
directory, and `untracked` keeps a path out of the cache key. See
[Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

### Why did a task in my flow never run?

Only calls reachable from the value your `@flow` returns are part of the run.
Return the call's result, on its own or inside a tuple, list or dict.
`ginkgo doctor` and `ginkgo run --dry-run` list calls that were dropped.

## Environments

### How does Ginkgo use Pixi to make task environments reproducible?

A task names a Pixi environment with `env=`, and Ginkgo installs it the first
time a task needs it. The environment is part of the task's cache key. See
[Environments](/ginkgo/guide/environments/).

### What environment file types can Ginkgo handle?

Pixi manifests (`pixi.toml`, or `pyproject.toml` with `[tool.pixi]`) and Conda
`environment.yml` files, which Ginkgo converts with Pixi. See
[Conda Environment Files](/ginkgo/guide/environments/#conda-environment-files).

### How does container-backed execution work, and when should I use it?

Set `env="docker://<image>"` on a shell, script or notebook task. Use it when a
tool is only published as an image, or to pin an exact image. See
[Container Environments](/ginkgo/guide/environments/#container-environments).

### Can different tasks in one workflow run in different environments?

Yes. Each shell, script or notebook task declares its own `env=`. Python tasks,
and tasks without `env=`, run in the environment the `ginkgo` command runs
from. See [Environments](/ginkgo/guide/environments/).

## Scheduling And Resources

### How does Ginkgo decide which task runs next?

A task becomes ready when every task it depends on has finished. Ginkgo starts
as many ready tasks as the run's budgets fit, and `priority` breaks ties. See
[Resources And Scheduling](/ginkgo/guide/resources/#priority).

### How does Ginkgo avoid oversubscribing CPU and memory?

Ginkgo starts a task only when the run's budgets have room for its `threads`
and `memory`. It limits memory only when you pass `--memory`, and never stops a
task for using more than it declared. See
[Resources And Scheduling](/ginkgo/guide/resources/).

### How do I cap total parallelism for a run?

Pass `--jobs`, `--cores` and `--memory` to `ginkgo run`, as in
`ginkgo run flow.py --jobs 4 --cores 8 --memory 32`. See
[Limiting What A Run Uses](/ginkgo/guide/resources/#limiting-what-a-run-uses).

## Caching

### How does Ginkgo cache results, and what does "content-addressed" mean?

A result's cache key comes from the task and the content of its inputs, not
from file names or dates. So renaming a file changes nothing, but changing its
bytes reruns the task. See
[Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

### What goes into a task's cache key?

The task's name, its `version`, its source code, its inputs and its
environment. See [Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

### When I edit a helper module that a task imports, does the cache notice?

Yes, for local modules it imports by an import statement. Bump `version=` on
the task when a dynamic import or a data file the code opens changes.

### Does changing a task's threads or memory invalidate its cache?

No. Resource and retry arguments are left out of the cache key. Other changes,
such as `kind=`, `env=` or `version=`, rerun the task. See
[Resources And Scheduling](/ginkgo/guide/resources/).

### Does changing an environment variable re-run a task?

Only when it reaches the task as an argument. Read it in the flow and pass the
value in. See [Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

### Does editing a file a task opens by its own path re-run it?

No. The cache can't see a file the task opens itself, or a script a shell
command runs. Pass the file in as a `file` argument instead.

### How do warm runs skip work?

Ginkgo checks the cache before it starts a task, and hashes a file again only
when its size or modification time changed. `--trust-mtimes` skips content
hashing and only checks that outputs exist.

### Can I run two workflows at once in one project?

Yes. Each run keeps its own record, but both runs may compute the same task.

### How do I force a task to re-run?

Pass `--rerun <task>` to `ginkgo run`. To change the task's cache key itself,
bump its `version=`. See [CLI](/ginkgo/guide/cli/).

### How do I clear or prune the cache?

```bash
ginkgo cache ls                                  # list entries with size and age
ginkgo cache clear <cache-key>                   # remove one entry
ginkgo cache prune --older-than 30d --dry-run    # preview an age-based prune
ginkgo cache prune --max-size 5GB                # keep the cache under a size budget
```

See [Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

### Where does the artifact store fit in for file and folder outputs?

Ginkgo copies each `file` and `folder` output into `.ginkgo/artifacts/`. On a
cache hit, it restores a missing or changed output from there without running
the task. See [Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

## Passing Values Between Tasks

### What can I pass between tasks?

Plain Python values, NumPy arrays, pandas DataFrames, and most objects `pickle`
can save. Define classes at module top level, because one made inside a
function can't pass between tasks.

### How do I hand a DataFrame or other Python value to a notebook, script, or shell task?

Add a small Python task that writes the value to a file and returns it as a
`file`, because notebook and script tasks receive their arguments as text. See
[Staging A Python Value For A Notebook](/ginkgo/guide/tasks-and-flows/#staging-a-python-value-for-a-notebook).

### How do I find out which task produced a file?

`ginkgo why <path>` names the task and run that produced a file, and the task's
inputs. It works only for files Ginkgo tracked, as a `file` return or an
`Out[file]`. See [Querying Provenance](/ginkgo/guide/querying-provenance/).

## Reports

### What's in the HTML report?

`ginkgo report`, from the `ginkgo-report` plugin, shows a finished run's
summary, task graph, task status and timing, failures and rendered notebooks.
See [Reports](/ginkgo/guide/reports/).

## Failures And Retries

### When a task fails, does the whole run stop?

By default, Ginkgo starts no new tasks, lets running tasks finish, and marks
the run `failed`. Use `on_failure="ignore"` or `--keep-going` to carry on. See
[When a Failure Should Not Stop the Run](/ginkgo/guide/resources/#when-a-failure-should-not-stop-the-run).

### How do retries work, and which failures are retried?

Set `retries=N` on the task to retry every failure. `retry_on` and
`retry_on_exit_codes` narrow which failures are retried. See
[Retry Policies](/ginkgo/guide/resources/#retry-policies).

### How do I debug a failed task?

Run `ginkgo debug` to see each failed task of the latest run with its log tail.
The full logs are in `.ginkgo/runs/<run_id>/logs/`. See [CLI](/ginkgo/guide/cli/).

## Remote Execution

### How do I run a task remotely?

Set `remote=True` to send a task to the executor `ginkgo run --executor <name>`
names, or `executor="name"` to pick one. Executors run on Kubernetes (including
GKE, EKS and OKE) or GCP Batch. See
[Remote Execution](/ginkgo/guide/remote-execution/).

### How do remote inputs reach a task?

In `direct` mode the worker downloads the input, in `stage` mode the driver
does, and in `fuse` mode the worker mounts the bucket. See
[Remote Inputs](/ginkgo/guide/remote-execution/#remote-inputs).

### What object-store schemes are supported?

`s3://`, `gs://` and `oci://`, each from its own plugin. See
[The Artifact Store](/ginkgo/guide/remote-execution/#the-artifact-store).

## Provenance

### What does Ginkgo record for each run, and where does it live?

Each run's tasks, attempts, inputs and outputs go in the
[provenance](/ginkgo/guide/concepts/#glossary) database, `.ginkgo/ginkgo.db`, and its
logs in `.ginkgo/runs/<run_id>/`. If `.ginkgo` is on a network filesystem, set
`GINKGO_DB=<path>` to put the database elsewhere. See
[Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

:::caution
Deleting `.ginkgo/` removes the run history and the cache. Back up
`.ginkgo/ginkgo.db` as you would `.git`.
:::

## Configuration And Secrets

### How do I configure a workflow, and do config values feed into cache keys?

Load a TOML or YAML file with `ginkgo.config("ginkgo.toml")`, and use the dict
it returns to shape the graph. A config value changes a cache key only if it
reaches a task as an argument. `ginkgo run --config <path>` loads other files
instead.

### How do I reference a secret without leaking it into logs, provenance, or the report?

Pass `ginkgo.secret("NAME", backend="env")` as a task argument. The task gets
the real value, but Ginkgo records only a placeholder and redacts the value
from task output and errors. A secret the task writes into an output file is
not redacted.

## Sub-Workflows And Composition

### How do I run one workflow inside another?

Write a task with `kind="subworkflow"` that returns
`subworkflow("child/flow.py", params={...})`. Ginkgo runs the child as its own
`ginkgo run`. See [Subworkflow Tasks](/ginkgo/guide/tasks-and-flows/#subworkflow-tasks).

### How is a child run linked to the parent's provenance?

The child is a run with its own run ID, which the calling task returns in a
`SubWorkflowResult`. Read it with `ginkgo runs show <child_run_id>`.
