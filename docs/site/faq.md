# FAQ

Short answers to common questions about Ginkgo. Each answer links to the guide
page that covers its topic in full. If you are new, start with
[Why Ginkgo](motivation.md) and the [Quickstart](getting-started/quickstart.md).

## Getting Started

### What problem does Ginkgo solve?

Ginkgo runs scientific workflows written as plain Python functions, marked with
`@flow` and `@task()`. It runs the tasks in parallel, caches their results, and
records what each run did. The workflow's graph can grow while it runs, as
tasks find out what further work the data needs. Ginkgo is also built to be
run by AI coding agents as well as by people. See [Why Ginkgo](motivation.md).

### How does Ginkgo compare to Snakemake, Nextflow, Prefect, and Dagster?

Snakemake builds its graph from rules and file-name patterns. Nextflow uses its
own language, based on Groovy, and passes data between tasks through channels.
Prefect and Dagster are Python, but running shell commands, scripts or
notebooks in their own environments takes extra work. In Ginkgo you write plain
Python, and each task can name its environment with `env=`, for example a Pixi
environment or a container image.

### What does the usual project layout look like, and how does Ginkgo find my flow?

You can lay out a project however you like. The usual layout has the config
files at the project root, a workflow package, and a tests directory:

```text
my-project/
├── pixi.toml
├── ginkgo.toml
├── workflow/              # the usual name; any name works
│   ├── __init__.py
│   ├── flow.py            # flow definitions and wiring; keep this thin
│   ├── modules/           # tasks
│   ├── envs/              # per-task environment manifests
│   ├── notebooks/         # notebook-task source
│   └── scripts/           # script-task source
└── tests/workflows/       # workflow validation checks
```

Ginkgo creates `results/` and `.ginkgo/` when a workflow runs. When you run
`ginkgo run` with no path, Ginkgo looks for a file named `flow.py` at the
project root or in one of its immediate subdirectories. The directory needs no
`__init__.py`. Ginkgo then picks the flow like this:

1. If it finds one `flow.py`, it runs that one.
2. If it finds several, it prefers `workflow/flow.py`.
3. Otherwise, it stops with an error, and you pass the path yourself.

An explicit path can name any file, anywhere. See [CLI](guide/cli.md) for
several workflows in one project.

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

`@task()` marks a function as a task, and `@flow` marks the entry point of the
workflow. Calling a task inside a flow does not run it. It returns an
[expression](guide/concepts.md), a placeholder that stands for that task call.
Ginkgo uses these placeholders to build the graph of tasks, and runs the task
bodies later. The graph is a directed acyclic graph ([DAG](guide/concepts.md)):
tasks joined by dependency arrows, with no loops. So Ginkgo can check the whole
graph before any task runs. See [Core Concepts](guide/concepts.md).

### How do I pass the output of one task as the input to another?

Pass the expression one task call returns as an argument to another task call.
That makes the second task depend on the first, and it receives the first
task's result when it runs:

```python
@flow
def main():
    raw = download(url="https://example.com/data.csv")
    return summarize(data=raw)  # `raw` is an expression; summarize depends on download
```

The dependency comes from the expression you pass, not from the value it
carries. A plain value creates no dependency. The one exception is a literal
path that matches another task's `Out[...]` path, which Ginkgo links to that
task. See [Tasks and Flows](guide/tasks-and-flows.md).

### How do I fan out over a list of inputs and combine the results?

Call `.map()` on a task with a list for the parameter that varies. Ginkgo runs
one branch of the task for each item. This is a [fan-out](guide/concepts.md).
To combine the results, pass the whole fan-out to one downstream task, which
waits for every branch:

```python
@flow
def main():
    per_sample = analyze.map(sample=["a", "b", "c"])   # one branch per sample
    return combine(results=per_sample)                 # depends on all branches
```

`.product_map()` runs one branch for each combination of several lists. See
[Tasks and Flows](guide/tasks-and-flows.md).

### How can the graph change while a workflow runs?

A task can return new task calls instead of a plain value. Ginkgo adds them to
the running graph, runs them, and then finishes the task with their results. So
a task can read its inputs, decide what further work is needed, and add it:

```python
@task()
def plan(manifest: file) -> list[str]:
    samples = read_samples(manifest)
    return [process(sample=s) for s in samples]  # the graph grows here
```

A Python task can't return `shell(...)`, `script(...)` or `notebook(...)`, and
Ginkgo raises an error if it does. A shell, script or notebook task must return
one. `ginkgo doctor` warns about a shell, script or notebook task that never
returns one. See [Tasks and Flows](guide/tasks-and-flows.md).

## Task Kinds

### What task kinds exist, and when do I use each?

Ginkgo has these task kinds:

- **`python`** (the default): the body is Python and returns a value.
- **`shell`**: the body returns `shell(...)`, for a command line.
- **`script`**: the body returns `script(...)`, for a Python or R script file.
- **`notebook`**: the body returns `notebook(...)`, for a marimo notebook. It
  needs the `ginkgo-notebook` [plugin](guide/concepts.md).
- **`subworkflow`**: the body returns `subworkflow(...)`, to run another
  workflow as its own `ginkgo run`.

Set the kind on the decorator, for example `@task(kind="shell")`. See
[Tasks and Flows](guide/tasks-and-flows.md).

### Are there path-oriented input/output types?

Yes. Ginkgo tracks paths by their annotation:

- `file` and `folder` mark a file or a directory the task reads.
- `Out[file]` and `Out[folder]` mark a path the task writes.
- `tmp_dir` gives the task a scratch directory, created fresh each time the
  task runs.
- `untracked` keeps a path out of the cache key.

Use `file` or `folder` for every path a task reads, and `Out[...]` for every
path it writes. Ginkgo rejects `pathlib.Path` as an annotation. See
[Caching and Provenance](guide/caching-and-provenance.md) for how each one
affects the cache.

### Why did a task in my flow never run?

The task graph is exactly what is reachable from the value your `@flow`
function returns. A call whose result is never used is not part of the run. For
example, a bare `plot(...)` statement written for its side effects. Or a
producer left behind because you passed a literal path to the consumer instead
of the producer's expression. Return the call's result, on its own or inside a
tuple, list or dict, to include it.

Ginkgo reports these calls rather than dropping them quietly. `ginkgo doctor`
emits an `unreachable_task_call` warning. `ginkgo run --dry-run` lists them
under "Dropped (not reachable from the flow return value)". A real run prints a
notice, with the same fix, before it starts.

## Environments

### How does Ginkgo use Pixi to make task environments reproducible?

A task names a Pixi environment with `env=`, and Ginkgo installs it with
`pixi install` the first time a task needs it. The task then runs inside that
environment, so every run uses the same packages. The environment is part of
the task's cache key. See [Environments](guide/environments.md), and
[Caching and Provenance](guide/caching-and-provenance.md) for which environment
changes rerun a task.

### What environment file types can Ginkgo handle?

Ginkgo reads Pixi manifests (`pixi.toml`, or a `pyproject.toml` with a
`[tool.pixi]` table) and Conda `environment.yml` files. It converts a Conda file
to a Pixi environment, so Pixi must be on your `PATH`. See
[Environments](guide/environments.md#conda-environment-files).

### How does container-backed execution work, and when should I use it?

Set `env="docker://<image>"` on a shell, script or notebook task. The container
sees the project root, and every `file`, `folder` and output the task declares.
Use a container when a tool is only published as an image, when you need more
isolation than a Pixi environment gives, or to pin an exact image. See
[Environments](guide/environments.md#container-environments).

### Can different tasks in one workflow run in different environments?

Yes. Each shell, script or notebook task declares its own `env=`, and Ginkgo
prepares each environment separately. A task without `env=` runs in the
environment the `ginkgo` CLI runs from, and so does every Python task.

```python
@task(kind="shell", env="bioinfo_tools")                   # named Pixi env
def align(...): ...

@task(kind="shell", env="envs/genomics/environment.yml")   # Conda file
def call_variants(...): ...

@task(kind="shell", env="docker://ubuntu:24.04")           # container image
def count_reads(...): ...
```

## Scheduling And Resources

### How does Ginkgo decide which task runs next?

A task becomes ready when every task it depends on has finished. When several
tasks are ready, Ginkgo starts as many as the run's budgets fit. A task's
`priority` only breaks ties between those choices. See
[Resources And Scheduling](guide/resources.md#priority).

### How does Ginkgo avoid oversubscribing CPU and memory?

Each task declares its `threads` and `memory`, and Ginkgo starts a task only
when the run's budgets have room for it. The total `threads` of running tasks
never exceeds `--cores`. Ginkgo limits memory only when you pass `--memory`.
Ginkgo also measures what each task actually uses, but it never stops a task
for using more. See [Resources And Scheduling](guide/resources.md).

### How do I cap total parallelism for a run?

Pass `--jobs`, `--cores` and `--memory` to `ginkgo run`:

```bash
# Run at most 4 tasks at once, within an 8-core, 32 GiB budget
ginkgo run flow.py --jobs 4 --cores 8 --memory 32
```

See [Limiting What A Run Uses](guide/resources.md#limiting-what-a-run-uses)
for what each flag caps, and its default.

## Caching

### How does Ginkgo cache results, and what does "content-addressed" mean?

Ginkgo stores every task result under `.ginkgo/cache/`. The key for a result
comes from the task and the content of its inputs, not from file names or
dates. That is what [content-addressed](guide/concepts.md) means. So moving or
renaming a file changes nothing, but changing its bytes gives a new key, and
the task runs again. See [Caching and Provenance](guide/caching-and-provenance.md).

### What goes into a task's cache key?

The key combines the task's name, its `version`, its source code, its inputs,
and its environment. See [Caching and Provenance](guide/caching-and-provenance.md).

### When I edit a helper module that a task imports, does the cache notice?

Yes. A task's source hash covers the task function and the local modules it
imports, so editing any of them reruns the task. Ginkgo finds these modules by
reading the import statements. It can't see a dynamic import, or a data file
the code opens. Bump `version=` on the task when one of those changes.

### Does changing a task's threads or memory invalidate its cache?

No. Ginkgo leaves the resource and retry arguments of `@task(...)` out of the
source hash, so you can retune them freely. Other changes to the decorator,
such as `kind=`, `env=` or `version=`, still rerun the task. See
[Resources And Scheduling](guide/resources.md).

### Does changing an environment variable re-run a task?

Only when the variable reaches the task as an argument. Read it in the flow
body, which runs while the graph is built, and pass the value in:
`call_peaks(..., threshold=float(os.environ.get("PEAK_THRESHOLD", "0.05")))`.
The cache can't see a variable read inside the task body. See
[Caching and Provenance](guide/caching-and-provenance.md).

### Does editing a file a task opens by its own path re-run it?

No. A task's key covers its arguments, its source and `version=`. So the cache
can't see a file the task opens itself, such as `open("data/genes.tsv")`. Nor
can it see a script that a shell command runs, such as `bash scripts/trim.sh`.
Pass the file in as a `file` argument instead. Declare a file another task
writes as that task's `Out[file]`.

### How do warm runs skip work?

Ginkgo checks the cache before it starts a task. On a hit, it uses the stored
result and does not run the task. It hashes a file again only when the file's
size or modification time has changed. `ginkgo run --trust-mtimes` skips content
hashing and only checks that output files exist. This is faster, but it trusts
the working tree instead of checking the bytes.

### Can I run two workflows at once in one project?

Yes. Each run keeps its own record, and the provenance database and the cache
take their writes in turn. A run only sees cache entries that already exist,
though, so both runs may compute the same task.

### How do I force a task to re-run?

Pass `--rerun <task>` to `ginkgo run`. Ginkgo then runs that task again instead
of using its cached result, and reruns tasks downstream only if its result
changes. To change the cache key itself, bump the task's `version=`. See
[CLI](guide/cli.md).

### How do I clear or prune the cache?

```bash
ginkgo cache ls                                  # list entries with size and age
ginkgo cache clear <cache-key>                   # remove one entry
ginkgo cache prune --older-than 30d --dry-run    # preview an age-based prune
ginkgo cache prune --max-size 5GB                # keep the cache under a size budget
ginkgo cache prune --max-entries 100             # keep at most N entries
```

`ginkgo cache clear` needs the full 64-character key, which `ginkgo cache ls`
lists. `ginkgo cache prune --max-entries 0` empties the cache. See
[Caching and Provenance](guide/caching-and-provenance.md).

### Where does the artifact store fit in for file and folder outputs?

Ginkgo copies each `file` and `folder` output into an artifact store under
`.ginkgo/artifacts/`. On a cache hit, it restores a missing or changed output
from that store, without running the task. So the store, not the output path,
holds the lasting copy. See
[Caching and Provenance](guide/caching-and-provenance.md).

## Passing Values Between Tasks

### What can I pass between tasks?

You can pass plain Python values (numbers, strings, lists and dicts), NumPy
arrays, pandas DataFrames, and most other objects that `pickle` can save.
Define a class at the top level of a module, because a class or `namedtuple`
made inside a function can't pass between tasks. Ginkgo stores large values on
disk for you. It stops with an error naming the type of a value it can't save.

### How do I hand a DataFrame or other Python value to a notebook, script, or shell task?

A notebook or script task receives its arguments as text on the command line,
so it can't take a DataFrame. Ginkgo stops with an error naming the argument.
Put a small Python task in between that writes the value to a file and returns
it as a `file`. A shell task's body is Python, so it can write the file itself
before it builds its command. See [Tasks and Flows](guide/tasks-and-flows.md).

### How do I find out which task produced a file?

`ginkgo why <path>` names the task and run that produced a file, the cache entry
that holds its bytes, and the task's inputs. From Python, `Query.why` in
`ginkgo.query` gives the same answer. It can only name a file Ginkgo tracked, so
annotate a task's return `-> file`, or write the file through an `Out[file]`
parameter. See [Querying Provenance](guide/querying-provenance.md).

## Reports

### What's in the HTML report?

`ginkgo report` comes from the `ginkgo-report` plugin. It turns a finished run
into an HTML report with the run summary, the task graph, each task's status and
timing, failures with their log tails, and links to rendered notebooks. Pass
`--single-file` for one self-contained HTML file. See [Reports](guide/reports.md).

## Failures And Retries

### When a task fails, does the whole run stop?

By default, yes. Ginkgo starts no new tasks, but lets running tasks finish, and
the run ends as `failed`. Tasks that never started are recorded `skipped`. Use
`on_failure="ignore"` or `--keep-going` to let the rest of the run carry on. See
[When a Failure Should Not Stop the Run](guide/resources.md#when-a-failure-should-not-stop-the-run).

Ctrl-C stops everything, whatever the failure policy. Ginkgo ends running
commands and cancels remote jobs.

### How do retries work, and which failures are retried?

Set `retries=N` on the task. With no other setting, every failure is retried.
`retry_on` and `retry_on_exit_codes` narrow which failures are retried, and
`retry_backoff` adds a wait. Each retry starts the task again from scratch, with
fresh scratch directories. See
[Retry Policies](guide/resources.md#retry-policies).

### How do I debug a failed task?

Start with `ginkgo debug`. These commands help:

- `ginkgo debug [RUN_ID]` shows each failed task with its log tail. It defaults
  to the latest run, and `--json` gives the same for a script.
- `ginkgo runs show [RUN_ID]` shows every task in the run. Add `--json` for each
  task's attempts, exit code, failure and log paths.
- The full logs are in `.ginkgo/runs/<run_id>/logs/`. A task's error quotes at
  most the last 4096 characters of each output stream, so the log is the only
  place with all of it.

See [CLI](guide/cli.md).

## Remote Execution

### How do I run a task remotely?

Set `remote=True` on the task to send it to the run's default executor, which
`ginkgo run --executor <name>` names. Set `executor="name"` to send it to one
[executor](guide/concepts.md) whatever the default is. A `gpu=N` task goes to
the default executor only when the local `--gpus` budget can't cover it.
Executors are Kubernetes or GCP Batch. GKE, EKS and OKE are all clusters that
the one Kubernetes executor talks to. See
[Remote Execution](guide/remote-execution.md).

### How do remote inputs reach a task?

A remote input, such as `remote_file("gs://my-bucket/sample.bam")`, reaches the
task in one of three modes. In `direct` mode the worker downloads it, and in
`stage` mode the driver downloads it and passes it on. In `fuse` mode the worker
mounts the bucket and streams reads. If a mount fails, Ginkgo downloads the
input instead and records why. See
[Remote Inputs](guide/remote-execution.md#remote-inputs).

### What object-store schemes are supported?

`s3://`, `gs://` and `oci://`, each from its own plugin. Any other scheme is an
error. See [Remote Execution](guide/remote-execution.md#the-artifact-store).

## Provenance

### What does Ginkgo record for each run, and where does it live?

Ginkgo records each run in the [provenance](guide/concepts.md) database,
`.ginkgo/ginkgo.db`, one per project. It holds every run's tasks, attempts,
inputs, outputs and dependencies. `ginkgo runs show`, `ginkgo debug` and
`ginkgo report` read it, even while a run is still going. Each run also gets a
directory, `.ginkgo/runs/<run_id>/`, for its logs and rendered notebooks. See
[Caching and Provenance](guide/caching-and-provenance.md).

Set `GINKGO_DB=<path>` to put the database somewhere else. Do this if `.ginkgo`
is on a network filesystem, because SQLite locking is unreliable over NFS,
Lustre, SMB and FUSE. Ginkgo warns once when it notices.

Ginkgo refuses a database written with a different schema version, and the
error names the project.

```{warning}
Deleting `.ginkgo/` removes the run history and the cache. Back up
`.ginkgo/ginkgo.db` as you would `.git`.
```

## Configuration And Secrets

### How do I configure a workflow, and do config values feed into cache keys?

A config value changes a task's cache key only if it reaches the task as an
argument. Changing a config entry that no task takes as an argument reruns
nothing.

Load a config file in the workflow module with `ginkgo.config("ginkgo.toml")`.
It reads TOML or YAML, and returns a nested dict you use to shape the graph. A
mistyped key raises a `KeyError` that names the file, the section and the keys
that are there:

```text
'min_lenght' is not a key in [qc] of ginkgo.toml. Available: min_length, min_quality, threads
```

`ginkgo run --config <path>` loads other config files instead of the project's
own. With several, Ginkgo merges them at the top level.

### How do I reference a secret without leaking it into logs, provenance, or the report?

Use `ginkgo.secret("NAME", backend="env")`, and pass what it returns as a task
argument. It returns a `SecretRef`, which Ginkgo resolves only when the task
runs. The backends are `env`, for environment variables, and a dotenv backend.
Ginkgo checks declared secrets before the run starts.

The task receives the real value. Provenance and cache records store only a
placeholder, such as `<secret:env:NAME>`, never the value. Ginkgo replaces any
secret value in a task's output with `[REDACTED]` before it logs it, and cleans
error messages the same way. So the secret does not reach the logs, the
provenance database or the report. This holds for every task kind. Ginkgo
redacts a shell, script or notebook command's output line by line.

Redaction covers what Ginkgo records about a task, not what the task writes. A
secret a task writes into an output file is stored in that file, and in its
cached copy under `.ginkgo/`, like any other content.

## Sub-Workflows And Composition

### How do I run one workflow inside another?

Write a task with `kind="subworkflow"` that returns `subworkflow(...)`, naming
the child workflow's file:

```python
from ginkgo import subworkflow, task


@task(kind="subworkflow")
def run_child(sample: str):
    return subworkflow(
        "child/flow.py",
        params={"sample": sample},
        config=["overrides.toml"],
    )
```

Ginkgo runs the child as its own `ginkgo run`, with `params` and `config` passed
as config. A child can run subworkflows of its own, up to 8 levels deep by
default. See
[Tasks and Flows](guide/tasks-and-flows.md).

### How is a child run linked to the parent's provenance?

The child is a run of its own, with its own run ID. The calling task returns a
`SubWorkflowResult` with the child's `run_id` and `status`, and the parent's
record keeps the child's run ID. Read what the child did with
`ginkgo runs show <child_run_id>`.
