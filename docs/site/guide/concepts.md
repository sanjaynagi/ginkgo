# Core Concepts

A flow describes the tasks and how they connect. The tasks do the work when
Ginkgo runs the workflow. Keep those two jobs apart and the rest of Ginkgo
follows.

## `@task()` Produces Deferred Work

A function decorated with `@task()` doesn't run when you call it. The call
returns an [expression](#glossary), which is a placeholder for the task's
result.

```python
from ginkgo import task


@task()
def clean_sample(sample_id: str) -> str:
    return sample_id.strip().lower()
```

Inside a flow, `clean_sample(sample_id="A01")` doesn't return a string yet. It
returns an `Expr[str]`, which you can pass to other task calls. Each time you
pass an expression to another task, you link the two tasks. The linked tasks
form the workflow's graph.

## `@flow` Builds The Initial Graph

A `@flow` function is where a workflow starts. Its job is to build the first
graph.

```python
from ginkgo import flow


@flow
def main():
    first = clean_sample(sample_id="A01")
    return first
```

The flow body runs as ordinary Python, but the task calls in it don't run yet.
The flow body does no heavy work. It wires tasks together. So Ginkgo can build
the whole first graph, and check it, before any task runs.

## Dynamic Expansion: A Task Can Add Tasks

When a task runs, its inputs have real values. So a task can look at its
inputs and return new task calls. Ginkgo adds the returned tasks to the graph
and runs them. This is called dynamic expansion. It lets the workflow take the
shape the data needs.

```python
from pathlib import Path

from ginkgo import file, flow, task


@task()
def qc(sample: str) -> str:
    return f"{sample}: ok"


@task()
def qc_each(samples_path: file) -> list[str]:
    samples = Path(samples_path).read_text().split()
    return qc().map(sample=samples)


@flow
def main():
    return qc_each(samples_path="data/samples.txt")
```

The flow builds a graph with one task, `qc_each`. Nobody knows how many
samples there are until `qc_each` reads the file. When it runs, it returns one
`qc` task per sample. With three samples in the file, the run ends with four
tasks. `qc_each`'s result is the list of the `qc` results.

[Dynamic Graphs: Returning Tasks From A Task](tasks-and-flows.md#dynamic-graphs-returning-tasks-from-a-task)
covers dynamic expansion in full.

## `.map()` Expresses Fan-Out

[Fan-out](#glossary) means running one task once for each input. Use `.map()`
for it:

```python
@task()
def qc(sample_id: str, min_length: int) -> str:
    return sample_id


results = qc(min_length=8).map(sample_id=["sample_a", "sample_b"])
```

Arguments in the call itself, here `min_length`, stay the same for every run.
Lists given to `.map()` vary: the first run gets the first item of each list,
the second run gets the second item, and so on. Ginkgo can run the resulting
tasks at the same time.

[Fan-Out With `.map()`](tasks-and-flows.md#fan-out-with-map) covers `.map()`,
`.product_map()` for every combination of inputs, and how to name output
paths.

## Path Marker Types Matter

Ginkgo uses a few special annotations for parameters that hold paths:

- `file` and `folder` mark a path the task reads. The path must already exist.
- `Out[file]` and `Out[folder]` mark a path the task writes.
- `tmp_dir` gives the task a scratch folder. Ginkgo creates it before the task
  runs. It deletes it if the task succeeds, and keeps it if the task fails, so
  you can debug.
- `untracked` marks a path that Ginkgo treats as a plain string. Ginkgo
  doesn't hash its contents, and doesn't use it to decide the order of tasks.

Ginkgo hashes the contents of the paths a task reads, so a changed file makes
the task run again. Use `file` and `folder` for those paths, rather than a
plain `str`. [Cache Correctness](caching-and-provenance.md#cache-correctness)
explains how Ginkgo treats a `str` path.

When one task writes a path and another task reads the same path, Ginkgo runs
the writer first.
[Declaring Outputs](outputs.md#dependencies-from-matching-paths)
explains how reads and writes are matched.

## Where Tasks Run

Ginkgo plans, schedules and records each run in the Python process you start.
Python tasks run in separate worker processes on the same computer. Shell,
script and notebook tasks run on the same computer too, in a Pixi environment
or container if they declare one.

An [executor](#glossary) can send chosen tasks to another system instead.
[Remote Execution](remote-execution.md) covers executors.

## Glossary

Branch
: One run of a task inside a fan-out. Each branch has its own inputs, its own
  result and its own cache entry.

Cache key
: A hash that Ginkgo computes from a task's code, its inputs and its
  environment. If a later run computes the same key, Ginkgo reuses the stored
  result instead of running the task.
  [Cache Identity](caching-and-provenance.md#cache-identity) gives the exact
  rule.

Content-addressed
: Stored and looked up by a hash of the content, not by a name or a path.
  Ginkgo's cache is content-addressed, so two files with the same bytes have
  the same hash.

DAG
: A directed acyclic graph. In Ginkgo, it is the graph of tasks. Each link
  runs from a task to a task that uses its result, and no task can depend on
  itself, directly or through other tasks.

Dynamic expansion
: A running task adding new tasks to the graph by returning them.

Executor
: A plugin that runs tasks on another system, such as a Kubernetes cluster or
  GCP Batch. Without an executor, tasks run on your computer.

Expression
: What a task call returns: a placeholder for the task's result. Passing an
  expression to another task call links the two tasks.

Fan-out
: Running one task once for each input, with `.map()` or `.product_map()`.
  Fan-in is the reverse: one task that takes the list of results.

Flow
: A function decorated with `@flow`. It builds the first graph and returns the
  expressions to run.

Plugin
: A separate package that adds features to Ginkgo, such as a task kind, a
  storage scheme or an executor. `ginkgo plugins` lists the installed plugins.

Project
: The directory a workflow runs in. It is the nearest directory that holds a
  `ginkgo.toml`, or the directory you run from if there is none. Ginkgo keeps
  its cache and the provenance database under the project's `.ginkgo/`.

Provenance
: The record of what ran: which tasks, with which inputs, and what they
  produced. Ginkgo keeps it in the provenance database, `.ginkgo/ginkgo.db`.

Task
: A function decorated with `@task()`. It is the unit of work that Ginkgo
  runs, caches and records.

## See Also

- [Tasks and Flows](tasks-and-flows.md): the full authoring model for each
  kind of task, fan-out, and dynamic expansion.
