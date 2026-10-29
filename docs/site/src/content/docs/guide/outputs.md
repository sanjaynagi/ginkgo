---
title: "Declaring Outputs"
---

A task that writes a file or folder declares the path as an `Out[file]` or
`Out[folder]` parameter. Ginkgo then checks that the task wrote it, stores a
copy, restores it on a cache hit, and runs any task that reads it afterwards.

## Reads And Writes

`file` and `folder` mark a path the task reads. The path must exist before the
task runs. `Out[file]` and `Out[folder]` mark a path the task writes. The path
need not exist yet.

```python
from ginkgo import Out, file, folder, task

@task()
def align(reads: file, bam: Out[file], qc_dir: Out[folder]) -> file: ...
```

Don't mark a path the task writes as `file`. The first run would fail, as the
file does not exist yet. Later runs would hash the old output as an input, so
every run would be a cache miss.

`Out[...]` can wrap a container or an optional value, such as
`Out[list[file]]` or `Out[file | None]`. It must be the outermost wrapper:
`list[Out[file]]` is an error.

`Out[file | None]` means the argument may be `None`, and then the task has
nothing to write there. A path that is passed must still be written. For a
shell, script or notebook output that the command may not write, use
[`optional()`](/ginkgo/guide/tasks-and-flows/#outputs-that-may-not-be-written) on its
`output=` entry instead.

## What Happens When The Task Runs

For each `Out[...]` path, Ginkgo:

1. Checks that it is a local path, and that anything already there is the
   declared kind.
2. Removes whatever is there, so the task sees only what this run writes.
3. Creates the parent directory, but not an `Out[folder]` itself.
4. Runs the task.
5. Fails the task, naming the parameter, unless the path now exists and is the
   declared kind.
6. Stores a copy in the artifact store, under `.ginkgo/artifacts/`.

On a cache hit, the task does not run. Ginkgo restores any declared output
that is missing or changed on disk, overwriting a hand edit. An edit that
keeps the file's size and modification time goes unnoticed (see
[What The Cache Cannot See](/ginkgo/guide/caching-and-provenance/#what-the-cache-cannot-see)).

`Out[...]` also works for a remote task (see
[Remote Execution](/ginkgo/guide/remote-execution/)).

## Dependencies From Matching Paths

A task can read another task's output by the same literal path. Ginkgo then
runs the writer first, with no return value or `.output[...]` reference
needed:

```python
@task()
def call_peaks(bam: file, out: Out[file]): ...

@task()
def summarise(peaks: file) -> str: ...

@flow
def main():
    peaks = call_peaks(bam="data/a.bam", out="results/a.bed")
    summary = summarise(peaks="results/a.bed")   # runs after call_peaks
    return [peaks, summary]
```

This also works for a path inside an `Out[folder]`, and for a `folder`
argument that holds a path another task writes.

Ginkgo matches only literal path strings, not a path built at run time from an
upstream value. In that case, pass the producer's return value, or its
`.output[...]`, to the reader.

### Overlapping Paths Are An Error

Ginkgo refuses these before anything runs:

- Two tasks declare the same `Out[...]` path, or one inside the other.
- A task's `Out[...]` path overlaps one of its own `file` or `folder` inputs,
  such as `Out[folder]` at `results/` with an input `results/samples.tsv`.
  Write the output to a path outside every input.

## Inferring The Return Value

A task with at least one `Out[...]` parameter and no return annotation takes
its return value from those parameters. You don't need to write
`return file(output_path)`:

```python
@task()
def align(reads: file, bam: Out[file], qc_dir: Out[folder]):
    ...  # writes bam and qc_dir, with no return statement
```

- With one `Out[...]` parameter, the value is its path, as a `file` or
  `folder`. A container `Out` gives a list or tuple, and an optional one that
  was not passed gives `None`.
- With several, the value is a tuple of their paths, in declaration order. The
  example above returns `tuple[file, folder]`.
- An explicit `-> None` turns inference off.
- The Python function itself must return `None`. To return something else,
  add a return annotation.
- Shell, script and notebook tasks infer their value the same way.

The inferred value is stored, restored and passed on just like an explicit
`-> file` return.

## Selecting One Output

`.output["name"]` passes one named `Out[...]` path of a task to another task.
It works whether the return is explicit or inferred. See
[Tasks And Flows](/ginkgo/guide/tasks-and-flows/#selecting-one-output-by-name-outputname)
for `.output["name"]` and the positional `.output[i]`.
