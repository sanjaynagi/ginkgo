# Declaring Outputs

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

Don't mark a path the task writes as `file`. A first run would fail, because
the file does not exist yet. A later run would hash the previous run's output
as an input, so every run would look like a cache miss.

`Out[...]` can wrap a container or an optional value, such as
`Out[list[file]]` or `Out[file | None]`. It must be the outermost wrapper:
`list[Out[file]]` is an error.

`Out[file | None]` means the argument may be `None`, and then the task has
nothing to write there. A path that is passed must still be written. For a
shell, script or notebook output that the command may not write, use
[`optional()`](tasks-and-flows.md#outputs-that-may-not-be-written) on its
`output=` entry instead.

## What Happens When The Task Runs

Ginkgo handles each `Out[...]` path in this order:

1. Before the task runs, Ginkgo checks the path. It must be a local path. If
   something already exists there, it must be the declared kind, so an
   `Out[file]` that points at a directory is an error.
2. Ginkgo removes whatever is at the path, a file or a whole folder. The task
   never sees its own output from an earlier run, so a task that lists its
   `Out[folder]` sees only the files this run wrote.
3. Ginkgo creates the parent directory. For an `Out[folder]`, it creates only
   the parent, not the folder itself, because tools differ on whether they
   want the folder to exist already.
4. The task runs.
5. After the task runs, the path must exist and be the declared kind.
   Otherwise the task fails, naming the parameter and the path.

Ginkgo then stores a copy of the output in the artifact store, under
`.ginkgo/artifacts/`.

On a cache hit, the task does not run. If a declared output is missing or has
changed on disk, Ginkgo restores it from the artifact store. A hand edit to
the output is overwritten. Ginkgo judges "changed" by the file's size and
modification time, so an edit that keeps both goes unnoticed (see
[What The Cache Cannot See](caching-and-provenance.md#what-the-cache-cannot-see)).

An `Out[...]` path also works for a remote task. See
[Remote Execution](remote-execution.md) for how outputs come back from a
worker.

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

Ginkgo also adds the dependency when the reader's path is inside a folder an
`Out[folder]` writes. It adds it too when a `folder` argument names a
directory that holds a path another task writes.

Ginkgo matches only paths written as literal strings. A path built at run time
from an upstream value is not matched. Pass the producer's return value, or
its `.output[...]`, to the reader instead.

### Overlapping Paths Are An Error

Ginkgo refuses these before anything runs:

- Two tasks declare the same `Out[...]` path, or one inside the other.
- A task's `Out[...]` path overlaps one of its own `file` or `folder` inputs.
  For example, `Out[folder]` at `results/` with an input
  `results/samples.tsv`. Running it would overwrite the input, and the
  damaged file would be cached as the input of a successful run. Write the
  output to a path outside every input.

Ginkgo does not check a plain `str` input for overlap, because it may be a
label that happens to look like a path.

## Inferring The Return Value

A task with at least one `Out[...]` parameter and no return annotation takes
its return value from those parameters. You don't need to write
`return file(output_path)`:

```python
@task()
def align(reads: file, bam: Out[file], qc_dir: Out[folder]):
    ...  # writes bam and qc_dir, with no return statement
```

- With one `Out[...]` parameter, the value is that parameter's path, as a
  `file` or `folder`. A container `Out` gives a list or tuple of them, and an
  optional one that was not passed gives `None`.
- With several, the value is a tuple of their paths, in the order the
  parameters are declared. The example above returns `tuple[file, folder]`.
- An explicit `-> None` turns inference off.
- A Python task that infers its return must return `None`. Returning anything
  else is an error. Add a return annotation if the task needs to return
  something else.
- A shell, script or notebook task infers its value the same way, from its
  `Out[...]` paths, once the command has run.

Ginkgo treats the inferred value as it treats an explicit `-> file` return. It
hashes and stores the outputs, restores them on a cache hit, and passes them
to later tasks.

## Selecting One Output

`.output["name"]` passes one named `Out[...]` path of a task to another task.
It works whether the return is explicit or inferred. See
[Tasks And Flows](tasks-and-flows.md#selecting-one-output-by-name-outputname)
for `.output["name"]` and the positional `.output[i]`.
