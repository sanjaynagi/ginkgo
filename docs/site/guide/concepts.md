# Core Concepts

Ginkgo is easiest to understand if you keep one distinction clear:

- flows build graphs
- tasks produce values when the runtime evaluates those graphs

## `@task()` Produces Deferred Work

A function decorated with `@task()` does not execute immediately when you call
it. Instead, it returns an expression node describing the work to be done.

```python
from ginkgo import task


@task()
def clean_sample(sample_id: str) -> str:
    return sample_id.strip().lower()
```

Calling `clean_sample(sample_id="A01")` inside a flow does not return the final
string yet. It returns an `Expr[str]` that can be wired into downstream tasks.

## `@flow` Builds The Initial Graph

A `@flow` function is the workflow entrypoint. Its job is to construct the
initial expression tree.

```python
from ginkgo import flow


@flow
def main():
    first = clean_sample(sample_id="A01")
    return first
```

The flow body executes as ordinary Python, but individual tasks remain deferred.

## Dynamic DAG Expansion Happens Inside Task Bodies

Task bodies receive resolved values at execution time. That means a task can
inspect its inputs and return new expressions conditionally.

This is the mechanism behind Ginkgo's dynamic DAG expansion. It lets workflows
branch based on real data without moving orchestration logic into the flow body.

Because the flow body holds no real computation, Ginkgo can build the whole
graph before any task runs, then validate it and compute cache keys up front.

## `.map()` Expresses Fan-Out

Use partial application plus `.map()` when one task should run independently for
multiple inputs.

```python
@task()
def qc(sample_id: str, min_length: int) -> str:
    return sample_id


results = qc(min_length=8).map(sample_id=["sample_a", "sample_b"])
```

The result is an `ExprList` of independent task expressions that Ginkgo can
schedule concurrently.

Use `.product_map()` when you want the Cartesian product of multiple varying
arguments instead of positional pairing.

```python
@task()
def train(sample_id: str, lr: float) -> str:
    return sample_id


results = train().product_map(sample_id=["sample_a", "sample_b"], lr=[0.01, 0.1])
```

Chained fan-out stays flat: existing branches are the outer loop, and new rows
introduced by `.map()` or `.product_map()` are the inner loop.

## Path Marker Types Matter

Ginkgo uses a few special path-oriented annotations to define runtime behavior:

- `file`
- `folder`
- `tmp_dir`
- `untracked`
- `Out[file]` / `Out[folder]`

These types influence validation, hashing, artifact handling, and scratch-space
lifecycle. `file`/`folder` are still the clearer, type-checked choice, but
leaving a path a task reads as plain `str` is no longer a correctness trap the
way it once was: a `str` value that names an existing *file* and reads as a
path (a separator or a file extension) is content-hashed by default, same as
`file`. What `str` still costs, when the path is really an upstream task's
output, is a dependency edge — writing it as a repeated literal string instead
of passing that task's return value means nothing in the graph records the
order, so the consumer can run before or concurrently with the producer
instead of after it. And a *directory* named by `str` is never auto-hashed
(hashing a whole tree as a side effect of a scalar would be a surprise), so it
still needs `folder` to be content-tracked — see
[Cache Correctness](caching-and-provenance.md#cache-correctness) for the exact
rule. `untracked` is the explicit way to keep a path keyed by its string only,
on purpose.

`file`/`folder` mean *read*: the path must already exist. A path a task is
about to *write* — `output_path`, in the corpus's own naming — is the other
common case, and `Out[...]` is the annotation for it: `Out[file]` and
`Out[folder]` wrap the same `file`/`folder` kinds but mark the parameter as a
write rather than a read. See
[Reads vs. Writes: `file` vs. `Out[file]`](caching-and-provenance.md#reads-vs-writes-file-vs-outfile)
for the full contract.

## The Runtime Is Local-First

Today, Ginkgo's orchestration logic stays in the local Python process. Graph
construction, scheduling, caching decisions, and provenance recording all happen
locally. Python task bodies run in a spawned subprocess worker pool; shell,
script, and notebook tasks run via Pixi environments or containers.

## See Also

- [Tasks and Flows](tasks-and-flows.md) &mdash; the full authoring model for
  Python tasks, shell tasks, and fan-out.
