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

These types influence validation, hashing, artifact handling, and scratch-space
lifecycle.

## The Runtime Is Local-First

Today, Ginkgo's orchestration logic stays in the local Python process. It can
dispatch shell work into Pixi environments or containers, but graph
construction, scheduling, caching decisions, and provenance recording remain
local and explicit.
