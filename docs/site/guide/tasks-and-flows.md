# Tasks And Flows

This guide covers the core authoring model: how to define tasks, structure a
flow, and decide what belongs in each layer.

## Keep Flows Thin

The canonical project layout expects workflow wiring in `workflow.py` and task
implementations in modules owned by the project package.

```text
<project-root>/
├── pixi.toml
├── ginkgo.toml
├── <project_package>/
│   ├── workflow.py
│   ├── modules/
│   └── envs/
```

The flow should primarily:

- load config or top-level metadata
- compose task calls
- define fan-out and fan-in structure
- return the final expression or expressions

Heavy transformation logic belongs in task bodies, not in the flow.

## Python Tasks

Use `@task()` for Python code that should execute in the scheduler's own Python
environment.

```python
from pathlib import Path

from ginkgo import file, task


@task()
def write_summary(rows: list[str]) -> file:
    output = Path("results/summary.txt")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return file(str(output))
```

Key points:

- task functions must live at module scope
- inputs and outputs should be explicit and typed
- top-level source changes participate in cache invalidation
- return ordinary Python values or supported Ginkgo marker types

## Shell Tasks

Use `@task(kind="shell")` when the real work is an external command.

```python
from ginkgo import file, shell, task


@task(kind="shell", env="bioinfo_tools")
def fastq_stats(sample_id: str, fastq: file) -> file:
    output = f"results/qc/{sample_id}.stats.tsv"
    return shell(
        cmd=f"seqkit stats -T {fastq} > {output}",
        output=output,
        log=f"logs/stats_{sample_id}.log",
    )
```

The wrapper function runs locally on the scheduler. It builds the concrete shell
payload from resolved values, and only that payload is executed in the foreign
environment.

## Partial Application And Fan-Out

You do not need to supply every required argument immediately. If you provide a
subset, Ginkgo returns a `PartialCall` that can be mapped later.

```python
filtered = filter_fastq(min_length=8).map(
    sample_id=samples["sample_id"],
    fastq=samples["fastq"],
)
```

This pattern keeps one fixed parameter set while varying the per-sample inputs.

Use `.product_map()` when the varying arguments should form a parameter grid
rather than being zipped by position.

```python
models = train().product_map(
    sample_id=["sample_a", "sample_b"],
    lr=[0.01, 0.1],
)
```

You can also chain fan-out calls. Chaining always returns a flat `ExprList`,
with existing branches treated as the outer loop and newly introduced rows as
the inner loop.

## Returning Expressions From Tasks

Tasks can return:

- a concrete Python value
- a `shell(...)` payload
- another expression
- an `ExprList`
- nested containers containing expressions

That gives you controlled dynamic graph expansion while keeping the authoring
model small.

## When To Split A Task

Split tasks when you need:

- a reusable cache boundary
- a separate execution environment
- clearer provenance
- cleaner failure isolation

Keep logic together when splitting it would only add indirection without adding
clarity or a real runtime boundary.
