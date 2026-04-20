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

## Declaring Resource Requirements

Every `@task` can declare the resources it needs. The scheduler respects
these declarations against the `--jobs`, `--cores`, and `--memory` budgets.

```python
@task(threads=4, memory="8Gi")
def align_reads(sample_id: str, reads: file) -> file:
    ...

@task(kind="shell", gpu=1, remote=True, memory="16Gi")
def train_model(dataset: folder) -> file:
    ...
```

- `threads=N` declares the CPU footprint. Tasks that read `threads` as a
  function parameter receive it automatically; shell tasks also see
  `GINKGO_THREADS` in their subprocess environment. Set
  `export_thread_env=True` to additionally export `OMP_NUM_THREADS` and
  related BLAS/OpenMP variables.
- `memory="8Gi"` declares the memory footprint. Format is Kubernetes-style
  (`512Mi`, `4Gi`, `16Gi`). Remote executors map this to pod resource
  requests.
- `gpu=N` and `remote=True` dispatch the task to the configured remote
  executor. Tasks with `gpu > 0` are implicitly remote.

## Priority And Retry Policies

Two optional decorator parameters control scheduling and resilience:

```python
# Highest-priority tasks run first when multiple are ready at once.
@task(priority=10)
def critical_path_step(...): ...

# Retry up to 3 times, only on IOError, with exponential backoff.
@task(retries=3, retry_on=IOError, retry_backoff=1.0)
def network_fetch(...): ...

# Retry only specific exit codes on shell tasks.
@task(kind="shell", retries=2, retry_on_exit_codes=(137,))  # OOM kills
def memory_intensive_step(...): ...
```

`priority` is a strict tiebreaker: it never lets a higher-priority task
block a larger set of lower-priority tasks from running. Retries with a
non-zero `retry_backoff` pause the task in a `waiting_retry` state for the
computed delay (capped at `retry_backoff_max`) before the scheduler picks
it up again.

## When To Split A Task

Split tasks when you need:

- a reusable cache boundary
- a separate execution environment
- clearer provenance
- cleaner failure isolation

Keep logic together when splitting it would only add indirection without adding
clarity or a real runtime boundary.
