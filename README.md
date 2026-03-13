# Ginkgo

Ginkgo is a Python workflow orchestrator for scientific computing, data science, and analytical pipelines.

It combines:

- a lazy expression-tree execution model
- dynamic branching inside task bodies
- content-addressed caching
- per-task Pixi environments
- concurrent scheduling with `--jobs` and `--cores`
- provenance logging and a local CLI

It works well for:

- data preparation and feature pipelines
- exploratory and productionised data science workflows
- bioinformatics and computational biology analyses
- research and scientific computing pipelines
- mixed Python and shell-based workflows

The project is currently implemented through **Phase 7**, with the first hardening slice of **Phase 8** now landed. The shipped system includes the Python DSL, concurrent runtime, cache, Pixi backend, CLI, run manifests, debug tooling, a local run browser, opt-in task retries, expression-graph cycle detection, and v1 cache pruning.

## What Ginkgo Looks Like

```python
import ginkgo
from ginkgo import flow, shell_task, task, file

cfg = ginkgo.config("ginkgo.toml")

@task(env="bioinfo_tools")
def qc(sample_id: str, fastq: file, min_length: int) -> file:
    output = f"results/{sample_id}.filtered.fastq"
    return shell_task(
        cmd=f"seqkit seq -m {min_length} {fastq} > {output}",
        output=output,
        log=f"logs/{sample_id}.log",
    )

@flow
def main():
    return qc(sample_id="sample_a", fastq="data/sample_a.fastq", min_length=8)
```

Run it with:

```bash
ginkgo run workflow.py
```

## Current Feature Set

- `@task()` and `@flow` for declarative workflow construction
- `Expr` / `ExprList` based lazy execution
- `.map()` fan-out with implicit fan-in through `list[...]` task arguments
- dynamic DAG expansion by returning new task expressions from a task body
- `file`, `folder`, and `tmp_dir` marker types
- content-addressed caching under `.ginkgo/cache/`
- opt-in task retries via `@task(retries=n)`
- explicit cycle detection for expression graphs before execution
- concurrent scheduling with OR-Tools CP-SAT resource selection
- Python task execution in worker processes, with a thread-pool fallback when process pools are blocked by the runtime environment
- shell task execution through `shell_task(...)`
- per-task Pixi environments resolved from `envs/<env>/pixi.toml`
- run provenance under `.ginkgo/runs/<run_id>/`
- a local React UI with `ginkgo ui` for browsing runs, a task graph, cache entries, and task logs
- `ginkgo run`, `ginkgo test --dry-run`, `ginkgo cache ls`, `ginkgo cache clear`, `ginkgo cache prune`, `ginkgo debug`, and `ginkgo ui`

## Project Layout

```text
src/ginkgo/
├── __init__.py
├── config.py
├── core/
├── runtime/
├── envs/
└── cli/
```

- `core/`: user-facing DSL types and decorators
- `runtime/`: evaluator, scheduler, cache, codecs, provenance, and worker logic
- `envs/`: execution backends, currently Pixi
- `cli/`: the `ginkgo` command-line interface

## Installation

### Local development with Pixi

This repository is already configured for Pixi-based development.

```bash
pixi install
pixi run test
pixi run typecheck
```

Prerequisite: if your workflows use Pixi-backed task environments, `pixi` must already be installed and available on `PATH`.

Then either:

```bash
pixi run python -m ginkgo.cli --help
```

or, if the package is installed in your active environment:

```bash
ginkgo --help
```

### Editable install

If you prefer a plain Python environment:

```bash
pip install -e .
```

That installs the `ginkgo` console script from `pyproject.toml`.

## Quick Start

Create a minimal workflow:

```python
# workflow.py
from pathlib import Path
from ginkgo import flow, task

@task()
def write_text(message: str, output_path: str) -> str:
    Path(output_path).write_text(message, encoding="utf-8")
    return output_path

@flow
def main():
    return write_text(message="hello", output_path="result.txt")
```

Run it:

```bash
ginkgo run workflow.py
```

Ginkgo will:

- import `workflow.py`
- autodiscover the single `@flow`
- build the expression tree
- evaluate the task graph
- write run history under `.ginkgo/runs/`

The CLI prints the run directory when it finishes:

```text
🌿 ginkgo run workflow.py

📦 Loading workflow...  done (0.01s)
🌱 Building expression tree...  1 tasks

  write_text                   [running]
  write_text                   [succeeded]

⏱ Completed in 0.02s - 1 tasks executed, 0 cached
Run directory: .ginkgo/runs/20260312_145430_affcc6b4
```

## Example Workflows

Ginkgo is domain-agnostic, but bioinformatics is one of its strongest early examples.

A runnable bioinformatics example lives in [examples/bioinfo](examples/bioinfo).

It includes:

- `workflow.py`
- `ginkgo.toml`
- tiny FASTQ inputs and a sample sheet
- a Pixi environment in `envs/bioinfo_tools/pixi.toml`
- a `.tests/` file so you can dry-run it immediately

From that directory:

```bash
cd examples/bioinfo
ginkgo run workflow.py
```

Ginkgo delegates environment materialization to `pixi run`, so the first execution will install the example environment automatically and later runs will reuse Pixi's cached environment.

Or validate it without executing tasks:

```bash
ginkgo test --dry-run
```

## Core Concepts

### `@task()`

`@task()` wraps a Python function so calls return deferred expressions rather than concrete values.

- Task bodies receive resolved argument values when they actually execute.
- Tasks must be defined at module scope.
- Tasks may return ordinary Python values, dynamic Ginkgo expressions, or `shell_task(...)`.

### `@flow`

`@flow` marks a workflow entrypoint.

- In CLI mode, each workflow file must expose exactly one `@flow`.
- CLI autodiscovery calls that flow with no arguments.
- Programmatic use can still pass arguments directly through normal Python calls.

### `.map()`

Fan-out is expressed by partial application followed by `.map()`:

```python
@task()
def process(sample_id: str, multiplier: int) -> str:
    return sample_id * multiplier

results = process(multiplier=2).map(sample_id=["a", "b", "c"])
```

This produces an `ExprList` of independent tasks that Ginkgo can schedule concurrently.

### Path types

Ginkgo provides three marker types:

- `file`: path to a file, validated for existence and hashed by contents
- `folder`: path to a directory, validated and hashed recursively
- `tmp_dir`: scratch directory created by Ginkgo per task execution

### Shell tasks

Shell commands are returned from a normal task body:

```python
@task(env="bioinfo_tools")
def align(sample_id: str, fastq: file) -> file:
    output = f"results/{sample_id}.txt"
    return shell_task(
        cmd=f"cat {fastq} > {output}",
        output=output,
        log=f"logs/{sample_id}.log",
    )
```

The declared `env=` applies to the whole task, including the shell command it returns.

### Dynamic branching

Tasks can return new expressions based on resolved input values:

```python
@task()
def route(value: int):
    if value > 10:
        return high(value=value)
    return low(value=value)
```

This is how Ginkgo supports dynamic DAG growth without a separate branching API.

## Positioning

Ginkgo is best understood as a Python-native workflow engine for reproducible analytical work.

It is a good fit when you want:

- task-level caching based on actual inputs
- dynamic control flow that depends on computed values
- local execution with sensible parallelism
- reproducible per-task environments
- lightweight provenance without adopting a large orchestration platform

Bioinformatics remains a major use case, but the core runtime is designed to be broadly useful across data science and scientific workflows.

## Configuration

`ginkgo.config(path)` loads TOML or YAML into a plain Python dict.

The default first-party convention is `ginkgo.toml`.

CLI overrides are supported:

```bash
ginkgo run workflow.py --config base.toml --config prod.toml
```

Behavior today:

- later config files win
- merging is shallow at the top-level key boundary
- `ginkgo.config("ginkgo.toml")` inside the workflow resolves to the CLI override set during `ginkgo run`

## Environments

The current backend is Pixi.

Named environments are resolved from:

```text
<project_root>/envs/<env_name>/pixi.toml
```

You can also point `env=` at an explicit `pixi.toml` path.

Example:

```python
@task(env="bioinfo_tools")
def qc(...):
    ...
```

At runtime Ginkgo:

- validates that the environment exists
- includes the environment's `pixi.lock` hash in the cache key
- runs shell tasks through `pixi run --manifest-path ...`
- runs Python tasks with `env=` inside the Pixi environment's Python interpreter

You do not need to pre-run `pixi install` for normal workflow execution. Pixi will install or update the environment on first use and cache it for later runs.

## Caching

Task results are cached under `.ginkgo/cache/<cache_key>/`.

The cache key includes:

- task identity
- task version
- resolved input hashes
- per-environment `pixi.lock` hash when `env=` is set

This means:

- unchanged work is skipped automatically
- re-running after a failure naturally resumes from cache
- modifying an input file or environment lockfile invalidates the relevant downstream work

## Provenance and Debugging

Each CLI run writes:

```text
.ginkgo/runs/<run_id>/
├── manifest.yaml
├── params.yaml
├── envs/
└── logs/
```

`manifest.yaml` records task-level fields such as:

- task name
- env
- status
- cached flag
- resolved inputs
- input hashes
- cache key
- log path
- exit code
- output or error

Use:

```bash
ginkgo debug
```

or:

```bash
ginkgo debug <run_id>
```

to inspect the most recent failed run, including the last 50 lines of the failing task log.

## CLI Reference

### Run a workflow

```bash
ginkgo run workflow.py [--config PATH ...] [--jobs N] [--cores N] [--dry-run]
```

### Validate local test workflows

```bash
ginkgo test --dry-run
```

This discovers `.tests/*.py` in the current working directory and requires exactly one `@flow` per file.

### List cache entries

```bash
ginkgo cache ls
```

This renders a Rich table with cache key, task, size, age, and created timestamp.

### Remove one cache entry

```bash
ginkgo cache clear <cache_key>
```

### Prune stale cache entries

```bash
ginkgo cache prune --older-than 30d [--dry-run]
```

### Inspect the latest failed run

```bash
ginkgo debug
```

## Development

Common local commands:

```bash
pixi run test
pixi run lint
pixi run format
pixi run typecheck
pixi run precommit
```

Install local git hooks with:

```bash
pixi run pre-commit install
```

The test suite currently covers:

- expression tree construction
- evaluator behavior
- caching
- failure and resume semantics
- resource-constrained scheduling
- Pixi backend behavior
- Phase 6 CLI and provenance behavior

## Current Limitations

These items are still planned rather than shipped:

- Docker backend
- R task support
- richer cache pruning commands

## Additional Documentation

- [ginkgo-design-doc.md](ginkgo-design-doc.md)
- [implementation-plan.md](implementation-plan.md)
- [examples/bioinfo/README.md](examples/bioinfo/README.md)
