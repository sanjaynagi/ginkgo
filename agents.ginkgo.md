# Agent Guide for Ginkgo

This document is a working contract for agents authoring, editing, or reviewing Ginkgo workflows.

The goal is not to produce generic Python code. The goal is to produce workflows that are:

- reproducible
- cache-friendly
- inspectable
- safe to run repeatedly
- easy for humans to debug later

## What Ginkgo Is

Ginkgo is a Python-native workflow engine for scientific, analytical, and data workflows.

It provides:

- lazy task graph construction through `@task()` and `@flow`
- dynamic DAG expansion by returning `Expr`, `ExprList`, or `shell_task(...)` from tasks
- content-addressed task caching
- per-task environments, currently via Pixi
- local concurrent scheduling with `--jobs` and `--cores`
- run manifests and per-task logs under `.ginkgo/runs/`

Agents should write code that works with these mechanics, not around them.

## Core Rules

### 1. Put workflow logic in tasks, not in side-effectful top-level code

Good:

- build work inside `@task()` bodies
- use `@flow` to wire tasks together
- keep module-level code light and deterministic

Avoid:

- expensive computation at import time
- top-level file writes
- hidden side effects outside tasks

Reason:

- CLI execution imports the workflow module and autodiscovers one `@flow`
- import-time side effects are hard to cache, test, or debug

### 2. Define tasks at module scope

Tasks must be importable by module path.

Good:

```python
@task()
def clean_data(path: file) -> file:
    ...
```

Avoid:

- nested task definitions
- closures that capture local state
- lambdas used as tasks

Reason:

- Python task execution uses worker processes when possible
- workers must be able to import the task by module path

### 3. Use `@flow` only to build the graph

`@flow` should wire together tasks and return the root expression.

Good:

- call tasks
- build fan-out with `.map()`
- pass task results into downstream tasks

Avoid:

- branching on unresolved task results inside the flow body

Reason:

- task calls in a flow return `Expr` objects, not concrete values
- dynamic control flow belongs inside task bodies, where values are resolved

### 4. Put dynamic branching inside tasks

Good:

```python
@task()
def route(metric: float):
    if metric > 0.8:
        return high_quality(metric=metric)
    return low_quality(metric=metric)
```

Reason:

- task bodies receive resolved values
- returning an `Expr` or `ExprList` is how Ginkgo expands the DAG at runtime

## Task Design

### Prefer small, meaningful tasks

Good tasks:

- have a clear input/output boundary
- do one analytical step
- produce an artifact or a concrete value that is worth caching

Avoid tasks that:

- wrap a whole pipeline end to end
- mix unrelated side effects
- do a lot of work but return too little provenance

### Use stable, explicit arguments

Prefer explicit task parameters over hidden global state.

Good:

- pass config values as task arguments
- pass paths explicitly
- pass analysis parameters explicitly

Avoid:

- reading random globals inside task bodies
- implicit dependence on process state that is not reflected in inputs

Reason:

- cache keys are based on resolved inputs, task identity, version, and env identity
- hidden inputs make cache reuse incorrect

### Bump `version=` when task logic changes materially

Good:

```python
@task(version=2)
def featurize(df: pd.DataFrame) -> pd.DataFrame:
    ...
```

Reason:

- Ginkgo does not hash task source code for cache invalidation
- if logic changes but `version=` does not, stale cached results may be reused

## Data and Artifact Types

### Use Ginkgo path types when the semantics matter

Use:

- `file` for file inputs or outputs
- `folder` for directory inputs or outputs
- `tmp_dir` for scratch space managed by Ginkgo

Reason:

- these types drive validation and cache hashing
- `file` is hashed by file contents
- `folder` is hashed recursively
- `tmp_dir` is auto-created and excluded from cache keys

### Use ordinary Python values for ordinary in-memory data

Ginkgo can transport and cache:

- scalars
- lists, tuples, dicts
- numpy arrays
- pandas dataframes
- other picklable Python objects

Prefer file- or artifact-based boundaries for very large data when practical.

Reason:

- large in-memory values can work, but file-oriented boundaries are often easier to inspect and debug in scientific workflows

### Avoid spaces in `file` and `folder` paths

Reason:

- Ginkgo validates path types and rejects paths with spaces
- shell interpolation is intentionally simple and assumes space-safe paths

## Shell Tasks

Use `shell_task(...)` when the real work is a command-line tool.

Good:

```python
@task(env="tools")
def convert(input_csv: file) -> file:
    output = "results/out.tsv"
    return shell_task(
        cmd=f"python scripts/convert.py {input_csv} > {output}",
        output=output,
        log="logs/convert.log",
    )
```

Rules:

- always provide a concrete `output` path
- prefer deterministic output names
- set `log=` when the shell output is useful to the user
- put the environment on `@task(env=...)`, not on `shell_task(...)`

Reason:

- Ginkgo validates that `output` exists after the command completes
- the output path is also the shell task's returned result

## Parallelism and Resources

### Use `.map()` for fan-out

Good:

```python
results = process(multiplier=2).map(item=items)
```

Reason:

- `.map()` creates an `ExprList` of independent tasks
- Ginkgo can schedule them concurrently

### Use fan-in naturally

Pass an `ExprList` into a downstream task that expects a `list[...]`.

Reason:

- the evaluator resolves all mapped tasks before running the consumer

### Use `threads` honestly

Today, Ginkgo uses the resolved `threads` argument as the task's CPU footprint for scheduling.

Good:

- declare `threads` when the task genuinely consumes multiple cores
- leave it out or keep it at `1` for single-core tasks

Avoid:

- inventing fake `threads` values that do not reflect actual resource use

Reason:

- Ginkgo's `--cores` budget is enforced using the task's `threads` value

## Environments

### Use `env=` for reproducible tool boundaries

Good uses:

- shell tools that depend on a specific scientific stack
- Python tasks that should run in a pinned Pixi environment

Reason:

- Ginkgo resolves envs from `envs/<env>/pixi.toml`
- the associated `pixi.lock` contributes to the cache key
- env lockfiles are copied into run provenance

### Keep env naming and ownership clear

Prefer env names like:

- `analysis_tools`
- `bioinfo_tools`
- `modeling_env`

Avoid ambiguous names like:

- `test`
- `default2`

## Logging and Outputs

### Give outputs stable names

Prefer outputs that are:

- deterministic from task inputs
- easy to inspect manually
- grouped under clear directories like `results/`, `artifacts/`, or `logs/`

Avoid random filenames unless the task truly needs scratch paths. Use `tmp_dir` for temporary work instead.

### Use per-task logs intentionally

For shell tasks, set `log=` when command output matters.

Remember:

- Ginkgo also keeps its own per-task provenance logs under `.ginkgo/runs/<run_id>/logs/`
- `shell_task(log=...)` is best for user-facing domain logs, not as the only place logs live

## Caching Discipline

Agents should think about cache correctness explicitly.

Good cache-friendly patterns:

- deterministic task outputs
- explicit parameters
- version bumps on logic changes
- separating expensive steps into their own tasks

Bad cache patterns:

- hidden reads from undeclared files
- dependence on ambient environment variables without surfacing them as inputs
- tasks that return stale paths to mutable outputs

## Provenance Expectations

Each run records provenance under `.ginkgo/runs/<run_id>/`.

Agents should assume users will inspect:

- `manifest.yaml`
- `params.yaml`
- per-task logs
- copied env lockfiles

Write workflows so those records are meaningful.

That means:

- clear task names
- clear env names
- readable output paths
- explicit inputs

## Testing and Validation

When editing a Ginkgo workflow or the runtime:

- validate that the workflow imports cleanly
- use dry-run validation when possible
- check that task boundaries still make sense
- check that cache semantics remain correct

For workflow authoring, prefer:

```bash
ginkgo test --dry-run
```

For runtime changes, add or update tests that cover:

- caching behavior
- dynamic branching
- fan-out / fan-in
- env handling
- provenance output

## Things Agents Should Avoid

- writing tasks that are not module-importable
- putting important control flow in `@flow` based on unresolved task results
- hiding material inputs from task signatures
- omitting `version=` changes when task logic changes significantly
- using `shell_task(...)` without a meaningful `output`
- relying on random ambient state for reproducibility
- assuming Ginkgo currently manages memory, GPU, or remote execution unless that functionality exists in the target repo

## Good Default Workflow Shape

When in doubt, use this structure:

1. `config = ginkgo.config(...)`
Prefer `ginkgo.config("ginkgo.toml")` unless the project already uses a different config layout.
2. a small set of module-level `@task()` definitions
3. one `@flow` entrypoint
4. explicit outputs under `results/` or `artifacts/`
5. explicit logs under `logs/` when useful
6. `.map()` for embarrassingly parallel work
7. downstream aggregation tasks for fan-in

## Suggested Prompt Snippet

If this file is being used as an actual agent prompt, this shorter instruction is usually enough:

> Build Ginkgo workflows using module-level `@task()` functions and one `@flow`. Keep task inputs explicit, use `file`/`folder`/`tmp_dir` annotations when path semantics matter, use `.map()` for fan-out, put dynamic branching inside task bodies, use `shell_task(...)` for CLI tools with explicit `output` paths, declare `env=` when reproducibility matters, and preserve cache correctness by bumping `version=` when task logic changes.
