# Ginkgo Architecture

Ginkgo is a Python-based workflow orchestrator for scientific and data workflows. The current implementation is local-first and centered on a lazy expression-tree DSL, content-addressed caching, reproducible task environments via Pixi, and run provenance that is inspectable from both the CLI and a local web UI.

## Current Status

The repository currently implements:

- Declarative workflow construction with `@flow`, `@task()`, `Expr`, and `ExprList`
- Workflow authoring helpers via `expand(...)`, `zip_expand(...)`, `flatten(...)`, and `slug(...)`
  for concise deterministic workflow authoring
- Dynamic DAG expansion when tasks return nested `Expr` or `ExprList` values
- Explicit task kinds via `@task(kind="python" | "shell")`
- Content-addressed caching for scalar values, files, folders, arrays, DataFrames, and other supported Python objects
- Concurrent scheduling with job, core, and memory constraints
- Python task execution through a `ProcessPoolExecutor`
- Scheduler-evaluated shell task wrappers dispatched as `shell(...)` payloads, including Pixi-backed shell execution without importing `workflow.py` in the foreign env
- Run provenance, per-task logs, cache inspection, and CLI debugging commands
- A local browser UI for browsing runs, tasks, task graphs, logs, and cache entries
- A multi-workspace local UI shell with an active workspace model and native
  folder-picker loading for switching between Ginkgo workspaces on one machine
- A canonical package-based workflow repository layout with root autodiscovery

## Canonical Workflow Project Layout

Ginkgo now treats the following repository structure as the canonical default for
workflow projects:

```text
<project-root>/
├── pixi.toml
├── ginkgo.toml
├── <project_package>/
│   ├── __init__.py
│   ├── workflow.py   # contains flow definition
│   ├── modules/      # contains tasks, grouped in modules
│   └── envs/
├── tests/
│   └── workflows/
├── results/          # runtime-created, optional
└── .ginkgo/          # runtime-created, optional
```

Within that layout:

- `<project_package>/workflow.py` is the canonical CLI entrypoint and should
  remain thin, containing flow definitions and graph wiring only.
- Reusable task implementations live under `<project_package>/modules/`.
- Task-specific Pixi manifests may live under `<project_package>/envs/`.
- `tests/workflows/` holds workflow validation files for `ginkgo test`.

The CLI now auto-discovers the canonical `<project_package>/workflow.py` when
`ginkgo run` is invoked from the repository root without an explicit workflow
argument. Legacy root-level `workflow.py` files and explicit workflow paths
remain supported for non-canonical project layouts.

## Package Layout

The current source tree is organized around the user-facing DSL, the execution engine, and environment backends:

```text
ginkgo/
├── __init__.py
├── config.py
├── helpers.py
├── core/
│   ├── expr.py
│   ├── flow.py
│   ├── shell.py
│   ├── task.py
│   └── types.py
├── runtime/
│   ├── backend.py        # TaskBackend protocol, LocalBackend, CompositeBackend
│   ├── cache.py
│   ├── evaluator.py
│   ├── module_loader.py
│   ├── provenance.py
│   ├── resources.py
│   ├── scheduler.py
│   ├── value_codec.py
│   └── worker.py
├── envs/
│   ├── container.py      # ContainerBackend (Docker/Podman)
│   ├── pixi.py
│   └── pixi_worker.py
├── cli/
│   ├── app.py
│   └── commands/
└── ui/
    ├── server.py
    └── static/
```

## Execution Model

### Flow Construction

`@task()`-decorated functions do not execute when called. They return `Expr[T]` values that describe deferred computation. A `@flow` function is the entrypoint that builds the initial expression tree.

`ExprList[T]` is produced by `.map()` on a partially applied task and represents fan-out across multiple independent task invocations.

Ginkgo also exposes small workflow-authoring helpers:

- `expand(template, **wildcards)` for Cartesian wildcard expansion in placeholder order
- `zip_expand(template, **wildcards)` for positional wildcard expansion with equal-length iterables
- `flatten(items)` for flattening nested list/tuple structures into a single list
- `slug(value)` for deterministic file-safe artifact names

### Dynamic DAG Expansion

Tasks receive resolved concrete argument values at execution time. A task can inspect those values and return:

- a concrete result
- a `ShellExpr`
- another `Expr`
- an `ExprList`
- a nested container containing `Expr` / `ExprList`

The evaluator registers those returned expressions dynamically and extends the graph during execution.

### Scheduling and Execution

The current evaluator is concurrent and futures-based:

- the scheduler tracks dependency completion
- ready tasks are selected subject to `--jobs`, `--cores`, and optional `--memory`
- shell tasks run via subprocesses
- Python tasks run in a `ProcessPoolExecutor`
- failures are fail-fast for new dispatch, but in-flight tasks are allowed to complete

The scheduler performs explicit cycle detection when registering expressions.

### Execution Backends

The evaluator dispatches work through a `TaskBackend` protocol (`runtime/backend.py`), which decouples environment resolution from the scheduling loop.

**LocalBackend** wraps `PixiRegistry` for existing Pixi-based execution (shell and Python tasks).

**ContainerBackend** (`envs/container.py`) supports Docker and Podman execution for **shell tasks only**. Container envs are declared via URI schemes: `env="docker://image:tag"` or `env="oci://image:tag"`. The project root is bind-mounted at its host-side absolute path so that paths in shell commands resolve without rewriting.

**CompositeBackend** routes env strings to the correct backend based on the URI scheme. Container env URIs go to `ContainerBackend`; everything else goes to `LocalBackend`.

Container environments do not support Python tasks — requiring Ginkgo installed inside every container image is an unreasonable constraint. This is enforced at validation time (before any work starts) and at the backend level.

Image digests (not mutable tags) are used for cache key identity, ensuring cache invalidation when image contents change.

## Task Model

### Python Tasks

`@task()` supports:

- `env=...`
- `version=...`
- `retries=...`

Python task bodies must be top-level importable functions for worker execution. Supported task inputs and outputs include:

- scalars and nested containers
- `file`, `folder`, `tmp_dir`
- `numpy.ndarray`
- `pandas.DataFrame`
- other values supported by the codec registry

### Shell Tasks

Shell execution is expressed by declaring `@task(kind="shell")` and returning `shell(...)` from the task body. The Python wrapper runs on the scheduler, constructs the concrete shell command from resolved values, and the runtime executes only that shell payload while validating the declared outputs.

For Pixi-backed shell tasks, the foreign environment no longer imports the task's defining `workflow.py` module. The scheduler evaluates the wrapper locally and dispatches the shell payload through Pixi, while true `kind="python"` tasks with `env=` still execute their Python bodies inside the foreign worker environment.

Shell tasks can also run inside Docker or Podman containers by declaring a container env:

```python
@task(kind="shell", env="docker://biocontainers/samtools:1.17")
def sort_bam(input_bam: file, output_bam: file) -> file:
    return shell(cmd=f"samtools sort {input_bam} -o {output_bam}", output=output_bam)
```

This completes the Phase 3 execution-boundary work from the implementation
roadmap: graph construction remains scheduler-local, shell-oriented tasks cross
the boundary as executable shell payloads, and foreign-environment Python tasks
now have a distinct execution contract from foreign-environment shell tasks.

### Special Types

Ginkgo currently ships three path-oriented marker types:

- `file`
- `folder`
- `tmp_dir`

These drive validation, caching, and scratch-directory lifecycle management.

## Caching

The cache lives under `.ginkgo/cache/` and is keyed by:

- task identity
- task version
- resolved input hashes
- environment lock hash when `env=` is used

Implemented cache hashing includes:

- scalar hashing via stable value hashing
- file-content hashing
- recursive folder-content hashing
- Pixi lock hashing for local environments
- container image digest hashing for container environments
- codec-based hashing for arrays, DataFrames, and other supported Python values

Cache entries are written atomically and reused across reruns when inputs are unchanged.

## Value Transport

Python task inputs and outputs cross process boundaries through the codec layer in `ginkgo/runtime/value_codec.py`.

The current implementation supports:

- direct transport for small values
- artifact-backed transport for large values
- pickle-based fallback for general Python objects
- optimized codecs for NumPy arrays
- parquet-first DataFrame transport with pickle fallback

The same codec layer is used for both task transport and cache persistence.

## Pixi Environment Integration

Tasks may declare `env="name"` to run against a Pixi environment under `envs/<name>/pixi.toml`, or against an explicit manifest path.

Implemented behavior includes:

- env discovery and validation
- Pixi lock hashing for cache invalidation
- environment preparation before dispatch
- shell execution through the Pixi environment
- Python task execution through the Pixi environment interpreter

## Provenance and Run State

Each run records provenance under `.ginkgo/runs/<run_id>/`:

```text
.ginkgo/runs/<run_id>/
├── manifest.yaml
├── params.yaml
├── envs/
└── logs/
```

The manifest records:

- run metadata and status
- resolved task inputs
- input hashes
- cache keys
- task dependencies and dynamic dependency ids
- retries and attempts
- outputs
- exit codes and errors
- run-level CPU and RSS summaries

## Local UI Workspace Model

The UI remains local-first and file-backed, but it no longer assumes that one
browser session only inspects one project.

The current UI server now supports:

- a set of loaded workspaces in one UI session
- one active workspace that scopes the default runs, cache, and workflow-launch
  views
- a native `Load workspace` action exposed by the UI, backed by a local
  folder-picker dialog
- workspace-scoped run and task routes so browser navigation remains stable
  after switching workspaces

Each loaded workspace still reads directly from that workspace's local
`.ginkgo/` provenance and cache directories. The UI does not yet depend on a
central database or remote control plane.
- execution backend type (`local` or `container`) and container image digest

## CLI

The current CLI supports:

- `ginkgo run`
- `ginkgo test`
- `ginkgo debug`
- `ginkgo ui`
- `ginkgo init`
- `ginkgo cache ls`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls`
- `ginkgo env clear`

Implemented CLI features include:

- dry-run validation
- merged config overrides
- human-readable run summaries
- cache inspection and eviction
- failed-task debugging

## Web UI

The local UI is implemented as a lightweight JSON API server plus a bundled React frontend.

The current UI supports:

- run history
- run summaries
- task tables
- task-graph visualization using recorded dependencies
- task detail drawers
- full log retrieval
- cache browsing and deletion
- live refresh via server-sent events

Full DAG-visualization polish and a true WebSocket event channel remain future work.

## Validation Workflows

The current implementation is validated against the canonical workflow families below:

- `VW-1` linear dependency chains
- `VW-2` fan-out / fan-in
- `VW-3` conditional branching
- `VW-4` mixed fan-out with conditional branches
- `VW-5` selective cache invalidation
- `VW-6` partial failure and resume
- `VW-7` core-aware resource contention
- `VW-8` memory-aware scheduling

These are exercised through the test suite and, from the CLI layer onward, through `ginkgo run` and `ginkgo test`.

Phase 2 of the implementation roadmap is now completed through the expanded
example suite under `examples/`. The repository-level validation corpus now
includes:

- `retail` for static fan-out, fan-in, and shell-generated delivery
  bundles
- `news` for runtime-determined `ExprList` expansion and dynamic dependency
  recording
- `supplychain` for multi-scenario analysis with richer artifact fan-in
- `chem` for chemistry-domain portfolio review with
  runtime-determined per-series packet generation
- `ml` for ML-domain candidate evaluation, promotion, and delivery
  packaging across a deeper static DAG

The foundational `bioinfo` example also demonstrates mixed execution
environments: Pixi-based shell tasks for bioinformatics tools, a Docker
container shell task for basic Unix processing, and local Python tasks
for data aggregation.

`tests/test_examples.py` runs these examples end to end in isolated workspaces
and asserts expected artifacts, manifests, dynamic dependency behavior, and
cache reuse on rerun.

## Current Constraints

The current runtime still has important boundaries and tradeoffs:

- worker-executed Python tasks must be importable by module path
- the authoritative run state for live execution is still in-memory, with YAML manifests as persisted exports

Those constraints drive several of the future roadmap items in the implementation plan.
