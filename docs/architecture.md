# Ginkgo Architecture

Ginkgo is a Python-based workflow orchestrator for scientific and data workflows. The current implementation is local-first and centered on a lazy expression-tree DSL, content-addressed caching, reproducible task environments via Pixi, and run provenance that is inspectable from both the CLI and a local web UI.

## Current Status

The repository currently implements:

- Declarative workflow construction with `@flow`, `@task()`, `Expr`, and `ExprList`
- Workflow authoring helpers via `expand(...)`, `zip_expand(...)`, `flatten(...)`, and `slug(...)`
  for concise deterministic workflow authoring
- Dynamic DAG expansion when tasks return nested `Expr` or `ExprList` values
- Explicit task kinds via `@task(kind="python" | "shell")` and `@notebook(...)`
- Content-addressed caching for scalar values, files, folders, arrays, DataFrames, and other supported Python objects
- Concurrent scheduling with job, core, and memory constraints
- Python task execution through a `ProcessPoolExecutor`
- Scheduler-evaluated shell task wrappers dispatched as `shell(...)` payloads, including Pixi-backed shell execution without importing `workflow.py` in the foreign env
- First-class notebook execution for Jupyter (`.ipynb`) and marimo notebooks with stable run-scoped HTML artifacts
- Run provenance, per-task logs, cache inspection, and CLI debugging commands
- A local browser UI for browsing runs, tasks, notebook artifacts, task graphs, logs, and cache entries
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
│   └── pixi.py
├── cli/
│   ├── app.py
│   └── commands/
└── ui/
    ├── server/
    │   ├── __init__.py      # re-exports create_ui_server
    │   ├── app.py           # HTTP/WebSocket handler and route wiring
    │   ├── live.py          # live-state capture and diffing
    │   ├── payloads.py      # run/task/workspace/cache payload builders
    │   ├── utils.py         # shared formatting helpers
    │   ├── websocket.py     # WebSocket framing
    │   └── workspaces.py    # WorkspaceRecord, WorkspaceRegistry, discovery
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

**LocalBackend** wraps `PixiRegistry` for existing Pixi-based execution.

**ContainerBackend** (`envs/container.py`) supports Docker and Podman execution for **shell tasks only**. Container envs are declared via URI schemes: `env="docker://image:tag"` or `env="oci://image:tag"`. The project root is bind-mounted at its host-side absolute path so that paths in shell commands resolve without rewriting.

**CompositeBackend** routes env strings to the correct backend based on the URI scheme. Container env URIs go to `ContainerBackend`; everything else goes to `LocalBackend`.

Foreign execution environments do not support Python tasks. Ginkgo treats `env=...` as a shell-task boundary only, which keeps foreign execution command-oriented and avoids requiring the Ginkgo runtime to be importable inside every target environment. This is enforced at validation time before any work starts.

Image digests (not mutable tags) are used for cache key identity, ensuring cache invalidation when image contents change.

## Task Model

### Python Tasks

`@task()` supports:

- `version=...`
- `retries=...`

Python tasks always execute in the scheduler's own Python environment. If a
task needs a different Pixi or container environment, it must be declared with
`kind="shell"` and invoke the desired script or command explicitly.

Python task bodies must be top-level importable functions for worker execution. Supported task inputs and outputs include:

- scalars and nested containers
- `file`, `folder`, `tmp_dir`
- `numpy.ndarray`
- `pandas.DataFrame`
- other values supported by the codec registry

### Shell Tasks

Shell execution is expressed by declaring `@task(kind="shell")` and returning `shell(...)` from the task body. The Python wrapper runs on the scheduler, constructs the concrete shell command from resolved values, and the runtime executes only that shell payload while validating the declared outputs.

For Pixi-backed shell tasks, the foreign environment no longer imports the task's defining `workflow.py` module. The scheduler evaluates the wrapper locally and dispatches only the shell payload through Pixi.

Shell tasks can also run inside Docker or Podman containers by declaring a container env:

```python
@task(kind="shell", env="docker://biocontainers/samtools:1.17")
def sort_bam(input_bam: file, output_bam: file) -> file:
    return shell(cmd=f"samtools sort {input_bam} -o {output_bam}", output=output_bam)
```

This completes the Phase 3 execution-boundary work from the implementation
roadmap: graph construction remains scheduler-local and foreign environments
are entered only for executable shell payloads.

### Notebook Tasks

Notebook execution is expressed with `@notebook(...)`. The decorated function
defines the typed parameter schema and UI description, while the notebook file
itself is treated as the executable source artifact.

Implemented notebook behavior includes:

- `.ipynb` execution through Papermill with standard parameters-cell injection
- marimo notebook execution through a CLI/script invocation with resolved task arguments forwarded as CLI parameters
- stable run-scoped notebook artifacts under `.ginkgo/runs/<run_id>/notebooks/`
- HTML export recorded in provenance as explicit task metadata rather than inferred from filenames
- notebook source hashing folded into cache identity so notebook edits invalidate cache even when the wrapper function body is unchanged

Notebook tasks run on the same driver-side shell execution path as other
shell-like tasks. This preserves existing scheduler semantics for dependency
resolution, retries, environment dispatch, cache recording, and provenance.

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
- task source hash
- notebook source hash for notebook-backed tasks
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

Phase 2 of the implementation roadmap is now complete for cache integrity. The
runtime now hashes the top-level task function source during task registration
and stores that `source_hash` in both the cache key payload and `meta.json`, so
task-body changes invalidate prior cache entries without requiring a manual
`version=` bump. If source extraction fails for a task definition, registration
now fails explicitly instead of silently weakening cache correctness.

File and folder outputs now flow through a formal `ArtifactStore` contract,
implemented locally by `LocalArtifactStore` in
`ginkgo/runtime/artifact_store.py`. Artifact identity is content-addressed:
files use `<sha256>.<ext>` and directories use `<sha256>`. This identity is now
recorded in cache metadata as `artifact_ids`, which gives later roadmap phases
a stable contract for remote storage and lineage features.

The local cache is now the source of truth for path outputs. When a task
produces a `file` or `folder`, Ginkgo copies the bytes into `.ginkgo/artifacts/`
as a read-only artifact and replaces the original output path with a symlink to
that artifact. On cache hit, symlink integrity is validated before reuse:
missing symlinks are recreated from the stored artifact, while regular files or
foreign symlinks at the output path are treated as external modification and
force re-execution.

`ginkgo cache prune` and related cache cleanup paths are now artifact-aware:
read-only artifacts have permissions restored before deletion so cache
maintenance can safely remove unreferenced stored outputs.

Phase 13 of the implementation roadmap is now complete for secrets and
credentials management. Workflows can declare runtime-only secret dependencies
via `secret(...)` references, which are resolved at execution time through a
pluggable resolver layer with environment-variable lookup and optional `.env`
support. Secret references remain identifiers during graph construction and
cache-keying, so rotating a credential value does not invalidate cache entries
that are otherwise still valid.

Secret-bearing inputs are redacted before they reach persisted provenance or
cache metadata, and task log capture now redacts resolved secret values before
they are written to per-task stdout/stderr logs. The CLI also exposes `ginkgo
secrets list`, `ginkgo secrets validate`, and `ginkgo doctor` checks for
declared but unresolvable secrets.

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

Shell tasks may declare `env="name"` to run against a Pixi environment under
`envs/<name>/pixi.toml`, or against an explicit manifest path.

Implemented behavior includes:

- env discovery and validation
- Pixi lock hashing for cache invalidation
- environment preparation before dispatch
- shell execution through the Pixi environment

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
- notebook artifact metadata including rendered HTML paths, executed notebook paths where applicable, and render status
- exit codes and errors
- run-level CPU and RSS summaries

## Local UI Workspace Model

The UI remains local-first and file-backed, but it no longer assumes that one
browser session only inspects one project, or that it must be launched from the
workspace directory.

The current UI server now supports:

- a set of loaded workspaces in one UI session
- one active workspace that scopes the default runs, cache, and workflow-launch
  views
- a native `Load workspace` action exposed by the UI, backed by a local
  folder-picker dialog
- workspace-scoped run and task routes so browser navigation remains stable
  after switching workspaces
- launching from any directory (including `~`): workspace validation uses a
  shallow probe rather than a recursive scan, so startup is immediate even when
  the launch directory is not itself a workspace
- workspace detection accepts `ginkgo.toml`, `.ginkgo/`, `pyproject.toml` +
  root-level `@flow` files, or `pixi.toml` + root-level `@flow` files, so
  projects with non-canonical layouts (e.g. a root-level `ginkgo_workflow.py`
  in a pixi project) are recognized correctly

### Pixi-aware workflow launch

When the UI launches a workflow subprocess for an external workspace, it
detects whether the workspace has a `.pixi/` environment directory. If pixi
is found, the subprocess command is `pixi run python -m ginkgo.cli run
<workflow>` (run in the workspace's own pixi environment), so that
workspace-specific dependencies are importable when the workflow module is
loaded. Workspaces without a pixi environment fall back to the current
interpreter (`sys.executable`).

Each loaded workspace still reads directly from that workspace's local
`.ginkgo/` provenance and cache directories. The UI does not yet depend on a
central database or remote control plane.

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

- sidebar-first desktop shell with primary navigation (Runs, Cache, Workspaces)
- multi-workspace session: load any number of local Ginkgo workspaces, switch
  the active workspace via the top bar, and scope runs/cache/workflow-launch to
  that workspace
- run history and run summaries
- task tables, task-graph visualization using recorded dependencies, and notebook artifact links derived from run provenance
- task detail drawers with full log retrieval
- cache browsing and deletion
- live updates via a WebSocket event channel (`/ws`): the server emits
  structured events derived from on-disk provenance changes; the frontend
  applies incremental state updates without full page reloads
- pixi-aware workflow launch for external workspaces (see Local UI Workspace
  Model section)

DAG layout improvements (fit-to-view, failure focus, richer positioning)
remain future work.

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
  bundles, now including a notebook-backed reporting step
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
