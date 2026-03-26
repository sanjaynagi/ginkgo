# Ginkgo Architecture

Ginkgo is a Python-based workflow orchestrator for scientific and data workflows. The current implementation is local-first and centered on a lazy expression-tree DSL, content-addressed caching, reproducible task environments via Pixi, and run provenance that is inspectable from both the CLI and a local web UI.

## Current Status

The repository currently implements:

- A lazy DSL built around `@flow`, `@task`, `Expr`, and `ExprList` for
  declarative workflow construction
- Concurrent local execution with dynamic DAG expansion, resource-aware
  scheduling, and explicit task kinds for Python, shell, notebook, and script
  work
- Content-addressed caching, artifact storage, and value transport for common
  Python values and path-based outputs
- Reproducible environment dispatch through Pixi for local shell execution and
  container-backed execution for shell tasks
- Provenance capture, logs, machine-readable runtime events, and structured
  inspection and diagnostics through the CLI
- A local-first web UI for runs, cache inspection, graphs, notebook artifacts,
  and multi-workspace browsing
- A canonical package-oriented project layout with workflow autodiscovery and
  scaffolded project initialization

## Agent Operability

Phase 4 introduced a machine-readable operability layer for AI agents and
other programmatic clients.

### Runtime Event Protocol

The evaluator now emits typed runtime events through an in-process event bus in
`ginkgo/runtime/events.py`. These events cover:

- run lifecycle
- task lifecycle
- cache hits and misses
- environment preparation
- dynamic graph expansion

This keeps runtime state changes explicit and lets multiple consumers observe
the same execution facts without duplicating scheduler logic.

### Human and Agent Output Modes

Rich CLI output and agent-mode JSONL output are now separate renderings of the
same runtime event stream.

- Human operators continue to use the Rich run renderer.
- Agents can use `ginkgo run --agent` to receive one JSON event per line on
  stdout.

The legacy structured stderr task stream used by direct `evaluate(...)`
callers remains available when no event bus is attached, preserving backward
compatibility for existing tests and programmatic use.

### Structured Inspection and Diagnostics

Ginkgo now exposes machine-readable post-hoc inspection and diagnostics:

- `ginkgo inspect workflow` returns a static task graph snapshot without
  execution.
- `ginkgo inspect run <run_id>` reconstructs a run snapshot from provenance.
- `ginkgo debug --json` returns failed-task diagnostics, including failure
  summaries and log tails.
- `ginkgo doctor --json` returns structured validation diagnostics.
- `ginkgo cache explain --run <run_id>` provides best-effort rerun reasons from
  cache metadata.

To support these surfaces, task provenance now records structured failure
summaries and a compact typed output index alongside the existing manifest
fields.

### Runtime Notifications

Ginkgo now includes a Slack notification path built on the same runtime event
stream used by CLI and agent renderers.

- Notification config is loaded from `ginkgo.toml` or explicit CLI config
  overlays, independent of whether the workflow module calls `ginkgo.config(...)`.
- Slack webhook credentials are resolved through the existing secrets resolver
  using secret references such as `{ env = "GINKGO_SLACK_WEBHOOK" }`.
- Supported events are:
  - run started
  - run completed successfully
  - run failed
  - task retry exhaustion
- Failure notifications are enriched from run provenance so they can include
  failed task names, exit codes, and truncated log tails.
- Notification dispatch is non-blocking and warning-only. Slack delivery
  failures do not affect workflow execution or provenance recording.

The implementation is intentionally narrow for now: Slack incoming webhooks are
the only supported notification channel, and channel routing is controlled by
the webhook configured in Slack rather than by a per-run channel override in
Ginkgo.

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
│   ├── notebook.py
│   ├── script.py
│   ├── shell.py
│   ├── task.py
│   └── types.py
├── runtime/
│   ├── backend.py        # TaskBackend protocol, LocalBackend, CompositeBackend
│   ├── cache.py
│   ├── evaluator.py
│   ├── module_loader.py
│   ├── notification_slack.py
│   ├── notifications.py
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

## Documentation Stack

End-user documentation now lives in a dedicated Sphinx + MyST site under
`docs/site/`.

- Sphinx provides navigation, API reference generation, and local static-site builds.
- MyST keeps the authored pages in Markdown rather than splitting the docs
  stack between Markdown and reStructuredText.
- The local docs build is wired through Pixi with `pixi run docs-build`, which
  writes the site to `docs/_build/dirhtml/`.

This published docs site is intentionally separate from the repository's
internal implementation plans and historical notes, which remain under `docs/`
as development artifacts rather than end-user pages.

## Execution Model

### Flow Construction

`@task()`-decorated functions do not execute when called. They return `Expr[T]` values that describe deferred computation. A `@flow` function is the entrypoint that builds the initial expression tree.

`ExprList[T]` is produced by `.map()` or `.product_map()` and represents
fan-out across multiple independent task invocations. `.map()` uses positional
zip semantics, while `.product_map()` uses Cartesian expansion. Chained
fan-out remains flat, with existing branches as the outer loop and newly
introduced rows as the inner loop.

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
Shell tasks may declare `env="name"` to run against a Pixi environment under
`envs/<name>/pixi.toml`, or against an explicit manifest path. This path is
responsible for env discovery, validation, lock hashing for cache invalidation,
environment preparation before dispatch, and shell execution through Pixi.

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

For Pixi-backed shell tasks, the foreign environment does not import the task's
defining `workflow.py` module. The scheduler evaluates the wrapper locally and
dispatches only the shell payload through Pixi.

Shell tasks can also run inside Docker or Podman containers by declaring a container env:

```python
@task(kind="shell", env="docker://biocontainers/samtools:1.17")
def sort_bam(input_bam: file, output_bam: file) -> file:
    return shell(cmd=f"samtools sort {input_bam} -o {output_bam}", output=output_bam)
```

Graph construction remains scheduler-local and foreign environments are entered
only for executable shell payloads.

### Notebook Tasks

Notebook execution is expressed by declaring `@task("notebook")` and returning
a `notebook(...)` sentinel from the task body. The task decorator defines the
typed parameter schema, while the notebook file itself is treated as the
executable source artifact.

Task body pattern:

```python
@task("notebook")
def analyze_data(*, input_file: file) -> file:
    return notebook(
        path="notebooks/analysis.ipynb",
        outputs="output.html"
    )
```

Implemented notebook behavior includes:

- `.ipynb` execution through Papermill with standard parameters-cell injection
- marimo notebook execution through a CLI/script invocation with resolved task arguments forwarded as CLI parameters
- stable run-scoped notebook artifacts under `.ginkgo/runs/<run_id>/notebooks/`
- HTML export recorded in provenance as explicit task metadata rather than inferred from filenames
- notebook source hashing folded into cache identity so notebook edits invalidate cache even when the task wrapper is unchanged
- explicit `outputs=` parameter for declaring and validating post-execution outputs (optional; runtime-managed artifacts are still recorded even when `outputs` is omitted)

Notebook tasks run on the same driver-side execution path as shell tasks,
preserving scheduler semantics for dependency resolution, retries, environment
dispatch, cache recording, and provenance.

### Script Tasks

Script execution is expressed by declaring `@task("script")` and returning a
`script(...)` sentinel from the task body. Scripts support Python and R languages
with automatic interpreter detection based on file extension.

Task body pattern:

```python
@task("script")
def process_data(*, input_file: file, threshold: float) -> file:
    return script(
        path="scripts/analyze.py",
        outputs="results.csv"
    )
```

Implemented script behavior includes:

- automatic interpreter detection: `.py` → `python`, `.R` or `.r` → `rscript`
- optional explicit interpreter override via `interpreter=` parameter
- resolved task inputs forwarded as CLI arguments (`--arg-name value`)
- explicit `outputs=` parameter for declaring and validating post-execution outputs (optional)
- source file hashing folded into cache identity so script edits invalidate cache

Script tasks, like notebook tasks, run on the driver-side execution path and
preserve full scheduler semantics.

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
- resolved input hashes
- environment lock hash when `env=` is used
- source file hash for driver tasks (notebook and script) folded at evaluation time

Implemented cache hashing includes:

- BLAKE3 as the canonical digest algorithm for cache keys, artifact IDs, input hashing, and source hashing
- scalar hashing via stable value hashing
- file-content hashing
- recursive folder-content hashing
- Pixi lock hashing for local environments
- container image digest hashing for container environments
- codec-based hashing for arrays, DataFrames, and other supported Python values

Cache entries are written atomically and reused across reruns when inputs are unchanged.

The runtime hashes the top-level task function source during task registration
and stores that `source_hash` in both the cache key payload and `meta.json`, so
task-body changes invalidate prior cache entries without requiring a manual
`version=` bump. If source extraction fails for a task definition, registration
fails explicitly instead of silently weakening cache correctness.

File and folder outputs now flow through a formal `ArtifactStore` contract,
implemented locally by `LocalArtifactStore` in
`ginkgo/runtime/artifact_store.py`. Artifact identity is content-addressed:
files use `<blake3-digest>.<ext>` and directories use `<blake3-digest>`. This identity is now
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

## Value Transport

Python task inputs and outputs cross process boundaries through the codec layer in `ginkgo/runtime/value_codec.py`.

The current implementation supports:

- direct transport for small values
- artifact-backed transport for large values
- pickle-based fallback for general Python objects
- optimized codecs for NumPy arrays
- parquet-first DataFrame transport with pickle fallback

The same codec layer is used for both task transport and cache persistence.

## Configuration and Secrets

Workflows can declare runtime-only secret dependencies via `secret(...)`
references, which are resolved at execution time through a pluggable resolver
layer with environment-variable lookup and optional `.env` support. Secret
references remain identifiers during graph construction and cache-keying, so
rotating a credential value does not invalidate cache entries that are
otherwise still valid.

Secret-bearing inputs are redacted before they reach persisted provenance or
cache metadata, and task log capture redacts resolved secret values before they
are written to per-task stdout/stderr logs.

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

## CLI

The current CLI supports:

- `ginkgo run`
- `ginkgo test`
- `ginkgo debug`
- `ginkgo doctor`
- `ginkgo inspect`
- `ginkgo secrets`
- `ginkgo ui`
- `ginkgo init`
- `ginkgo cache ls`
- `ginkgo cache explain`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls`
- `ginkgo env clear`

Implemented CLI features include dry-run validation, merged config overrides,
human-readable run summaries, structured inspection and diagnostics, secret
discovery and validation, cache inspection and eviction, and failed-task
debugging.

## Web UI

The local UI is implemented as a lightweight JSON API server plus a bundled React frontend.

The current UI supports:

- sidebar-first desktop shell with primary navigation (Runs, Cache, Workspaces)
- multi-workspace session: load any number of local Ginkgo workspaces, switch
  the active workspace via the top bar, and scope runs/cache/workflow-launch to
  that workspace
- local-first workspace loading from any directory: a shallow workspace probe
  keeps startup fast and supports both canonical and non-canonical layouts
- run history and run summaries
- task tables, task-graph visualization using recorded dependencies, and notebook artifact links derived from run provenance
- task detail drawers with full log retrieval
- cache browsing and deletion
- live updates via a WebSocket event channel (`/ws`): the server emits
  structured events derived from on-disk provenance changes; the frontend
  applies incremental state updates without full page reloads
- workspace-scoped routes so browser navigation remains stable after switching
  workspaces
- native `Load workspace` integration backed by a local folder picker

When the UI launches a workflow subprocess for an external workspace, it checks
for a `.pixi/` environment directory. If pixi is present, the subprocess
command is `pixi run python -m ginkgo.cli run <workflow>` in that workspace's
own environment so workspace-specific dependencies are importable when the
workflow module is loaded. Workspaces without a pixi environment fall back to
the current interpreter (`sys.executable`).

Each loaded workspace reads directly from that workspace's local `.ginkgo/`
provenance and cache directories. The UI does not depend on a central database
or remote control plane.

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

The repository-level validation corpus includes:

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
- the scheduler's authoritative live execution state is still in-memory, with
  persisted run state exported incrementally to `manifest.yaml` and
  `events.jsonl`

Those constraints drive several of the future roadmap items in the implementation plan.
