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
- Early cache completion for warm runs, including prepare-phase cache hits that
  avoid environment preparation for cached tasks and allow version-pinned
  remote inputs to skip staging on warm reruns
- Reproducible environment dispatch through Pixi for local shell execution and
  container-backed execution for shell tasks
- Provenance capture, logs, machine-readable runtime events, and structured
  inspection and diagnostics through the CLI, with append-only hot-path
  provenance updates in `events.jsonl` and a reconstructed/finalized
  `manifest.yaml`
- Remote task execution via Kubernetes (GKE, EKS, OKE) and GCP Batch, with
  per-task GPU/memory/CPU resource declarations, code-sync packaging, and
  full provenance integration
- A local-first web UI for runs, cache inspection, graphs, notebook artifacts,
  embedded notebook viewing, and multi-workspace browsing
- A canonical package-oriented project layout with workflow autodiscovery and
  scaffolded project initialization
- An example-driven benchmark harness with generated benchmark inputs, checked-
  in baselines, and a separate CI lane for slowdown detection

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
│   ├── evaluator.py      # _ConcurrentEvaluator scheduler/lifecycle loop
│   ├── module_loader.py
│   ├── notebook_kernels.py
│   ├── scheduler.py
│   ├── worker.py
│   ├── events.py
│   ├── remote_executor.py   # RemoteExecutor / RemoteJobHandle protocols
│   ├── diagnostics.py
│   ├── task_validation.py     # TaskValidator: contracts, inputs, coercion
│   ├── task_runners/
│   │   ├── shell.py           # ShellRunner: subprocess + shell driver tasks
│   │   └── notebook.py        # NotebookRunner: notebook + script driver tasks
│   ├── caching/
│   │   ├── cache.py           # CacheStore (content-addressed)
│   │   ├── provenance.py      # RunProvenanceRecorder
│   │   ├── hash_memo.py
│   │   ├── hashing.py
│   │   └── materialization_log.py
│   ├── artifacts/
│   │   ├── artifact_store.py  # content-addressed artifact storage
│   │   ├── artifact_model.py
│   │   ├── asset_store.py     # asset catalog metadata
│   │   └── value_codec.py     # cross-process value serialization
│   ├── notifications/
│   │   ├── notifications.py
│   │   └── slack.py
│   └── environment/
│       ├── secrets.py         # SecretResolver and redaction
│       └── resources.py
├── remote/
│   ├── backend.py           # RemoteStorageBackend protocol
│   ├── code_bundle.py       # code packaging for remote workers
│   ├── fsspec_backends.py   # S3, OCI, GCS backends
│   ├── gcp_batch.py         # GCP Batch executor
│   ├── kubernetes.py        # Kubernetes executor
│   ├── publisher.py         # remote output publishing
│   ├── resolve.py           # backend factory
│   ├── staging.py           # remote input staging
│   └── worker.py            # remote worker entry point
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

## Benchmarking

Ginkgo now includes a benchmark harness centered on the runnable workflows
under `examples/`.

- The benchmark entry point is `pixi run benchmark`, which runs
  `python -m benchmarks.run`.
- Structured benchmark results are written under `benchmarks/results/`.
- Checked-in slowdown baselines live under `benchmarks/baselines/` as JSON.
- Benchmark runs print a readable terminal summary table in addition to writing
  structured JSON results.
- A dedicated GitHub Actions workflow runs the benchmark lane separately from
  correctness and quality checks.

### Benchmark Input Provenance

Benchmark-only source manifests live under `benchmarks/sources/`.

- These manifests pin upstream repository, commit SHA, metadata URL, and read
  URL base for generated benchmark datasets.
- The heavier bioinformatics benchmark uses
  [bioinfo_agam.toml](/Users/sanjay.nagi/Software/ginkgo/benchmarks/sources/bioinfo_agam.toml)
  to fetch a pinned metadata table, inject `fastq_1` and `fastq_2`, download
  the selected FASTQs into a benchmark workspace, and point the copied
  `examples/bioinfo` workflow at the generated sample sheet via a config
  overlay.

This keeps the canonical checked-in examples stable for documentation and
correctness tests while still allowing the benchmark lane to exercise a larger
input set.

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
- tasks with `gpu > 0` or `remote=True` are dispatched to the configured remote
  executor (Kubernetes or GCP Batch) when `--executor` is set
- failures are fail-fast for new dispatch, but in-flight tasks are allowed to complete

The scheduler performs explicit cycle detection when registering expressions.

**Per-task thread declaration.** A task's CPU footprint is declared on the
decorator (`@task(threads=4)`). The scheduler uses this value as the task's
core budget against `--cores`. When a task function's signature includes a
`threads` parameter, the declared value is injected automatically so the task
body can reference it. Shell tasks additionally receive `GINKGO_THREADS` in
their subprocess environment, and `@task(threads=N, export_thread_env=True)`
also exports `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `OPENBLAS_NUM_THREADS`,
and `NUMEXPR_NUM_THREADS` so ordinary BLAS/OpenMP tools honour the budget
without per-workflow boilerplate.

**Fan-out concurrency caps.** `.map()` and `.product_map()` accept an optional
`max_concurrent=N` argument that caps how many branches from a single
fan-out may run simultaneously, independent of the global `--jobs` and
`--cores` budgets. The scheduler tracks one ephemeral concurrency group per
fan-out and enforces the limit in the CP-SAT selection model alongside
cores, jobs, and memory constraints.

**Runtime profiling (`--profile`).** `ginkgo run --profile` enables a coarse
phase-timer recorder that attributes wall time to CLI startup, workflow
module import, flow construction, evaluator validation, scheduler prepare /
dispatch / wait / consume phases, event emission, resource monitor lifecycle,
provenance finalize, manifest load, and renderer finish. The recorder is a
no-op when `--profile` is not set and does not run when disabled, so the
default path is not instrumented. The phase totals are persisted under
`timings.profile` in the run manifest, printed as a Rich summary table at
the end of the run, and exposed by `ginkgo inspect run`.

### Remote References and Staged Access

Phase 6 introduced first-class remote input support without changing the
task-facing path model.

- Workflows can declare external object-store inputs with explicit immutable
  remote reference values:
  - `remote_file("s3://bucket/key")`
  - `remote_folder("s3://bucket/prefix/")`
- Parameters annotated as `file` or `folder` also support narrow
  annotation-aware coercion from raw `s3://...` and `oci://...` strings.
  Plain `str` parameters remain plain strings.
- Remote references are kept distinct from Ginkgo-managed artifacts produced by
  the local artifact store. Remote staging handles external inputs; the
  artifact store handles managed outputs.
- The evaluator resolves remote inputs into normal local filesystem paths
  before task execution, so Python, shell, and notebook tasks continue to
  consume ordinary local paths rather than provider-specific streams.
- File-shaped refs are downloaded into a dedicated worker-local staging cache.
  Folder-shaped refs are materialized as local directory trees rooted in the
  same staging area.
- Remote identity participates in cache and provenance metadata through
  explicit reference identity, version IDs, and staged content metadata rather
  than treating mutable URIs as stable cache keys.
- The remote I/O layer is isolated behind backend and staging abstractions in
  `ginkgo/core/remote.py`, `ginkgo/remote/backend.py`, and
  `ginkgo/remote/staging.py`, which keeps object-store concerns out of task
  code and out of the scheduler's general artifact logic.
- This design is intentionally staging-first. Mounted or FUSE-like access
  remains a possible later optimization, but staged local access is the current
  correctness path and the compatibility model for future pod-local workers.

### Worker-Affine Remote Staging

Phase 6D made remote staging an explicit execution phase rather than hidden
argument preprocessing.

- Ready tasks now reserve scheduler capacity before any remote downloads begin.
- Tasks with remote inputs transition through `waiting -> staging -> running`,
  and `task_started` is emitted only after staging completes successfully.
- Remote hydration runs on a dedicated bounded thread pool that is configured
  independently from CPU task concurrency, with `GINKGO_STAGING_JOBS` and
  `remote.staging_jobs` support.
- Concurrent tasks deduplicate in-flight staging of the same remote reference,
  so one download fan-outs to multiple waiting tasks on the same worker.
- The staging root remains worker-local by contract, which keeps the local
  runtime aligned with a future Kubernetes or pod-local execution model.

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
- `threads=...`, `memory=...`, `gpu=...` for resource declarations
- `remote=True` for explicit remote dispatch

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
- managed Ginkgo kernelspecs under `.ginkgo/jupyter/` for `.ipynb` execution,
  with explicit `ipykernel` validation and deterministic kernel naming derived
  from the selected execution environment
- marimo notebook execution through a CLI/script invocation with resolved task arguments forwarded as CLI parameters
- stable run-scoped notebook artifacts under `.ginkgo/runs/<run_id>/notebooks/`
- HTML export recorded in provenance as explicit task metadata rather than inferred from filenames
- notebook source hashing folded into cache identity so notebook edits invalidate cache even when the task wrapper is unchanged
- explicit `outputs=` parameter for declaring and validating post-execution outputs (optional; runtime-managed artifacts are still recorded even when `outputs` is omitted)

For Papermill-backed notebooks, Ginkgo now prefers the runtime-selected task
environment over embedded notebook kernelspec metadata. When a notebook task
declares `env=...`, the managed kernelspec is prepared from that environment;
otherwise the current interpreter environment is used.

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
`ginkgo/runtime/artifacts/artifact_store.py`. Artifact identity is content-addressed:
files use the blob digest and directories use a manifest digest. That identity
is recorded in cache metadata as `artifact_ids`, which gives later roadmap
phases a stable contract for remote storage and lineage features.

The artifact store is the canonical immutable source of truth for managed path
outputs, while the working tree is a writable materialized view. When a task
produces a `file` or `folder`, Ginkgo copies the bytes into
`.ginkgo/artifacts/` as a read-only artifact but leaves the working-tree output
in place as an ordinary writable file or directory. On cache hit, Ginkgo
compares each managed output path against the cached artifact content and
restores only paths that are missing, type-mismatched, or have diverged. If a
working-tree output already matches the cached artifact, it is left untouched.

`ginkgo cache prune` and related cache cleanup paths are now artifact-aware:
read-only artifacts have permissions restored before deletion so cache
maintenance can safely remove unreferenced stored outputs.

## Assets

Ginkgo now includes a file-backed asset catalog layered over the cache and
artifact store. Assets add stable logical identity and lineage to managed
outputs without changing the run-centric execution model.

The asset layer is implemented by:

- `ginkgo/core/asset.py` for the public asset types and builders
- `ginkgo/runtime/artifacts/asset_store.py` for the local catalog metadata store
- evaluator integration in `ginkgo/runtime/evaluator.py`

The current asset model supports:

- `asset(path, name=..., metadata=...)` as a task return wrapper for file
  outputs
- immutable `AssetVersion` records keyed by logical `AssetKey`
- resolved `AssetRef` values passed to downstream tasks
- alias pointers and version history in `.ginkgo/assets/`
- upstream lineage edges recorded from consumed `AssetRef` inputs
- provenance records that include asset metadata alongside cache keys and
  artifact identifiers

The catalog is metadata-only. Asset bytes are never stored in the asset store
itself; every asset version points to an immutable `artifact_id` in the
artifact store. This keeps three identities distinct:

- logical asset identity (`AssetKey`)
- physical materialization (`artifact_id`)
- cache entry identity (`cache_key`)

`AssetRef` values participate directly in cache and transport semantics. The
value codec can serialize and deserialize them, cache metadata summarizes them
recursively, and downstream cache invalidation follows `AssetRef.version_id`
rather than re-hashing artifact bytes.

The current implementation is intentionally narrow:

- file assets and the wrapped asset kinds (`table`, `array`, `fig`, `text`)
  are supported
- assets do not drive scheduling
- the asset store is local and file-backed

Model assets, staleness reporting, and asset-aware lifecycle policy remain
future work.

### Wrapped Asset Sentinels

Task bodies can tag selected return values as immutable assets with
kind-specific metadata by wrapping them with one of four sentinel factories:

- `table(df, name=..., metadata=...)` — pandas, polars (eager or lazy),
  pyarrow Table/Dataset, DuckDB relation, or CSV/TSV path. Stored as Parquet.
- `array(arr, name=..., metadata=...)` — numpy, xarray, zarr, or dask. Stored
  as a zipped zarr store when the `zarr` package is installed, or as a `.npy`
  blob otherwise for numpy-only inputs. Non-numpy backends require `zarr`.
- `fig(figure, name=..., metadata=...)` — matplotlib (PNG), plotly (HTML),
  bokeh (HTML), or a path to an existing PNG/SVG/HTML file.
- `text(body, name=..., format=..., metadata=...)` — string, dict (stored as
  JSON), or a `Path` to a text document. Format is `{plain, markdown, json}`.
- `model(clf, name=..., framework=..., metrics=..., metadata=...)` — trained
  ML models. Supports scikit-learn estimators, xgboost/lightgbm sklearn
  wrappers (all via `joblib`), PyTorch `nn.Module` (via `torch.save`), and
  Keras/TensorFlow (via the native `.keras` archive). `metrics` is a
  first-class `dict[str, float]` field stored on the asset version so the
  UI and `ginkgo models` can render training metrics without walking
  free-form metadata. All ML backends (`joblib`/`scikit-learn`,
  `torch`, `keras`, `xgboost`, `lightgbm`) are user-managed
  dependencies, lazy-imported at serialisation/load time.

Wrappers follow the same pattern as `shell()`/`ShellExpr`: the user calls a
factory inside the task body, returns the sentinel, and the evaluator
replaces it with a resolved `AssetRef` after registering the serialised
payload with the artifact store.

Implementation is split between:

- `ginkgo/core/wrappers.py` — sentinel dataclasses and factories, with
  sub-kind detection via MRO walks (no optional backend imports at
  construction).
- `ginkgo/runtime/artifacts/wrapper_serialization.py` — per-kind serializers
  producing Parquet/zarr/PNG/HTML/text bytes plus kind-specific metadata.
- `ginkgo/runtime/artifacts/wrapper_loaders.py` — a small loader registry
  used by the CLI read path and any future programmatic consumers.
- `ginkgo/runtime/artifacts/asset_registration.py` — extended to unwrap
  sentinels at task completion, serialising each payload, storing the bytes
  through `ArtifactStore.store_bytes`, and registering an `AssetVersion` in
  the wrapper-specific namespace (`table` / `array` / `fig` / `text` /
  `model`). Dict return values are walked recursively so sentinels nested
  inside task-level result dicts register alongside list/tuple outputs.

Named outputs use the asset key `<task_fn>.<name>`. Unnamed outputs are
indexed per kind as `<task_fn>.<kind>[<index>]`. Duplicate explicit names
within a single task raise a `ValueError` before any artifact is written.
Serialisation errors surface as `WrapperSerializationError` identifying the
offending wrapper by name and index.

Kind-specific metadata stored on each `AssetVersion`:

- `table`: `sub_kind`, `schema`, `row_count`, `byte_size`
- `array`: `sub_kind`, `shape`, `dtype`, `chunks`, `coordinates`, `byte_size`
- `fig`: `sub_kind`, `source_format`, `byte_size`, `dimensions`
- `text`: `sub_kind`, `format`, `byte_size`, `line_count`
- `model`: `sub_kind`, `framework`, `metrics`, `byte_size`

`ginkgo asset show <key>` renders this metadata through the CLI without
re-reading the stored bytes. The UI asset payload surfaces the same fields
under a `kind_metadata` key for future frontend consumers.

#### Rehydration on receive

Downstream tasks that declare `pd.DataFrame`, `np.ndarray`, `str`, or a
trained-model parameter receive the live Python object rather than the
`AssetRef` produced by an upstream wrapper. The evaluator rehydrates wrapped refs
in `_resolve_task_args` via `_rehydrate_wrapped_refs`, which consults a
per-run `LivePayloadRegistry` before falling back to the on-disk
`wrapper_loaders.load_from_ref` path.

The live registry
(`ginkgo/runtime/artifacts/live_payloads.py`) is a capped-LRU cache
keyed by `artifact_id`. When `AssetRegistrar` serialises a wrapped
payload it stores the producer's Python object in the registry, so a
subsequent consumer in the same evaluator process is served from memory
and avoids a Parquet/zarr round-trip. The on-disk loader path is the
fallback for subprocess workers, cache resumes, and cross-run
consumers. `fig` refs are left as `AssetRef` since binary image
payloads are rarely consumed as live Python objects. `file` refs are
untouched — the existing `file` coercion path handles them.

Rehydration is transparent to task authors: a task annotated
`compounds: pd.DataFrame` continues to work unchanged when its upstream
switches from returning a raw DataFrame to `table(df, name="...")`.
The examples in `examples/chem/.../inputs.py::annotate_compounds` and
`examples/retail/.../inputs.py::enrich_orders` demonstrate this pattern
in a real multi-stage workflow.

## Value Transport

Python task inputs and outputs cross process boundaries through the codec layer in `ginkgo/runtime/artifacts/value_codec.py`.

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
- asset versions and metadata for asset-producing tasks
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
- `ginkgo asset ls`
- `ginkgo asset versions`
- `ginkgo asset inspect`
- `ginkgo models`
- `ginkgo cache ls`
- `ginkgo cache explain`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls`
- `ginkgo env clear`

Implemented CLI features include dry-run validation, merged config overrides,
human-readable run summaries, structured inspection and diagnostics, secret
discovery and validation, cache inspection and eviction, failed-task
debugging, and asset catalog inspection for local workspaces.

## Web UI

The local UI is implemented as a lightweight JSON API server plus a bundled React frontend.

The current UI supports:

- sidebar-first desktop shell with primary navigation (Runs, Assets, Cache, Workspaces)
- multi-workspace session: load any number of local Ginkgo workspaces, switch
  the active workspace via the top bar, and scope runs/cache/workflow-launch to
  that workspace
- local-first workspace loading from any directory: a shallow workspace probe
  keeps startup fast and supports both canonical and non-canonical layouts
- run history and run summaries
- task tables, task-graph visualization using recorded dependencies, and notebook artifact links derived from run provenance
- task detail drawers with full log retrieval
- asset explorer with catalog list, version history, lineage, and metadata
- asset previews for tables/dataframes, figures, PDFs, and text artifacts, plus
  generic metadata views for other asset kinds
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

## Remote Execution

Ginkgo supports dispatching individual tasks to cloud infrastructure while the
rest of the workflow runs locally. Remote dispatch is opt-in at the task level:
tasks with `gpu > 0` or `remote=True` are sent to the configured executor;
everything else stays local.

### Remote Executor Protocol

The evaluator dispatches remote work through a `RemoteExecutor` protocol
(`runtime/remote_executor.py`). The protocol defines:

- `RemoteExecutor.submit(attempt=...)` → `RemoteJobHandle`
- `RemoteJobHandle.state()` → `RemoteJobState` (PENDING / RUNNING / SUCCEEDED / FAILED / CANCELLED)
- `RemoteJobHandle.result()` → `RemoteJobResult` (blocking wait + result)
- `RemoteJobHandle.cancel()` / `RemoteJobHandle.logs_tail()`

This keeps executor implementations fully decoupled from the scheduling loop.
The evaluator polls handles on dedicated watcher threads and processes results
through the same code path as local worker completions.

### Executor Implementations

**KubernetesExecutor** (`remote/kubernetes.py`) submits `batch/v1` Jobs to any
Kubernetes cluster. Resource declarations on `@task` map to pod resource
requests: `threads` → CPU, `memory` → memory, `gpu` → `nvidia.com/gpu`. GPU
tasks receive a `cloud.google.com/gke-accelerator` node selector when
`gpu_type` is configured, enabling automatic GPU node provisioning on GKE
Autopilot.

**GCPBatchExecutor** (`remote/gcp_batch.py`) submits jobs to GCP Batch, a
serverless batch compute service. No cluster required — each job runs on
Google-managed infrastructure. GPU tasks use the Batch accelerator allocation
policy. Job logs are retrieved from Cloud Logging.

### Remote Worker

The worker entry point (`remote/worker.py`) runs as
`python -m ginkgo.remote.worker` inside the container. It:

1. Reads the task payload from `GINKGO_WORKER_PAYLOAD` (base64-encoded JSON)
2. Optionally downloads and extracts a code bundle (code-sync mode)
3. Calls the standard `run_task()` worker function
4. Prints a JSON result line to stdout for the handle to parse

The same worker image serves both K8s and GCP Batch executors.

### Code Sync

Two modes for making workflow code available to remote workers:

- **Baked** (default): the worker image already contains the code.
- **Sync**: the evaluator creates a tarball of the workflow package, uploads
  it to cloud storage (content-addressed by SHA-256), and includes the bundle
  coordinates in the task payload. Workers download and extract the bundle
  before importing task functions.

Code sync is configured via `[remote.k8s.code]` or `[remote.batch.code]`
with `mode = "sync"` and `package = "<dir>"`. The bundle is published to the
remote artifact backend configured in `[remote.artifacts]`.

### Remote Provenance and Events

Remote execution integrates fully with the existing provenance and event
systems:

- `TaskStarted` events carry `execution_backend` ("local" / "remote") and
  are rendered as `↑ submitted` in the CLI for remote tasks.
- `TaskRunning` events are emitted when a remote pod transitions from PENDING
  to RUNNING, updating the CLI to `◐ running`.
- `TaskCompleted` and `TaskFailed` events carry `remote_job_id`.
- Provenance records include `execution_backend`, `remote_job_id`, and
  `resources` for remote tasks.
- `ginkgo inspect run` surfaces all remote metadata.
- Pod/container logs are captured at task completion via `handle.logs_tail()`.

### GCS Backend

`GCSFileSystemBackend` (`remote/fsspec_backends.py`) extends the fsspec base
class with Google Cloud Storage support via `gcsfs`. It supports
`head()`, `download()`, `upload()`, and `list_prefix()` operations and is
used for both remote input staging and code bundle publishing.

### Infrastructure Scripts

- `scripts/gke-setup.sh` — creates a GKE Autopilot cluster, Artifact Registry,
  IAM bindings, K8s namespace, and builds/pushes the worker image.
- `scripts/gke-teardown.sh` — deletes the cluster and registry.

### Package Layout (Remote)

```text
ginkgo/
├── remote/
│   ├── backend.py           # RemoteStorageBackend protocol
│   ├── code_bundle.py       # tarball creation, publish, download+extract
│   ├── fsspec_backends.py   # S3, OCI, GCS fsspec backends
│   ├── gcp_batch.py         # GCPBatchExecutor + GCPBatchJobHandle
│   ├── kubernetes.py        # KubernetesExecutor + KubernetesJobHandle
│   ├── publisher.py         # RemotePublisher for remote outputs
│   ├── resolve.py           # resolve_backend() factory
│   ├── staging.py           # remote input staging
│   └── worker.py            # remote worker entry point
├── runtime/
│   ├── remote_executor.py   # RemoteExecutor / RemoteJobHandle protocols
│   └── ...
└── ...
```

## Current Constraints

The current runtime still has important boundaries and tradeoffs:

- worker-executed Python tasks must be importable by module path
- the scheduler's authoritative live execution state is still in-memory, with
  persisted run state exported incrementally to `manifest.yaml` and
  `events.jsonl`

Those constraints drive several of the future roadmap items in the implementation plan.
