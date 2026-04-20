# Execution Model

## Flow Construction

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

## Dynamic DAG Expansion

Tasks receive resolved concrete argument values at execution time. A task can inspect those values and return:

- a concrete result
- a `ShellExpr`
- another `Expr`
- an `ExprList`
- a nested container containing `Expr` / `ExprList`

The evaluator registers those returned expressions dynamically and extends the graph during execution.

## Scheduling and Execution

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

**Task priority.** `@task(priority=N)` declares a relative dispatch priority
(range `[-1000, 1000]`, default `0`). When several tasks are ready
simultaneously and contend for the same resources, the CP-SAT selection
model prefers higher-priority tasks. Priority is a strict tiebreaker: it
never overrides the scheduler's primary objective of dispatching as many
ready tasks as possible, nor its secondary objective of filling the core
budget. Workloads that do not set `priority` are unaffected.

**Selective retries and backoff.** `@task(retries=N)` enables retries; the
retry policy is narrowed by:

- `retry_on=IOError` (or a tuple of exception classes) to retry only
  specific failure modes;
- `retry_on_exit_codes=(137,)` for shell tasks to retry only specific
  exit codes;
- `retry_backoff=<seconds>` with `retry_backoff_multiplier` and
  `retry_backoff_max` to apply exponential delay between attempts.

Retry-delayed tasks transition through a `waiting_retry` scheduler state
with a ready-at deadline; the scheduler wakes on the earliest deadline
without busy-looping. `TaskRetrying` events carry the scheduled
`delay_seconds`.

**Runtime profiling (`--profile`).** `ginkgo run --profile` enables a coarse
phase-timer recorder that attributes wall time to CLI startup, workflow
module import, flow construction, evaluator validation, scheduler prepare /
dispatch / wait / consume phases, event emission, resource monitor lifecycle,
provenance finalize, manifest load, and renderer finish. The recorder is a
no-op when `--profile` is not set and does not run when disabled, so the
default path is not instrumented. The phase totals are persisted under
`timings.profile` in the run manifest, printed as a Rich summary table at
the end of the run, and exposed by `ginkgo inspect run`.

## Remote References and Staged Access

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

## Worker-Affine Remote Staging

Phase 6D made remote staging an explicit execution phase rather than hidden
argument preprocessing.

- Ready tasks reserve scheduler capacity before any remote downloads begin.
- Tasks with remote inputs transition through `waiting -> staging -> running`,
  and `task_started` is emitted only after staging completes successfully.
- Remote hydration runs on a dedicated bounded thread pool that is configured
  independently from CPU task concurrency, with `GINKGO_STAGING_JOBS` and
  `remote.staging_jobs` support.
- Concurrent tasks deduplicate in-flight staging of the same remote reference,
  so one download fan-outs to multiple waiting tasks on the same worker.
- The staging root remains worker-local by contract, which keeps the local
  runtime aligned with a future Kubernetes or pod-local execution model.

## Execution Backends

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

## Sub-workflow Composition (Opaque Mode)

Ginkgo supports invoking one workflow from inside another as an opaque
subprocess. A task declared with `kind="subworkflow"` returns a
`subworkflow(path, params=..., config=...)` descriptor; the evaluator
dispatches it by running `ginkgo run <path>` in a child process. The child
run is self-contained: it writes its own `.ginkgo/runs/<child_id>/`
directory, executes its own DAG, and exits. Its run id is returned to the
parent task as a `SubWorkflowResult` and recorded on the parent manifest
entry under `sub_run_id`.

```python
from ginkgo import flow, task, subworkflow, SubWorkflowResult


@task(kind="subworkflow")
def screen_region(region: str) -> SubWorkflowResult:
    return subworkflow("workflows/screening.py", params={"region": region})


@flow
def parent():
    return screen_region.map(region=["emea", "apac", "amer"])
```

Key properties:

- **Opaque only.** The sub-workflow's DAG is not expanded into the parent.
  Its internal tasks do not appear in the parent UI, and the parent
  scheduler sees a single task per `call_workflow` invocation.
- **Shared workspace cache.** Parent and child share `.ginkgo/cache/` by
  construction, so identical sub-task work is reused across depth without
  any cross-run cache key composition. The parent task's own cache key
  hashes its inputs, source, and any declared `version=` — a change to
  the child workflow file's contents alone does not invalidate the
  parent's "skip the subprocess" short circuit. Users needing strict
  invalidation should bump `version=` on the parent task or pass the
  child workflow path as a `file` parameter.
- **Parameters via `--config`.** `params={...}` is serialised to a
  temporary YAML file and forwarded to the child as an extra `--config`
  overlay. Additional config paths can be passed via `config=...`.
- **Run-id stitching.** The child emits a machine-readable
  `GINKGO_CHILD_RUN_ID=<id>` line on stdout when
  `GINKGO_CALLED_FROM_PARENT_RUN` is set in its environment. The parent
  runner captures this line and records the child id on the parent task's
  manifest entry, making it discoverable via `ginkgo inspect run`.
- **Failure propagation.** Non-zero child exit raises `SubWorkflowError`
  in the parent task, which triggers normal retry / fail-fast behaviour.
  The child run directory remains for debugging.
- **Recursion guard.** `GINKGO_CALL_DEPTH` increments per hop. Dispatch
  refuses to spawn a child when the next depth would exceed a small
  default (8), catching accidental recursive workflow calls before they
  exhaust the machine.

Non-goals for this mode:

- No inline expansion of sub-workflow tasks into the parent DAG.
- No unified scheduling budget across parent and child processes — each
  child honours its own `--jobs` / `--cores`, but the sum across siblings
  is not bounded.
- No plan-time cycle detection across workflows; the depth guard catches
  recursion at dispatch time.
