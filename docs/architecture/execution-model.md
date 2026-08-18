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
- `per_branch(template)` for values derived from a fan-out branch's own arguments
- `flatten(items)` for flattening nested list/tuple structures into a single list
- `slug(value)` for deterministic file-safe artifact names

### Axes versus derived columns in fan-out

A fan-out argument is one of two things, and the difference is carried by its
type rather than by convention (issue #198):

- A **column**: a list of values. `.map()` consumes columns row by row;
  `.product_map()` treats each as an axis and crosses them.
- A **derived value**: `per_branch("...{arg}...")`, rendered once per generated
  branch from that branch's own arguments (fan-out row values first, then
  arguments fixed on the task call). It generates no branches of its own, takes
  no part in labels, and so cannot fall out of step with the values it names.

`expand()`/`zip_expand()` return `ExpandedTemplate`, a `list[str]` subclass that
remembers its template. Such a list is one value per wildcard combination —
already aligned to the values it came from — so `.product_map()` rejects it,
naming the argument. The error offers a concrete `per_branch()` template only
when every wildcard already names one of the call's varying arguments;
placeholders are resolved by name, never by position, so the assumption that
caused the original mislabelling does not reappear in the error path. Where a
placeholder resolves to nothing, the message names it instead of printing a
template that would fail on the next run. Passed to
`.product_map()` it would have become a further axis crossed with the axes it
was derived from: an N×M grid with an N·M-element path list silently became
N·M·N·M branches, several writing each path, last writer winning, with the
surviving file's name contradicting its contents.

`_materialize_varying_columns` in `ginkgo/core/task.py` performs the split into
`_VaryingArgs(columns, derived)`; `_branch_args` renders the derived templates
per branch. String and bytes fan-out arguments are also rejected there, since
`list("path")` would fan out over characters.

### Reachability and dropped calls

The graph is exactly what is reachable from the flow's return value:
`ConcurrentEvaluator._register_value` walks the returned expression, and a task
call the flow constructs but never returns is not part of the run. That rule
stands, but it used to be silent — a bare side-effecting statement, or a literal
path written where an upstream expression belonged, produced a smaller graph
with no message (issue #122).

Constructed calls are now recorded so the drop can be reported. `record_call`
in `ginkgo/core/expr.py` appends a `ConstructedCall` for every `Expr` minted by
`TaskDef.__call__` and every `ExprList` minted by the fan-out helpers, but only
while a `record_constructed_calls()` context manager is open. A chained
`ExprList.map()` rebuilds its base branches, so the helper calls
`supersede_call` to drop the superseded entry rather than report it.

Callers that build a flow (`cli/commands/run.py`, `runtime/diagnostics.py`) open
the recorder around the flow body and pass the log to the evaluator as
`constructed_calls`. `ConcurrentEvaluator.unreachable_calls` then diffs it
against `_expr_nodes`, keyed by object identity. Nothing is recorded outside
that context manager, so expressions built by library users or minted inside
running tasks are unaffected.

Dropped calls surface as `warning`-severity `unreachable_task_call` diagnostics
in `ginkgo doctor`, as a "Dropped" section in the `--dry-run` plan, and as a
warning on stderr before a real run, alongside the `param_read_from_global`
warning that shares its shape. They are warnings, not errors: a flow may build
an expression and discard it deliberately.

## Dynamic DAG Expansion

Tasks receive resolved concrete argument values at execution time. A task can inspect those values and return:

- a concrete result
- a `ShellDirective`
- another `Expr`
- an `ExprList`
- a nested container containing `Expr` / `ExprList`

The evaluator registers those returned expressions dynamically and extends the graph during execution.

## Scheduling and Execution

The current evaluator is concurrent and futures-based:

- the scheduler tracks dependency completion
- ready tasks are selected subject to `--jobs`, `--cores`, `--gpus`, and
  optional `--memory`
- shell tasks run via subprocesses
- Python tasks run in a `ProcessPoolExecutor`
- placement is requirement-driven: `executor="name"` tasks go to that named
  executor; `remote=True` tasks, and tasks whose `gpu` requirement exceeds the
  local `--gpus` budget, go to the run's default executor (`--executor`); any
  route without a usable executor is a build error
- failures are fail-fast for new dispatch, but in-flight tasks are allowed to complete

The scheduler performs explicit cycle detection when registering expressions.

**Per-task thread declaration.** A task's CPU footprint is declared on the
decorator (`@task(threads=4)`) and may be overridden per site via the
`[resources.overrides]` runtime-config table; the evaluator resolves one
*effective* value per task. The scheduler uses the effective value as the
task's core budget against `--cores`. When a task function's signature
includes a `threads` parameter, the effective value is injected automatically
so the task body can reference it. Shell tasks additionally receive `GINKGO_THREADS` in
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

## Execution Environments

The evaluator dispatches work through an `ExecutionEnvironment` protocol (`runtime/backend.py`), which decouples environment resolution from the scheduling loop.

**LocalEnvironment** wraps `PixiRegistry` for existing Pixi-based execution.
Shell tasks may declare `env="name"` to run against a Pixi environment under
`envs/<name>/`, where the manifest is either a `pixi.toml` or a `pyproject.toml`
carrying a `[tool.pixi]` section (Pixi accepts both natively), or against an
explicit manifest path. This path is
responsible for env discovery, validation, lock hashing for cache invalidation,
environment preparation before dispatch, and shell execution through Pixi.

**ContainerBackend** (`envs/container.py`) supports Docker and Podman execution for shell, notebook, and script tasks. Container envs are declared via URI schemes: `env="docker://image:tag"` or `env="oci://image:tag"`. The project root is bind-mounted at its host-side absolute path so that paths in shell commands resolve without rewriting.

Paths outside the project root are mounted too, derived from what the task declares rather than configured by hand. `ShellRunner.run_logged_command` passes `exec_argv` a `mounts` list built from the node's path-shaped arguments (read-only, with `tmp_dir` read-write), and each driver runner adds its directive's declared outputs (read-write); `envs/mounts.py` normalises the set. Symlinks resolve on the host side and mount at the path as given, so a command written against a symlink still resolves. Mounts the project-root mount already covers are dropped and nested same-mode mounts collapse.

A `file` input mounts its *directory*, not the file: tools routinely read a sibling of the file they are handed (`ref.fa.fai`, `.bai`), and a mount of the file alone makes an index that exists on the host invisible. Read-only means a tool that wants to *write* an index fails and says so — declaring that index as an output is what makes it writable and keeps it, since anything written into the container's own layer is discarded with the container. A declared output mounts its parent, because the output does not exist yet and the runtime would create a directory at that path to satisfy the mount. A directive's `log` is not mounted: it is captured host-side from the client's pipes, so the container never opens it.

Only *declared* paths are visible, so a path interpolated into a command from config is not — the same pressure to annotate that cache correctness already applies.

Mounts carry their origin. A *declared* mount may be widened from read-only to read-write by another declared mount; a *configured* one from `extra_mounts` is the user's decision, so a derived read-write mount over it raises rather than silently widening it. The filesystem root, system directories (under both their literal and resolved names, since macOS reaches them through `/private`), and the home directory are refused: an output written straight into `$HOME` would otherwise hand the image `~/.ssh` and `~/.aws` along with it.

`user = "auto"` picks whatever leaves outputs owned by the invoking user, which is runtime-specific. Docker runs as the image's user and needs an explicit `-u uid:gid` (with `HOME=/tmp`, since that uid has no passwd entry). Rootless Podman already maps the container's root to the invoking user, and passing `-u` there maps into the *subordinate* uid range instead — leaving files the user cannot chmod or delete — so nothing is passed.

Ginkgo's own computed environment — `GINKGO_THREADS`, plus the BLAS/OpenMP variables under `export_thread_env` — is forwarded as bare `-e NAME`, which both runtimes resolve from the client process's environment, so the values are not spelled out in the argument vector. Nothing else from `os.environ` crosses the boundary.

`[container]` in `ginkgo.toml` configures `runtime`, `pull_policy`, `user`, `shell`, `auto_mount`, and `extra_mounts`; `container_backend_from_config` builds the backend from it, rejecting unknown keys and mistyped values the way `[resources.overrides]` does rather than falling back to defaults a reader of the file would not expect.

`exec_failure_hint` on the `ExecutionEnvironment` protocol lets a backend diagnose a failure the raw output does not name; every driver runner asks through `ShellRunner.failure_hint`, since each raises its own error type. `ContainerBackend` uses it to identify an image that ships no `bash`, matching only on markers the runtime itself emits and on the shell as a quoted token — a shell that started and then failed on a CRLF script says "no such file or directory" too, and diagnosing that as a missing shell would cost its author more time than saying nothing.

**CompositeEnvironment** routes env strings to the correct backend based on the URI scheme. Container env URIs go to `ContainerBackend`; everything else goes to `LocalEnvironment`.

Foreign execution environments do not support Python tasks. `env=...` is valid on shell, notebook, and script tasks only. This keeps foreign execution command-oriented and avoids requiring the Ginkgo runtime to be importable inside every target environment. This is enforced at validation time before any work starts.

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
