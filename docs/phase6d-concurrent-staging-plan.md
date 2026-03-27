# Phase 6D Plan: Concurrent Worker-Affine Remote Staging

## Problem Definition

Phase 6B established remote references and staged access as the correctness
path for remote inputs. That works, but the current implementation stages
remote inputs synchronously during task preparation in the evaluator thread.

This has two immediate drawbacks:

1. **Serial staging bottleneck**: when multiple ready tasks need independent
   remote inputs, those downloads happen one at a time even if the run has
   spare execution capacity.
2. **Weak execution locality model**: staging currently happens as part of
   scheduler-side preparation, which does not map cleanly to future Kubernetes
   or cloud workers where staged files should live on the same worker that will
   execute the task.

The bioinformatics example makes this visible: multiple FASTQ inputs can be
staged independently, but they are currently hydrated in sequence before their
tasks are dispatched.

## Goals

- Allow multiple ready tasks to stage independent remote inputs concurrently.
- Keep staged files worker-local and treat staging as part of task execution
  rather than scheduler-global preprocessing.
- Preserve the current path-based task contract: tasks still receive ordinary
  local paths.
- Avoid duplicate downloads when multiple tasks require the same remote input
  concurrently.
- Keep the design compatible with future Kubernetes or cloud worker execution.
- Preserve explicit `staging` and `running` task lifecycle states in Rich CLI
  and JSONL output.

## Non-Goals

- Implementing byte-range reads, sparse hydration, or streaming mounts.
- Building a full distributed data-locality scheduler.
- Replacing staged access with a FUSE-like mount.
- Introducing a shared network filesystem for staged inputs.
- Publishing outputs to remote storage as part of this phase.

## Proposed Solution

Phase 6D should make staging an explicit execution phase with worker affinity.

The core model is:

- the scheduler decides which task is eligible to run
- a worker execution slot is reserved for that task
- remote inputs for that task are staged in that same worker context
- the task starts only after staging succeeds

This gives a clearer lifecycle:

```text
waiting -> staging -> running -> succeeded|failed|cached
```

For the current local runtime, "worker context" can still be implemented with a
local staging thread or per-task staging executor. For future Kubernetes
execution, the same contract maps to a pod-local worker that stages inputs into
its own writable volume before launching the task process.

## Design Principles

- Treat staging as part of task execution cost, not an external pre-step.
- Keep staging local to the execution worker that will consume the files.
- Use bounded concurrency for staging so object storage, disk, and local I/O
  are not flooded.
- Deduplicate in-flight staging of the same remote ref across tasks.
- Preserve the current explicit remote reference model so worker-side staging
  remains transport-agnostic.
- Avoid any design that assumes the scheduler and workers share a filesystem.

## Architecture

### 1. Explicit Staging Phase

Remote input handling should be separated from generic argument resolution.

Instead of hiding staging inside `_resolve_task_args()`, the evaluator should
represent it as a first-class phase:

- resolve non-remote literals and dependencies
- detect remote refs
- emit `task_staging`
- stage remote inputs
- rewrite staged args to local `file` / `folder` paths
- emit `task_started`
- launch task execution

This makes the lifecycle observable and removes the ambiguity where a task
appears "running" before its remote bytes are available locally.

### 2. Worker-Affine Staging

Staging should happen in the same worker context that will execute the task.

That implies the scheduler should:

- reserve a job/core/memory slot before staging begins
- bind the task to an execution context
- keep the task on that same context after staging completes

For local execution, this can still be implemented inside the current process,
but it should be modeled as worker-affine so the runtime contract already
matches a future Kubernetes worker.

For Kubernetes, the intended mapping is:

- one worker pod receives the task
- that pod stages remote inputs into `emptyDir`, local SSD, or another
  pod-local writable volume
- the same pod runs the task against those staged paths

### 3. Concurrent Staging Pool

Introduce a dedicated staging executor for I/O-bound remote hydration.

Properties:

- thread-based by default
- bounded concurrency, distinct from CPU task concurrency
- configurable independently from `jobs` and `cores`
- suitable for overlapping remote download latency

Suggested default:

- `staging_jobs = min(jobs, 4)` when unset

Possible future config:

```toml
[remote]
staging_jobs = 4
```

or environment override:

```bash
GINKGO_STAGING_JOBS=4
```

### 4. In-Flight Deduplication

Concurrent staging makes duplicate downloads more likely. The runtime should
therefore add an in-memory coordination layer above `StagingCache`.

Behavior:

- compute a normalized remote-ref identity key
- if no staging is in progress for that key, start one
- if staging is already in progress, attach as a waiter
- when the first staging completes, all waiters receive the same staged path

This is especially important for:

- repeated shared references in fan-out workflows
- multiple tasks consuming the same pinned remote file
- future multi-tenant workers where redundant downloads waste local bandwidth

### 5. Staging Root as Worker-Local Storage

The staging root should be treated as execution-worker-local by design.

The plan should explicitly assume:

- local runs may use `.ginkgo/staging/`
- cloud workers may use pod-local ephemeral storage
- workers do not depend on the scheduler's local disk

This keeps the contract compatible with:

- Kubernetes `emptyDir`
- node-local SSD
- container-local writable volumes

It intentionally avoids dependence on:

- NFS
- EFS-like shared POSIX mounts
- scheduler-side pre-hydrated shared directories

### 6. Scheduler and Worker Responsibilities

The long-term division should be:

**Scheduler responsibilities**

- dependency resolution
- cache-key planning
- task readiness
- resource-slot reservation
- worker assignment
- staging-state tracking

**Worker responsibilities**

- stage assigned remote refs locally
- rewrite remote refs to local paths
- execute the task
- report outputs, logs, and failures

This is a natural extension of the current local runtime and avoids a later
architecture split when remote workers are introduced.

## Proposed Runtime Flow

### Local Runtime

1. Scheduler marks a node ready.
2. Scheduler selects the node for execution and reserves capacity.
3. Node enters `staging` if it has remote inputs.
4. A staging worker stages those inputs into the local worker cache.
5. On success, node enters `running`.
6. Task executes using the staged local paths.

### Future Kubernetes Runtime

1. Scheduler assigns a ready node to a worker pod.
2. Worker pod stages remote inputs into pod-local storage.
3. Worker rewrites args to local paths.
4. Worker launches the task.
5. Worker reports completion and output metadata.

The key invariant is unchanged:

- the worker that stages is the worker that runs

## Implementation Stages

### Stage 1: Split staging from generic arg resolution

- Refactor evaluator flow so remote staging is a distinct phase.
- Emit `task_staging` only when a task actually has remote inputs.
- Emit `task_started` only after staging completes successfully.
- Preserve current cache-key semantics and path rewriting.

### Stage 2: Add bounded concurrent staging

- Introduce a staging executor in the evaluator.
- Allow multiple ready tasks with remote inputs to stage concurrently.
- Keep staging concurrency independent from CPU worker count.
- Ensure tasks without remote inputs are not delayed unnecessarily by staging
  queue behavior.

### Stage 3: Add in-flight staging deduplication

- Add a staging coordinator keyed by normalized remote-ref identity.
- Share one staging result across concurrent requesters.
- Ensure dedup works for both remote files and later folder-manifest identities.

### Stage 4: Formalize worker-local staging config

- Add explicit staging concurrency config.
- Keep staging-root config documented as worker-local.
- Make the runtime contract clear enough for Kubernetes pod-local volumes.

### Stage 5: Prepare remote-worker execution contract

- Define the payload boundary where remote refs remain explicit until the worker
  stages them.
- Ensure current local execution can evolve into this model without changing
  user-facing workflow code.

## Risks and Tradeoffs

- Reserving worker capacity during staging may reduce apparent scheduler
  throughput.
  This is acceptable because staging is part of the task's real execution cost.

- More concurrent downloads may increase pressure on object storage or local
  disk.
  This should be controlled with bounded staging concurrency and, if needed
  later, backend-specific throttling.

- In-flight dedup adds coordination state to the evaluator.
  This complexity is justified once staging becomes concurrent.

- Moving to worker-affine staging may make scheduler-side cache warmup less
  appealing.
  That is a deliberate tradeoff in favor of portability and future cloud
  correctness.

## Success Criteria

- Independent remote-input tasks can stage concurrently on a local run.
- The bioinformatics example no longer stages all remote inputs strictly one by
  one.
- Duplicate concurrent staging of the same remote input is deduplicated.
- Rich CLI and JSONL output show `staging` distinctly from `running`.
- Staging paths are treated as worker-local rather than scheduler-global.
- The execution contract remains compatible with future Kubernetes workers.

## Future Extensions

This plan should leave clean extension points for later work:

- task-aware prefetch
- folder-manifest-aware deduplication
- byte-range or partial download strategies
- worker-local cache eviction policies
- optional FUSE-like mounted access as a later optimization layer

None of those are prerequisites for concurrent worker-affine staged access.
