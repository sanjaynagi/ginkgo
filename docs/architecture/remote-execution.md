# Remote Execution

Ginkgo supports dispatching individual tasks to cloud infrastructure while the
rest of the workflow runs locally. Placement is decided per task from its
declared requirement and the available capability: `executor="name"` pins a
task to one configured executor, `remote=True` is an explicit directive to the
run's *default* executor (`--executor`), and a task whose `gpu` requirement
exceeds the local `--gpus` budget is dispatched to that default when one is
configured. Any route without a usable executor is a build error — a task never
silently runs somewhere its requirement cannot be met. Remote-placed tasks
consume a `--jobs` slot but no local core/memory/GPU budget; their resources
are satisfied by the executor.

## Executor Registry

`runtime/executor_registry.py` owns the name → backend mapping. It parses
`[remote.executors.<name>]` tables (each with a `type` of `k8s` or `batch`,
backend settings, and an optional `code` sub-table), plus the legacy
`[remote.k8s]` / `[remote.batch]` sections as implicitly named executors
`k8s` and `batch`. `--executor` selects the run default; `local` means the run
has none. Backends are constructed lazily on first dispatch, so a configured
executor no task reaches is never contacted, and unknown names — on the flag or
on a task — fail at build time with the configured names listed.

`_resolve_placement` returns the executor's name or `None` for local
placement; `NodeRun.executor_name` carries it, and `NodeRun.remote` is the
derived predicate. That name is what events and provenance record as
`execution_backend` (`local` for local placement), and what
`RemoteDispatchManager` uses to look up the backend, the per-executor code
bundle, and the per-executor submission counters.

## Remote Executor Protocol

The evaluator dispatches remote work through a `RemoteExecutor` protocol
(`runtime/remote_executor.py`). The protocol defines:

- `RemoteExecutor.submit(attempt=...)` → `RemoteJobHandle`
- `RemoteJobHandle.state()` → `RemoteJobState` (PENDING / RUNNING / SUCCEEDED / FAILED / CANCELLED)
- `RemoteJobHandle.result()` → `RemoteJobResult` (blocking wait + result)
- `RemoteJobHandle.cancel()` / `RemoteJobHandle.logs_tail()`

This keeps executor implementations fully decoupled from the scheduling loop.
The evaluator polls handles on dedicated watcher threads and processes results
through the same code path as local worker completions.

## Executor Implementations

**KubernetesExecutor** (`remote/kubernetes.py`) submits `batch/v1` Jobs to any
Kubernetes cluster. Resource declarations on `@task` map to pod resource
requests: `threads` → CPU, `memory` → memory, `gpu` → `nvidia.com/gpu`. GPU
tasks receive a `cloud.google.com/gke-accelerator` node selector when an
accelerator type is available — `@task(gpu_type=...)` per task, falling back
to the executor-level `gpu_type` — enabling automatic GPU node provisioning
on GKE Autopilot.

**GCPBatchExecutor** (`remote/gcp_batch.py`) submits jobs to GCP Batch, a
serverless batch compute service. No cluster required — each job runs on
Google-managed infrastructure. GPU tasks use the Batch accelerator allocation
policy; a GPU task with no accelerator type (task-level or executor-level) is
rejected at submit rather than silently scheduled without a GPU. Job logs are retrieved from Cloud Logging.

## Remote Worker

The worker entry point (`remote/worker.py`) runs as
`python -m ginkgo.remote.worker` inside the container. It:

1. Reads the task payload from `GINKGO_WORKER_PAYLOAD` (base64-encoded JSON)
2. Optionally downloads and extracts a code bundle (code-sync mode)
3. Calls the standard `run_task()` worker function
4. Prints a JSON result line to stdout for the handle to parse

The same worker image serves both K8s and GCP Batch executors.

## Code Sync

Two modes for making workflow code available to remote workers:

- **Baked** (default): the worker image already contains the code.
- **Sync**: the evaluator creates a tarball of the workflow package, uploads
  it to cloud storage (content-addressed by SHA-256), and includes the bundle
  coordinates in the task payload. Workers download and extract the bundle
  before importing task functions.

Code sync is configured via `[remote.k8s.code]` or `[remote.batch.code]`
with `mode = "sync"` and `package = "<dir>"`. The bundle is published to the
remote artifact backend configured in `[remote.artifacts]`.

## Remote Provenance and Events

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

## GCS Backend

`GCSFileSystemBackend` (`remote/fsspec_backends.py`) extends the fsspec base
class with Google Cloud Storage support via `gcsfs`. It supports
`head()`, `download()`, `upload()`, and `list_prefix()` operations and is
used for both remote input staging and code bundle publishing.

## Infrastructure Scripts

- `scripts/gke-setup.sh` — creates a GKE Autopilot cluster, Artifact Registry,
  IAM bindings, K8s namespace, and builds/pushes the worker image.
- `scripts/gke-teardown.sh` — deletes the cluster and registry.

## Package Layout (Remote)

```text
ginkgo/
├── remote/
│   ├── backend.py           # ObjectStore protocol
│   ├── code_bundle.py       # tarball creation, publish, download+extract
│   ├── fsspec_backends.py   # S3, OCI, GCS fsspec backends
│   ├── gcp_batch.py         # GCPBatchExecutor + GCPBatchJobHandle
│   ├── kubernetes.py        # KubernetesExecutor + KubernetesJobHandle
│   ├── publisher.py         # RemotePublisher for remote outputs
│   ├── resolve.py           # resolve_backend() factory
│   ├── staging.py           # remote input staging
│   └── worker.py            # remote worker entry point
├── runtime/
│   ├── executor_registry.py # named executors: config, lookup, lazy build
│   ├── remote_executor.py   # RemoteExecutor / RemoteJobHandle protocols
│   └── ...
└── ...
```
