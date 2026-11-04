# Remote Execution

Ginkgo supports dispatching a workflow's tasks to cloud infrastructure while
the driver runs locally. Placement is decided per task: `executor="name"` pins
a task to one configured executor and `executor="local"` keeps it on the
driver; otherwise every task whose kind is `remote_capable` (`python`, `shell`
and `script`) goes to the run's *default* executor (`--executor`), and the rest
run on the driver. Notebook and subworkflow tasks therefore run on the driver
for now, even under `--executor`. Naming an executor on a kind that is not
remote-capable is a build error, as is a remote task that declares a container
`env=` and a locally placed task whose `gpu` requirement exceeds the local
`--gpus` budget — a task never silently runs somewhere its requirement cannot be met. Cached tasks are never dispatched:
placement is resolved only after a cache miss. Remote-placed tasks consume a
`--jobs` slot but no local core/memory/GPU budget; their resources are
satisfied by the executor.

## Executor Registry

`runtime/executor_registry.py` owns the name → backend mapping. It parses
`[remote.executors.<name>]` tables, each with a `type`, backend settings, and
an optional `code` sub-table. These tables are the only way to configure an
executor. A `type` is an `ExecutorType` some installed plugin contributes (see
[Plugins](plugins.md)): `k8s` from ginkgo-k8s, `batch` from ginkgo-gcp. A type
no installed plugin provides fails while the config is parsed, naming the table,
the available types and, for a first-party type, the distribution to install.
`--executor` selects the run default by executor *name*, so `--executor k8s`
needs a table named `k8s`; `local` means the run has none. Backends are constructed lazily on first dispatch, so a configured
executor no task reaches is never contacted, and unknown names — on the flag or
on a task — fail at build time with the configured names listed. Lazy
construction would otherwise defer settings errors to the first dispatch, so
once the graph is known `ExecutorRegistry.validate_settings` checks the
executors the run will reach (each `ExecutorType.required_settings` is the one
home for which keys a backend cannot be built without); a table no task reaches
stays unvalidated, so a stale `[remote.executors.k8s]` never fails a local run.

`Placement.resolve` (`runtime/placement.py`) returns the executor's name or `None` for local
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

`remote/executor_support.py` holds the helpers an executor plugin uses to speak
the worker protocol: `encode_payload` (the `GINKGO_WORKER_PAYLOAD` value),
`generate_job_name`, `payload_requires_fuse`, and `parse_worker_output` (the
worker's JSON result line, read from job logs).

## Executor Implementations

Core ships no executor. Both implementations live in first-party plugin
distributions under `plugins/` (see [Plugins](plugins.md#first-party-plugins)).

**KubernetesExecutor** (`ginkgo_k8s/executor.py`, distribution ginkgo-k8s,
type `k8s`) submits `batch/v1` Jobs to any
Kubernetes cluster. Resource declarations on `@task` map to pod resource
requests: `threads` → CPU, `memory` → memory, `gpu` → `nvidia.com/gpu`. GPU
tasks receive a `cloud.google.com/gke-accelerator` node selector when an
accelerator type is available — `@task(gpu_type=...)` per task, falling back
to the executor-level `gpu_type` — enabling automatic GPU node provisioning
on GKE Autopilot.

**GCPBatchExecutor** (`ginkgo_gcp/batch.py`, distribution ginkgo-gcp, type
`batch`) submits jobs to GCP Batch, a
serverless batch compute service. No cluster required — each job runs on
Google-managed infrastructure. GPU tasks use the Batch accelerator allocation
policy; a GPU task with no accelerator type (task-level or executor-level) is
rejected at submit rather than silently scheduled without a GPU. Job logs are retrieved from Cloud Logging.

## Remote Worker

The worker entry point (`remote/worker.py`) runs as
`python -m ginkgo.remote.worker` inside the container. It:

1. Reads the task payload from `GINKGO_WORKER_PAYLOAD` (base64-encoded JSON)
2. Optionally downloads and extracts a code bundle (code-sync mode)
3. Downloads `file`/`folder` inputs and large values from the artifact store
   into its own scratch directory
4. Calls the standard `run_task()` worker function. The payload carries no
   driver-local paths (log files, transport directory), so task
   output goes to the container's stdout/stderr and appears in the pod logs.
   A driver kind's body (shell, script) returns its directive here, against
   the pod's paths, and `run_task` runs it on the kind's own runner in a local
   subprocess. The runner's result goes back as an ordinary encoded value,
   marked `ran_directive` so the driver completes the task with it, and a
   failed command's exit code travels in the error so `retry_on_exit_codes`
   still applies. For a task with a Pixi `env=`, the worker first builds the
   same `EnvironmentRouter` the driver does, rooted at the extracted code
   bundle (or its working directory without one), and searches the env
   directories `resolve_envs_workflow_root` names there. It checks the env's
   `env_identity` against the one the driver sent in the payload, fails the
   task naming the env if they differ, and runs `prepare` (`pixi install`)
   before the command runs through the env's `exec_argv`. A remote task with
   a container `env=` is rejected at placement, since the worker has no
   container runtime
5. Uploads outputs and large result values, then prints a JSON result line to
   stdout for the handle to parse. A result holding task expressions is
   pickled whole with its `file`/`folder` leaves uploaded beside it, and the
   driver expands it as it would a local task's

The same worker image serves both K8s and GCP Batch executors. It installs
core and the object-store plugins (ginkgo-aws, ginkgo-gcp, ginkgo-oci), so a
worker can stage and publish to any supported store, and the `pixi` binary
with ginkgo-pixi, so it can install a task's Pixi env. It needs no executor
plugin, since it runs tasks and never submits them; ginkgo-gcp brings the Batch
client libraries along with its `gs://` store regardless.

## Code Sync

Two modes for making workflow code available to remote workers:

- **Baked** (default): the worker image already contains the code.
- **Sync**: the evaluator creates a tarball of the workflow package, uploads
  it to cloud storage (content-addressed by SHA-256), and includes the bundle
  coordinates in the task payload. Workers download and extract the bundle
  before importing task functions. The bundle carries each env's manifest
  and lock but leaves out `.pixi/`, the driver's install, so a worker
  installs its own copy of an env in the bundle's scratch directory the
  first time it runs a task in it. With a baked image, an env the image
  installed at build time makes that `pixi install` a quick check.

Code sync is configured per executor via `[remote.executors.<name>.code]`
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
- `ginkgo runs show --json` surfaces all remote metadata.
- Pod/container logs are captured at task completion via `handle.logs_tail()`.

## Object Stores

Each store is a plugin contribution (`ObjectStoreType`): `s3://` from
ginkgo-aws (`s3fs`), `gs://` from ginkgo-gcp (`gcsfs`) and `oci://` from
ginkgo-oci (`ocifs`). Each subclasses `FsspecBackend`
(`remote/fsspec_backends.py`), which provides `head()`, `download()`,
`upload()` and `list_prefix()` over the plugin's filesystem, and is used for
remote input staging, output publishing and code bundles.

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
│   ├── executor_support.py  # worker-protocol helpers for executor plugins
│   ├── fsspec_backends.py   # FsspecBackend, the base the store plugins extend
│   ├── publisher.py         # RemotePublisher for remote outputs
│   ├── resolve.py           # resolve_backend() factory
│   ├── staging.py           # remote input staging
│   └── worker.py            # remote worker entry point
├── runtime/
│   ├── executor_registry.py # named executors: config, lookup, lazy build
│   ├── remote_executor.py   # RemoteExecutor / RemoteJobHandle protocols
│   └── ...
└── ...

plugins/                     # repository root, not part of the ginkgo package
├── ginkgo-k8s/src/ginkgo_k8s/
│   └── executor.py          # KubernetesExecutor + KubernetesJobHandle
└── ginkgo-gcp/src/ginkgo_gcp/
    └── batch.py             # GCPBatchExecutor + GCPBatchJobHandle
```
