# Remote Execution

Ginkgo can dispatch individual tasks to cloud infrastructure while keeping the
rest of the workflow running locally. This is useful when specific tasks need
GPUs, large memory, or other resources not available on the local machine.

## How It Works

Remote execution is opt-in at the task level. `remote=True` dispatches to the
run's **default executor** — whichever one `--executor` names. `executor="name"`
pins a task to one specific configured executor whatever the run default is. A
`gpu=` requirement only goes remote when the local `--gpus` budget can't satisfy
it — a `gpu=1` task still runs locally if `ginkgo run --gpus 1` (or higher) is
passed, even with `--executor` set. Any route without a usable executor is a
build error rather than a silent local run. The evaluator resolves cache hits
before dispatching, so only tasks that genuinely need to execute are sent to the
cloud.

```python
from ginkgo import task

# Runs locally (default).
@task()
def preprocess(data_path: str) -> str:
    ...

# Runs locally if --gpus covers the request; otherwise dispatched to the
# run's default executor (requires --executor to be set).
@task(threads=8, memory="16Gi", gpu=1)
def train_model(dataset: str) -> str:
    ...

# Explicitly remote, on whichever executor --executor names.
@task(remote=True, memory="32Gi")
def large_computation(input_path: str) -> str:
    ...

# Always on this executor, whatever the run default is.
@task(executor="gpu-k8s", gpu=1, gpu_type="nvidia-l4")
def train(dataset: str) -> str:
    ...
```

Run the workflow with a remote executor:

```bash
ginkgo run --executor k8s flow.py
# or
ginkgo run --executor batch flow.py
# or any name from [remote.executors]
ginkgo run --executor cheap-batch flow.py
```

## Named Executors

A workflow can span several backends: training on a GPU Kubernetes namespace,
bulk work on a cheap batch queue. Name each one under `[remote.executors]` and
route tasks to it by name:

```toml
# ginkgo.toml
[remote.executors.gpu-k8s]
type = "k8s"                 # k8s | batch
namespace = "ml"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
gpu_type = "nvidia-l4"

[remote.executors.cheap-batch]
type = "batch"
project = "my-gcp-project"
region = "europe-west2"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"

[remote.executors.cheap-batch.code]
mode = "sync"
package = "my_workflow"
```

Each table takes a `type` plus the same settings the corresponding
single-executor section accepts (see below), and its own optional `code`
sub-table for code sync.

- **`@task(executor="gpu-k8s")`** always runs on that executor. An unknown name
  fails at build time, before any task runs, listing the configured names.
- **`@task(remote=True)`** runs on the run's default executor — the one
  `--executor` names — which keeps a workflow portable across sites: the same
  file runs on Kubernetes at one site and GCP Batch at another with no code
  change.
- **`--executor local`** (the default) leaves the run with no default executor.
  Tasks that name an executor still dispatch to it; `remote=True` tasks and
  GPU overflow fail with a build error.
- Executor clients are constructed on first dispatch, so a configured executor
  no task reaches is never contacted.

`[remote.k8s]` and `[remote.batch]` keep working and are read as executors
implicitly named `k8s` and `batch`, so `--executor k8s` and
`@task(executor="k8s")` both resolve to `[remote.k8s]`.

## Task Resource Declarations

The `@task` decorator accepts resource hints that control scheduling locally
and map to cloud resource requests remotely:

| Parameter | Type | Effect |
|-----------|------|--------|
| `threads` | `int` | CPU cores (local scheduler budget + pod CPU request) |
| `memory` | `str` | Memory in K8s notation, e.g. `"4Gi"` (local scheduler budget + pod memory request) |
| `gpu` | `int` | GPU count (no local effect; maps to `nvidia.com/gpu` on K8s, accelerator on GCP Batch) |
| `remote` | `bool` | Force dispatch to the run's default executor, even without GPU |
| `executor` | `str` | Force dispatch to a named executor from `[remote.executors]` |

## Supported Executors

Both backend types below can also be declared under `[remote.executors.<name>]`
with a `type` key, several times over, as shown above.

### Kubernetes (`--executor k8s`)

Submits tasks as `batch/v1` Jobs on any Kubernetes cluster. Works with GKE,
EKS, OKE, or any standard K8s installation.

```toml
# ginkgo.toml
[remote.k8s]
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
namespace = "ginkgo"
gpu_type = "nvidia-l4"        # GKE accelerator node selector (GPU tasks only)
service_account = "ginkgo-worker"  # optional
pull_policy = "IfNotPresent"       # optional
ttl_seconds_after_finished = 300   # auto-cleanup delay (default 3600)
node_selector = { "cloud.google.com/gke-nodepool" = "pool-1" }  # optional
tolerations = []                   # optional, list of V1Toleration dicts
ephemeral_storage = "10Gi"         # optional, per-pod scratch disk (default shown)
backoff_limit = 2                  # optional, K8s-level pod retries (default shown)
unschedulable_timeout = 300.0      # optional, seconds before an unschedulable pod fails
```

**Setup with GKE Autopilot:**

```bash
bash scripts/gke-setup.sh     # creates cluster, registry, builds/pushes image
bash scripts/gke-teardown.sh   # deletes everything when done
```

### GCP Batch (`--executor batch`)

Submits tasks as serverless GCP Batch jobs. No cluster to manage -- each job
runs on Google-managed infrastructure and you pay only for compute time.

```toml
# ginkgo.toml
[remote.batch]
project = "my-gcp-project"
region = "europe-west2"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
gpu_type = "nvidia-l4"             # optional, for GPU tasks
gpu_driver_version = "LATEST"      # optional
max_run_duration = "3600s"         # optional, default 1 hour
service_account = "sa@proj.iam"    # optional
```

**Prerequisites:**

```bash
gcloud services enable batch.googleapis.com
pip install google-cloud-batch google-cloud-logging
```

## Code Sync

By default, the worker image must already contain your workflow code ("baked"
mode). For iterative development, **code-sync** mode bundles your workflow
package as a tarball, uploads it to cloud storage, and each worker pod
downloads and extracts it before executing the task.

```toml
# ginkgo.toml — add alongside your executor config

[remote.k8s.code]       # or [remote.batch.code], or [remote.executors.<name>.code]
mode = "sync"
package = "my_workflow"  # directory name of your Python package
exclude = ["*.ipynb_checkpoints", "notebooks/scratch/"]  # optional extra excludes

[remote.artifacts]
store = "gs://my-bucket/ginkgo-artifacts/"
```

The code bundle is content-addressed (SHA-256). Unchanged code is not
re-uploaded, and executors sharing a package and exclude list share one
bundle rather than uploading it twice.

Code config is read from the executor that runs the task, and only from it.
A `[remote.k8s.code]` table does not apply to `--executor batch` — give each
executor that needs code sync its own `code` table. When one executor syncs
code and an executor the run dispatches to has no `code` table, the run warns
before starting, since tasks there would silently use the code baked into the
image.

## Worker Docker Image

The worker image needs Python 3.11+ and ginkgo installed:

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
COPY ginkgo/ ginkgo/
RUN pip install --no-cache-dir ".[cloud]"

ENTRYPOINT ["python", "-m", "ginkgo.remote.worker"]
```

Build for Linux (required for cloud VMs, even when building on macOS):

```bash
docker buildx build --platform linux/amd64 -t <image-uri> --push .
```

### Adding workflow dependencies on top of the base image

Most workflows need extra Python packages (PyTorch, scikit-learn, biopython,
...) that should not live in ginkgo's base image. The recommended pattern is
to build a **project-specific image** that extends the ginkgo worker base with
your `[project].dependencies` from `pyproject.toml`:

```dockerfile
FROM <registry>/ginkgo/worker:v2
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

A helper script, `scripts/build-worker.sh` in the ginkgo repo, automates
this. It reads `[project].dependencies` from your project's `pyproject.toml`,
content-addresses the image tag by hashing the dependency list (so it rebuilds
only when deps change), skips the build if the image already exists in the
registry, and prints the exact `image = "..."` line to paste into
`ginkgo.toml`:

```bash
export GINKGO_REGISTRY=europe-west2-docker.pkg.dev/my-project/ginkgo
cp <ginkgo-repo>/scripts/build-worker.sh ./scripts/build-worker.sh
./scripts/build-worker.sh
```

The script is registry-agnostic — it works against Artifact Registry, GHCR,
ECR, Docker Hub, Harbor, and any other OCI-compliant registry (uses
`docker manifest inspect` for the existence check, not cloud-specific CLIs).

Optional overrides via env var:
`GINKGO_BASE_IMAGE` (default: `${GINKGO_REGISTRY}/worker:v2`),
`GINKGO_REPO_NAME` (default: `<project-dir>-worker`).

## CLI Feedback

The CLI distinguishes remote task states:

| Symbol | State | Meaning |
|--------|-------|---------|
| `⚙` | preparing env | Environment (image/deps) being prepared |
| `↓` | staging | Remote inputs downloading/hydrating before the task starts |
| `↑` | submitted | Job created, waiting for cloud resources |
| `◐` | running | Pod/container is actively executing |

The header line also reflects the executor:

```
☁️  Running on Kubernetes (CPU 8.1%  RSS 258 MiB  Procs 4)
☁️  Running on GCP Batch (CPU 8.1%  RSS 258 MiB  Procs 4)
```

## Provenance

Remote task execution is fully tracked in run provenance:

- `execution_backend` records where a task ran: `local`, or the executor's
  name (`gpu-k8s`, `k8s`, ...)
- `remote_job_id` records the K8s job name or GCP Batch job ID
- `resources` records the CPU, memory, and GPU requests
- Pod logs are captured at task completion

All of this is visible in `ginkgo inspect run <run_id>`.

## Tips

- **Cache still works.** The evaluator checks cache before dispatching, so
  re-runs skip completed tasks without touching the cloud.
- **Local tasks stay local.** Only tasks with `gpu > 0`, `remote=True`, or an
  `executor=` name are sent to an executor. Everything else runs on your
  machine.
- **Retries work.** Each retry submits a new cloud job. The existing retry
  mechanism applies unchanged.
- **Device portability.** Use `device = "auto"` in your config so PyTorch
  picks MPS on Mac, CUDA on cloud GPUs, or CPU as fallback.

## Remote-Input Access: Stage or Stream

Remote inputs (`gs://`, `s3://`, `oci://`, …) reach a worker in one of two
modes. **Stage** (default) downloads the whole object to local disk before
the task starts. **Fuse** mounts the bucket in-container and streams reads
on demand — useful for sparse random access (BAM index lookups, Parquet
column projection) or whole-file reads that would otherwise block on a
multi-GB download.

Declare the mode per input:

```python
from ginkgo import remote_file, task
from ginkgo.core.types import file

@task(remote=True, remote_input_access="fuse", streaming_compatible=True)
def count_reads(bam: file) -> int:
    import pysam
    with pysam.AlignmentFile(str(bam), "rb") as f:
        return sum(1 for _ in f.fetch("chr1", 1_000_000, 1_001_000))

bam = remote_file("gs://my-bucket/sample.bam", access="fuse")
count_reads(bam=bam)
```

The access policy resolves layered: `ref.access` → task decorator
(`remote_input_access=`) → pattern match → size-based auto-enable heuristic
→ config default.

Config defaults live under `[remote.access]`:

```toml
[remote.access]
default = "stage"          # or "fuse"
auto_fuse = false          # enable the size-based auto-fuse heuristic
auto_fuse_min_bytes = 2147483648  # 2GiB default; threshold for auto_fuse
default_for_pattern = [    # optional, first match wins over `default`
    { glob = "*.bam", access = "fuse" },
]
```

### Worker image for streaming

Fuse mode needs a worker image with `gcsfuse` / `mountpoint-s3` /
`rclone` and (on most clouds) a privileged container. The repo ships
`Dockerfile.worker-fuse`:

```bash
docker buildx build --platform linux/amd64 \
  -f Dockerfile.worker-fuse \
  -t <registry>/ginkgo/worker-fuse:latest --push .
```

Wire it in `ginkgo.toml`:

```toml
[remote.k8s]        # or [remote.batch]
image = "<registry>/ginkgo/worker:v2"            # regular workloads
fuse_image = "<registry>/ginkgo/worker-fuse:latest"  # swapped in when a
                                                     # task needs fuse
fuse_privileged = true   # required on EKS, GKE Standard, GCP Batch
fuse_annotations = { "gke-gcsfuse/volumes" = "true" }  # optional override
```

Any pod that needs fuse gets the `gke-gcsfuse/volumes: "true"` annotation
by default, regardless of `fuse_privileged` — override it with
`fuse_annotations` if you're not on GKE. GKE Autopilot rejects privileged
pods; use the gcsfuse CSI sidecar by keeping `fuse_privileged = false`, and
the default annotation wires the sidecar in automatically.

### When fuse falls back

If a mount fails (driver missing, `/dev/fuse` not accessible, permission
denied) the worker falls back to staged download and the CLI shows a
`TaskNotice`:

```
⚠ FUSE access fell back to staging: gcsfuse failed: rc=1 ...
```

The fallback reason is also recorded in
the task's `remote_input_access.fallback_reason`, so silent
downgrades can't hide.

### Diagnostics

```bash
ginkgo doctor
```

reports available FUSE drivers, `/dev/fuse` presence, and whether
`fuse_image` is configured.
