# Remote Execution

Ginkgo can dispatch individual tasks to cloud infrastructure while keeping the
rest of the workflow running locally. This is useful when specific tasks need
GPUs, large memory, or other resources not available on the local machine.

## How It Works

Remote execution is opt-in at the task level. Tasks that declare `gpu=` or
`remote=True` are dispatched to the configured executor; everything else runs
locally as usual. The evaluator resolves cache hits before dispatching, so
only tasks that genuinely need to execute are sent to the cloud.

```python
from ginkgo import task

# Runs locally (default).
@task()
def preprocess(data_path: str) -> str:
    ...

# Dispatched to the remote executor when --executor is set.
@task(threads=8, memory="16Gi", gpu=1)
def train_model(dataset: str) -> str:
    ...

# Explicitly remote, even without GPU.
@task(remote=True, memory="32Gi")
def large_computation(input_path: str) -> str:
    ...
```

Run the workflow with a remote executor:

```bash
ginkgo run --executor k8s workflow.py
# or
ginkgo run --executor batch workflow.py
```

## Task Resource Declarations

The `@task` decorator accepts resource hints that control scheduling locally
and map to cloud resource requests remotely:

| Parameter | Type | Effect |
|-----------|------|--------|
| `threads` | `int` | CPU cores (local scheduler budget + pod CPU request) |
| `memory` | `str` | Memory in K8s notation, e.g. `"4Gi"` (local scheduler budget + pod memory request) |
| `gpu` | `int` | GPU count (no local effect; maps to `nvidia.com/gpu` on K8s, accelerator on GCP Batch) |
| `remote` | `bool` | Force remote dispatch even without GPU |

## Supported Executors

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
ttl_seconds_after_finished = 300   # auto-cleanup delay
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

[remote.k8s.code]       # or [remote.batch.code]
mode = "sync"
package = "my_workflow"  # directory name of your Python package

[remote.artifacts]
store = "gs://my-bucket/ginkgo-artifacts/"
```

The code bundle is content-addressed (SHA-256). Unchanged code is not
re-uploaded.

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

## CLI Feedback

The CLI distinguishes remote task states:

| Symbol | State | Meaning |
|--------|-------|---------|
| `↑` | submitted | Job created, waiting for cloud resources |
| `◐` | running | Pod/container is actively executing |

The header line also reflects the executor:

```
☁️  Running on Kubernetes (CPU 8.1%  RSS 258 MiB  Procs 4)
☁️  Running on GCP Batch (CPU 8.1%  RSS 258 MiB  Procs 4)
```

## Provenance

Remote task execution is fully tracked in run provenance:

- `execution_backend` records whether a task ran locally or remotely
- `remote_job_id` records the K8s job name or GCP Batch job ID
- `resources` records the CPU, memory, and GPU requests
- Pod logs are captured at task completion

All of this is visible in `ginkgo inspect run <run_id>`.

## Tips

- **Cache still works.** The evaluator checks cache before dispatching, so
  re-runs skip completed tasks without touching the cloud.
- **Local tasks stay local.** Only tasks with `gpu > 0` or `remote=True` are
  sent to the executor. Everything else runs on your machine.
- **Retries work.** Each retry submits a new cloud job. The existing retry
  mechanism applies unchanged.
- **Device portability.** Use `device = "auto"` in your config so PyTorch
  picks MPS on Mac, CUDA on cloud GPUs, or CPU as fallback.
