---
title: "Remote Execution"
---

Ginkgo can send single tasks to cloud machines while the rest of the workflow
runs on your own machine. Use this when a task needs a GPU, a lot of memory, or
anything else your machine does not have.

## How It Works

The **driver** is the `ginkgo run` process on your machine. It submits remote
tasks as jobs to an **[executor](/ginkgo/guide/concepts/#glossary)**, such as a Kubernetes
cluster or GCP Batch, that you name in `ginkgo.toml`. A **worker** runs one
task on the executor, inside a Docker image you provide.

Tasks run on your machine by default. A task goes to an executor when:

- it sets `remote=True`. It goes to the run's default executor, named by
  `ginkgo run --executor <name>`.
- it sets `executor="name"`. It goes to that executor, whatever the default.
- it sets `gpu=N` and the local `--gpus` budget can't cover it. It goes to the
  default executor.

```python
@task(remote=True, memory="32Gi")
def large_computation(input_path: file) -> str:
    ...
```

```bash
ginkgo run --executor gpu-k8s flow.py
```

If a task has no executor to go to, the run fails before any task starts.
Ginkgo never falls back to your machine. Cached tasks are never sent, and each
retry submits a new job. A task's `threads`, `memory` and `gpu` become its
job's resource request. See
[Resources And Scheduling](/ginkgo/guide/resources/#where-a-task-runs).

## Installing an Executor

Executors are [plugins](/ginkgo/guide/concepts/#glossary), not part of core Ginkgo. Each executor
type comes from a separate package in the Ginkgo repository:

| Type | Backend | Plugin |
|------|---------|--------|
| `k8s` | Kubernetes | `ginkgo-k8s` |
| `batch` | GCP Batch | `ginkgo-gcp` |

Install Ginkgo with every first-party plugin:

```bash
uv tool install --force "ginkgo-workflow[all]"
```

Or add only the one you need:

```bash
uv tool install --force "ginkgo-workflow[pixi,gcp]"
```

`ginkgo plugins` lists the installed executor types. A config that names a type
no installed plugin provides fails before the run starts:

```text
[remote.executors.k8s]: No installed plugin provides executor type 'k8s'. Available: none. Install ginkgo-k8s to add it.
```

Install the executor plugins where you run `ginkgo run`. A worker does not need
them, because it runs tasks rather than submitting them.

## Named Executors

Every executor is a named table under `[remote.executors]`, with a `type` that
says which backend it uses. A workflow can use several at once:

```toml
# ginkgo.toml
[remote.executors.gpu-k8s]
type = "k8s"                 # an installed executor type: k8s | batch
namespace = "ml"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
gpu_type = "nvidia-l4"

[remote.executors.cheap-batch]
type = "batch"
project = "my-gcp-project"
region = "europe-west2"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
```

You choose the names. There is no built-in executor for each type, so
`--executor k8s` needs a table named `k8s`. An unknown name fails before any
task runs.

`remote=True` keeps a workflow portable: the same file can run on Kubernetes
at one site and GCP Batch at another. `--executor local`, the default, gives
the run no default executor, so a `remote=True` task fails the run. Ginkgo
contacts an executor only when it sends a task to it.

## The Artifact Store

Every remote run needs an artifact store: a bucket where Ginkgo passes files
and large results between the driver and its workers. Without it, the run
stops before anything is submitted.

```toml
[remote.artifacts]
store = "gs://my-bucket/ginkgo-artifacts/"
```

Each URI scheme comes from a plugin. See
[Plugins](/ginkgo/getting-started/installation/#plugins).

| Scheme | Object store | Plugin |
|--------|--------------|--------|
| `s3://` | Amazon S3 | `ginkgo-aws` |
| `gs://` | Google Cloud Storage | `ginkgo-gcp` |
| `oci://` | Oracle Cloud Infrastructure (OCI) Object Storage | `ginkgo-oci` |

The same schemes name [remote inputs](#remote-inputs). This `oci://` is
unrelated to the `oci://` container images in
[Environments](/ginkgo/guide/environments/#container-environments).

### OCI Object Storage Sign-In

Ginkgo signs `oci://` requests with the API key in `~/.oci/config` by default.
This applies wherever Ginkgo reads `oci://`: the artifact store and remote
inputs, on your machine and on a worker. Set another method in `[remote.oci]`:

```toml
[remote.oci]
auth = "oke_workload_identity"   # or "api_key", "instance_principal", "resource_principal"
region = "uk-london-1"
```

The environment variables `GINKGO_REMOTE_OCI_AUTH` and
`GINKGO_REMOTE_OCI_REGION` override these settings. A worker gets them that
way: set them in the executor's `env`. Workload Identity lets a pod on Oracle
Kubernetes Engine (OKE) sign requests as its Kubernetes service account, so no
key is mounted. See [Running On OKE](/ginkgo/guide/remote-oke/).

Workflow code that reads or writes OCI Object Storage itself can call
`ginkgo_oci.filesystem()`. It returns an `ocifs` filesystem that signs requests
the same way.

## Supported Executors

You can configure each type below as many `[remote.executors.<name>]` tables as
you need. The examples name each executor after its type.

### Kubernetes (`type = "k8s"`)

The Kubernetes executor submits each task as a Kubernetes Job, and needs the
`ginkgo-k8s` plugin. Nothing in it is specific to Google Kubernetes Engine
(GKE), except some defaults below.

```toml
# ginkgo.toml
[remote.executors.k8s]
type = "k8s"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
namespace = "ginkgo"
gpu_type = "nvidia-l4"        # GPU tasks only
```

`image` is required. The other settings are optional, and an unknown one is an
error:

| Setting | Meaning | Default |
|---------|---------|---------|
| `namespace` | Namespace the Jobs run in | `"default"` |
| `gpu_type` | GPU kind a GPU task asks for, as a node label value | none |
| `gpu_node_selector_key` | Node label that `gpu_type` sets | `"cloud.google.com/gke-accelerator"` |
| `service_account` | Kubernetes service account for worker pods | none |
| `pull_policy` | Image pull policy | `"IfNotPresent"` |
| `image_pull_secrets` | Secrets for a private worker image | none |
| `node_selector` | Node labels a pod must match | none |
| `tolerations` | Tolerations, as a list of tables | none |
| `ephemeral_storage` | Scratch disk for each pod | `"10Gi"` |
| `backoff_limit` | Times Kubernetes retries a failed pod. Raise it on Autopilot or spot nodes | `0` |
| `ttl_seconds_after_finished` | Seconds before Kubernetes deletes a finished Job | `3600` |
| `unschedulable_timeout` | Seconds before a pod that can't be placed fails | `300.0` |
| `env` | Environment variables for every worker container | none |
| `pod_annotations` | Annotations on every worker pod | none |
| `pod_labels` | Labels on every worker pod and Job | none |
| `fuse_image`, `fuse_privileged`, `fuse_annotations` | Settings for streamed inputs, covered later | |

A task's `threads` and `memory` set the pod's CPU and memory request and
limit, and `gpu` sets an `nvidia.com/gpu` limit.

`scripts/gke-setup.sh` and `scripts/gke-teardown.sh` in the Ginkgo repository
create and delete a GKE Autopilot cluster, registry and image. To run a whole
workflow inside an OKE cluster, see [Running On OKE](/ginkgo/guide/remote-oke/).

### GCP Batch (`type = "batch"`)

GCP Batch runs each task as a job on machines that Google manages. There is no
cluster to manage, and you pay only for compute time.

```toml
# ginkgo.toml
[remote.executors.batch]
type = "batch"
project = "my-gcp-project"
region = "europe-west2"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
gpu_type = "nvidia-l4"             # optional, for GPU tasks
gpu_driver_version = "LATEST"      # optional
max_run_duration = "3600s"         # optional, default 1 hour
service_account = "sa@proj.iam"    # optional
```

`project` and `image` are required, and `region` defaults to `europe-west2`. A
GPU task needs a `gpu_type`, from the task or from this table.

Before you start:

1. Install the `ginkgo-gcp` plugin where you run `ginkgo`, as
   [Installing an Executor](#installing-an-executor) shows. It brings
   `google-cloud-batch` and `google-cloud-logging` with it.
2. Turn on the Batch API:

   ```bash
   gcloud services enable batch.googleapis.com
   ```

## Worker Docker Image

A worker image needs Python 3.11 or later, Ginkgo, and the plugin for each
object store the worker reads. The repository's `Dockerfile` builds one:

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
COPY src/ginkgo/ src/ginkgo/
COPY plugins/ginkgo-aws/ plugins/ginkgo-aws/
COPY plugins/ginkgo-gcp/ plugins/ginkgo-gcp/
COPY plugins/ginkgo-oci/ plugins/ginkgo-oci/
COPY plugins/ginkgo-k8s/ plugins/ginkgo-k8s/
RUN pip install --no-cache-dir . ./plugins/ginkgo-aws ./plugins/ginkgo-gcp ./plugins/ginkgo-oci \
    ./plugins/ginkgo-k8s
ENTRYPOINT ["python", "-m", "ginkgo.remote.worker"]
```

This image also has `ginkgo-k8s`, so it can run the driver inside a cluster
too, as [Running On OKE](/ginkgo/guide/remote-oke/) does.

Build it for Linux, because cloud machines run Linux. This holds even when you
build on macOS:

```bash
docker buildx build --platform linux/amd64 -t <image-uri> --push .
```

### Adding Workflow Dependencies To The Image

Build your workflow's extra Python packages, such as PyTorch, into a project
image on top of the base image:

```dockerfile
FROM <registry>/ginkgo/worker:v2
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

`scripts/build-worker.sh` in the Ginkgo repository builds this image from
`[project].dependencies` in your `pyproject.toml`. It tags the image with a
hash of the dependencies and base image, skips the build if the registry
already has it, and prints the `image = "..."` line for `ginkgo.toml`:

```bash
export GINKGO_REGISTRY=europe-west2-docker.pkg.dev/my-project/ginkgo
cp <ginkgo-repo>/scripts/build-worker.sh ./scripts/build-worker.sh
./scripts/build-worker.sh
```

It works with any OCI-standard registry, such as Artifact Registry, GHCR, ECR
or Docker Hub. `GINKGO_BASE_IMAGE` and `GINKGO_REPO_NAME` override the base
image (default `${GINKGO_REGISTRY}/worker:v2`) and the image name (default
`<project-dir>-worker`).

## Code Sync

By default, the worker image must already contain your workflow code
(**baked** mode). For iterative development, use **sync** mode: Ginkgo uploads
your workflow package to the artifact store, and each worker unpacks it before
it runs the task. Add a `code` table to the executor:

```toml
# ginkgo.toml, alongside your executor config
[remote.executors.k8s.code]   # [remote.executors.<name>.code]
mode = "sync"
package = "my_workflow"  # directory name of your Python package
exclude = ["*.ipynb_checkpoints", "notebooks/scratch/"]  # optional extra excludes
```

Unchanged code is not uploaded again. Give each executor that needs code sync
its own `code` table. If one executor syncs and another the run uses doesn't,
Ginkgo warns, because tasks on the second would use the code baked into its
image.

## Remote Inputs

A remote input is a file in an object store, such as
`remote_file("gs://my-bucket/sample.bam")`. It reaches a worker in one of these
modes:

- **`direct`** (the default). The worker downloads the whole object before the
  task starts. The worker needs read access to the bucket.
- **`stage`**. The driver downloads the object and passes it to the worker
  through the artifact store. Use it for workers that can't read the source
  bucket.
- **`fuse`**. The worker mounts the bucket as a folder and reads the object as
  the task asks for it. This helps with sparse reads, such as BAM index
  lookups, and avoids waiting for a multi-GB download. Each object-store plugin
  brings its own FUSE driver: mountpoint-s3, gcsfuse or rclone.

A remote input works anywhere in a task's arguments, including inside a list,
tuple or dict.

### What A Worker Needs To Read

Grant the worker's identity (the Kubernetes or GCP Batch service account) read
and write access to the artifact store, and, in `direct` mode, read access to
the bucket of every remote input. A worker without access fails, and the error
names the URI. To keep source-bucket access on the driver alone, use `stage`
mode.

### Choosing The Mode

Set the mode on an input, or as a default for a task:

```python
from ginkgo import file, remote_file, task

@task(remote=True, remote_input_access="fuse", streaming_compatible=True)
def count_reads(bam: file) -> int:
    import pysam
    with pysam.AlignmentFile(str(bam), "rb") as f:
        return sum(1 for _ in f.fetch("chr1", 1_000_000, 1_001_000))

bam = remote_file("gs://my-bucket/sample.bam", access="fuse")
count_reads(bam=bam)
```

Set defaults for the whole run under `[remote.access]`:

```toml
[remote.access]
default = "direct"         # or "stage", "fuse"
auto_fuse = false          # true: use fuse for large inputs
auto_fuse_min_bytes = 2147483648  # 2 GiB default; the size auto_fuse starts at
default_for_pattern = [    # optional; the first matching glob wins
    { glob = "*.bam", access = "fuse" },
]
```

A pattern matches the object's key or its full URI. For each input, Ginkgo uses
the first of these that sets a mode:

1. `access=` on the input.
2. `remote_input_access=` on the task.
3. The first `default_for_pattern` entry that matches.
4. `auto_fuse`, for an input at least `auto_fuse_min_bytes` in size.
5. `[remote.access] default`.

Set `streaming_compatible=False` on a task that can't read a file as a stream.
Ginkgo then never uses `fuse` for it, and downloads its inputs instead.

### Downloading Many Inputs

Ginkgo fetches remote inputs 16 at a time across the run, whatever `-j` is. So
a task with thousands of inputs doesn't wait for them one by one. Change
the number with `[remote] staging_jobs`, or with the `GINKGO_STAGING_JOBS`
environment variable, which wins. A worker downloads its `direct` inputs the
same number at a time.

### A Worker Image For Streaming

`fuse` mode needs a worker image with `gcsfuse`, `mountpoint-s3` or `rclone`.
On most clouds it also needs a privileged container. The repository has
`Dockerfile.worker-fuse` for this:

```bash
docker buildx build --platform linux/amd64 \
  -f Dockerfile.worker-fuse \
  -t <registry>/ginkgo/worker-fuse:latest --push .
```

Set it in the executor table. Ginkgo uses `fuse_image` instead of `image` for a
task that needs `fuse`:

```toml
[remote.executors.k8s]
type = "k8s"
image = "<registry>/ginkgo/worker:v2"                # regular tasks
fuse_image = "<registry>/ginkgo/worker-fuse:latest"  # tasks that need fuse
fuse_privileged = true   # required on EKS, GKE Standard, GCP Batch
fuse_annotations = { "gke-gcsfuse/volumes" = "true" }  # optional override
```

On Kubernetes, a `fuse` pod gets the annotation `gke-gcsfuse/volumes: "true"`
by default, which adds GKE's gcsfuse sidecar. On GKE Autopilot, keep
`fuse_privileged = false`, because Autopilot rejects privileged pods. On a
cluster that is not GKE, set `fuse_annotations` to what your cluster needs, or
to `{}`.

### When Fuse Falls Back

If a mount fails, the worker downloads that input instead, and the CLI shows a
notice. A mount can fail because the driver is missing, `/dev/fuse` is not
available, or permission is denied:

```
⚠ FUSE access fell back to staging: gcsfuse failed: rc=1 ...
```

The task's provenance also records the reason, in
`remote_input_access.fallback_reason`. So a fallback is never silent.

### Diagnostics

```bash
ginkgo doctor
```

`ginkgo doctor` reports which FUSE drivers are available, whether `/dev/fuse`
exists, and whether `fuse_image` is set.

## CLI Feedback

The CLI shows these states for a remote task:

| Symbol | State | Meaning |
|--------|-------|---------|
| `⚙` | preparing env | The environment (image or dependencies) is being prepared |
| `↓` | staging | Remote inputs are downloading before the task starts |
| `↑` | submitted | The job exists and is waiting for cloud resources |
| `◐` | running | The pod or container is running the task |

The header line also shows the executor:

```
☁️  Running on Kubernetes (CPU 8.1%  RSS 258 MiB  Procs 4)
☁️  Running on GCP Batch (CPU 8.1%  RSS 258 MiB  Procs 4)
```

## Provenance

The [provenance](/ginkgo/guide/concepts/#glossary) database records these facts for each remote
task:

- `execution_backend`: where the task ran. This is `local`, or the executor's
  name, such as `gpu-k8s` or `k8s`.
- `remote_job_id`: the Kubernetes Job name or the GCP Batch job ID.
- `resources`: the CPU, memory and GPU requests.
- The pod logs, captured when the task finishes.

`ginkgo runs show <run_id>` shows all of this.
