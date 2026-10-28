# Remote Execution

Ginkgo can send single tasks to cloud machines while the rest of the workflow
runs on your own machine. Use this when a task needs a GPU, a lot of memory, or
anything else your machine does not have.

## How It Works

A few terms come first:

- The **driver** is the `ginkgo run` process. It runs on your machine, schedules
  the tasks, and submits remote tasks as jobs.
- An **[executor](concepts.md#glossary)** is a configured place where tasks can run,
  such as a Kubernetes cluster or GCP Batch. You name executors in
  `ginkgo.toml`.
- A **worker** is the process that runs one task on the executor. It runs inside
  a Docker image you provide.

Tasks run on your machine by default. A task goes to an executor in one of these
ways:

- `remote=True` sends the task to the run's **default executor**. That is the
  executor `ginkgo run --executor <name>` names.
- `executor="name"` sends the task to that executor, whatever the run's default
  is.
- `gpu=N` sends the task to the default executor only when the local `--gpus`
  budget can't cover it. A `gpu=1` task still runs on your machine with
  `ginkgo run --gpus 1`, even when `--executor` is set.

If a task's route has no executor to use, the run fails before any task starts.
Ginkgo never falls back to running it on your machine.

Ginkgo checks the cache before it sends a task. So only tasks that really need
to run reach the cloud, and a rerun skips the finished tasks without contacting
it. Each retry of a remote task submits a new job.

```python
from ginkgo import file, task

# Runs locally (default).
@task()
def preprocess(data_path: file) -> str:
    ...

# Runs locally if --gpus covers the request; otherwise goes to the
# run's default executor (requires --executor to be set).
@task(threads=8, memory="16Gi", gpu=1)
def train_model(dataset: str) -> str:
    ...

# Always remote, on whichever executor --executor names.
@task(remote=True, memory="32Gi")
def large_computation(input_path: file) -> str:
    ...

# Always on this executor, whatever the run default is.
@task(executor="gpu-k8s", gpu=1, gpu_type="nvidia-l4")
def train(dataset: str) -> str:
    ...
```

Run the workflow with a default executor, named after a table in
`[remote.executors]`:

```bash
ginkgo run --executor gpu-k8s flow.py
# or
ginkgo run --executor cheap-batch flow.py
```

A task's `threads`, `memory` and `gpu` become the resource request of its remote
job. See [Resources And Scheduling](resources.md#where-a-task-runs) for how
remote tasks count against the run's budgets.

## Installing an Executor

Executors are [plugins](concepts.md#glossary), not part of core Ginkgo. Each executor
type comes from a separate package in the Ginkgo repository:

| Type | Backend | Plugin |
|------|---------|--------|
| `k8s` | Kubernetes | `ginkgo-k8s` |
| `batch` | GCP Batch | `ginkgo-gcp` |

Install Ginkgo with every first-party plugin:

```bash
uv tool install --force "ginkgo[all] @ git+https://github.com/sanjaynagi/ginkgo.git@main"
```

Or add only the one you need:

```bash
uv tool install --force "ginkgo @ git+https://github.com/sanjaynagi/ginkgo.git@main" \
  --with "ginkgo-gcp @ git+https://github.com/sanjaynagi/ginkgo.git@main#subdirectory=plugins/ginkgo-gcp"
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
says which backend it uses. A workflow can use several backends at once. For
example, training on a GPU Kubernetes namespace, and bulk work on a cheap batch
queue. Name each executor, and route tasks to it by name:

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

Each table takes a `type` and the settings for that type. You choose the name.
`--executor k8s` needs a table named `k8s`, because there is no built-in
executor for each type.

- **`@task(executor="gpu-k8s")`** always runs on that executor. An unknown name
  fails before any task runs, and the error lists the configured names.
- **`@task(remote=True)`** runs on the run's default executor. This keeps a
  workflow portable. The same file can run on Kubernetes at one site and on GCP
  Batch at another, with no code change.
- **`--executor local`** is the default. It gives the run no default executor.
  Tasks that name an executor still go to it. A `remote=True` task, or a GPU
  task that doesn't fit `--gpus`, fails the run before any task starts.

Ginkgo contacts an executor only when it sends the first task to it. So a
configured executor that no task uses is never contacted.

## The Artifact Store

Every remote run needs an artifact store. This is a bucket where Ginkgo passes
files and large results between the driver and its workers. Set it in
`ginkgo.toml`:

```toml
[remote.artifacts]
store = "gs://my-bucket/ginkgo-artifacts/"
```

Without it, a task placed on an executor stops the run before anything is
submitted.

The store's URI scheme says which object store it uses. Each scheme comes from a
plugin. See [Plugins](../getting-started/installation.md#plugins).

| Scheme | Object store | Plugin |
|--------|--------------|--------|
| `s3://` | Amazon S3 | `ginkgo-aws` |
| `gs://` | Google Cloud Storage | `ginkgo-gcp` |
| `oci://` | Oracle Cloud Infrastructure (OCI) Object Storage | `ginkgo-oci` |

The same schemes name remote input files, which a later section covers. This
`oci://` is unrelated to the `oci://` container images in
[Environments](environments.md#container-environments).

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
key is mounted. See [Running On OKE](remote-oke.md).

Workflow code that reads or writes OCI Object Storage itself can call
`ginkgo_oci.filesystem()`. It returns an `ocifs` filesystem that signs requests
the same way.

## Supported Executors

You can configure each type below as many `[remote.executors.<name>]` tables as
you need. The examples name each executor after its type.

### Kubernetes (`type = "k8s"`)

The Kubernetes executor submits each task as a Kubernetes Job. A Job runs a pod
until it finishes, and a pod is one or more running containers on the cluster.
The executor has run on Google Kubernetes Engine (GKE) Autopilot. Nothing in it
is specific to GKE, except some defaults in the table below. It needs the
`ginkgo-k8s` plugin.

```toml
# ginkgo.toml
[remote.executors.k8s]
type = "k8s"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
namespace = "ginkgo"
gpu_type = "nvidia-l4"        # GPU tasks only
```

`image` is required. These settings are optional:

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

An unknown setting is an error, so a misspelt key fails at once.

A task's `threads` sets the pod's CPU request and limit, and `memory` sets its
memory request and limit. `gpu` sets an `nvidia.com/gpu` limit.

To set up a GKE Autopilot cluster, a registry and an image, and to delete them
when you are done, run the scripts in the Ginkgo repository:

```bash
bash scripts/gke-setup.sh     # creates cluster, registry, builds/pushes image
bash scripts/gke-teardown.sh   # deletes everything when done
```

To run a whole workflow inside an OKE cluster, see
[Running On OKE](remote-oke.md).

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
too, as [Running On OKE](remote-oke.md) does.

Build it for Linux, because cloud machines run Linux. This holds even when you
build on macOS:

```bash
docker buildx build --platform linux/amd64 -t <image-uri> --push .
```

### Adding Workflow Dependencies To The Image

Most workflows need extra Python packages, such as PyTorch, scikit-learn or
biopython. Keep them out of Ginkgo's base image. Build a project image on top of
the base image instead, with your workflow's dependencies:

```dockerfile
FROM <registry>/ginkgo/worker:v2
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

Here `requirements.txt` lists your workflow's dependencies. The helper script
`scripts/build-worker.sh` in the Ginkgo repository builds this image for you.
It does these steps:

1. Reads `[project].dependencies` from your project's `pyproject.toml`.
2. Names the image tag after a hash of the dependency list and the base image,
   so it rebuilds only when either changes.
3. Skips the build if the registry already has that image.
4. Prints the `image = "..."` line to paste into `ginkgo.toml`.

```bash
export GINKGO_REGISTRY=europe-west2-docker.pkg.dev/my-project/ginkgo
cp <ginkgo-repo>/scripts/build-worker.sh ./scripts/build-worker.sh
./scripts/build-worker.sh
```

The script works with any registry that follows the Open Container Initiative
standard, for example Artifact Registry, GHCR, ECR, Docker Hub or Harbor. It
checks for an image with `docker manifest inspect`, not with a cloud-specific
tool.

Two environment variables change its defaults. `GINKGO_BASE_IMAGE` sets the base
image (default `${GINKGO_REGISTRY}/worker:v2`). `GINKGO_REPO_NAME` sets the image
name (default `<project-dir>-worker`).

## Code Sync

By default, the worker image must already contain your workflow code. This is
**baked** mode: your code is copied into the image when you build it.

For iterative development, use **sync** mode instead. Ginkgo packs your workflow
package into a tarball and uploads it to the artifact store. Each worker
downloads and unpacks it before it runs the task. Add a `code` table to the
executor:

```toml
# ginkgo.toml, alongside your executor config
[remote.executors.k8s.code]   # [remote.executors.<name>.code]
mode = "sync"
package = "my_workflow"  # directory name of your Python package
exclude = ["*.ipynb_checkpoints", "notebooks/scratch/"]  # optional extra excludes
```

Ginkgo names the bundle after a hash of its contents, so unchanged code is not
uploaded again. Executors with the same package and exclude list share one
bundle.

Give each executor that needs code sync its own `code` table. A
`[remote.executors.k8s.code]` table does not apply to an executor named
`batch`. If one executor syncs code and another that the run uses does not,
Ginkgo warns before the run starts. Tasks on the second executor would use the
older code baked into its image.

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
  the task asks for it. This uses FUSE (Filesystem in Userspace). It helps with
  sparse reads, such as BAM index lookups or reading a few Parquet columns. It
  also helps a task start without waiting for a multi-GB download.

Each object-store plugin brings its own FUSE driver: mountpoint-s3 for `s3://`,
gcsfuse for `gs://` and rclone for `oci://`.

Ginkgo reads a remote input the same way wherever it sits in a task's
arguments. That includes inside a list, tuple or dict, under any annotation or
none. It also makes no difference whether the workflow named the input or an
upstream task returned it.

### What A Worker Needs To Read

A worker reads and writes object storage itself. It reads and writes the
artifact store, for `file` and `folder` values and large results. In `direct`
mode, it also reads the bucket of every remote input it gets.

Grant the worker's identity read access to those source buckets, and read and
write access to the artifact store. The identity is the Kubernetes service
account, or the GCP Batch service account. A worker without access fails on the
input it can't read, and the error names the URI.

To keep source-bucket access on the driver alone, use `stage` mode. Set
`[remote.access] default = "stage"`, or `access="stage"` on the input.

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

On Kubernetes, a pod that needs `fuse` gets the annotation
`gke-gcsfuse/volumes: "true"` by default. On GKE, this annotation adds the
gcsfuse sidecar container to the pod. On GKE Autopilot, keep
`fuse_privileged = false`, because Autopilot rejects privileged pods. The
sidecar then mounts the bucket. On a cluster that is not GKE, set
`fuse_annotations` to the annotations your cluster needs, or to `{}` for none.

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

The [provenance](concepts.md#glossary) database records these facts for each remote
task:

- `execution_backend`: where the task ran. This is `local`, or the executor's
  name, such as `gpu-k8s` or `k8s`.
- `remote_job_id`: the Kubernetes Job name or the GCP Batch job ID.
- `resources`: the CPU, memory and GPU requests.
- The pod logs, captured when the task finishes.

`ginkgo runs show <run_id>` shows all of this.
