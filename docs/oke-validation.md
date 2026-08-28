# OCI Validation Checklist

Ginkgo's remote execution has never been exercised on Oracle Cloud. Two
paths to validate: the existing Kubernetes executor against OKE (works in
principle today — GKE/EKS/OKE are just clusters to the one k8s executor),
and, once built, the SSH executor against plain OCI VMs.

## Path 1 — OKE (Kubernetes executor, no code changes expected)

### Prerequisites

1. **Worker image.** Build an image containing ginkgo + task dependencies
   (+ `ocifs` for the artifact store) and push it to OCIR
   (`<region>.ocir.io/<tenancy>/<repo>:<tag>`).
2. **Cluster access.** `kubectl` works against the OKE cluster from the
   driver machine (`oci ce cluster create-kubeconfig ...`). The k8s executor
   uses the active kubeconfig.
3. **Image pull.** OCIR is private by default: create a `docker-registry`
   secret in the target namespace and attach it to the default (or a
   dedicated) service account. **Note:** the executor config has
   `service_account` but no `image_pull_secrets` passthrough — attaching the
   secret to the service account sidesteps this; if that proves awkward,
   adding `image_pull_secrets` to `[remote.k8s]` is a small change.
4. **Object storage.** A bucket for the artifact store, addressed as
   `oci://<bucket>@<namespace>/<prefix>/`.

### The key unknown: worker-side OCI auth inside the pod

The driver authenticates ocifs via `~/.oci/config` (ginkgo passes
`config_path`/`profile`/`region` through). The **pod** also needs to reach
object storage to hydrate inputs and stage outputs. Options, in order of
preference:

- **Instance principals**: give the OKE node pool's dynamic group a policy
  allowing object storage access; ocifs then needs to be told to use the
  instance-principal signer rather than a config file. Verify whether ginkgo's
  worker-side backend construction allows a config-file-less ocifs with
  instance principals — if not, this is the one likely (small) code fix from
  this validation: an `auth = "instance_principal"` knob on the OCI backend.
- **Mounted config**: mount an OCI config + key as a k8s secret into the pod
  and set the artifact-store config path accordingly. Works today with no
  code changes but distributes long-lived keys.

### Config sketch

```toml
[remote.k8s]
image = "eu-frankfurt-1.ocir.io/<tenancy>/ginkgo-worker:latest"
namespace = "ginkgo"
service_account = "ginkgo-runner"

[remote.k8s.code]
mode = "sync"
package = "my_workflow"

[remote.artifacts]
store = "oci://ginkgo-artifacts@<namespace>/runs/"
```

### Smoke sequence

1. `ginkgo doctor` on the driver — executor config extraction, driver checks.
2. Minimal workflow: one local task feeding one `@task(remote=True)` python
   task returning a small value. Run `--executor k8s`; verify the Job
   appears, completes, and the result folds into the run manifest.
3. Same but with a `file` input and `file` output — exercises artifact-store
   upload/hydrate/publish both directions (the real OCI test).
4. Code-sync check: edit the workflow module, rerun, confirm a new bundle is
   uploaded and the worker runs the new code.
5. Failure surface: point the task at a raising body; confirm exit code,
   `logs_tail` content in the failure panel, and retry behaviour.
6. Cancel: Ctrl-C mid-run; confirm the Job is deleted (ttl/cancel path) and
   no orphan pods remain.
7. (If GPUs on the pool) `gpu=1` + `gpu_type` → confirm `nvidia.com/gpu`
   limit lands; note the GKE-specific accelerator node-selector key
   (`cloud.google.com/gke-accelerator`) is meaningless on OKE — GPU node
   targeting on OKE must use `node_selector` config directly. Possible small
   follow-up: make the accelerator selector key configurable.

### Known OKE-specific watch-items

- `cloud.google.com/gke-accelerator` selector is GKE-only (harmless but dead
  on OKE).
- Fuse/streaming input mounts (`fuse_image`, privileged pods) untested on
  OKE; out of scope for the first pass — validate staged access only.

## Path 2 — SSH executor on OCI VMs (after implementation)

Per-VM setup: SSH key, Python env with ginkgo + deps + ocifs at
`worker_python`, OCI auth for object storage (instance principals make this
clean on OCI VMs), network egress to object storage. Then repeat smoke steps
2–6 with `--executor ssh`, plus: kill a VM mid-task and confirm the task
fails with actionable logs and retries on the surviving host.
