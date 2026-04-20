# Remote Input Access

Ginkgo supports two strategies for making a remote object (an
`s3://` / `gs://` / `oci://` URI referenced by a task input) available
inside the worker pod:

1. **Staged access** (default). The driver downloads the object to a
   local content-addressed cache, uploads it to the shared remote
   artifact CAS, and the worker downloads it again into the pod before
   the task starts. Three passes over the same bytes.
2. **FUSE-mounted access**. The driver does not touch the bytes. The
   worker pod mounts the bucket (using `gcsfuse`, `mountpoint-s3`, or
   `rclone mount`) and the task reads byte ranges on demand. Sparse
   access patterns pay only for the bytes the task actually touches.

Streaming is **opt-in** per input, per task, or via config default. The
staged path remains the correctness fallback.

## Strategy interface

Both strategies satisfy `RemoteInputAccess` in
`ginkgo/remote/access/__init__.py`:

```python
class RemoteInputAccess(Protocol):
    def materialize_file(self, *, ref: RemoteFileRef) -> Path: ...
    def materialize_folder(self, *, ref: RemoteFolderRef) -> Path: ...
    def release(self, *, paths: Iterable[Path]) -> None: ...
    def stats(self) -> AccessStats: ...
```

Implementations:

- `StagedAccess` — wraps the existing `StagingCache`.
- `MountedAccess` — establishes one mount per unique `(scheme, bucket)`
  pair referenced by a task, torn down in `close()`.

`AccessStats` carries the per-mount counters (bytes read, range
requests, cache hits, mount / unmount seconds). The worker folds these
into the result envelope as `remote_input_access`; the evaluator folds
them into provenance via
`RunProvenanceRecorder.update_task_extra(remote_input_access=...)`.

## Per-input policy resolution

`ginkgo/remote/access/resolver.py::resolve_access` layers policies in
order of decreasing precedence:

1. `ref.access` on the `RemoteFileRef` / `RemoteFolderRef`
   (`remote_file("s3://...", access="fuse")`).
2. `@task(remote_input_access="fuse")` decorator default.
3. `[remote.access] default_for_pattern` glob match in `ginkgo.toml`.
4. Auto-enable heuristic
   (`auto_fuse=true` + `known_size >= auto_fuse_min_bytes`
   + `streaming_compatible=True` + `doctor_ok=True`).
5. `[remote.access] default` (defaults to `"stage"`).

The canonical policy returned is one of `"stage"`, `"fuse"`,
`"fuse (auto)"`, or `"stage (fallback)"` (produced by the worker when a
mount fails).

`ref.access` participates in `__repr__` but is **excluded from cache
identity** — toggling streaming on or off for an unchanged ref does not
invalidate existing cache entries.

## Mount lifecycle

The driver never mounts. Per-input policy is resolved on the driver
side; fuse-marked refs are shipped in the task payload as
`{"__ginkgo_type__": "fuse_file" | "fuse_folder", ...}` marker dicts
(see `ginkgo/remote/access/protocol.py`).

The worker:

1. Receives the payload, identifies fuse markers via `is_fuse_ref`.
2. Constructs a `MountedAccess` and replaces each marker with
   `file(mount_path)` / `folder(mount_path)` via
   `ginkgo.remote.access.worker_hydration.hydrate_fuse_refs`.
3. Runs the task body against the local mount paths.
4. On exit (success or failure), calls `MountedAccess.close()` which
   unmounts every active mount and records `unmount_seconds`.
5. Folds `AccessStats.to_dict()` into the result envelope as
   `remote_input_access`.

## Pod security

Pods running FUSE need one of:

- The GKE Autopilot gcsfuse CSI driver — enabled per-pod via the
  `gke-gcsfuse/volumes: "true"` annotation on the pod template. This is
  `KubernetesExecutor`'s default (`fuse_annotations`).
- `securityContext.privileged: true` on the worker container, for
  clusters without a FUSE device plugin. Opt-in via
  `fuse_privileged=True` on `KubernetesExecutor` or `GCPBatchExecutor`.

The executor checks the payload for fuse markers (via
`_payload_requires_fuse`) and applies the annotations / security
context only when streaming is actually in use; tasks that do not
stream get the original minimal pod spec.

When `fuse_image` is configured on the executor, fuse-required pods
run the streaming-capable image (`Dockerfile.worker-fuse`) while
non-streaming pods continue to use the baseline `image`.

## Fallback semantics

If `MountedAccess.materialize_*` raises at hydration time (driver
binary missing, capability denied, health check failed), the worker
falls back to `StagedAccess(policy="stage (fallback)")` for that
individual ref. The fallback reason is appended to
`AccessStats.fallback_reason` and surfaced in provenance. The task
does not fail because of a mount failure unless the staged fallback
also fails.

## Configuration

```toml
[remote.access]
default = "stage"
auto_fuse = false
auto_fuse_min_bytes = 2147483648   # 2 GiB
default_for_pattern = [
  { glob = "*.fastq.gz", access = "fuse" },
  { glob = "*.bai",       access = "stage" },
]

[remote.k8s]
fuse_image = "gcr.io/<project>/ginkgo-worker-fuse:<tag>"
fuse_privileged = false
# fuse_annotations defaults to {"gke-gcsfuse/volumes" = "true"}
```

## Doctor probes

`ginkgo doctor` runs the streaming probes in
`ginkgo/remote/access/doctor.py` whenever the workflow's config could
trigger streaming (`auto_fuse=true`, `default="fuse"`, or any
`default_for_pattern` entry with `access="fuse"`). Probes check:

- The FUSE driver binaries on the driver host's PATH (informational —
  not required).
- `/dev/fuse` availability (warning — expected to be absent on macOS).
- Whether a `fuse_image` is configured on the executor when streaming
  is enabled (warning).

## Deferred work

The following items from the Phase 9 plan are deliberately deferred and
will be picked up post-benchmark:

- **Predictive prefetch.** `PrefetchPlanner` + format-aware warmup.
  The plumbing exists (`@task(fuse_prefetch=...)` on `TaskDef`) but no
  prefetch actually runs today.
- **Background output upload.** Overlapping upload with task execution
  via an inotify watcher.
- **Benchmark harness lane.** The per-workload scenario grid, cost
  model, and comparison report.
- **Inspect rendering.** The `remote_input_access` block is stored in
  provenance but not rendered by `ginkgo inspect run`.
- **Custom FUSE driver.** A potential Phase 10 item if the OSS drivers
  miss the acceptance bar on the sequential whole-file benchmark.

## Key code locations

| Concern | Location |
|---|---|
| Ref access field | `ginkgo/core/remote.py` |
| Strategy interface + stats | `ginkgo/remote/access/protocol.py` |
| Staged strategy | `ginkgo/remote/access/staged.py` |
| Mounted strategy | `ginkgo/remote/access/mounted.py` |
| Driver dispatch + wrappers | `ginkgo/remote/access/drivers/` |
| Policy resolver + config | `ginkgo/remote/access/resolver.py` |
| Doctor probes | `ginkgo/remote/access/doctor.py` |
| Driver-side routing | `ginkgo/runtime/remote_input_resolver.py` |
| Payload pass-through | `ginkgo/runtime/artifacts/remote_arg_transfer.py` |
| Worker hydration | `ginkgo/remote/access/worker_hydration.py` |
| Worker entry point | `ginkgo/remote/worker.py` |
| Executor pod spec | `ginkgo/remote/kubernetes.py`, `ginkgo/remote/gcp_batch.py` |
| Worker image | `Dockerfile.worker-fuse` |
