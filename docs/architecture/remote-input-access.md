# Remote Input Access

Ginkgo has three modes for making a remote object (an
`s3://` / `gs://` / `oci://` URI referenced by a task input) available
inside the worker pod:

1. **Direct access** (default). The driver does not touch the bytes. The
   worker downloads the object from its source into the pod before the
   task starts: one pass over the bytes. The worker needs read access to
   the source bucket.
2. **Staged access**. The driver downloads the object to a local
   content-addressed cache, uploads it to the shared remote artifact CAS,
   and the worker downloads it again into the pod before the task starts.
   Three passes over the same bytes, but only the driver needs access to
   the source.
3. **FUSE-mounted access**. The driver does not touch the bytes. The
   worker pod mounts the bucket (using `gcsfuse`, `mountpoint-s3`, or
   `rclone mount`) and the task reads byte ranges on demand. Sparse
   access patterns pay only for the bytes the task actually touches.

For a task that runs on this host, `direct` and `stage` are the same: the
input is downloaded into the local staging cache. Streaming is **opt-in**
per input, per task, or via config default; a failed mount falls back to a
download.

Both the **remote dispatch path** and the **local process-pool path**
can stream. Local streaming applies to worker (Python) tasks on a host
with a healthy driver + `/dev/fuse`; driver-kind tasks (`notebook` /
`script` / `shell`) always stage locally (see
[Local FUSE](#local-fuse)).

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
into the result envelope as `remote_input_access`; the evaluator records
them against the task as `TaskAnnotated(fields={"remote_input_access": …})`,
which lands in the ledger's `tasks.extra`.

A remote input's cache key holds its store metadata, not its bytes, so the
content is recorded separately. Whoever downloads an input records its digest
(`StagingCache.digest`: BLAKE3 of a file's bytes, or a folder's listing
digest): the driver's `RemoteStager` for inputs it stages, the worker for
`direct` inputs and for a mount that fell back to a download. The evaluator
merges them per task into `TaskAnnotated(fields={"remote_input_digests":
{uri: digest}})`. A FUSE-mounted input is never downloaded whole, so it records
none.

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
5. `[remote.access] default` (defaults to `"direct"`).

The canonical policy returned is one of `"direct"`, `"stage"`, `"fuse"`
or `"fuse (auto)"`.

`ref.access` participates in `__repr__` but is **excluded from cache
identity** — toggling streaming on or off for an unchanged ref does not
invalidate existing cache entries.

## Cache identity

A remote input is keyed by its store's metadata, never by its downloaded
bytes: `version_id` when pinned, otherwise a file's ETag and size or a
folder's listing digest (`RemoteRef.identity`). When a dispatched node has an
input without one, the staging pool resolves it first
(`RemoteStager.resolve_task_identities`, one HEAD or LIST per URI per run,
memoised so every task reading a URI sees the same version), then the cache
is checked, and only a miss stages. A FUSE marker carries the same identity
and keys exactly as its ref does.

Staging checks the bytes it fetches against that identity: a file whose ETag
moved, or a folder whose listing changed, after the key was built fails with
`RemoteIdentityError` instead of running on bytes the key does not
describe. So does an object whose store reports no ETag. Neither is retried:
within a run the memoised identity stays the same, so only a new run helps.

## Mount lifecycle

The driver never mounts. Per-input policy is resolved on the driver
side; a `direct` or `fuse` input is shipped in the task payload as a
`{"__ginkgo_type__": "remote_input_file" | "remote_input_folder", "policy": …}`
marker dict carrying the ref and its identity (see
`ginkgo/remote/access/protocol.py`).

The worker:

1. Receives the payload, identifies markers via `is_remote_input`.
2. Replaces each marker with `file(path)` / `folder(path)` via
   `ginkgo.remote.access.worker_hydration.hydrate_remote_inputs`: a
   `direct` input is downloaded into the pod's scratch staging cache, which
   checks it against the marker's identity; a `fuse` input is mounted
   through a `MountedAccess`.
3. Runs the task body against the local mount paths.
4. On exit (success or failure), calls `MountedAccess.close()` which
   unmounts every active mount and records `unmount_seconds`.
5. Folds `AccessStats.to_dict()` into the result envelope as
   `remote_input_access`.

## Local FUSE

Local (non-dispatched) worker tasks stream through the same
`MountedAccess` strategy without any pod / privileged-container / CSI
surface — just the driver binary + `/dev/fuse`.

- **Gating.** `RemoteStager.stage_remote_refs`
  (`ginkgo/runtime/remote_input_resolver.py`) lifts the remote-only
  short-circuit for local worker tasks. Fuse selection is resolved
  per scheme with `driver_available=local_streaming_available(scheme=...)`
  (`ginkgo/remote/access/doctor.py`) — `/dev/fuse` present **and** the
  scheme's driver passing its health check. When the host cannot stream,
  `resolve_access` degrades to `stage`. Driver-kind tasks
  (`notebook` / `script` / `shell`) run on the scheduler thread with no
  mount lifecycle and are still forced to `stage`. macOS has no
  `/dev/fuse`, so it is stage-only there.

- **Hydration + teardown.** `ginkgo/runtime/worker.py::run_task` mirrors
  `remote/worker.py`: it detects fuse markers in the decoded args, mounts
  them through a `MountedAccess` rooted **under the task's transport dir**
  (`<transport>/fuse`, cache `<transport>/fuse-cache`), runs the body,
  and tears the mount down in a `finally`. The per-task mount root keeps
  concurrent process-pool workers from colliding on a shared bucket mount
  point. On success `AccessStats.to_dict()` is folded into the result
  envelope as `remote_input_access`.

- **Provenance.** `ConcurrentEvaluator._fold_remote_input_access` records
  the stats from `_on_worker_done`, covering both local
  and remote workers, and surfaces a notice on staged fallback.

This is **one `MountedAccess` per task**, not one shared across the pool;
a shared, ref-counted mount pool is a possible later optimisation.

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
downloads that individual ref instead, through the same `StagedAccess`
it uses for `direct` inputs. The fallback reason is appended to
`AccessStats.fallback_reason` and surfaced in provenance. The task
does not fail because of a mount failure unless the staged fallback
also fails.

## Configuration

```toml
[remote.access]
default = "direct"
auto_fuse = false
auto_fuse_min_bytes = 2147483648   # 2 GiB
default_for_pattern = [
  { glob = "*.fastq.gz", access = "fuse" },
  { glob = "*.bai",       access = "stage" },
]

[remote.executors.k8s]
type = "k8s"
image = "gcr.io/<project>/ginkgo-worker:<tag>"
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
- Whether a `fuse_image` is configured, for *every* configured executor
  section, when streaming is enabled (warning per section). A task can be
  pinned to any executor, so diagnosing one section would clear a run whose
  other executors cannot mount.

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
  provenance but not rendered by `ginkgo runs show --json`.
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
| Executor pod spec | `plugins/ginkgo-k8s/src/ginkgo_k8s/executor.py`, `plugins/ginkgo-gcp/src/ginkgo_gcp/batch.py` |
| Worker-protocol helpers | `ginkgo/remote/executor_support.py` |
| Worker image | `Dockerfile.worker-fuse` |
