# Phase 7 Follow-up Plan

Phase 7 (remote execution on Kubernetes) landed with a working happy path,
validated end-to-end on GKE Autopilot against a real workflow (gnn-gwss:
6 GNN training pods + 300 local simulations + evaluation + notebook). During
that validation a stack of latent bugs and rough edges surfaced. Most of
the hot-path bugs are now fixed on `feature/phase7-remote-execution`; this
document captures the remaining work — bugs, gaps, and polish — that needs
attention before remote execution is ready for broader use.

Items are ordered roughly by impact. Each item has a short description,
why it matters, and a sketch of the fix.

Items marked ~~strikethrough~~ are done.

---

## Bugs

### ~~1. fsspec artifact backend has no token refresh~~

**What:** The `_RefreshingApi` proxy added in commit `895b414` wraps
`BatchV1Api` / `CoreV1Api` so 401s from GKE bearer-token expiry trigger a
`load_config` reload and a single retry. The artifact store backend
(`GCSFileSystemBackend`, `S3FileSystemBackend`, `OCIFileSystemBackend`)
has no equivalent: the underlying `gcsfs.GCSFileSystem` / `s3fs.S3FileSystem`
instance is built once per run and reuses whatever credentials it
captured at construction.

**Why it matters:** A multi-hour run that stages artifacts past the 1h
GKE token window will hang or fail indefinitely in upload/download, the
same way the K8s API client used to. This is almost certainly the next
thing a long run will hit.

**Fix sketch:** Wrap `backend.upload`/`download`/`head`/`list_prefix` so
that on `google.auth.exceptions.RefreshError` or HTTP 401 we rebuild the
filesystem via a factory (mirroring `_RefreshingApi`). The factory
should call `_get_filesystem()` which re-runs the credential discovery.
Unit-test with a mock fsspec that raises once then succeeds.

---

### ~~2. No timeout on fsspec operations~~

**What:** `backend.upload` / `download` have no timeout. If the underlying
network call hangs (expired creds retrying forever, partial TCP
disconnect, GKE pod stuck waiting on a stuck GCS request), the entire
evaluator blocks indefinitely. The 1h46m hang we saw before the 401-refresh
fix was this mechanism.

**Why it matters:** A single bad blob freezes the whole workflow. The
evaluator has no watchdog.

**Fix sketch:** Wrap blob upload/download in a `concurrent.futures`
timeout (10 min per blob seems reasonable for multi-GB trees). On
timeout, log a warning, rebuild the backend (item 1), and retry.
Alternatively configure urllib3/requests-level timeouts on the
underlying fsspec clients.

---

### ~~3. Dataset re-staging re-hashes every file on every run~~

**What:** `stage_args_for_remote` calls `remote_store.store(src_path=folder)`
which walks the folder and hashes every file to mint an `ArtifactRecord`.
The HEAD-skip added in commit `895b414` makes the subsequent upload
cheap, but re-hashing 2.4 GiB of files still takes 30–120 seconds on
every run.

**Why it matters:** Re-runs with all tasks cached still pay a staging
cost proportional to dataset size. For iterative development this is
the dominant latency.

**Fix sketch:** Persist `_known_digests` (path → artifact_id) to
`.ginkgo/remote-staged.json`, keyed on (absolute path, mtime, size).
On hit, skip `store()` entirely — the backend HEAD confirms the blob is
still on the remote. On miss, call `store()` as before and update the
map. Roughly 30 lines in `remote_staging.py`.

---

### 4. Cache misses on `train_and_evaluate_gnn` between runs

**What:** During validation we observed four of six GNN tasks hit the
cache on run N+1, then a subsequent rerun missed all six. Never
root-caused.

**Why it matters:** Re-running a workflow to pick up one failed task
should be near-instant. Instead we sometimes pay the full GNN training
cost. This is opaque to the user.

**Fix sketch:** Instrument `_compute_cache_key` to log the component
hashes (source, args, environment, …) on each call, then rerun the
scenario and diff the hash inputs between a hit and a miss. The drift
is almost certainly one of: (a) a non-deterministic arg that shouldn't
be part of the key; (b) a code-bundle digest being folded into the
source hash; (c) a file input whose mtime-based materialization hash
changed. Once identified, pin or exclude it.

---

### ~~5. `_remote_published_artifacts` is in-memory only~~

**What:** The in-process set that records which artifacts have been
uploaded to the remote store is empty at startup. On every fresh run
we HEAD-check every blob before skipping, which is fast but still O(N)
API calls. More importantly, a single process crash loses the state
entirely.

**Why it matters:** Wasted round-trips on every run, and related to
item 3 above (path-hash cache).

**Fix sketch:** Persist `_remote_published_artifacts` alongside
`_known_digests` in `.ginkgo/remote-staged.json`. One file, same
read/write hook. When adding an artifact, append to both.

---

### ~~6. `build_worker_remote_store` uses hardcoded `/tmp/ginkgo-*` paths~~

**What:** The worker constructs its `RemoteArtifactStore` with
`local_root=Path("/tmp/ginkgo-remote-cas")` and hydrates inputs into
`Path("/tmp/ginkgo-inputs")`. These paths live on the container's
ephemeral-storage volume.

**Why it matters:** (a) collides with any user PVC mount at `/tmp`;
(b) the Autopilot 10Gi ephemeral-storage ceiling applies to the sum of
all `/tmp` writes, so staging a big dataset leaves very little headroom
for user-task temp files; (c) not configurable — a workflow with a
workdir preference can't override.

**Fix sketch:** Honour `$TMPDIR` first, then fall back to
`$GINKGO_SCRATCH_ROOT` (new), then `/tmp`. Document the contract in
`remote-execution.md`. Thread the root through the worker payload so
the client can declare a preference.

---

### ~~7. `_check_unschedulable_timeout` calls `list_namespaced_pod` on every Pending poll~~

**What:** `state()` calls `_check_unschedulable_timeout()` on every
pending-state read. That function lists pods via label selector.

**Why it matters:** Harmless functionally but chatty — O(polls × pods)
API calls against the K8s API server while any pod is still Pending.
With a 5-second poll interval and a 5-minute unschedulable timeout,
that's 60 unnecessary list calls per pending pod.

**Fix sketch:** Memoize the pod list for a short window (e.g. 15s)
inside the handle. Or — cleaner — only check on transitions out of
Active. The unschedulable state is sticky, so there's no value in
polling it every 5s.

---

### ~~8. Job name collisions on rerun~~

**What:** Job names are `f"ginkgo-{run_id}-{task_id}-{attempt}"`. If a
task fails and ginkgo resubmits with the same run_id / task_id /
attempt (e.g. a retry loop internal to the evaluator), the second
`create_namespaced_job` call returns HTTP 409.

**Why it matters:** Doesn't happen in the current code path because
`attempt` increments, but fragile. Any future resubmission path (manual
retry, ginkgo-level retry) will trip this.

**Fix sketch:** Suffix job names with a short random token, or
idempotently handle 409 by fetching the existing job. Former is
simpler.

---

### ~~9. `_capture_remote_logs` writes only to `stdout_path`~~

**What:** Remote task logs come back as one merged stream (the worker
`print`s JSON to stdout and errors to stderr, but both end up in the
pod's container log). The evaluator writes that merged stream to
`node.stdout_path`. `node.stderr_path` is never touched for remote
tasks.

**Why it matters:** Confusing when debugging — you look at a task's
stderr expecting the traceback, find an empty file, then discover the
traceback is in stdout instead.

**Fix sketch:** For the remote branch, either split the merged stream
on a known marker and write each side to the right file, or mirror the
same text into both files. Document the convention in the failure
panel.

---

## Gaps

### 10. GCP Batch executor is completely unexercised

**What:** `ginkgo/remote/gcp_batch.py` exists and is plumbed into the
CLI, but this session didn't touch it. The bugs fixed in the K8s path
(value codec ordering, HEAD-first upload skip, token refresh, terminal
state caching, TTL / backoff / ephemeral storage defaults) almost all
have analogues in the Batch executor that were never tested.

**Why it matters:** Anyone who picks the Batch executor will
immediately hit at least the value-codec bug (file outputs) and
probably the token-refresh bug on long runs.

**Fix sketch:** Audit `gcp_batch.py` side-by-side with `kubernetes.py`.
For each fix committed on the K8s side, decide whether the Batch path
needs the same treatment. Then run an end-to-end validation against a
real GCP Batch job (analogous to the gnn-gwss validation run).

---

### 11. No end-to-end GPU test

**What:** The gnn-gwss validation run fell back to CPU because L4s were
out of stock in europe-west2 during the session. The GPU code path
(`gpu_type`, `cloud.google.com/gke-accelerator` node selector, GPU
resource requests) is entirely untested on a real cluster.

**Why it matters:** GPU workloads are the primary motivation for
remote execution. "CPU works" is not the demo.

**Fix sketch:** Either wait for L4 stock in europe-west2, or switch to
us-central1 where A100 / L4 stock is typically better. Re-run the
gnn-gwss sweep with `gpu=1`, `gpu_type="nvidia-l4"`, and confirm: the
pod gets scheduled on a GPU node, the worker sees the GPU via
`torch.cuda.is_available()`, and the training throughput matches
expectations.

---

### ~~12. Code-sync mode tars the whole package regardless~~

**What:** `create_code_bundle` walks the package directory and tars
every file. No filtering, no `.gitignore` honouring, no
`pyproject.toml include/exclude` honouring.

**Why it matters:** For small workflows this is fine. For a workflow
with `results/`, `__pycache__`, large checkpoints, or notebook outputs
inside the package tree, we end up uploading hundreds of megabytes on
every run.

**Fix sketch:** Respect `.gitignore` via `pathspec` (already a
transitive dep) or accept an explicit `include`/`exclude` list in
`[remote.k8s.code]`. `rsync --filter` semantics are the right mental
model.

---

### 13. Token refresh / retry logic is not unit-tested against the real shapes

**What:** The `_RefreshingApi` proxy has unit tests against MagicMock.
No test constructs a real `kubernetes.client.exceptions.ApiException`
or exercises the `load_config` reload path. Same for the
terminal-state caching, 404-before-observed handling, and the
backoff_limit conditions check.

**Why it matters:** The unit tests reproduce the shape of each bug,
not the cause. A future refactor could satisfy the mock while breaking
the real behaviour.

**Fix sketch:** Write a small integration harness that spins up a
`kubernetes` fake client (or a minikube) and exercises the real
control flow. Or at minimum, import `ApiException` in the tests and
raise the real class.

---

### 14. Multi-cluster, quota exhaustion, network partition — untested

**What:** The validation run touched exactly one cluster, one
namespace, one project, one region, one VPC. No test for: GKE quota
exhaustion mid-run, cross-region latency, VPC peering failures, the
Anthos / multi-cluster selectors, workload identity misconfiguration,
or RBAC holes.

**Why it matters:** These are the failure modes a real production user
will hit first. They'll each surface differently (429, 403, timeout,
connection refused) and the evaluator's error-handling story is
currently "raise and die".

**Fix sketch:** Each failure mode needs an explicit handling path.
Probably: classify API errors by `status` into
{transient, auth, quota, config} and react accordingly. Document each
classification in `remote-execution.md`.

---

## Polish

### 15. Metrics / observability for remote dispatch

**What:** The evaluator has no counters for: number of K8s jobs
submitted, number of preempted pods retried, time-in-pending vs
time-running, artifact upload bytes, artifact download bytes, 401
retries, 404 retries. You have to grep logs to reconstruct what
happened.

**Why it matters:** Debugging a flaky cluster is a forensic exercise.

**Fix sketch:** Wire the existing `ProfileRecorder` (phase 20) to
record remote-dispatch events as spans. Surface a summary in the
`ginkgo run` output: "6 remote tasks, 2 preempted and retried, 1
unschedulable, 4.2 GiB uploaded, 400 MiB downloaded".

---

### 16. `build-worker.sh` does not verify the base image exists

**What:** `GINKGO_BASE_IMAGE` defaults to `${REGISTRY}/worker:v2`. If
a user overrides and typos the tag, the script fails deep inside
`docker buildx build` with a confusing error.

**Fix sketch:** Run `docker manifest inspect "$BASE_IMAGE"` before
building, and error early with a clear message listing the available
tags.

---

### 17. No documented path for debugging a failed remote task

**What:** When a remote task fails, the user gets a failure panel and
a log tail. They do not get: the pod name, the cluster/namespace,
instructions for `kubectl describe pod`, the code bundle digest, the
artifact IDs of staged inputs, or the reproduction command.

**Fix sketch:** Expand the failure panel for remote tasks with the
metadata listed above, and add a `ginkgo run --debug-remote <task>`
shortcut that dumps all of it for the most recent failed attempt.
