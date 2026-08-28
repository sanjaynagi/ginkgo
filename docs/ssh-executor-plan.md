# SSH Executor Plan

## Problem definition

Remote dispatch today requires a control plane: GCP Batch or a Kubernetes
API. A user with plain VMs (Ben's OCI instances) has no way to send tasks to
them without first installing k8s. Ginkgo should be able to treat a fixed
pool of SSH-reachable machines as an executor.

The storage side already works — `oci://` is a first-class scheme for the
remote artifact store and staging — so this is purely a compute-executor gap.

## Why the worker contract makes this cheap

The existing `RemoteExecutor` protocol (`runtime/remote_executor.py`) is
small: `submit(attempt) -> RemoteJobHandle` with `state() / result() /
cancel() / logs_tail()`. The worker (`remote/worker.py`) reads a base64 JSON
payload from `GINKGO_WORKER_PAYLOAD` and prints exactly one JSON result line
to stdout (`_parse_worker_output` searches backwards for it). Code bundles,
input/output staging via the artifact store, and fuse hydration all live in
the worker and are executor-agnostic. An SSH executor is therefore mostly
process management, not new distribution machinery.

## Proposed design

### New module: `src/ginkgo/remote/ssh.py`

**`SSHExecutor`** implementing `RemoteExecutor`:

- **Host pool with slot accounting.** Config declares hosts and per-host
  slots. `submit()` picks the host with a free slot (round-robin among
  least-loaded); when every host is full, `submit()` blocks briefly or the
  handle starts in PENDING with a queued launch — v1: block in the watcher
  thread, which is already off the scheduler's hot path. Slots are the whole
  bin-packing story in v1: no per-host core/memory matching (documented
  limitation vs k8s; a `resources`-aware fit can come later).
- **Launch.** One local `ssh` subprocess per job:
  `ssh <opts> <host> 'GINKGO_WORKER_PAYLOAD=$(cat) <python> -m ginkgo.remote.worker'`
  with the payload written to stdin. Stdin avoids ARG_MAX limits; the env
  var stays within OS env limits because large values already travel via the
  artifact store, not the payload. `<python>` comes from config
  (`worker_python`, e.g. a venv/pixi path on the VMs).
- **Transport seam for tests.** The command builder is a pure function
  (`_ssh_argv(host, remote_command, options) -> list[str]`); tests swap the
  transport for `bash -c` so the full executor runs against localhost with no
  SSH daemon. This mirrors how the container tests mock Docker argv.

**`SSHJobHandle`**:

- `state()`: PENDING until the process starts, RUNNING while alive,
  SUCCEEDED/FAILED from the exit code plus the parsed worker line.
- `result()`: wait on the process, parse stdout with the shared
  `_parse_worker_output` (`source_label="ssh output"`); release the host slot.
- `cancel()`: terminate the local ssh process, then best-effort
  `ssh <host> pkill -f <job marker>` — the job name from
  `_generate_job_name` is exported as `GINKGO_JOB=<name>` in the remote
  command so the pkill pattern is precise.
- `logs_tail()`: tail of captured stderr/stdout (both are captured to
  spooled files, like the driver-task log path).

### Config

```toml
[remote.ssh]
hosts = ["opc@10.0.0.12", "opc@10.0.0.13"]
slots_per_host = 2                      # concurrent tasks per host
worker_python = "/opt/ginkgo-env/bin/python"
ssh_options = ["-i", "~/.ssh/oci_key", "-o", "BatchMode=yes",
               "-o", "ConnectTimeout=10"]
connect_timeout = 10

[remote.artifacts]
store = "oci://bucket@namespace/ginkgo-artifacts/"

[remote.ssh.code]                        # same shape as k8s/batch
mode = "sync"
package = "my_workflow"
```

Per-host slot override via `hosts = [{host = "...", slots = 4}, ...]` is a
possible v2; v1 keeps a flat list + global `slots_per_host`.

### Wiring

- `--executor ssh` added to the CLI choices; `_build_ssh_executor` in
  `cli/commands/run.py` mirrors `_build_k8s_executor` (requires `hosts`
  non-empty, resolves code-bundle config the same way).
- `gpu_type` is meaningless here; a task with `gpu > 0` dispatched to SSH
  simply trusts the operator's host pool (documented). Threads/memory are
  not enforced on the VM in v1 — the payload carries them for provenance.
- `ginkgo doctor`: a check that each configured host answers
  `ssh <host> <worker_python> -c "import ginkgo"` within the timeout —
  catches the "ginkgo not installed on the VM" failure class before a run.

### VM prerequisites (documented, not automated)

Each host needs: SSH key access, a Python environment with ginkgo (and task
deps) at `worker_python`, network access to the artifact store, and object
storage credentials (OCI config file or instance principals — ocifs picks up
both). With code-sync mode the workflow package itself does NOT need
installing; ginkgo + third-party deps do.

## Failure modes

- **Host unreachable / ssh non-zero before worker starts**: handle reports
  FAILED with stderr in logs; ginkgo's normal task retries apply, and the
  retry naturally lands on another host via slot selection. No custom
  host-blacklisting in v1.
- **Driver dies mid-run**: remote processes are children of local ssh
  processes, so they die with the driver (unlike Batch/k8s, no orphan jobs).
  This is a feature for VM hygiene, a limitation for long tasks; documented.
- **Two ginkgo runs sharing a pool**: slots are per-driver-process; two
  drivers can oversubscribe a host. Documented limitation (k8s is the answer
  for shared pools).

## Risks and tradeoffs

- SSH flakiness (dropped connections) shows up as task failures; retries
  mitigate but a long task on a flaky link wastes work. `ServerAliveInterval`
  in default ssh options reduces silent hangs.
- No resource-aware packing: a memory-hungry task can land next to another.
  Acceptable for v1 (Snakemake's `--cluster ssh`-style users live with
  worse); revisit if real usage demands it.
- Windows driver support: out of scope (ssh subprocess assumed POSIX).

## Testing strategy

1. **Unit**: argv/remote-command construction (payload via stdin, job marker
   export), slot accounting (acquire/release/exhaustion), config parsing and
   validation errors.
2. **Executor-level with local transport**: swap ssh for `bash -c`, run a
   real payload end-to-end through `SSHExecutor.submit()` →
   `handle.result()` — worker executes on localhost, result parses, slot is
   released. Cancel path: long-running payload, `cancel()`, assert process
   death and FAILED/CANCELLED state.
3. **Evaluator integration**: one `remote=True` task through
   `ConcurrentEvaluator` with the local-transport executor.
4. **Manual OCI validation** (with Ben's VMs): the checklist in
   `docs/oke-validation.md` §SSH.

## Success criteria

- `ginkgo run --executor ssh` sends `remote=True` (and GPU-overflow) python
  tasks to the configured hosts and folds results, caching, and provenance
  in exactly as the k8s executor does.
- A dead host fails the task with actionable logs, and a retry can succeed
  on another host.
- `ginkgo doctor` verifies the pool before a run.
- All executor-level tests run without any SSH daemon.

## Effort estimate

- `remote/ssh.py` (executor + handle + slot pool + transport seam): ~1.5 days
- CLI wiring, config parsing, doctor check: ~0.5 day
- Tests (unit + local-transport + evaluator integration): ~1 day
- Docs (site guide + architecture note) and manual OCI validation with a
  real VM: ~0.5–1 day

**Total: ~3.5–4 days.** No changes to the evaluator, worker, scheduler, or
payload format — the protocol boundary holds as-is.
