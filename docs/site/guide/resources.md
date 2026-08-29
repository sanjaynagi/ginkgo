# Resources And Scheduling

Every `@task` can declare the resources it needs and how it should be scheduled.
The runtime uses these declarations to pack ready tasks against the run's
resource budget and to decide ordering and retry behaviour.

## Declaring Resource Requirements

Pass resource arguments to `@task`. Resources declare what a task *needs*,
never where it runs. The scheduler packs them against the `--jobs`, `--cores`,
`--memory`, and `--gpus` budgets passed to `ginkgo run`.

```python
@task(threads=4, memory="8Gi")
def align_reads(sample_id: str, reads: file) -> file:
    ...

@task(gpu=1, gpu_type="nvidia-tesla-t4", memory="16Gi")
def train_model(dataset: folder) -> file:
    ...
```

- `threads=N` declares the CPU footprint. Tasks that read `threads` as a
  function parameter receive it automatically; shell tasks also see
  `GINKGO_THREADS` in their subprocess environment. Set `export_thread_env=True`
  to additionally export `OMP_NUM_THREADS` and related BLAS/OpenMP variables.
- `memory="8Gi"` declares the memory footprint. Format is Kubernetes-style
  (`512Mi`, `4Gi`, `16Gi`). Remote executors map this to pod resource requests.
- `gpu=N` declares a GPU requirement. It is satisfied from the local `--gpus`
  budget when it fits, dispatched to the remote executor when one is
  configured (`--executor`), and a build error otherwise. `gpu_type` selects
  the accelerator for remote execution, overriding the executor-level default.
- `remote=True` explicitly dispatches a python task to the run's default
  executor (whichever `--executor` names); running without `--executor` is a
  build error rather than a silent local fallback.
- `executor="name"` pins a python task to one executor declared under
  `[remote.executors]`, whatever the run default is — an unknown name is a
  build error. See [remote execution](remote-execution.md).

The local `--gpus` budget is scheduler bookkeeping: it stops ginkgo
oversubscribing GPUs across concurrent tasks, but it does not pin devices —
every local task still sees all GPUs (no `CUDA_VISIBLE_DEVICES` isolation).

## Custom Resource Dimensions

Threads, memory, and GPUs cover hardware. Some tasks are constrained by
something else entirely — a third-party API's rate limit, a shared database's
connection pool — and `custom` lets the scheduler pack against those too:

```python
@task(resources={"api_calls": 2})
def fetch_records(query: str) -> file:
    ...
```

Each dimension is budgeted separately from `[resources.budgets]` in the
runtime config, and/or repeated `--resource name=value` flags:

```toml
[resources.budgets]
api_calls = 10
```

```bash
ginkgo run flow.py --resource api_calls=10 --resource db_connections=4
```

The CLI flag wins over the config value per dimension. A dimension that
tasks request but that has no budget name in either source is
**unconstrained** — the same opt-in behaviour as `--memory`. Custom names
cannot shadow `threads`, `memory`, `gpu`, or `gpu_type`; use the dedicated
argument for those instead.

Unlike the built-in dimensions, custom demands are **not** zeroed out for
remote-placed tasks — they count wherever the task runs. An API quota or a
database connection pool doesn't stop applying because the task went to
Kubernetes, so a saturated custom budget can hold back remote dispatch as
well as local.

## Measured Usage

Declared resources are a guess; ginkgo also records what a task actually
used, so right-sizing a declaration doesn't require guesswork. Every task
run measures peak memory (RSS) and CPU time — `resource.getrusage` for
Python tasks, periodic `ps` sampling of the subprocess tree for shell,
notebook, script, and subworkflow tasks — and persists both alongside the
declared `threads`/`memory` in the run manifest. Measurement happens on
failure too: usage recorded right before an OOM kill is exactly what you
need to size the retry.

`ginkgo report` surfaces it as a **Peak RSS** column in the task ledger
(`measured / declared`, e.g. `3.2 GiB / 16 GiB`); `ginkgo runs show`
includes the raw `resource_usage` record per task. See
[Assets and Reports](assets.md#html-reports).

## Site Overrides

The same workflow file can run on a laptop, an HPC node, and the cloud with
per-site sizing. A `[resources.overrides]` table in the runtime config merges
over the decorator declarations, keyed by task name:

```toml
[resources.overrides.align_reads]
threads = 16
memory = "64Gi"

[resources.overrides."variant_*"]      # fnmatch glob
memory = "32Gi"
```

A selector matches a task's short name (`align_reads`) or fully qualified
name (`workflow.modules.align.align_reads`), and may be an `fnmatch` glob
over either. Exact matches beat globs; among globs, the first selector in
config order wins. Keys an override omits keep their declared values. An
override's `custom` table replaces the declared `custom` dict wholesale
rather than merging key by key.
Overridden `threads` flow everywhere the declaration would: the scheduler,
the dry-run plan, the injected `threads` parameter, and `GINKGO_THREADS`.
Note that for tasks declaring a `threads` parameter the injected value is a
task input, so a site override changes those tasks' cache keys — the same
way editing the declaration would.

## Retrying With More Memory

For tools whose memory needs are input-dependent, declare a baseline and let
retries escalate instead of sizing every run for the worst case:

```python
@task(memory="16Gi", retries=2, memory_retry_multiplier=2)
def sort_bam(bam: file) -> file:   # attempts run at 16, 32, then 64 GiB
    ...
```

Escalation applies exponentially per retry attempt. Locally it is capped at
the run's `--memory` budget so a retry always remains dispatchable (a task
notice reports the escalated figure); remote-placed tasks escalate uncapped
because the executor satisfies their request.

## Priority

```python
# Highest-priority tasks run first when several are ready at once.
@task(priority=10)
def critical_path_step(...): ...
```

`priority` orders tasks that become ready at the same time. It is a strict
tiebreaker: it never lets a higher-priority task block a larger set of
lower-priority tasks from running.

## Retry Policies

```python
# Retry up to 3 times, only on IOError, with exponential backoff.
@task(retries=3, retry_on=IOError, retry_backoff=1.0)
def network_fetch(...): ...

# Retry only specific exit codes on shell tasks.
@task(kind="shell", retries=2, retry_on_exit_codes=(137,))  # OOM kills
def memory_intensive_step(...): ...
```

`retries` sets how many times a failed task is re-attempted. Narrow what counts
as retryable with `retry_on` (exception types, for Python tasks) or
`retry_on_exit_codes` (for shell tasks).

Retries with a non-zero `retry_backoff` pause the task in a `waiting_retry`
state for a computed delay before the scheduler picks it up again. The delay
grows by `retry_backoff_multiplier` on each attempt and is capped at
`retry_backoff_max`.

## See Also

- [Tasks and Flows](tasks-and-flows.md) &mdash; the task authoring model.
- [Remote Execution](remote-execution.md) &mdash; running tasks on Kubernetes or
  GCP Batch.
- [CLI](cli.md) &mdash; the `--jobs`, `--cores`, `--memory`, `--gpus`, and
  `--resource` run budgets.
- [Assets and Reports](assets.md#html-reports) &mdash; the Peak RSS column in
  `ginkgo report`.
