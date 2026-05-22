# Resources And Scheduling

Every `@task` can declare the resources it needs and how it should be scheduled.
The runtime uses these declarations to pack ready tasks against the run's
resource budget and to decide ordering and retry behaviour.

## Declaring Resource Requirements

Pass resource arguments to `@task`. The scheduler respects them against the
`--jobs`, `--cores`, and `--memory` budgets passed to `ginkgo run`.

```python
@task(threads=4, memory="8Gi")
def align_reads(sample_id: str, reads: file) -> file:
    ...

@task(kind="shell", gpu=1, remote=True, memory="16Gi")
def train_model(dataset: folder) -> file:
    ...
```

- `threads=N` declares the CPU footprint. Tasks that read `threads` as a
  function parameter receive it automatically; shell tasks also see
  `GINKGO_THREADS` in their subprocess environment. Set `export_thread_env=True`
  to additionally export `OMP_NUM_THREADS` and related BLAS/OpenMP variables.
- `memory="8Gi"` declares the memory footprint. Format is Kubernetes-style
  (`512Mi`, `4Gi`, `16Gi`). Remote executors map this to pod resource requests.
- `gpu=N` and `remote=True` dispatch the task to the configured remote executor.
  Tasks with `gpu > 0` are implicitly remote.

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
- [CLI](cli.md) &mdash; the `--jobs`, `--cores`, and `--memory` run budgets.
