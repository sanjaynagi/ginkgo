# Resources And Scheduling

Every `@task` can declare the resources it needs, such as CPU threads and
memory. Ginkgo starts a task only when the run has enough of each left. A task
can also declare its priority, and how to retry when it fails.

## Limiting What A Run Uses

A run has a budget for each resource. You set the budgets with flags on
`ginkgo run`:

- `--jobs N` caps how many tasks run at once. It defaults to the number of CPUs
  on the machine.
- `--cores N` caps the total `threads` of the tasks running at once. It defaults
  to the `--jobs` value.
- `--memory N` caps the total declared memory of the tasks running at once, in
  GiB. It has no default. Without it, Ginkgo does not limit memory.
- `--gpus N` caps the total GPUs of the tasks running at once. It defaults to 0.

```bash
# Run at most 4 tasks at once, within an 8-core, 32 GiB budget
ginkgo run flow.py --jobs 4 --cores 8 --memory 32
```

When several tasks are ready, Ginkgo starts as many of them as the budgets fit.

## Declaring Resource Requirements

Pass resource arguments to `@task` to say how much a task needs:

```python
@task(threads=4, memory="8Gi")
def align_reads(sample_id: str, reads: file) -> file:
    ...

@task(gpu=1, gpu_type="nvidia-tesla-t4", memory="16Gi")
def train_model(dataset: folder) -> file:
    ...
```

- `threads=N` declares how many CPU threads the task uses. A task that has a
  `threads` parameter receives the value. A shell task also sees it as
  `GINKGO_THREADS` in its environment. Set `export_thread_env=True` to also set
  `OMP_NUM_THREADS` and the related BLAS and OpenMP variables.
- `memory="8Gi"` declares how much memory the task uses. Write it as a number
  and a unit, such as `512Mi`, `4Gi` or `16Gi`. `Mi` means mebibytes and `Gi`
  means gibibytes (about 1.07 GB). For the `--memory` budget, Ginkgo rounds up to
  a whole GiB, with a minimum of 1 GiB. So `512Mi` counts as 1 GiB, and `1.5Gi`
  counts as 2 GiB.
- `gpu=N` declares how many GPUs the task uses. `gpu_type` names the kind of GPU
  a remote [executor](concepts.md#glossary) should provide.

The local `--gpus` budget only stops Ginkgo from giving the same GPUs to too
many tasks at once. It does not assign devices. Every local task still sees
every GPU, because Ginkgo does not set `CUDA_VISIBLE_DEVICES`.

Resource settings are not part of a task's cache key. Changing `threads`,
`memory` or `gpu`, or the run's budgets, reruns nothing. The exception is a task
that has a `threads` parameter. It receives `threads` as an input, so a new
value reruns it.

## Where A Task Runs

Resource declarations say how much a task needs. In one case they also decide
where it runs. A `gpu=1` task runs on your machine when `--gpus` can cover it.
If it can't, Ginkgo sends the task to the run's remote executor. If the run has
no executor, the run fails before any task starts.

You can also send a task to an executor directly, with `remote=True` or
`executor="name"`. See [Remote Execution](remote-execution.md).

A task on a remote executor uses one of the run's `--jobs` slots. Its `threads`,
`memory` and `gpu` do not count against your machine's budgets, because the
executor provides them. Custom resources still count, as the next section
explains.

## Custom Resource Dimensions

Some tasks are limited by something other than hardware. For example, a
third-party API's rate limit, or a shared database's connection pool. Declare
those with `resources=`, and Ginkgo budgets them too:

```python
@task(resources={"api_calls": 2})
def fetch_records(query: str) -> file:
    ...
```

Set the budget for each name in `[resources.budgets]` in the runtime config, or
with a `--resource name=value` flag. Repeat the flag for each name:

```toml
[resources.budgets]
api_calls = 10
```

```bash
ginkgo run flow.py --resource api_calls=10 --resource db_connections=4
```

These rules apply:

- **The flag wins.** A `--resource` flag overrides the config value for the same
  name.
- **No budget means no limit.** A name that tasks use but neither source sets
  is not limited, just as memory is not limited without `--memory`.
- **Built-in names are reserved.** A custom name can't be a built-in name such
  as `threads`, `memory`, `gpu` or `gpu_type`. Use the dedicated argument for
  those.
- **Remote tasks still count.** A custom resource counts wherever the task runs.
  An API rate limit still applies when the task runs on Kubernetes. So a full
  custom budget can hold back remote tasks as well as local ones.

## Priority

Give a task a priority to start it before other ready tasks when the budgets
can't fit them all:

```python
@task(priority=10)
def critical_path_step(...): ...
```

`priority` defaults to 0, and a higher number starts first. It only breaks ties.
Ginkgo first picks the set of ready tasks that starts the most tasks and uses the
most cores. Priority then chooses between sets that tie. A high-priority task
never stops a larger set of lower-priority tasks from starting.

## Retry Policies

```python
# Retry up to 3 times, only on IOError, waiting longer each time.
@task(retries=3, retry_on=IOError, retry_backoff=1.0)
def network_fetch(...): ...

# Retry only specific exit codes on shell tasks.
@task(kind="shell", retries=2, retry_on_exit_codes=(137,))  # OOM kills
def memory_intensive_step(...): ...
```

`retries` sets how many times Ginkgo tries a failed task again. It defaults to
0. With no other setting, every failure is retried. To retry only some failures,
set `retry_on` (exception types, for Python tasks) or `retry_on_exit_codes` (for
shell tasks). Each retry starts the task again from scratch.

`retry_backoff` sets how long Ginkgo waits before a retry, in seconds. It
defaults to 0, which means no wait. With `retry_backoff=1.0`, Ginkgo waits 1
second before the first retry. It multiplies the wait by
`retry_backoff_multiplier` (default 2.0) for each later retry, up to
`retry_backoff_max` seconds (default 60). So the wait before retry *k* is
`retry_backoff * retry_backoff_multiplier ** (k - 1)`.

## Retrying With More Memory

Some tools need more memory for some inputs than others. Declare a baseline, and
let each retry ask for more, instead of sizing every run for the worst case:

```python
@task(memory="16Gi", retries=2, memory_retry_multiplier=2)
def sort_bam(bam: file) -> file:   # attempts run at 16, 32, then 64 GiB
    ...
```

Each retry multiplies the memory by `memory_retry_multiplier` again. On your own
machine, the memory never goes above the `--memory` budget, so the retry can
still start. A notice on the task reports the new figure. Remote tasks have no
cap, because the executor provides the memory.

## When A Task Fails

By default, one failed task stops the run, once its retries run out. Tasks that
are already running finish, and no new task starts. No task is left without an
outcome:

- A task that never started is recorded `skipped` in `ginkgo runs show`. It is
  either blocked by the failure, or was not started because the run stopped.
- A task that fails while the run is stopping is not retried, even with retries
  left. Nor is a task already waiting to retry. Both are recorded `failed`, with
  a notice such as
  `retry 1/2 not attempted: the run is stopping after an earlier failure`.

## When a Failure Should Not Stop the Run

In a run over thousands of samples, stopping at the first failure throws away
the results of every other sample. Retries can't help with a malformed input,
because it fails the same way every time.

Use `on_failure="ignore"` on a task to let the rest of the run carry on when it
fails:

```python
# One bad sample must not cost the other 4,999.
@task(retries=2, on_failure="ignore")
def load_sample(sample: str) -> file: ...
```

`on_failure="ignore"` applies after the retries run out. The failed task's
siblings keep running, and so does every task that does not depend on it.
`ginkgo run --keep-going` does the same for every task in the run, without
editing the workflow.

Ginkgo ignores a failure it detects itself in the same way as an exception from
the task body. For example, a task that returns a value of the wrong type.

Use `--keep-going` to carry on past any failure in the whole run. Use
`on_failure="ignore"` on a single task whose failure you expect.

### What Ignoring A Failure Does Not Do

- **It does not make the run pass.** The task is recorded `failed`, and so is
  the run. `ginkgo run` exits with code 3, not 0. Code 1 means the run stopped
  at the first failure. Code 3 means the run ran everything it could, and some
  tasks failed.
- **It does not run the tasks downstream.** A task that needs the failed task's
  output can't run, so it is recorded `skipped`, naming the failure. An
  aggregator over a [fan-out](concepts.md#glossary) depends on every branch, so one
  ignored failure skips it. If the flow returns the aggregator, the run has no
  result, and it says so.

The successful branches are still cached, and their outputs are still there.
After you fix the input, a rerun does only the work that is left.

### Aggregating The Branches That Succeeded

To aggregate over a fan-out whose branches may fail, make the failure part of
the branch's result instead of the run's. In a Python task, catch the exception
inside the task body and return `None`. Annotate the return as optional, and
have the aggregator drop the `None` entries. The branch then succeeds, so the
aggregator is not skipped:

```python
@task()
def load_sample(sample: str) -> str | None:
    try:
        return parse(sample)
    except ValueError:
        return None       # logged by the task, recorded as succeeded


@task()
def combine(loaded: list[str | None]) -> str:
    kept = [item for item in loaded if item is not None]
    ...
```

The run then records those branches as succeeded, so log or return enough detail
to find them later. This works only for Python tasks. A shell task's failure is
its exit status, which Ginkgo can't turn into a `None` result.

## Site Overrides

The same workflow file can run on a laptop, an HPC cluster and the cloud, with
different sizes at each site. A `[resources.overrides]` table in the runtime
config changes the decorator's values, keyed by task name:

```toml
[resources.overrides.align_reads]
threads = 16
memory = "64Gi"

[resources.overrides."variant_*"]      # a pattern
memory = "32Gi"
```

These rules decide which override applies:

- A selector matches a task's short name (`align_reads`) or its full name. The
  full name is `workflow.modules.align.align_reads`, or `<workflow>.align_reads`
  for a task in a single-file workflow.
- A selector can be a pattern, where `*` matches any text. For example,
  `variant_*`.
- An exact name wins over a pattern. Among patterns, the first in the file wins.
- Keys the override leaves out keep the decorator's value.
- An override's `custom` table holds custom resources. It replaces the
  decorator's `resources=` dict as a whole, and does not merge key by key.

An overridden `threads` applies everywhere the decorator's value would: the
scheduler, the dry-run plan, the `threads` parameter, and `GINKGO_THREADS`. So
for a task that has a `threads` parameter, an override changes the cache key,
just as editing the decorator would.

## Measured Usage

Declared resources are a guess. Ginkgo also records what each task actually
used, so you can set the declaration from real numbers. For every task, Ginkgo
records peak memory, as resident set size (RSS), and CPU time. It records them
when a task fails too, so a task killed for using too much memory still shows
how much it used.

A Python task's figure covers only the task body. It leaves out the memory the
worker process already used before the task started, so a task that allocates
nothing reads close to zero. A shell, script, notebook or subworkflow task's
figure covers the whole process tree that its command starts.

Ginkgo measures a shell or script task by checking it every quarter second. A
task that finishes sooner has no memory figure at all. A task that runs a little
longer is measured from one or two checks, which can miss a short spike. So a
quick task's missing or small figure means "too short to measure", not "used
nothing".

`ginkgo report` shows peak memory in its task table, as measured against
declared. For example, `3.2 GiB / 16Gi`. A cache hit measured nothing in its
run, so the report shows the peak from the earlier run that produced the result,
and names that run: `3.2 GiB (run 9e1f3e6c)`. See [Reports](reports.md).
`ginkgo runs show --json` includes the full `resource_usage` record for each
task.

### Across Runs

One run tells you what a task used that time. To size the declaration, you need
what it uses across many runs:

```console
$ ginkgo history align_reads --resources

Peak RSS over 12 executions (28 runs, 16 cached)
  p50 3.1 GiB   p95 4.8 GiB   max 5.2 GiB
  declared 16 GiB  ·  p95 is 30% of declared
CPU time
  p50 4m 12s   p95 5m 30s   total 52m
  declared 4 threads
```

Only real executions measure anything. So a task with 28 runs and 16 cache hits
has 12 samples, and the summary shows that count. `--by-label` gives one summary
per branch of a fan-out, largest peak first, because one branch can need much
more than another. `--json` returns the summary with the rows.

Failed attempts are left out of the figures, and counted separately. A task
killed at its memory limit used more than the limit, so its peak is not a real
measurement. When a retry asked for more memory, the summary shows the budget
the attempt ran with too.

The same numbers are columns on the `tasks` table, so `ginkgo query` can ask its
own questions of them. See [Querying Provenance](querying-provenance.md).

## See Also

- [Tasks and Flows](tasks-and-flows.md) &mdash; how to write tasks.
- [Remote Execution](remote-execution.md) &mdash; running tasks on Kubernetes or
  GCP Batch.
- [CLI](cli.md) &mdash; every `ginkgo run` flag.
- [Reports](reports.md) &mdash; the peak memory column in `ginkgo report`.
