# Custom resource dimensions (issue #181, item 4)

## Problem definition

Tasks can only declare CPU/memory/GPU footprints, but real pipelines contend
for other scarce things: database connections, license seats, API quotas.
Today the only tool is `concurrency_group`, which is the unit-weight special
case (every task costs exactly one slot). There is no way to say "this task
uses 2 of the 10 available API call slots".

## Proposed solution

Arbitrary named integer budgets, Snakemake-style:

```python
@task(resources={"api_calls": 2})
def fetch(...): ...
```

scheduled against budgets from config and CLI:

```toml
[resources.budgets]
api_calls = 10
```

```
ginkgo run flow.py --resource api_calls=10
```

CLI wins over config per key. A dimension a task requests but no budget
names is unconstrained (matching `--memory`'s opt-in semantics).

### Semantics

- **Run-level, not node-local.** Unlike threads/memory/gpu, custom
  dimensions are counted for remote-placed tasks too — an API quota or DB
  connection pool does not stop applying because the task runs on
  Kubernetes. This is the one deliberate semantic divergence from the
  existing dimensions.
- A task whose demand for a budgeted dimension exceeds the total budget is
  an error at preparation time (mirrors the `threads > cores` check).
- Reserved names (the `Resources` field names: `threads`, `memory`, `gpu`,
  `gpu_type`, `memory_retry_multiplier`) are rejected as custom dimensions.
- Values must be positive integers.
- Site overrides work for free: `_OVERRIDE_KEYS` derives from `Resources`
  fields, so `[resources.overrides."name"] custom = {api_calls = 3}` merges
  like any other field (whole-dict replacement).

### Changes by file

- `core/resources.py` — `Resources.custom: dict[str, int]` with validation;
  `parse_resource_budget_args` (CLI `name=value` strings) and
  `resource_budgets_from_config` (the `[resources.budgets]` table) helpers.
- `core/task.py` — `@task(resources=...)` keyword feeding `Resources.custom`.
- `runtime/scheduler.py` — `SchedulableTask.custom`; one weighted-sum CP-SAT
  constraint per budgeted dimension.
- `runtime/evaluator.py` — `resource_budgets` field + `evaluate()` parameter;
  `NodeRun.custom_resources` copied from effective resources at prepare and
  reset on retry; `_running_custom()` accounting (no remote exclusion);
  budget feasibility check in `_prepare_node`; dispatch passes remaining
  budgets; `TaskReady`/`TaskStarted` resource payloads carry a `custom`
  entry when non-empty.
- `cli/app.py` + `cli/commands/run.py` — repeatable `--resource` flag,
  parsed and merged over `[resources.budgets]`.
- `runtime/dry_run.py` + `cli/renderers/dry_run.py` — per-task custom
  demands and a summary line, shown only when custom dimensions exist.
- Deferred (per the issue): unifying `concurrency_group` on top of this.

## Risks and tradeoffs

- `Resources` is a frozen dataclass; adding a dict field silently makes it
  unhashable. Nothing hashes `Resources` today (it is only stored in dicts
  as a value), so this is acceptable; noted here in case that changes.
- Counting custom dimensions for remote tasks means a saturated custom
  budget can hold back remote dispatch — intended, but worth documenting.

## Success criteria

- Concurrency of tasks sharing a budgeted dimension is capped by the budget
  (end-to-end evaluator test), while unbudgeted dimensions are ignored.
- Demand exceeding a budget fails fast with a clear error.
- CLI/config precedence covered by tests; full suite stays green.
