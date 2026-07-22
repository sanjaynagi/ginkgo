# FAQ

Answers to common questions about how Ginkgo works. If you are new, start with
[Why Ginkgo](motivation/) and the [Quickstart](getting-started/quickstart/);
this page is a reference to dip into by topic.

## Getting Started

### What problem does Ginkgo solve?

Ginkgo lets you write a scientific workflow as ordinary Python functions
decorated with `@flow` and `@task()`, then runs them as a dependency graph with
parallelism, content-addressed caching, and a recorded provenance trail.
Calling a task does not run it — it returns a deferred expression, so Ginkgo can
build and validate the whole graph before anything executes, and a task can
inspect its resolved inputs and add new steps at runtime (dynamic DAG
expansion).

Ginkgo fills a gap for data scientists and bioinformaticians who want to code
their workflows in plain Python — mixing Python, shell, script, and notebook steps, each
in its own environment, with caching and provenance — without adopting a
separate workflow language. Ginkgo was built with the specific aim of maintaining a simple, aesthetic
Pythonic syntax.

### How does Ginkgo compare to Snakemake, Nextflow, Prefect, and Dagster?

Snakemake can not handle dynamic DAGs natively. Nextflow is written in Groovy and requires using
the abstraction of channels to pass data between steps. We take the view that a scientific workflow
orchestrator should be plain Python, handle dynamic DAGs natively and should not require the abstraction of channels. Prefect and Dagster are also Python, but they are not well suited to running shell commands, scripts,
and notebooks in foreign environments (e.g an isolated pixi, conda environment or container image).
Ginkgo runs those natively each task can declare its own foreign environment right on the @task() decorator.


### What does the canonical project layout look like, and how does autodiscovery find my flows?

A Ginkgo workflow may be structured however the user desires. The canonical layout, however, is a project root with the config files, one or more workflow packages, and a tests directory:

```text
my-project/
├── pixi.toml
├── ginkgo.toml
├── my_project/
│   ├── __init__.py
│   ├── workflow.py        # flow definitions and wiring — keep this thin
│   ├── modules/           # contain tasks
│   └── envs/              # per-task environment manifests
└── tests/workflows/       # workflow validation checks
```

`results/` and `.ginkgo/` are created at runtime. When you run `ginkgo run` with
no explicit path, autodiscovery scans the project root's child directories and
picks the Python package (one with `__init__.py`) that contains a `workflow.py`.
Exactly one candidate is used automatically; several candidates raise an error
asking you to pass an explicit path; if none are found it falls back to a legacy
root-level `./workflow.py`.

### What's the smallest possible workflow?

A single task plus a flow that returns its deferred call:

```python
from ginkgo import flow, task


@task()
def write_text(message: str, output_path: str) -> str:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(message)
    return output_path


@flow
def main():
    return write_text(message="hello from ginkgo", output_path="hello.txt")
```

```bash
ginkgo run workflow.py
```

## The Workflow DSL

### What are `@flow`, `@task`, `Expr`, and `ExprList`, and how do they fit together?

`@task` (used as `@task()`, with parentheses) turns a plain function into a
`TaskDef` — a lazy task definition carrying the function plus its resource and
caching settings. `@flow` (used without parentheses) marks the pipeline entry
point; calling the flow runs its body to *build* a graph rather than to compute
results. Calling a `TaskDef` produces an `Expr` (a deferred single task
invocation) when every required argument is supplied, or a `PartialCall` when
some are still missing; fanning out over inputs produces an `ExprList` (an
ordered collection of `Expr` objects). A flow body composes task calls and
returns the resulting `Expr` / `ExprList` tree, which the evaluator walks to
build the DAG.

```python
from ginkgo import flow, task


@task()
def write_text(message: str, output_path: str) -> str:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(message)
    return output_path


@flow
def main():
    return write_text(message="hello", output_path="hello.txt")  # returns an Expr
```

### What does it mean that the DSL is "lazy" — when does my code actually run?

Calling a task does not execute the function body. `TaskDef.__call__` validates
the arguments and returns a deferred `Expr` (or `PartialCall`) that records
*which* task to run and *what* arguments to run it with — no task code runs yet.
Your flow body therefore just assembles a graph of these deferred expressions;
the actual task functions run only later, when the evaluator schedules and
executes the nodes. This is what lets Ginkgo build and validate the whole DAG
before anything executes.

### How do I pass the output of one task as the input to another?

Pass the `Expr` returned by one task call directly as a keyword argument to
another task call. When the evaluator registers the graph, any `Expr` (or
`ExprList`, or list/dict/tuple containing them) found in a task's arguments
becomes a dependency edge, and the upstream result is substituted in before the
downstream task runs.

The dependency comes from the `Expr` you pass, not from the value it will carry.
A task whose only job is a side effect can return `None`: pass its `Expr` and the
downstream task both waits for it and receives that `None`. A plain value that is
not an `Expr` (a literal `None`, say) creates no dependency, because nothing links
it back to a producing task.

```python
@flow
def main():
    raw = download(url="https://example.com/data.csv")
    return summarize(data=raw)  # `raw` is an Expr; summarize depends on download
```

If a task returns a tuple and you want a single element, use the `.output`
proxy: `expr.output[0]` yields an `OutputIndex` selecting element 0 of the
upstream result. Applied to an `ExprList`, `.output[i]` returns a new `ExprList`
selecting that element from every branch.

### How do I fan out over a list of inputs (map) and combine results (reduce)?

Call a task with its fixed arguments (leaving the varying parameter unset, which
yields a `PartialCall`) and then `.map(param=[...])` to produce one `Expr` per
element — the result is an `ExprList`. `.map()` zips the varying columns
positionally (all columns must be equal length); `.product_map()` instead
produces one branch per Cartesian combination. To reduce, pass the whole
`ExprList` as one argument to a downstream task — that task then depends on
every branch and receives their collected results.

```python
from ginkgo import flow, task


@flow
def main():
    per_sample = analyze.map(sample=["a", "b", "c"])   # ExprList: one Expr per sample
    return combine(results=per_sample)                 # reduce: depends on all branches
```

`ExprList.map(...)` also exists to extend each existing branch with further
zipped columns, and `max_concurrent=` on any of these throttles how many
generated branches run at once, independently of the global `--jobs`/`--cores`
budget. For building the input/output path lists you map over,
`ginkgo.expand(template, **wildcards)` (Cartesian product) and
`ginkgo.zip_expand(...)` (positional zip) format a `str.format`-style template
into a list of strings.

### How does Ginkgo build and edit dynamic DAGs at runtime?

The graph the flow returns is the *static* DAG, registered up front by walking
the expression tree. Dynamic expansion happens when a **task body, at runtime,
returns new deferred expressions** instead of a plain value. When a Python task
completes, Ginkgo inspects its return value; if it is or contains an `Expr`,
`ExprList`, or `OutputIndex`, Ginkgo registers those returned expressions as new
graph nodes and makes the parent task wait on them. The new nodes are scheduled
like any others; once they finish, the parent task completes using their
results. So a task looks at its inputs, decides what further steps are
needed, and returns task calls describing them, and Ginkgo grafts them into the
running graph.

```python
@task()
def plan(manifest: file) -> list[str]:
    # read inputs, then return more task calls to run
    samples = read_samples(manifest)
    return [process(sample=s) for s in samples]  # returns Exprs -> graph expands
```

A Python task (`kind="python"`) may return dynamic expressions but must not
return an execution directive (`shell(...)`, `notebook(...)`, etc.) — doing so
raises `TypeError`. Driver tasks (shell/script/notebook/subworkflow) return
their directive, and may also return dynamic expressions.

## Task Kinds

### What task kinds exist, and when do I use each?

There are exactly five kinds — `notebook`, `python`, `script`, `shell`, and
`subworkflow` — all declared through the same `@task` decorator and differing
only in what the body does. The kind is passed positionally (`@task("shell")`)
or by keyword (`@task(kind="shell")`); `python` is the default.

- **Python** — `@task()`. The body is ordinary Python and returns a value. Use
  for in-process computation. It runs in a spawned subprocess worker.
- **Shell** — `@task("shell")`. The body returns
  `shell(cmd=..., output=..., log=...)`. Use when the real work is an external
  command line; it can run inside a declared `env`.
- **Script** — `@task("script")`. The body returns
  `script(path, output=..., interpreter=...)`. Use to run a standalone script
  file; resolved task inputs are forwarded as `--param-name value` CLI
  arguments, and the interpreter is inferred from the extension (`.py` →
  `python`, `.R`/`.r` → `rscript`) unless overridden.
- **Notebook** — `@task("notebook")`. The body returns
  `notebook(path, output=..., log=...)`. Use to execute a `.ipynb`
  (Jupyter/Papermill) or `.py` (marimo) notebook; it is rendered to HTML, and
  when `output` is omitted the managed rendered-HTML path is returned.
- **Subworkflow** — `@task("subworkflow")`. The body returns
  `subworkflow(path, params=..., config=...)`. Use to run a nested workflow as a
  self-contained child `ginkgo run`, yielding a `SubWorkflowResult`.

```python
from ginkgo import notebook, script, shell, subworkflow, task


@task("shell", env="bioinfo_tools")
def filter_reads(reads: file) -> file:
    return shell(cmd="seqkit seq ...", output="results/filtered.fastq")


@task("script")
def build_brief(card: file, output_path: str) -> file:
    return script("scripts/build_brief.py", output=output_path)
```

### Are there path-oriented input/output types?

The task model uses three marker types (`ginkgo.file`, `ginkgo.folder`,
`ginkgo.tmp_dir`, all `str` subclasses) to give paths special handling rather
than treating them as opaque strings. A parameter or return annotated `file` is
validated to exist (before execution for inputs, after for outputs) and
contributes its BLAKE3 **content** digest to the cache key; `folder` behaves the
same over a directory's sorted recursive contents. A `tmp_dir` parameter is a
Ginkgo-managed scratch directory, created fresh per task execution, auto-deleted
on success (kept on failure for debugging), and deliberately excluded from the
cache key — you do not pass it yourself; it is auto-injected from the
annotation.

## Environments

### How does Ginkgo use Pixi to make task environments reproducible?

Pixi is Ginkgo's default mechanism for reproducible task environments. Each
named environment lives in a directory (typically `envs/<name>/`) containing a
`pixi.toml` (or a `pyproject.toml` with a `[tool.pixi]` section), and a task
references it by name with `env=`. Before running, Ginkgo materialises the
environment with `pixi install` and runs each command inside it, so the task
sees the locked set of dependencies. Ginkgo also folds the environment's
identity into the cache key by hashing the neighbouring `pixi.lock`, so a change
to the locked dependencies invalidates cached results.

```python
@task(kind="shell", env="bioinfo_tools")
def fastq_stats(sample_id: str, fastq: file) -> file:
    ...
```

### What environment file types can Ginkgo handle — Pixi, Conda, others?

Two manifest types are supported today, both routed through Pixi. First, native
Pixi manifests: a `pixi.toml`, or a `pyproject.toml` carrying a `[tool.pixi]`
table. Second, Conda environment files: a file named exactly `environment.yml`
or `environment.yaml` is imported into a generated Pixi workspace via
`pixi init --import`, stored in a neighbouring `.ginkgo-pixi/` directory and
regenerated automatically when the source file changes. A Conda file must be
referenced by path (so `env` contains a `/`, e.g.
`envs/genomics/environment.yml`), not by bare name. There is no separate/native
Conda runner and no support for `requirements.txt`, plain `venv`, or bare
`conda activate` — everything resolves to a Pixi manifest, and Pixi must be
installed on `PATH`.

### How does container-backed execution work for shell tasks, and when should I use it?

Set `env` to a container URI — `docker://<image>` or `oci://<image>` — and
Ginkgo routes the task to the container backend instead of Pixi. It runs the
task inside that image, with Docker by default, and bind-mounts the project root
at its host path so baked-in paths resolve. Use containers when a tool is only
distributed as an image, when you need OS-level isolation beyond what a
Pixi/Conda spec captures, or to pin an exact published image. Container execution
targets shell (and script/notebook) tasks — Python task bodies still run in the
scheduler's own Python process, not in the container.

### Can different tasks in one workflow run in different environments?

Yes — the environment is declared per task through the `env=` argument on
`@task`, so every task can target a different Pixi environment, Conda file, or
container image, and they are prepared independently. If `env` is omitted
(`None`), the task runs in the current environment with no foreign-environment
wrapping.

```python
@task(kind="shell", env="bioinfo_tools")                   # named Pixi env
def align(...): ...

@task(kind="shell", env="envs/genomics/environment.yml")   # Conda file
def call_variants(...): ...

@task(kind="shell", env="docker://ubuntu:24.04")           # container image
def count_reads(...): ...
```

## Scheduling And Resources

### How does Ginkgo decide task execution order, and how does task priority act as a tiebreaker?

Ordering is driven by the dependency graph (a task becomes "ready" only once its
dependencies complete) plus a resource-packing solve, not a simple sorted queue.
On each dispatch cycle Ginkgo packs the ready tasks into the run's budget,
choosing them by a single ordered objective: (1) dispatch as many tasks as
possible, then (2) fill the core budget, then (3) prefer higher-priority tasks,
then (4) break remaining ties in declaration order. So `priority` (default `0`,
higher wins) is a strict tiebreaker among simultaneously-ready tasks — it
influences the choice only when task-count and core-fill are already tied, and
never lets a high-priority task block a larger feasible set of lower-priority
ones.

### How does Ginkgo monitor and control CPU resources, and avoid oversubscribing CPU/memory?

Control is static and declarative: each `@task` declares `threads=N` (CPU
footprint) and `memory="…"` in Kubernetes notation (`512Mi`, `4Gi`, `8G`, `Ti`,
`Ki`; rounded up to whole GiB). The scheduler enforces these against the run
budget as hard constraints — `sum(threads) <= cores` and, when a memory budget
is set, `sum(memory_gb) <= memory` — subtracting the footprint of
already-running tasks each cycle, so it never oversubscribes the declared budget.
Separately, a resource monitor *observes* actual usage: it samples the task
process tree roughly once a second and surfaces live/peak CPU and memory in the
CLI and run record. This monitor is observational only — it reports usage but does not
throttle or kill tasks; enforcement comes entirely from the declared-footprint
budgets. Note that memory-aware scheduling is off unless you pass `--memory`;
without it, only the core/job budgets constrain packing.

### How do I cap total parallelism for a run?

Use the `ginkgo run` flags `--jobs`, `--cores`, and `--memory`. `--jobs` caps the
number of tasks running concurrently; `--cores` caps the total summed `threads`
in flight; `--memory` (GiB) caps total declared memory. By default `--jobs`
falls back to the machine's CPU count, `--cores` defaults to the resolved
`--jobs` value, and `--memory` is unset (memory-aware packing disabled).

```bash
# Run at most 4 tasks at once, within an 8-core, 32 GiB budget
ginkgo run workflow.py --jobs 4 --cores 8 --memory 32
```

## Caching

### How does Ginkgo perform caching, and what makes it "content-addressed"?

Ginkgo caches every task result under `.ginkgo/cache/`, keyed by a hash of what
the task *is* and what it *was given* rather than by name or timestamp. Before a
task runs, Ginkgo assembles a canonical JSON payload and hashes it; if an entry
already exists for that key, the stored result is reused and the task never
executes. All content hashing uses BLAKE3 (chosen for speed and native
multi-core hashing of large files). Because the key is derived from content,
moving or renaming a file changes nothing, but changing its bytes produces a new
key.

### What exactly goes into a task's cache key?

The key hashes a sorted object with exactly these fields: the task name, the task
`version`, the task `source_hash` (source plus local import closure), the
resolved `inputs`, the declared `env`, and an `env_hash`. Each input is hashed by
declared type: `file`/`folder` arguments are hashed by content, an `AssetRef`
contributes its content hash, a remote reference contributes its object-store
version id (staging first if needed), primitives are hashed from their `repr`,
and any other object is hashed via the value codec. `tmp_dir` parameters are
deliberately excluded. The `env_hash` is `None` when the task declares no env;
otherwise it is the environment name plus the resolved lock-file digest.

### When I edit a helper module that a task imports, will the cache correctly invalidate?

Yes. A task's `source_hash` is computed from the task function's own source
*and* its local import closure: Ginkgo walks the statically-imported modules that
live under the same source root and folds their source into the hash. Editing
any reachable helper module changes the combined hash and invalidates the task,
even if the specific edited symbol is not called. This tracking is deliberately
conservative and static-only. If a module in the closure cannot be read or
parsed, Ginkgo now raises an error rather than silently skipping it, so a syntax
error no longer quietly truncates the closure and masks a stale cache.
Runtime-only dependencies (dynamic imports, data files) still cannot be tracked
this way; bump `version=` on the task when those change.

### How do warm runs skip work?

Cache lookups happen during node *preparation*, before any worker is dispatched.
Once a task's inputs are resolved, Ginkgo tries to satisfy the node straight from
cache; on a hit the node is marked complete and no execution slot is ever used.
The default path builds the full content-addressed key and also validates that
cached file/folder outputs are still materialised correctly. Running with
`--trust-workspace` enables a faster path that skips content hashing and only
checks that output files exist — quicker for warm reruns, but weaker, since it
trusts the working tree instead of re-verifying bytes.

### How do I evict or prune the cache, and how do I force a re-run?

```bash
ginkgo cache ls                                  # list entries with size and age
ginkgo cache clear <cache-key>                   # remove one entry
ginkgo cache prune --older-than 30d --dry-run    # preview an age-based prune
ginkgo cache prune --max-size 5GB                # keep the cache under a size budget
ginkgo cache prune --max-entries 100             # keep at most N entries
```

`prune` requires at least one of `--older-than`, `--max-size`, or
`--max-entries`, and `--dry-run` previews without deleting; pruning and clearing
also garbage-collect orphaned artifacts. There is no `--no-cache` or force-rerun
flag on `ginkgo run`. To force a task to re-execute, either bump its `version=`
(a dedicated cache-busting tag that feeds directly into the key), change its
source, or `ginkgo cache clear <cache-key>` for that entry.

### Where does the artifact store fit in for file/folder outputs?

File and folder outputs are copied into a content-addressed artifact store under
`.ginkgo/artifacts/` (hashed with BLAKE3), and that store — not the task's
declared output path — is the durable source of truth. On a cache hit, the
artifact store re-materialises the output into the working tree; large
serialized return values are also offloaded here rather than inlined.

## Value Transport And Serialization

### How does Ginkgo serialize values passed between tasks?

Values crossing a task boundary go through a codec that turns a Python object
into a JSON-safe payload tagged with a type marker. Primitives (`None`, `bool`,
`int`, `float`, `str`) pass through as-is; `file`/`folder`/`tmp_dir`,
`AssetRef`, and `AssetResult` get typed wrappers; and `list`/`tuple`/`dict` are
encoded recursively. Anything else falls through to a binary encoder, which
serializes a NumPy array via `.npy`, a pandas DataFrame via Parquet (falling
back to pickle), and every other object via `pickle` (protocol 5).

### What's the inline-versus-artifact threshold?

Binary payloads are base64-inlined into the payload when they are at or below
256 KiB. Above that, Ginkgo offloads the bytes: to the artifact store when one is
supplied (cache persistence), or to an ephemeral directory for process
transport. The decoded value is reconstructed transparently either way, so
callers do not see the difference.

### How does serialization differ between local and remote execution?

The codec is the same in both cases; the difference is what carries the bytes.
Locally, the evaluator encodes task args into a transport directory and the
process-pool worker decodes them; results often come back as a direct in-memory
Python object with no re-serialization, while the Pixi subprocess path uses a
pickle-over-JSON bridge. For remote execution the encoded payload is layered with
a remote-transfer step: `file`/`folder` artifacts are uploaded to a shared
remote artifact store (object storage) as references, and the remote worker
downloads them before running, then pushes outputs back the same way — so
nothing large travels inside the job payload itself.

### What types can be passed between tasks, and what happens to a value with no special codec?

Any value the codec can encode: the typed cases above, plus anything that
survives the binary encoder. A value with no dedicated codec is pickled
(protocol 5) as the final fallback, so most standard Python objects work — but an
object that cannot be pickled fails. Ginkgo validates process-bound values up
front and raises an error naming the offending value's type if it cannot be
encoded, so unpicklable objects fail loudly rather than mid-transport.

## Assets And Outputs

### What is an asset, and how is it different from an ordinary task return value?

An asset is a typed, named, versioned task output, produced by returning
`asset(...)` (or a typed helper) instead of a plain value. Where an ordinary
`file` return is just bytes at a path, an asset also carries a *kind*, a stable
*key* (`namespace/name`), a content hash, a producer task, and metadata, and it
is registered in a catalog under `.ginkgo/assets/` and tracked across runs.
Re-running a task that produces identical content adds a new *version* pointing
at the same bytes, so the key stays a stable handle with full version history.
Assets can also be consumed by downstream tasks, which receive an `AssetRef`
they can `load()` or open `as_file()`.

### What asset kinds exist, and how does Ginkgo detect the kind?

The kinds are `file`, `table`, `array`, `fig`, `text`, and `model`. Ginkgo does
not sniff the top-level kind from the payload — you choose it by calling the
matching helper (`table()`, `array()`, `fig()`, `text()`, `model()`) or
`asset(payload, kind=...)`. Within a kind, Ginkgo inspects the payload to pick a
serialization *sub-kind*: for example `table()` accepts a
pandas/polars/pyarrow/duckdb object or a `.csv`/`.tsv` path, and `model()`
derives the framework from the payload's top-level module (or from an explicit
`framework=`). A `file` asset expects a path-like payload and is the only kind
that stores its bytes by copying the declared source path directly.

### What are asset checks, and what happens to a run when a check fails?

Checks are small data-quality functions passed as `checks=[...]`; each receives
the wrapped payload and must return a `bool`. During registration Ginkgo runs
every check *before* it writes the catalog version — if a check returns `False`,
returns a non-bool, is not callable, or raises, Ginkgo raises `AssetCheckError`,
which aborts registration and fails the producing task. This means a failed check
leaves **no** asset version written for that asset. Passing outcomes are stored
on the version under the `ginkgo_checks` metadata key and shown both on HTML
report cards and by `ginkgo asset show`. Define checks as importable top-level
functions (not lambdas or closures) so they survive transport to worker and
remote execution, and note that checks are not re-run for cached assets.

### How do captions and groups affect how assets appear in the report?

`group` and `caption` are stored on the version under the `ginkgo_group` and
`ginkgo_caption` metadata keys. Assets sharing a `group` are rendered together
under a named heading in the HTML report, and assets without one fall under an
"Ungrouped assets" section. The `caption` is shown as a short subtitle on the
asset's report card and is also printed by `ginkgo asset show`.

### How do I browse, inspect, and load a specific asset version from the CLI?

The `ginkgo asset` command has four subcommands:

```bash
ginkgo asset ls                 # every asset key, its latest version, and version count
ginkgo asset versions <key>     # full version history for one key
ginkgo asset show <ref>         # kind-specific metadata: caption, check outcomes, schema/shape/metrics
ginkgo asset inspect <ref>      # raw AssetVersion record, including the on-disk artifact path
```

A `<ref>` is an asset key with an optional version/alias selector resolved by the
catalog; `<key>` is the plain `namespace/name`. There is no CLI subcommand that
streams the payload bytes for you — `ginkgo asset inspect` prints the resolved
artifact path so you can open the bytes directly, and within a workflow a
downstream task loads a version through the `AssetRef` it receives. Separately,
`ginkgo models [run_id]` lists model assets with their recorded metrics.

## Reports

### What's in the exported HTML report, and how is it structured/bundled?

`ginkgo report` renders a completed run (status `succeeded` or `failed`) into a
self-contained HTML report; running or pending runs are rejected.

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The report contains the run summary and stat cards, run parameters, an SVG
task-graph laid out as a layered DAG, per-task status and timing, failure cards
with log tails, asset previews (tables, figures, arrays, text, model metrics)
with any check outcomes, and links to rendered notebooks. By default it is
written as a directory bundle at `.ginkgo/reports/<run-id>/` that you can open or
share. Useful flags: `--single-file` (inline CSS, fonts, figures, and logs into
one file), `--out <dir>`, `--embed-full-assets` (copy single-file artifact bytes
into the bundle; directory-backed artifacts such as zarr are excluded),
`--max-log-lines N` (default 80), and `--open`/`--no-open`. To list rendered
notebook artifacts across runs from the terminal, use `ginkgo notebooks`.

## Failures And Retries

### When a task fails, does the whole run stop, and do in-flight tasks finish?

The run stops scheduling new work but does not kill tasks that are already
running. On the first unretryable failure the evaluator records the failure and
cancels only the futures that are still queued and have not started. The main
loop then keeps waiting on the already-running futures until they complete — it
simply stops dispatching new tasks — after which it re-raises the stored failure
and the run ends with status `failed`. An external interrupt such as Ctrl-C is
different: it terminates subprocesses, cancels remote job handles, and shuts down
the executor pools.

### How do retry policies and exponential backoff work, and which failures are retried?

Retries are declared on the task decorator:

```python
import ginkgo

@ginkgo.task(
    retries=3,
    retry_on=(ConnectionError, TimeoutError),   # optional: restrict by exception class
    retry_on_exit_codes=(1, 75),                 # optional: restrict by process exit code
    retry_backoff=2.0,                           # base delay in seconds; 0 means no delay
    retry_backoff_multiplier=2.0,
    retry_backoff_max=60.0,
)
def fetch(...):
    ...
```

`retries` defaults to `0` (no retries). When `retries > 0` and neither
`retry_on` nor `retry_on_exit_codes` is set, **every** exception is retried up to
the limit; setting `retry_on` and/or `retry_on_exit_codes` narrows retries to
matching failures. The delay before attempt *n* is
`retry_backoff * retry_backoff_multiplier ** (n - 1)`, capped at
`retry_backoff_max`; if `retry_backoff` is `0` (the default) retries happen
immediately with no backoff. On each retry the node's scratch dirs are removed
and its resolved args, cache key, and secrets are cleared so the attempt reruns
from scratch.

### How do I debug a failed task — where are the logs, and what do the commands show?

Per-task `stdout`/`stderr` are written to `<run_dir>/logs/` (under
`.ginkgo/runs/<run_id>/logs/`). `ginkgo debug [RUN_ID]` prints a panel per failed
task with the classified category and a log tail (add `--json` for
machine-readable output); it defaults to the latest run if no id is given.
`ginkgo inspect run [RUN_ID]` prints a normalized JSON snapshot drawn from the
manifest — per-task status, attempts, cache key, exit code, the `failure` record,
log paths, timings, dependencies, and (for remote tasks) the remote job id and
backend.

## Remote Execution

### How do I mark a task to run remotely?

Remote dispatch is opt-in per task on the `@task` decorator. A task is sent to
the remote executor when it declares either `remote=True` or `gpu` greater than
zero. Dispatch only actually happens if a remote executor was configured for the
run; if you declare `remote=True` but run without `--executor`, the task simply
runs locally. Everything else runs in the local process pool as usual.

```python
from ginkgo import task

@task(remote=True, memory="32Gi")
def large_computation(input_path: str) -> str:
    ...

@task(gpu=1, threads=8)          # gpu > 0 also triggers remote dispatch
def train_model(dataset: str) -> str:
    ...
```

### Which remote backends are actually supported?

Two executors exist: Kubernetes (`--executor k8s`) and GCP Batch
(`--executor batch`); the CLI `--executor` choices are exactly `local` (default),
`k8s`, and `batch`. There is a single Kubernetes executor that submits
`batch/v1` Jobs, so GKE / EKS / OKE are not separate backends — they are just
clusters the one Kubernetes executor talks to. GCP Batch is a distinct
serverless executor.

```bash
ginkgo run --executor k8s workflow.py
ginkgo run --executor batch workflow.py
```

### How does my code get packaged and synced to a remote worker?

By default the worker image is expected to already contain your code ("baked"
mode). Opt into code-sync by adding a `code` table under your executor config
with `mode = "sync"` and `package = "<your_package_dir>"`, plus a
`[remote.artifacts] store` URI. On the first remote dispatch Ginkgo tars the
package directory, content-addresses it, and uploads it to the artifact store;
each worker downloads and extracts it before running. The bundle is built once
per run and reused, and unchanged code is not re-uploaded.

```toml
[remote.k8s.code]        # or [remote.batch.code]
mode = "sync"
package = "my_workflow"

[remote.artifacts]
store = "gs://my-bucket/ginkgo-artifacts/"
```

### How do I request a GPU, or set per-task memory / CPU for a remote task?

Use the `@task` decorator resource hints: `threads` (int, CPU cores), `memory`
(a string in Kubernetes notation such as `"16Gi"`), and `gpu` (int). These are
sent to the worker as the job's resource request. `gpu` also serves as an
implicit remote trigger. The GPU accelerator type itself is not set per task — it
comes from the executor config (`gpu_type` under `[remote.k8s]` or
`[remote.batch]`).

```python
@task(remote=True, threads=8, memory="16Gi", gpu=1)
def train(dataset: str) -> str:
    ...
```

### How does remote execution integrate with provenance and caching?

The cache is checked before dispatch, so a cache hit never touches the cloud. A
remote job returns a payload with the same shape as a local worker result and is
completed through the same code path, so a remote result populates the same
content-addressed cache as a local run — outputs are mirrored via the remote
artifact store and pulled back locally. Provenance records the execution backend
(`local` vs `remote`), the remote job id (K8s job name or Batch job id), and the
resource request; captured pod/job logs are attached at completion.

## Remote Input Access

### When are remote inputs streamed via FUSE versus staged/downloaded?

Each remote input (`gs://`, `s3://`, `oci://`) is resolved to one of two modes:
**stage** (default) downloads the whole object to local disk before the task
runs, or **fuse** mounts the bucket in-container and streams reads on demand.
`stage` is the safe default; `fuse` is only chosen when a driver is available for
the scheme and the task is streaming-compatible.

### How do I control the access mode — per ref, per task, by pattern, or via config?

The precedence is: (1) an explicit `access=` on the ref
(`remote_file(..., access="fuse")`) wins; (2) the task decorator default
`remote_input_access="fuse"|"stage"`; (3) a pattern match from config; (4) an
auto-enable size heuristic; (5) the config default. A task can veto streaming
with `streaming_compatible=False`, which forces `stage` even when `fuse` was
requested. Pattern rules live under `[remote.access]` as `default_for_pattern`
entries with `glob` and `access` keys (globs are matched against the object key
and the full URI).

```toml
[remote.access]
default = "stage"       # or "fuse"
auto_fuse = false       # size-gated auto-promotion; needs a passing doctor probe

[[remote.access.default_for_pattern]]
glob = "*.bam"
access = "fuse"
```

```python
from ginkgo import remote_file, task

@task(remote=True, remote_input_access="fuse", streaming_compatible=True)
def count_reads(bam: file) -> int:
    ...

bam = remote_file("gs://my-bucket/sample.bam", access="fuse")   # per-ref override
```

### What happens if a FUSE mount fails — is there a fallback?

Yes. On the worker, each fuse-marked input is materialised through a mounted
access strategy; if the mount raises (driver missing, `/dev/fuse` unavailable,
permission denied, etc.), the hydrator catches the exception and falls back to a
staged download of that ref. The failure reason is recorded on the access stats
so the downgrade is visible in run provenance rather than silent, and the CLI
surfaces a notice. Fallback is per-ref, not all-or-nothing for the task.

### What object-store schemes are supported?

Three: `s3`, `gs`, and `oci`. Each maps to an fsspec-backed object store. For
FUSE streaming the drivers are: `gs` → gcsfuse, `s3` → mountpoint-s3, `oci` →
rclone. Any other scheme raises an "unsupported remote scheme" error.

## Provenance

### What does Ginkgo record for each run, and where does it live?

Each run gets a directory at `.ginkgo/runs/<run_id>/` (the id is a UTC timestamp
plus a discriminator). Inside it:

- `manifest.yaml` — run id, workflow path, jobs/cores/memory, status, start/finish
  times, aggregate resources and timings, and a `tasks` map. Each task entry holds
  status, attempt/attempts, cache key, `cached`, exit code, env, kind, dependency
  ids, the `failure` record, outputs, log paths, per-task timings, and (for remote
  tasks) the job id and execution backend.
- `events.jsonl` — an append-only event stream (task started/running/
  completed/failed/retrying, cache hits/misses, etc.).
- `params.yaml` — the workflow parameters.
- `envs/` — copies of the environment lock files used by the run.
- `logs/` — per-task stdout/stderr.

## Configuration And Secrets

### How do I configure a workflow, and do config values feed into cache keys?

Load a config file inside the workflow module with
`ginkgo.config("ginkgo.toml")` (TOML and YAML are both supported); it returns a
plain nested dict you use to shape the graph. At the CLI, `--config <path>`
supplies override paths — when overrides are given for a session they define the
runtime config, otherwise the canonical project config is loaded, and multiple
mappings are merged at the top level. Config values do **not** feed into cache
keys directly: the cache key is built from the task, its version, source hash,
env, env hash, and its resolved *arguments*. A config value therefore only
influences a task's cache key if it flows into that task's arguments — changing a
config entry that no task consumes as an argument will not invalidate the cache.

### How do I reference a secret without leaking it into logs, provenance, or the report?

Use `ginkgo.secret("NAME", backend="env")` and pass the returned reference as a
task argument; it is a deferred `SecretRef` resolved only at execution time
(backends: `env` for environment variables, and a dotenv backend). Declared
secrets are validated before the run starts, and the real value is substituted
into the arguments the worker actually receives — but provenance and cache
metadata store the *template*, so a `SecretRef` is recorded as redacted and its
string form is a placeholder like `<secret:env:NAME>`, never the value. Task
stdout/stderr pass through a redacting writer that replaces any resolved secret
value with `[REDACTED]`, and exceptions are sanitised the same way before being
recorded or displayed, so the plaintext secret does not reach logs, the manifest,
or the report.

## Sub-Workflows And Composition

### How do I compose one workflow inside another with `@task(kind="subworkflow")`?

Write a task with `kind="subworkflow"` whose body — called with fully resolved
argument values — returns a `subworkflow(...)` descriptor pointing at the child
workflow file:

```python
from ginkgo import task
from ginkgo.core.subworkflow import subworkflow


@task(kind="subworkflow")
def run_child(sample: str):
    return subworkflow(
        "child/workflow.py",
        params={"sample": sample},
        config=["overrides.toml"],
    )
```

The runtime runs the child as a self-contained subprocess
(`python -m ginkgo.cli run <path> --config ...`); `params` is serialized to a
temporary YAML file and forwarded as an extra `--config`, and any `config` paths
are forwarded too. Recursion is bounded by a call-depth limit (default 8, tracked
via the `GINKGO_CALL_DEPTH` environment variable), which raises an error when
exceeded.

### How is a child run stitched into the parent's provenance?

The child run gets its own full run directory and `manifest.yaml` under
`.ginkgo/runs/<child_run_id>/`; the parent stores a reference to it rather than
inlining the child's tasks. The child process prints a machine-readable
`GINKGO_CHILD_RUN_ID=<run_id>` line, which the parent captures and returns to the
calling task as a `SubWorkflowResult` (with `run_id`, `status`, and
`manifest_path`). In the parent's manifest, the calling task records `sub_run_id`
(the child's run id) on success, and on failure the child's run id is still
recorded via the raised error, so you can trace into the child run either way.
