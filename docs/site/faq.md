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

Snakemake builds its DAG from rules and file-name patterns. Its `checkpoint` rules re-evaluate
that DAG once an output exists, but the dynamic part must be expressed through that mechanism
rather than in ordinary code. Nextflow is written in Groovy and requires using
the abstraction of channels to pass data between steps. We take the view that a scientific workflow
orchestrator should be plain Python, handle dynamic DAGs natively and should not require the abstraction of channels.
Prefect and Dagster are also Python, but they require DIY effort to run shell commands, scripts,
and notebooks in foreign environments (e.g. an isolated pixi, conda environment or container image).
Ginkgo runs those natively: each task can declare its own foreign environment right on the `@task()` decorator.


### What does the canonical project layout look like, and how does autodiscovery find my flows?

A Ginkgo workflow may be structured however the user desires. The canonical layout, however, is a project root with the config files, one or more workflow packages, and a tests directory:

```text
my-project/
├── pixi.toml
├── ginkgo.toml
├── workflow/              # always named "workflow"
│   ├── __init__.py
│   ├── flow.py            # flow definitions and wiring — keep this thin
│   ├── modules/           # contain tasks
│   ├── envs/              # per-task environment manifests
│   ├── notebooks/         # notebook-task source
│   └── scripts/           # script-task source
└── tests/workflows/       # workflow validation checks
```

`results/` and `.ginkgo/` are created at runtime. When you run `ginkgo run` with
no explicit path, autodiscovery looks for a file named `flow.py` at the project
root or in one of its immediate subdirectories. The directory name does not
matter, and `__init__.py` is not required. Exactly one candidate is used
automatically. With several, `workflow/flow.py` wins over a `flow.py` in any
other subdirectory, so a child workflow in its own folder does not get in the
way; otherwise several candidates raise an error asking you to pass an explicit
path. An explicit path accepts any file name, anywhere.

### What's the smallest possible workflow?

A single task plus a flow that returns its deferred call:

```python
from ginkgo import Out, file, flow, task


@task()
def write_text(message: str, output_path: Out[file]):
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(message)


@flow
def main():
    return write_text(message="hello from ginkgo", output_path="hello.txt")
```

```bash
ginkgo run flow.py
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
from ginkgo import Out, file, flow, task


@task()
def write_text(message: str, output_path: Out[file]):
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(message)


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
it back to a producing task — with one exception: a literal path string that
matches another task's declared `Out[...]` path is matched up and turned into a
real dependency edge too, even though it is not an `Expr`. See
[Reads vs. Writes: `file` vs. `Out[file]`](guide/caching-and-provenance.md#reads-vs-writes-file-vs-outfile).

```python
@flow
def main():
    raw = download(url="https://example.com/data.csv")
    return summarize(data=raw)  # `raw` is an Expr; summarize depends on download
```

If a task returns several outputs and you want one of them, use the `.output`
proxy — see
[Selecting One Output With `.output[i]`](guide/tasks-and-flows.md#selecting-one-output-with-outputi).

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

`ExprList.map(...)` also exists to chain a second fan-out: each existing
branch is combined with each new zipped row, giving one flat `ExprList`, so
two branches mapped over three rows make six (see
[Chaining Fan-Out](guide/tasks-and-flows.md#chaining-fan-out)).
`max_concurrent=` on any of these throttles how many generated branches run
at once, independently of the global `--jobs`/`--cores`
budget. For building the input/output path lists you map over, see
[Building The Varying Lists With `expand()`](guide/tasks-and-flows.md#building-the-varying-lists-with-expand);
for one output path per grid cell, see
[Per-Cell Output Paths With `per_branch()`](guide/tasks-and-flows.md#per-cell-output-paths-with-per_branch).

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
their directive, and may also return dynamic expressions. A driver task whose
every `return` is a plain string or other literal can never return its
directive, so `ginkgo doctor` and `ginkgo run --dry-run` warn about it
(`missing_directive_return`) before a run fails there.

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
  `notebook(path, output=..., log=...)`. Use to run a marimo notebook (a `.py`
  file), which reads the task's inputs with `mo.cli_args()`; it is rendered to
  HTML, and when `output` is omitted the rendered-HTML path is returned. This
  kind comes from the `ginkgo-notebook` plugin (`from ginkgo_notebook import
  notebook`).
- **Subworkflow** — `@task("subworkflow")`. The body returns
  `subworkflow(path, params=..., config=...)`. Use to run a nested workflow as a
  self-contained child `ginkgo run`, yielding a `SubWorkflowResult`.

```python
from pathlib import Path

from ginkgo import Out, file, script, shell, subworkflow, task


@task("shell", env="bioinfo_tools")
def filter_reads(reads: file) -> file:
    return shell(cmd="seqkit seq ...", output="results/filtered.fastq")


_SCRIPTS_DIR = Path(__file__).resolve().parent / "scripts"


@task("script")
def build_brief(card: file, output_path: Out[file]) -> file:
    return script(_SCRIPTS_DIR / "build_brief.py")
```

### Are there path-oriented input/output types?

The task model uses marker types (`ginkgo.file`, `ginkgo.folder`,
`ginkgo.tmp_dir`, `ginkgo.untracked`, all `str` subclasses, plus the
`ginkgo.Out[...]` wrapper below) to give paths special handling rather than
treating them as opaque strings. A parameter or return annotated `file` is
validated to exist (before execution for inputs, after for outputs) and
contributes its BLAKE3 **content** digest to the cache key; `folder` behaves
the same over a directory's sorted recursive contents. A `tmp_dir` parameter
is a Ginkgo-managed scratch directory, created fresh per task execution,
auto-deleted on success (kept on failure for debugging), and deliberately
excluded from the cache key — you do not pass it yourself; it is
auto-injected from the annotation. `pathlib.Path` is rejected outright as a
task parameter annotation: it is neither path- nor content-tracked (hashed
as an opaque pickled object), so there is no legitimate use for it — use
`file`, `folder`, or `Out[file]` instead.

**A `str` (or `Any`/`list[str]`/...) parameter naming an existing file is
content-tracked by default**, whatever its annotation. If the value is a
`str`/`PathLike` that reads as a path — it contains a separator (`data/x.tsv`)
or carries a file extension (`x.tsv`) — and names an existing regular file on
disk, its BLAKE3 content digest goes into the cache key exactly as it would
under `file`. This closes the historical trap where editing a file, or an
upstream task returning a different one, left a `str`-typed consumer serving
a stale, now-wrong cached result. A bare word with neither a separator nor an
extension (`"alpha"`) does not qualify, even if a same-named file exists — it
is tracked by its own value, not a path.

**A directory is never auto-hashed this way** — a plain `str` naming an
existing directory ("." or "results", say) stays tracked by its path string
only, since hashing an entire tree as a side effect of an ordinary scalar
parameter would be a surprise no one asked for. Annotate it `folder` to track
its contents. Ginkgo still warns when it can tell the difference: if an
argument resolved from an upstream task arrives as a plain `str` naming an
existing *directory*, the run prints a notice naming both ends and the
`folder` annotation to add.

Use **`ginkgo.untracked`** to deliberately key a path by its string only,
whatever exists there — a shared log file several tasks append to, a
staging path that is not itself the tracked artifact, or any path whose
content genuinely should not participate in the cache key. It never
content-hashes, never warns, and is labelled `untracked` by
`ginkgo cache explain`.

For a path a task only *writes* — an output location — annotate it
`Out[file]` / `Out[folder]` rather than `str`: `Out[...]` marks the
parameter itself as a write rather than a read, so it is validated for
writability (not existence) before execution, contributes only its path
string to the cache key, and is checked to exist — with the right kind —
after execution. Ginkgo also creates the parent directory for you, and a
task whose only outputs are its declared `Out[...]` parameters needs no
return statement at all — see
[Reads vs. Writes: `file` vs. `Out[file]`](guide/caching-and-provenance.md#reads-vs-writes-file-vs-outfile).
`Out[...]` is also the fix for a legacy task that still writes to a
`str`-annotated `output_path`: without it, that path is content-hashed like
any other root input the first time this rule sees it, and — if you never
migrate — Ginkgo excludes a path it can already see is this task's own prior
output (from its cache-artifact record) so the task does not self-invalidate;
but that exclusion only fires for a path the task actually *returns* as
`file`/`folder` — a path written but never returned still needs `Out[...]`
or `untracked`, since Ginkgo has no other way to know it is not meant to be
read.

A separate, purely static check catches a related mistake earlier: if a
parameter's name looks like a path (`path`, `output_dir`, `report_files`,
...) but is annotated a bare `str` shape (`str`, `str | None`, `list[str]`,
`tuple[str, ...]`), `ginkgo doctor` and `ginkgo run --dry-run` emit a
`path_like_str_param` warning naming the task and parameter, whether or not the
workflow has ever run — no filesystem access, no execution required. The
warning is name-based, so it fires even when the value turns out to be an
existing file that reads as a path (which is content-tracked, as above). What
it still guards against: a directory, a bare word with no separator or
extension, or any other non-file path is tracked by its string alone, only `file`/`folder` check that the path exists, and a path the task
writes belongs in `Out[...]`. It suggests `file`/`folder` for a path the task
reads, or `Out[file]`/`Out[folder]` for one it writes. `untracked` never triggers this
warning: it is a declared choice, not the trap the warning exists to flag.

### Why did a task in my flow never run?

The task graph is exactly what is reachable from the value your `@flow` function
returns. A call whose result is never referenced — a bare `plot(...)` statement
written for its side effects, or a producer orphaned because you passed a
literal path to the consumer instead of the upstream expression — is not part of
the run. Return its result (on its own, or inside a tuple, list, or dict) to
include it.

Ginkgo reports these rather than dropping them quietly: `ginkgo doctor` emits an
`unreachable_task_call` warning, `ginkgo run --dry-run` lists them under
"Dropped (not reachable from the flow return value)", and a real run prints a
notice, with the same fix, before starting.

## Environments

### How does Ginkgo use Pixi to make task environments reproducible?

Pixi is Ginkgo's default mechanism for reproducible task environments. Each
named environment lives in a directory (typically `envs/<name>/`) containing a
`pixi.toml` (or a `pyproject.toml` with a `[tool.pixi]` section), and a task
references it by name with `env=`. Before running, Ginkgo materialises the
environment with `pixi install` and runs each command inside it, so the task
sees the locked set of dependencies — see
[Environments](guide/environments.md#ginkgo-materialises-declared-environments)
for what that means for a first run and what must be on your `PATH`.

Ginkgo folds the environment into the cache key by hashing the manifest, so
editing the declared dependencies invalidates cached results. The neighbouring
`pixi.lock` is not part of the key: it is written by `pixi install`, which runs
after the key is built, so keying on it made the second run of every workflow
redo its work. It is checked instead. Each cache entry records the digest of the
environment as it was installed when the entry was written, and a cache hit is
only served if the environment installed here now matches. So `pixi update`,
which re-solves every dependency without touching `pixi.toml`, does invalidate —
the key is unchanged, but the recorded environment no longer matches. The same
holds for containers: an image repointed under a mutable tag invalidates once
that image has been pulled here.

What does *not* invalidate: a hit served on a machine where the environment has
never been installed, or an image never pulled. There is no local evidence to
compare against, and getting some would mean installing or pulling an
environment to serve a cache hit. Entries written by an older version of Ginkgo,
which carry no recorded environment, are likewise accepted. Pin
`image@sha256:...`, or bump `version=`, when you want the key itself to carry
that precision.

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
ginkgo run flow.py --jobs 4 --cores 8 --memory 32
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

Hashing a large file on every run would be slow, so each digest is memoised
under the file's device, inode, size and modification time, and reused while
those four agree. A rewrite that keeps the size and the modification time —
a tool that restores timestamps, `touch -r`, or a filesystem with coarse
timestamps — is therefore not re-hashed, and the old digest stands. `touch`
the file to make the next run hash it again.

### What exactly goes into a task's cache key?

The key hashes a sorted object with exactly these fields: the task name, the task
`version`, the task `source_hash` (source plus local import closure), the
resolved `inputs`, the declared `env`, and an `env_hash`. Each input is hashed by
declared type: `file`/`folder` arguments are hashed by content, a remote
reference contributes its object-store
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

A data file opened by a literal path inside a task body is one of these: the
cache never sees it, and nothing warns. Pass the path in as a `file` argument
instead and its bytes become part of the key. See the note in the
[CLI guide](guide/cli.md#workflow-parameters).

### Does changing a task's threads or memory invalidate its cache?

No. Before any source text is hashed — the task definition itself, and every
file in its import closure — Ginkgo deletes the resource-only arguments of each
`@task(...)` call from that text. `threads`, `memory`, `gpu`, `gpu_type`,
`memory_retry_multiplier`, `resources`, `priority`, `on_failure` and the
`retry*` family are all filtered, so retuning them leaves the cache key
untouched and cached results stay valid.

Nothing else about the source is normalised. `kind=`, `env=` and `version=` all
keep invalidating, as does a decorator of your own stacked on the task: Ginkgo
cannot know which arguments of an unfamiliar decorator are safe to ignore, so it
hashes them. The same goes for `@task` reached under an alias
(`from ginkgo import task as run`), and for a task nested inside another
function whose body holds an unindented line inside a triple-quoted string —
both fall back to hashing the decorator text, which invalidates more often than
strictly needed but never less.

### Does changing an environment variable re-run a task?

Yes, when the task reads it by a literal name: `os.environ["NAME"]`,
`os.environ.get("NAME")`, `os.getenv("NAME")` or `"NAME" in os.environ`, in the
task body, in a function it calls, or in a module-level assignment it reads
(`SCALE = float(os.environ.get("SCALE", "1"))`), within the task's own module
and the local modules it imports. A digest of the value, or of its absence, is
part of the key, and `ginkgo cache explain` names the variable that moved
(`environ.NAME`). A read without a literal name (`os.environ[name]`,
`env=os.environ`) cannot be keyed, so `ginkgo run`, `--dry-run` and `ginkgo
doctor` warn that its result may be stale. See
[Environment Variables](guide/caching-and-provenance.md#environment-variables).

### Does editing a file a task opens by a literal path re-run it?

No. A task's key covers its arguments and source, so a file it opens itself
(`open("data/genes.tsv")`), a script a shell command runs (`bash
scripts/trim.sh`), or a figure a notebook embeds by path is invisible to it.
`ginkgo run`, `--dry-run` and `ginkgo doctor` name each such project path
(`undeclared_literal_path`, or a notebook notice). Pass the file in as a `file`
argument, and declare a file another task writes as that task's `Out[file]`.

### How do warm runs skip work?

Cache lookups happen during node *preparation*, before any worker is dispatched.
Once a task's inputs are resolved, Ginkgo tries to satisfy the node straight from
cache; on a hit the node is marked complete and no execution slot is ever used.
The default path builds the full content-addressed key and also validates that
cached file/folder outputs are still materialised correctly. It hashes a file
only when its stat metadata has changed since the digest was memoised (see
above). Running with `--trust-mtimes` goes further: it skips content hashing and
only checks that output files exist — quicker for warm reruns, but weaker again,
since it trusts the working tree instead of re-verifying bytes.

Two runs started at the same time in one project are safe, since each keeps its
own run record and the ledger and cache take their writes in turn. A run only
sees cache entries that already exist, though, so both may compute the same task.

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

`cache clear` needs the *full* 64-character key: a prefix, even a long one, is
reported as `Cache entry not found`. `ginkgo cache ls` lists the full keys, one
per line, and `ginkgo cache ls --task fst` narrows them to one task (it also
takes a branch label); `ginkgo cache explain <run_id>` prints each task's key
beside its branch label (`fst[2L,YRI,CEU]`), which is the easiest way to find
the key of one branch of a fan-out. `--json` on `cache ls` gives the same list
to a script. To discard everything and start cold, run
`ginkgo cache prune --max-entries 0`.

### Where does the artifact store fit in for file/folder outputs?

File and folder outputs are copied into a content-addressed artifact store under
`.ginkgo/artifacts/` (hashed with BLAKE3), and that store — not the task's
declared output path — is the durable source of truth. On a cache hit, the
artifact store re-materialises the output into the working tree: a tracked
output file or folder that is missing or modified on disk is restored without
re-running the task, overwriting any hand edits. An edit counts when it moves the
file's size or modification time; one that keeps both is taken as unchanged and
stays (see
[What The Cache Cannot See](guide/caching-and-provenance.md#what-the-cache-cannot-see)). Only when no stored artifact
exists to restore from, such as a task with an explicit `-> None` return, does
the missing output count as a miss and the task re-run. Large serialized return
values are also offloaded to the store rather than inlined.

## Value Transport And Serialization

### How does Ginkgo serialize values passed between tasks?

Values crossing a task boundary go through a codec that turns a Python object
into a JSON-safe payload tagged with a type marker. The same codec carries a
value to a worker, back to the driver, and into and out of the cache, so a
consumer sees the same value whether its producer ran or was served from the
cache.

- `None` and exact `bool`, `int`, `float` and `str` values pass through as-is.
- `file`, `folder` and `tmp_dir` get typed wrappers.
- An exact `list`, `tuple` or `dict` is encoded item by item.
- Anything else goes to a binary encoder: a NumPy array as `.npy`, a pandas
  DataFrame as Parquet (falling back to pickle), and every other object with
  `pickle` (protocol 5).

Only the exact builtins are encoded structurally. A subclass is pickled whole,
so it comes back as itself: a NumPy scalar such as `np.float64`, a
`namedtuple`, an `OrderedDict`, a `defaultdict` (with its default factory) or
your own `list` subclass. A `file` or `folder` inside such a container is still
tracked by content in the consumer's cache key.

A class or enum defined in a workflow file round-trips too, on fresh, cached
and `--rerun` runs alike, and after the project is moved or copied. A
single-file workflow is loaded under a module name that only one process
knows, so Ginkgo pickles its classes by the workflow file's path relative to
the project root and finds that module again wherever the value is decoded.

### What's the inline-versus-artifact threshold?

Binary payloads are base64-inlined into the payload when they are at or below
256 KiB. Above that, Ginkgo offloads the bytes: to the artifact store when one is
supplied (cache persistence), or to an ephemeral directory for process
transport. The decoded value is reconstructed transparently either way, so
callers do not see the difference.

### How does serialization differ between local and remote execution?

The codec is the same in both cases; the difference is what carries the bytes.
Locally, the evaluator encodes task args into a transport directory and the
process-pool worker decodes them, then encodes the result the same way for the
driver to decode. Only a result holding task expressions (a dynamic task)
comes back as one pickled object instead. For remote execution the encoded payload is layered with
a remote-transfer step: `file`/`folder` artifacts are uploaded to a shared
remote artifact store (object storage) as references, and the remote worker
downloads them before running, then pushes outputs back the same way — so
nothing large travels inside the job payload itself.

### What types can be passed between tasks, and what happens to a value with no special codec?

Any value the codec can encode: the typed cases above, plus anything that
survives the binary encoder. A value with no dedicated codec is pickled
(protocol 5) as the final fallback, so most standard Python objects work — but an
object that cannot be pickled fails. Pickle finds a class by name, so it must
be defined at module level: a `namedtuple` or class created inside a function
cannot cross a task boundary. Ginkgo validates process-bound values up front
and raises an error naming the offending value's type if it cannot be
encoded, so unpicklable objects fail loudly rather than mid-transport.

## Outputs

### How do I hand a DataFrame or other Python value to a notebook, script, or shell task?

Between `python` tasks a DataFrame, array, or any other picklable object passes
as a plain value through the codec above. A notebook or script task forwards its
arguments to another process as text, so a Python value has no form there and is
refused by name. Put a small `python` task in between that writes the value to
the format the notebook or script expects and returns it as a `file`; the
downstream task then takes that `file`, with the dependency edge and the cache
key that come with it. A shell task's body is Python, so it can take the value
itself and write the file before building its command.
[Staging A Python Value For A Notebook](guide/tasks-and-flows.md#staging-a-python-value-for-a-notebook)
works this through end to end.

### How do I find out which task produced a file?

`ginkgo why <path>` names the task and run that produced a materialized file,
the cache entry that holds its bytes, and the inputs that task recorded. It also
takes an artifact id, and `--json` emits the same as a structured payload. From
Python, `ginkgo.query.Query.why` returns the same answer.

It can only name a file ginkgo tracked. A task that returns a path as a plain
`str` leaves no record of the file, and `why` says nothing produced it: annotate
the return `-> file` (or `-> folder`), or write the file through an `Out[file]`
parameter.

## Reports

### What's in the exported HTML report, and how is it structured/bundled?

`ginkgo report` (from the `ginkgo-report` plugin) renders a completed run (status `succeeded`, `failed` or
`cancelled`) into an HTML report. By default this produces a directory bundle
(`index.html` plus an `assets/` folder); pass `--single-file` to emit a single
self-contained HTML file instead. Running or pending runs are rejected.

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The report contains the run summary and stat cards, run parameters, an SVG
task-graph laid out as a layered DAG, per-task status and timing, failure cards
with log tails, and links to rendered pages such as notebooks. By default it is written as a
directory bundle at `.ginkgo/reports/<run-id>/` that you can open or share.
Useful flags: `--single-file` (inline CSS, fonts and script into one file,
written to `.ginkgo/reports/<run-id>.html`; rendered pages and logs are linked
but not included), `--out <path>` (a directory for a bundle, or the `.html`
file with `--single-file`), `--max-log-lines N` (default 80), and
`--open`/`--no-open`. In the terminal, `ginkgo run` lists the run's rendered
pages when it finishes; they live under the run directory's `notebooks/`.

## Failures And Retries

### When a task fails, does the whole run stop, and do in-flight tasks finish?

The run stops scheduling new work but does not kill tasks that are already
running. On the first unretryable failure the evaluator records the failure and
cancels only the futures that are still queued and have not started. The main
loop then keeps waiting on the already-running futures until they complete — it
simply stops dispatching new tasks — after which it re-raises the stored failure
and the run ends with status `failed`. No task is left without an outcome:
tasks that never started are recorded `skipped`, those downstream of the
failure as blocked by it and the rest as not started because the run stopped.
A task that fails after that point is not retried, even with retries left; nor
is one already waiting to retry. Each is recorded `failed` with its last
failure, and a notice on the task says which retry was not attempted.

That is the default. `@task(on_failure="ignore")`, or `--keep-going` for the
whole run, makes a failure non-fatal: dispatch continues, only the tasks
downstream of the failure are skipped, and `ginkgo run` exits 3 — see
[When a Failure Should Not Stop the Run](guide/resources.md#when-a-failure-should-not-stop-the-run).

An external interrupt such as Ctrl-C is different whatever the policy: it
terminates subprocesses, cancels remote job handles, and shuts down the
executor pools. Failures that drain out behind it, or behind a fatal failure,
are recorded as the failures they are rather than as ignored ones: the policy
cannot carry on a run that has already ended.

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
`.ginkgo/runs/<run_id>/logs/`). The logs are the only place a command's full
output is kept: a failed shell, script or notebook task quotes at most the last
4096 characters of each stream in its error, which is what the terminal, the
ledger and the manifest record. Output is decoded as UTF-8, so bytes that are
not valid UTF-8 are logged as the replacement character `�` (three bytes each);
a command printing binary to stdout leaves a log larger than what it printed —
redirect binary output to a file instead. `ginkgo debug [RUN_ID]` prints a panel per failed
task with the classified category, followed by its log tail (add `--json` for
machine-readable output); it defaults to the latest run if no id is given. A
retried task's tail shows its last attempt, the one that failed. In `--json`,
each failure carries its `status` and its error as `failure.message`; the
top-level `error` is set only when the run failed for a reason no task's
failure already gives.
`ginkgo runs show [RUN_ID]` prints the run and its tasks; add `--json` for the
full snapshot — per-task status, attempts, cache key, exit code, the `failure`
record, log paths, timings, dependencies, and (for remote tasks) the remote job
id and backend. A task that was retried also lists each attempt, with its
status and failure, under `attempt_history`; the text view lists them under
**Attempts**.

## Remote Execution

### How do I mark a task to run remotely?

Remote placement is decided per task from its declaration and the run's
capability. `remote=True` is an explicit directive: the task is sent to the
executor configured with `--executor`, and running without one is a build
error (no silent local fallback). A task declaring `gpu > 0` runs locally
when the `--gpus` budget covers it, is dispatched remotely when it doesn't
and an executor is configured, and is a build error otherwise. Everything
else runs in the local process pool as usual.

```python
from ginkgo import file, task

@task(remote=True, memory="32Gi")
def large_computation(input_path: file) -> str:
    ...

@task(gpu=1, threads=8)          # remote only if --gpus can't satisfy it
def train_model(dataset: str) -> str:
    ...
```

### Which remote backends are actually supported?

Two executor *types* exist: Kubernetes (`k8s`, from the `ginkgo-k8s` plugin)
and GCP Batch (`batch`, from the `ginkgo-gcp` plugin). Neither ships with core;
install the plugin you need (see
[Installing an Executor](guide/remote-execution.md#installing-an-executor)).
`--executor` takes `local` (the default) or any name declared under
`[remote.executors]` in ginkgo.toml. Several named executors of either type can
be configured at once, and a task routes to one with
`@task(executor="name")`. There is a single
Kubernetes executor implementation that submits
`batch/v1` Jobs, so GKE / EKS / OKE are not separate backends — they are just
clusters the one Kubernetes executor talks to. GCP Batch is a distinct
serverless executor.

```toml
[remote.executors.k8s]
type = "k8s"
image = "europe-west2-docker.pkg.dev/my-project/ginkgo/worker:latest"
```

```bash
ginkgo run --executor k8s flow.py   # the table named k8s
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
[remote.executors.k8s.code]   # [remote.executors.<name>.code]
mode = "sync"
package = "my_workflow"

[remote.artifacts]
store = "gs://my-bucket/ginkgo-artifacts/"
```

### How do I request a GPU, or set per-task memory / CPU for a remote task?

Use the `@task` decorator resource declarations: `threads` (int, CPU cores),
`memory` (a string in Kubernetes notation such as `"16Gi"`), `gpu` (int), and
`gpu_type` (accelerator type, e.g. `"nvidia-tesla-t4"`). These are sent to the
worker as the job's resource request. A per-task `gpu_type` overrides the
executor-level default (`gpu_type` in the `[remote.executors.<name>]` table);
GCP Batch rejects a GPU job that has no accelerator type from either source.

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

What happened goes into one SQLite database per workspace, at
`.ginkgo/ginkgo.db`. It holds an append-only event log — task started, running,
completed, failed, retrying, cache hits and misses — and the tables the CLI
reads: runs, tasks, attempts, inputs, outputs, and the dependency graph. Ask it
questions with `ginkgo runs show`, `ginkgo debug` and `ginkgo report`, all of
which work on a run that is still going.

Set `GINKGO_DB=<path>` to put the database somewhere else. Do that if `.ginkgo`
is on a network filesystem: SQLite locking is unreliable over NFS, Lustre, SMB
and FUSE, and ginkgo warns once when it notices.

Each run also gets a directory at `.ginkgo/runs/<run_id>/` (the id is a UTC
timestamp plus a discriminator) holding the bytes:

- `manifest.yaml` — everything `ginkgo runs show --json` shows for that run, written
  as YAML once it finishes. It is there for you to read; ginkgo never reads it
  back. Back up `ginkgo.db` as you would `.git`: if it is lost, so is the run
  history.
- `envs/` — copies of the environment lock files used by the run.
- `logs/` — per-task stdout/stderr.
- `notebooks/` — rendered notebook HTML pages (`task_NNNN.html`).

Before 1.0 there are no schema migrations: a database written by a ginkgo with
a different schema version is refused, with an error naming the workspace.
Delete `.ginkgo/` and run again.

## Configuration And Secrets

### How do I configure a workflow, and do config values feed into cache keys?

Load a config file inside the workflow module with
`ginkgo.config("ginkgo.toml")` (TOML and YAML are both supported); it returns a
nested dict you use to shape the graph. A mistyped key raises a `KeyError` whose
message names the file, the section, and the keys that are there — `'min_lenght'
is not a key in [qc] of ginkgo.toml. Available: min_length, min_quality,
threads` — rather than the key alone. At the CLI, `--config <path>`
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
or the report. That holds for every task kind: a shell, script or notebook
command's output is redacted line by line before it is logged.

Redaction covers what ginkgo records about a task, not what the task writes. A
secret a task writes into an output file is stored in that file, and in its
cached copy under `.ginkgo/`, like any other content.

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
        "child/flow.py",
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

The child is a run in its own right, with its own directory under
`.ginkgo/runs/<child_run_id>/`; the parent stores a reference to it rather than
inlining the child's tasks. The parent tells the child who called it through
`GINKGO_PARENT_RUN_ID` and `GINKGO_PARENT_TASK_ID`; the child records both, and
the parent reads the child's run id back out of the database once the subprocess
exits. The calling task gets a `SubWorkflowResult` (`run_id` and `status`) and
records `sub_run_id`; on failure the child's run id is still attached to the
raised error. Either way, `ginkgo runs show <child_run_id>` is how you read
what the child did — the run id is the handle, and there is no second path to
the same facts.
