# Tasks And Flows

This guide covers the core authoring model: how to define tasks, structure a
flow, and decide what belongs in each layer. If you have not read
[Core Concepts](concepts.md) yet, start there &mdash; it explains why task calls
return deferred expressions rather than running immediately.

## Keep Flows Thin

The canonical project layout expects workflow wiring in `workflow.py` and task
implementations in modules owned by the project package.

```text
<project-root>/
├── pixi.toml
├── ginkgo.toml
├── <project_package>/
│   ├── workflow.py
│   ├── modules/
│   └── envs/
```

The flow should primarily:

- load config or top-level metadata
- compose task calls
- define fan-out and fan-in structure
- return the final expression or expressions

Heavy transformation logic belongs in task bodies, not in the flow.

## Task Kinds

Ginkgo has five task kinds. They share the same `@task` decorator, typed inputs
and outputs, caching, and resource declarations — they differ only in what the
task body does:

| Kind | Decorator | Body returns | Executes |
|---|---|---|---|
| Python | `@task()` | a Python value | in a spawned subprocess worker (`ProcessPoolExecutor`) |
| Shell | `@task("shell")` | `shell(cmd=..., output=...)` | a shell command, optionally inside a declared environment |
| Script | `@task("script")` | `script(path, output=...)` | a `.py` or `.R` script file, with inputs passed as `--flags` |
| Notebook | `@task("notebook")` | `notebook(path, output=...)` | a Jupyter or marimo notebook, rendered to HTML |
| Subworkflow | `@task("subworkflow")` | `subworkflow(path, params=...)` | a nested workflow, as a self-contained `ginkgo run` |

The kind can be passed positionally (`@task("shell")`) or by keyword
(`@task(kind="shell")`). Shell, script, and notebook tasks can each declare an
`env`; Python tasks cannot declare an `env` and always run in a spawned worker
process.

Side by side, the five bodies look like this:

```python
from ginkgo import (
    SubWorkflowResult,
    file,
    notebook,
    script,
    shell,
    subworkflow,
    task,
)


@task()                                     # python
def summarize(rows: list[str]) -> file:
    ...                                     # plain Python, returns a value

@task("shell", env="bioinfo_tools")         # shell
def filter_fastq(fastq: file) -> file:
    return shell(cmd="seqkit seq ...", output="results/filtered.fastq")

@task("script", env="analysis_tools")       # script
def build_brief(normalized_card: file, output_path: str) -> file:
    return script("scripts/build_brief.py", output=output_path)

@task("notebook", env="analysis_tools")     # notebook
def render_overview(summary_path: file) -> file:
    return notebook("notebooks/overview.ipynb")

@task("subworkflow")                        # subworkflow
def run_child(dataset: file) -> SubWorkflowResult:
    return subworkflow("child/workflow.py")
```

The sections below cover each kind.

## Python Tasks

Use `@task()` for pure Python computation. The task body runs in a spawned
subprocess worker (`ProcessPoolExecutor`, spawn context). A `ThreadPoolExecutor`
fallback is used in environments that disallow process spawning.

```python
from pathlib import Path

from ginkgo import file, task


@task()
def write_summary(rows: list[str]) -> file:
    output = Path("results/summary.txt")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return file(str(output))
```

Key points:

- task functions must live at module scope
- inputs and outputs should be explicit and typed
- top-level source and statically imported local-helper changes participate in
  cache invalidation
- return ordinary Python values or supported Ginkgo marker types

## Shell Tasks

Use `@task(kind="shell")` when the real work is an external command.

```python
from ginkgo import file, shell, task


@task(kind="shell", env="bioinfo_tools")
def fastq_stats(sample_id: str, fastq: file) -> file:
    output = f"results/qc/{sample_id}.stats.tsv"
    return shell(
        cmd=f"seqkit stats -T {fastq} > {output}",
        output=output,
        log=f"logs/stats_{sample_id}.log",
    )
```

The wrapper function runs locally on the scheduler. It builds the concrete shell
payload from resolved values, and only that payload is executed in the foreign
environment. Shell, script, and notebook tasks can all declare an `env`; Python
tasks cannot.

## Script Tasks

Use `@task("script")` to run a standalone script file — Python (`.py`) or R
(`.r`/`.R`). The body returns a `script(...)` expression. Resolved task inputs
are forwarded to the script as `--param-name value` command-line arguments.
Underscores in a parameter name become hyphens on the command line: a task
parameter `normalized_card` arrives as `--normalized-card`, so the script's
argument parser must declare the hyphenated form. The same conversion applies to
marimo notebook tasks, which are also executed as scripts with `--flags`;
Jupyter notebooks receive parameters through Papermill under their original
names.

```python
from pathlib import Path

from ginkgo import file, script, task

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@task("script", env="analysis_tools")
def build_brief(item: str, normalized_card: file, output_path: str) -> file:
    return script(_SCRIPTS_DIR / "build_brief.py", output=output_path)
```

The interpreter is inferred from the file extension (`.py` → `python`,
`.r`/`.R` → `rscript`); pass `interpreter=...` to override it. A script task
lets you reuse an existing analysis script without rewriting it as a Python
task.

## Notebook Tasks

Use `@task("notebook")` to run a Jupyter or marimo notebook as a workflow step.
The body returns a `notebook(...)` expression, and the rendered HTML becomes a
tracked run artifact.

```python
from ginkgo import file, notebook, task


@task("notebook")
def render_overview(summary_path: file, run_label: str) -> file:
    """Render an HTML overview notebook for the run."""
    return notebook("notebooks/overview.ipynb")
```

The decorated function defines the typed parameter schema; its resolved inputs
are passed into the notebook as parameters. The notebook file is source material
for both execution and cache identity — when the notebook changes, the task's
cache key changes.

`notebook()` takes two optional arguments beyond the path:

- `output` — a declared output path, or list of paths, validated after the
  notebook runs. When omitted, the task result is the managed rendered-HTML
  artifact.
- `log` — a path to capture stdout/stderr during execution.

Notebook tasks support `.ipynb` execution through Papermill as well as marimo
notebooks. The HTML export is recorded in provenance and appears in the
[run report](assets.md). A notebook task can declare an `env` so the notebook
runs against that environment's kernel.

## Subworkflow Tasks

Use `@task("subworkflow")` to run another workflow as a single task. The body
returns a `subworkflow(...)` expression; the child workflow runs as a
self-contained `ginkgo run` subprocess, and its `run_id` and manifest path come
back to the parent as a `SubWorkflowResult`.

```python
from ginkgo import SubWorkflowResult, file, subworkflow, task


@task("subworkflow")
def run_child(dataset: file) -> SubWorkflowResult:
    return subworkflow("child/workflow.py", params={"dataset": str(dataset)})
```

`subworkflow()` accepts `params` (parameter overrides forwarded to the child as
a config file) and `config` (additional `--config` paths). Subworkflows let you
compose large pipelines from independently runnable units.

## Fan-Out With `.map()`

A plain task call runs the task once. To run a task across many inputs, split
its arguments into two groups: the ones that stay **fixed** for every call, and
the ones that **vary** from call to call.

Pass the fixed arguments to the task call itself, then `.map()` over the varying
arguments:

```python
filtered = filter_fastq(min_length=8).map(
    sample_id=samples["sample_id"],
    fastq=samples["fastq"],
)
```

In this example:

- `min_length=8` is **fixed** — every fanned-out call receives the same value.
- `sample_id` and `fastq` are the **varying** arguments. Each is a list, and
  `.map()` runs `filter_fastq` once per list position, taking one `sample_id`
  and one `fastq` from the same index each time.

The varying lists are **zipped by position**, so they must all be the same
length. `.map()` returns an `ExprList` — one task expression per row.

Supplying only some of a task's arguments is what makes this work: the call
returns a `PartialCall` instead of running, and `.map()` then fills in the rest,
one set of values per fanned-out call.

### Building The Varying Lists With `expand()`

The varying arguments are plain lists, so they can come from anywhere — a
config value, a samples frame, a directory listing. When they are output paths
that follow a naming pattern, `ginkgo.expand()` builds them from a
`str.format`-style template. This is the idiom the `ginkgo init` scaffold uses:

```python
from ginkgo import expand, flow


@flow
def main():
    items = ["alpha", "beta"]
    seed_paths = expand("results/seed/{item}.txt", item=items)
    # ["results/seed/alpha.txt", "results/seed/beta.txt"]

    return write_seed_card().map(item=items, output_path=seed_paths)
```

`expand(template, **wildcards)` takes the Cartesian product of the wildcard
values, so every placeholder combination appears once, in deterministic order:

```python
expand("results/{item}/{rep}.txt", item=["a", "b"], rep=[1, 2])
# ["results/a/1.txt", "results/a/2.txt", "results/b/1.txt", "results/b/2.txt"]
```

`zip_expand(template, **wildcards)` instead zips the wildcards positionally,
producing one string per position. All iterables must be the same length or it
raises `ValueError`:

```python
from ginkgo import zip_expand

zip_expand("results/{item}/{rep}.txt", item=["a", "b"], rep=[1, 2])
# ["results/a/1.txt", "results/b/2.txt"]
```

Every placeholder in the template must be supplied as a keyword, and a keyword
that does not appear in the template is an error. The pairing
matches `.map()` (positional zip) and `.product_map()` (Cartesian product), so
use `zip_expand()` with `.map()` and `expand()` with `.product_map()` when a
template has more than one wildcard.

### `.product_map()` — Every Combination

Use `.product_map()` when the varying arguments should form a **grid** — every
combination — rather than being zipped by position:

```python
models = train().product_map(
    sample_id=["sample_a", "sample_b"],
    lr=[0.01, 0.1],
)
```

This runs `train` four times: each `sample_id` paired with each `lr`.

### Chaining Fan-Out

You can chain fan-out calls. Chaining always returns a flat `ExprList`, with
existing branches treated as the outer loop and newly introduced rows as the
inner loop.

## Returning Expressions From Tasks

Tasks can return:

- a concrete Python value
- a `shell(...)` payload
- another expression
- an `ExprList`
- nested containers containing expressions

Returning expressions is how a workflow's graph expands at runtime.

### Selecting One Output With `.output[i]`

A task that produces several files returns them together — a shell task
declaring `output=[...]`, or a Python task returning a tuple or list. To wire
one of those outputs into a downstream task, index into the result with the
`.output` proxy:

```python
from ginkgo import file, shell, task


@task("shell")
def normalize_seed_card(seed_card: file, output_path: str, check_path: str) -> list[file]:
    return shell(cmd=..., output=[output_path, check_path])
```

```python
norm_results = normalize_seed_card().map(
    seed_card=seed_cards,
    output_path=normalized_paths,
    check_path=check_paths,
)
normalized_cards = norm_results.output[0]
checksums = norm_results.output[1]
```

`expr.output[i]` on a single expression yields an `OutputIndex` — a deferred
selection of element `i`, resolved once the upstream task has run. On an
`ExprList` (the result of `.map()` or `.product_map()`), `.output[i]` returns a
new `ExprList` selecting element `i` from every branch, so the two lists above
stay aligned with the branches that produced them. Either result can be passed
straight to another task call or `.map()`.

## See Also

- [Resources and Scheduling](resources.md) &mdash; declaring CPU, memory, GPU,
  priority, and retries.
- [Environments](environments.md) &mdash; how shell, script, and notebook tasks
  resolve Pixi and container environments.
- [Caching and Provenance](caching-and-provenance.md) &mdash; how task
  boundaries become cache boundaries.
- [Assets and Reports](assets.md) &mdash; return typed, versioned outputs
  instead of plain files.
- [Bioinformatics Workflow](../examples/bioinfo-workflow.md) &mdash; these
  patterns in a complete, runnable example.
