# Tasks And Flows

This guide covers the core authoring model: how to define tasks, structure a
flow, and decide what belongs in each layer. If you have not read
[Core Concepts](concepts.md) yet, start there &mdash; it explains why task calls
return deferred expressions rather than running immediately.

## Keep Flows Thin

The canonical project layout expects workflow wiring in `flow.py` and task
implementations in modules owned by the `workflow` package. The package
directory is always named `workflow`, whatever the project is called.

```text
<project-root>/
├── pixi.toml
├── ginkgo.toml
├── workflow/
│   ├── __init__.py
│   ├── flow.py
│   ├── modules/
│   ├── envs/
│   ├── notebooks/
│   └── scripts/
```

The layout is a convention, not a contract: `ginkgo run <entry.py>` runs an
entry file whatever the surrounding structure. The canonical layout is what
auto-discovery targets.

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
| Notebook | `@task("notebook")` | `notebook(path, output=...)` | a marimo notebook, rendered to HTML (needs the `ginkgo-notebook` plugin) |
| Subworkflow | `@task("subworkflow")` | `subworkflow(path, params=...)` | a nested workflow, as a self-contained `ginkgo run` |

The kind can be passed positionally (`@task("shell")`) or by keyword
(`@task(kind="shell")`). Shell, script, and notebook tasks can each declare an
`env`; Python tasks cannot declare an `env` and always run in a spawned worker
process.

Side by side, the five bodies look like this:

```python
from pathlib import Path

from ginkgo import (
    Out,
    SubWorkflowResult,
    file,
    script,
    shell,
    subworkflow,
    task,
)
from ginkgo_notebook import notebook

_WORKFLOW_DIR = Path(__file__).resolve().parent  # the directory holding flow.py


@task()                                     # python
def summarize(rows: list[str]) -> file:
    ...                                     # plain Python, returns a value

@task("shell", env="bioinfo_tools")         # shell
def filter_fastq(fastq: file) -> file:
    return shell(cmd="seqkit seq ...", output="results/filtered.fastq")

@task("script", env="analysis_tools")       # script
def build_brief(normalized_card: file, output_path: Out[file]) -> file:
    return script(_WORKFLOW_DIR / "scripts" / "build_brief.py")

@task("notebook", env="analysis_tools")     # notebook
def render_overview(summary_path: file) -> file:
    return notebook(_WORKFLOW_DIR / "notebooks" / "overview.py")

@task("subworkflow")                        # subworkflow
def run_child(dataset: file) -> SubWorkflowResult:
    return subworkflow("child/flow.py")
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
- a plain `int`, `float`, `str` or `bool` annotation is checked, on every task
  kind's arguments and on the return value: `n: int` given `"30"` or `3.0`
  fails before the task runs. `float` accepts an `int`, `str` a `Path`, and a
  NumPy scalar satisfies the matching Python annotation (it is passed on
  unchanged, still a NumPy scalar); `None` is not checked
- top-level source and statically imported local-helper changes participate in
  cache invalidation
- return ordinary Python values or supported Ginkgo marker types

## Shell Tasks

Use `@task(kind="shell")` when the real work is an external command.

```python
from shlex import quote

from ginkgo import file, shell, task


@task(kind="shell", env="bioinfo_tools")
def fastq_stats(sample_id: str, fastq: file) -> file:
    output = f"results/qc/{sample_id}.stats.tsv"
    return shell(
        cmd=f"seqkit stats -T {quote(fastq)} > {quote(output)}",
        output=output,
        log=f"logs/stats_{sample_id}.log",
    )
```

The wrapper function runs locally on the scheduler. It builds the concrete shell
payload from resolved values, and only that payload is executed in the foreign
environment. Shell, script, and notebook tasks can all declare an `env`; Python
tasks cannot.

### A background process keeps the task running

A shell task finishes when its command has exited *and* everything it
started has closed the task's output. A process left running in the
background with `&` inherits stdout and stderr, so the task keeps waiting
until that process exits too. Redirect a background process's output away
from the task, or `wait` for it:

```bash
long_running_server > server.log 2>&1 &
```

### Quoting values in the command

`cmd=` is one string, and the shell splits it on whitespace and expands `$`,
`*`, `;` and quotes in it. Ginkgo cannot quote what the body formats into that
string, so wrap each interpolated value in `shlex.quote`, as above: a sample
name like `a;b` or `x*` then reaches the command as one literal word.

A shell task's `file`, `folder` and `Out[...]` paths must also not contain
spaces; the task fails before it runs if one does. Python tasks have no such
rule, and neither do script and notebook tasks, whose arguments Ginkgo quotes
itself.

### Which shell runs the command

Every shell task runs its `cmd=` under strict mode, `-eo pipefail`: the
command stops at the first line that fails, and a pipeline fails when any
stage fails, so `nosuchtool | cat > out` fails the task rather than caching an
empty file. The shell itself depends on where the task runs:

- **No `env` (local), or a Pixi or Conda `env`.** `bash`, so bash syntax such
  as `<(...)` works.
- **A container `env`.** The shell named by `[container] shell` in
  `ginkgo.toml`, `bash` by default (see [Environments](environments.md)). It
  must accept `-o pipefail`; bash, zsh and Alpine's `sh` do, Debian's `dash`
  does not.

When a command fails, ginkgo removes whatever it left at its declared output
paths, so a partial or empty file never reads as a result.

A command that expects a failing step has to say so. `grep` finding no match
exits 1, and a stage that `head` stops reading early is killed by `SIGPIPE`
(exit 141), which `pipefail` reports as a failure:

```bash
grep pattern in.txt > out.txt || true
(gzip -cd reads.fq.gz || true) | head -n 4000 > sample.fq
```

### Declaring outputs with `Out[...]` instead of `output=`

When a shell, script, or notebook task's outputs are declared as `Out[...]`
parameters, `output=` can be omitted — it is inferred from those parameters'
declared paths, and the task's return is inferred too (see
[Reads vs. Writes](caching-and-provenance.md#reads-vs-writes-file-vs-outfile)):

```python
from shlex import quote

from ginkgo import Out, file, shell, task


@task(kind="shell")
def normalize(src: file, dest: Out[file], check: Out[file]):
    return shell(
        cmd=f"tr a-z A-Z < {quote(src)} > {quote(dest)} && shasum {quote(dest)} > {quote(check)}"
    )


norm = normalize().map(src=[...], dest=[...], check=[...])
normalized = norm.output["dest"]
```

If `output=` is given explicitly alongside `Out[...]` parameters, its paths
must match the declared `Out[...]` paths exactly (in either order) — a shell
task with `Out[...]` parameters and no `output=` at all is only an error if
it declares none.

### Outputs that may not be written

Every declared output must exist once the task finishes, or the task fails.
When a command writes a file only sometimes, wrap that entry of `output=` in
`optional()`. A written file is hashed, cached and restored like any other
output. A missing one resolves to `None`, so annotate that element
`file | None` and have consumers check for `None`:

```python
from pathlib import Path

from ginkgo import file, flow, optional, shell, task


@task("shell")
def call_variants(sample: str, emit_warnings: bool) -> tuple[file, file | None]:
    warn = f" && echo low-depth > results/{sample}.warnings.txt" if emit_warnings else ""
    return shell(
        cmd=f"mkdir -p results && echo {sample} > results/{sample}.vcf{warn}",
        output=(f"results/{sample}.vcf", optional(f"results/{sample}.warnings.txt")),
    )


@task()
def report(vcf: file, warnings: file | None) -> str:
    if warnings is None:
        return f"{Path(vcf).name}: no warnings"
    return f"{Path(vcf).name}: {Path(warnings).read_text().strip()}"


@flow
def main():
    calls = call_variants(sample="s1", emit_warnings=False)
    return report(vcf=calls.output[0], warnings=calls.output[1])
```

`optional()` works the same way on `script()` and `notebook()` outputs. It
takes a path string; wrapping a `Path` is an error. It applies only to
`output=` entries. An `Out[file | None]` parameter does not make its path
optional: it lets the caller pass `None`, but a path that is passed must
still be written, whether or not it also appears in `optional()`.

## Script Tasks

Use `@task("script")` to run a standalone script file — Python (`.py`) or R
(`.r`/`.R`). The body returns a `script(...)` expression. Resolved task inputs
are forwarded to the script as `--param-name value` command-line arguments.
Underscores in a parameter name become hyphens on the command line: a task
parameter `normalized_card` arrives as `--normalized-card`, so the script's
argument parser must declare the hyphenated form. The same conversion applies to
notebook tasks, which also receive their inputs as `--flags` (see
[Notebook Tasks](#notebook-tasks) for how they receive lists and booleans).

A `list`/`tuple` parameter whose items are scalars or paths (a fan-in from
`.map()`, say) is forwarded as the option followed by each item as its own
token — declare it with `nargs="*"` (or `"+"` to require at least one):

```python
parser.add_argument("--clusters", nargs="*", default=[])
```

An empty list arrives as the bare option, which is exactly what `nargs="*"`
leaves as `[]`. A `bool` parameter is forwarded as a flag: `True` arrives as
the bare option and `False` omits it entirely, so declare it with
`action="store_true"`:

```python
parser.add_argument("--verbose", action="store_true")
```

A list holding anything other than scalars or paths — `None`, a `bool`, a
nested list, a dict — has no positional command-line form and is forwarded
as a single JSON-encoded token instead, the same as a dict parameter always
is; parse it with `json.loads(args.name)`.

```python
from pathlib import Path

from ginkgo import Out, file, script, task

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@task("script", env="analysis_tools")
def build_brief(item: str, normalized_card: file, output_path: Out[file]) -> file:
    return script(_SCRIPTS_DIR / "build_brief.py")
```

A relative path passed to `script()` or `notebook()` is *not* resolved against
the file that contains the call. It resolves against the process working
directory, and `ginkgo` changes that to the project root (the nearest
directory holding `ginkgo.toml`) before it runs a workflow, wherever you
invoked it from. So `script("scripts/build_brief.py")` looks for
`<project-root>/scripts/build_brief.py`, which is not where the canonical
layout above keeps scripts (`workflow/scripts/`). Build the path from
`Path(__file__)`, as above, to make it independent of where the project root
sits. The same applies to `notebook()`.

The interpreter is inferred from the file extension (`.py` → `python`,
`.r`/`.R` → `rscript`); pass `interpreter=...` to override it. A script task
lets you reuse an existing analysis script without rewriting it as a Python
task.

## Notebook Tasks

Use `@task("notebook")` to run a [marimo](https://marimo.io) notebook as a
workflow step. Notebook tasks come from the `ginkgo-notebook` plugin (see
[Plugins](../getting-started/installation.md#plugins)); without it, a
`@task("notebook")` fails when the workflow graph is built, naming the plugin
to install. The body returns a `notebook(...)` expression, and the rendered
HTML becomes a tracked run artifact.

```python
from pathlib import Path

from ginkgo import file, task
from ginkgo_notebook import notebook

_NOTEBOOKS_DIR = Path(__file__).resolve().parent / "notebooks"


@task("notebook")
def render_overview(summary_path: file, run_label: str) -> file:
    """Render an HTML overview notebook for the run."""
    return notebook(_NOTEBOOKS_DIR / "overview.py")
```

`notebook()` takes a marimo notebook, which is a `.py` file; any other file is
refused. The decorated function defines the typed parameter schema; its
resolved inputs are passed to the notebook as command-line arguments, which the
notebook reads with `mo.cli_args()`. As with script tasks, underscores become
hyphens, so `summary_path` is read as `summary-path`:

```python
import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo

    args = mo.cli_args()
    summary_path = args.get("summary-path") or "results/summary.json"
    run_label = args.get("run-label") or "Draft"
    return run_label, summary_path


@app.cell
def _(run_label, summary_path):
    import json
    from pathlib import Path

    summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    print(run_label, summary["row_count"])
    return


if __name__ == "__main__":
    app.run()
```

The fallbacks are what you get when you open the notebook by hand with
`marimo edit`; during a run the task's arguments win. The notebook file is
source material for both execution and cache identity — when the notebook
changes, the task's cache key changes.

`mo.cli_args()` collects a repeated option into a list but joins
space-separated values into one string. So a `list`/`tuple` parameter of
scalars or paths arrives as the option repeated once per item
(`--clusters a.tsv --clusters b.tsv`), and an empty list omits the option. Read
it with `get_all`, which returns a list whatever the length:

```python
clusters = mo.cli_args().get_all("clusters")  # [] when the fan-in is empty
```

A `bool` parameter arrives as `--flag true` or `--flag false`, which
`mo.cli_args()` converts back to a boolean.

`notebook()` takes two optional arguments beyond the path:

- `output` — a declared output path, or list of paths, validated after the
  notebook runs. When omitted, the task result is the rendered HTML.
- `log` — a path to capture stdout/stderr during execution.

Each notebook task is one `marimo export html` subprocess, which runs the
notebook with the task's arguments and renders it in the same pass. marimo
rewrites the file it exports (stamping `__generated_with`), so Ginkgo puts the
notebook's original bytes back afterwards; and, as for any marimo export, it
writes a session file under `__marimo__/` beside the notebook, which you can
ignore in version control. A
notebook task can declare an `env`, and then runs `python -m marimo` inside
that environment, which must therefore provide marimo. Without `env=`, the
notebook runs under Ginkgo's own interpreter.

If a cell fails, the task fails, but marimo still writes the page, showing the
failing cell; the error message points at it. The rendered page is recorded
against the task, listed under "Rendered pages" at the end of the run, and
linked from the [run report](reports.md).

Inside the run directory the rendered page is stored under
`.ginkgo/runs/<run_id>/notebooks/` with a generated name (`task_0000.html`). To
export it under the task's name, build the report bundle with the
`ginkgo-report` plugin installed: `ginkgo report --out deliver` writes
`deliver/pages/<task_key>_<task>.html` next to `deliver/index.html`. See
[Reports](reports.md). A cache hit does not re-render: the task points at the
page the producing run rendered.

### Staging A Python Value For A Notebook

A notebook task's arguments cross into another process as text, so a Python
value such as a DataFrame, an array, or a fitted model cannot bind to one of its
parameters: the payload has no text form. Passing one is refused by name when
the notebook task's arguments are rendered.

Do not reach for the path as a plain `str` instead. A literal path is just a
string to Ginkgo: it creates no dependency edge, so the notebook can run before
the file exists, and it stays out of the cache key, so the notebook serves a
stale render after the upstream data changes.

Put a small `python` task in between. It takes the value — annotated `object`
or the payload's own type — writes the format the notebook expects, and returns
that path as a `file`. The notebook task then takes a real `file`, with the
edge and the cache key that come with it:

```python
from pathlib import Path

import pandas as pd

from ginkgo import Out, file, flow, task
from ginkgo_notebook import notebook

_NOTEBOOKS_DIR = Path(__file__).resolve().parent / "notebooks"


@task()
def differential_expression() -> pd.DataFrame:
    """Fit contrasts and return the result table."""
    return fit_contrasts()


@task()
def stage_de_csv(de_table: pd.DataFrame, output_path: Out[file]) -> file:
    """Write the table as the CSV the notebook reads."""
    output = Path(output_path)
    de_table.to_csv(output, index=False)
    return file(output)


@task("notebook")
def render_rnaseq_report(de_csv_path: file, lfc_threshold: float) -> file:
    """Render the report notebook against the staged CSV."""
    return notebook(_NOTEBOOKS_DIR / "rnaseq_report.py")


@flow
def main():
    de_table = differential_expression()
    de_csv = stage_de_csv(de_table=de_table, output_path="results/de.csv")
    return render_rnaseq_report(de_csv_path=de_csv, lfc_threshold=1.0)
```

The `output_path: Out[file]` parameter is the staging task declaring where its
own output goes, which is not the same move as handing a path from one task to
another: what the notebook depends on is `de_csv`, the value the staging task
returns.

Writing the CSV inside the notebook task's own body does not work: the runner
forwards *every* resolved argument to the notebook process, so the DataFrame
still reaches the text boundary and is refused. If the bytes on disk, rather
than the Python value, are what downstream tasks want in the first place, have
the producer write the file and return `file(...)` directly, and skip the
staging task.

## Subworkflow Tasks

Use `@task("subworkflow")` to run another workflow as a single task. The body
returns a `subworkflow(...)` expression; the child workflow runs as a
self-contained `ginkgo run` subprocess, and its `run_id` and manifest path come
back to the parent as a `SubWorkflowResult`.

```python
from ginkgo import SubWorkflowResult, file, subworkflow, task


@task("subworkflow")
def run_child(dataset: file) -> SubWorkflowResult:
    return subworkflow("child/flow.py", params={"dataset": str(dataset)})
```

`subworkflow()` accepts `params` (parameter overrides forwarded to the child as
a config file) and `config` (additional `--config` paths). Subworkflows let you
compose large pipelines from independently runnable units.

The parent task's cache key includes every `.py` file under the child
workflow's directory and the `config` files, so editing the child, or a module
beside it, re-runs the parent task. The child's own tasks keep their cache, so
that re-run repeats only what the edit changed.

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
one set of values per fanned-out call. So `.map()` is refused, with a pointer to
this pattern, on a call that already has every required argument and on the
bare task (`filter_fastq.map(...)`). A single value such as `.map(min_length=8)`
is refused too, naming the argument: it belongs on the call, not in `.map()`.

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

Every list passed to `.product_map()` is an **axis** of the grid. An argument
that is a *function* of the grid cell rather than an axis of it — an output path
above all — belongs in `per_branch()`, described next.

### Per-Cell Output Paths With `per_branch()`

A grid sweep usually writes one file per cell, named after the cell's parameter
values. Pass that argument as a `per_branch()` template whose placeholders name
the call's own arguments; it is rendered once per cell, from that cell's values:

```python
from ginkgo import Out, file, flow, per_branch, task


@task()
def simulate(temperature: float, defect_density: float, output_path: Out[file]) -> file:
    ...


@flow
def main():
    return simulate().product_map(
        temperature=[300, 400],
        defect_density=[0.01, 0.02],
        output_path=per_branch("results/{temperature}_{defect_density}.json"),
    )
```

This is four branches — one per cell — and each `output_path` is derived from
the parameters of the branch that writes it, so a file's name can never
contradict its contents.

`per_branch()` works with `.map()` too, and its placeholders can also name
arguments fixed on the task call. A template with no placeholders is an error:
every branch would receive the same value. So is a placeholder naming an
argument that holds another task's result, including a selected output such as
`t.output[1]`: it has no value until that task runs.

Do **not** pass an `expand()` list to `.product_map()`. `expand()` already
returns one string per combination, so `.product_map()` would treat it as a
further axis and cross it with the axes it was built from — Ginkgo rejects that
call and points you at `per_branch()`.

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
that does not appear in the template is an error.

Both helpers return a column that is **already aligned row-for-row** with the
values it was built from, so both pair with `.map()`, which consumes columns row
by row. Neither is an axis, so neither can be passed to `.product_map()`; use
`per_branch()` there.

For a multi-wildcard template, that means the parameter columns you `.map()`
over must be flattened to match the expansion, one entry per row:

```python
temperatures = [300, 400]
densities = [0.01, 0.02]

simulate().map(
    temperature=[t for t in temperatures for _ in densities],
    defect_density=[d for _ in temperatures for d in densities],
    output_path=expand("results/{t}_{d}.json", t=temperatures, d=densities),
)
```

`per_branch()` with `.product_map()` expresses the same sweep without the
flattening, and without depending on `expand()`'s ordering, so prefer it for
grids.

### Fan-Out Over A Non-Rectangular Set Of Combinations

When the combinations are not a grid — every unordered pair of populations on
each chromosome, say — `.product_map()` cannot express them. Build one row per
combination in a DataFrame and `.map()` over its columns, which `.map()` zips by
position. `per_branch()` derives each output path from the row's own values:

```python
import itertools

import pandas as pd

from ginkgo import Out, file, flow, per_branch, task

rows = pd.DataFrame(
    [
        {"chrom": chrom, "pop_a": a, "pop_b": b}
        for chrom in ["2L", "3R"]
        for a, b in itertools.combinations(["YRI", "CEU", "CHB"], 2)
    ]
)


@task()
def fst(chrom: str, pop_a: str, pop_b: str, out: Out[file]): ...


@flow
def main():
    return fst().map(
        **rows.to_dict("list"),
        out=per_branch("results/{chrom}_{pop_a}_{pop_b}.txt"),
    )
```

This runs six branches (`results/2L_YRI_CEU.txt`, `results/2L_YRI_CHB.txt`, ...),
one per row, with no hand-written paths.

### Chaining Fan-Out

You can chain fan-out calls. Chaining always returns a flat `ExprList`, with
existing branches treated as the outer loop and newly introduced rows as the
inner loop.

### How Branches Are Labelled

Each branch is shown under a label built from its varying values. The same
label appears in the live table, `--dry-run`, `runs show`, `debug` and the HTML
report, and `ginkgo history "de[batch]"` finds that branch. `cache ls` shows the
label from the most recent run that recorded each cache entry.

- `.product_map()` labels every varying argument as `name=value`:
  `train[sample=sample_a,lr=0.01]`.
- `.map()` uses bare values. It takes the varying arguments in the order you
  pass them, adding one at a time until the labels tell every branch apart.
  `.map(factor=["condition", "batch"])` gives `de[condition]` and `de[batch]`.
  `.map(factor=["c", "c", "b"], adjust_for=["batch", "", "batch"])` needs both
  arguments and gives `de[c,batch]`, `de[c,]` and `de[b,batch]`; an empty
  string still counts as a value.
- `.map()` skips a value longer than 24 characters, or one that looks like a
  path (it contains `/` or ends in a file extension). When no varying argument
  has a usable value, a branch takes the label of the upstream branch it was
  mapped over, or else the first varying argument's value in full.
- Chained fan-out joins the labels: `train[sample_a,lr=0.01,epochs=10]`.
- A call that is neither mapped nor product-mapped has no label and is shown by
  its task name.

When two tasks would be shown under the same name or label, the second and
later take an ordinal in graph order: `child`, `child[2]`, `child[3]`. This
covers plain calls, branches whose values do not tell them apart, and the tasks
a dynamic task returns.

A `.map()` label comes from the values, so changing the `.map()` arguments can
move a label onto different inputs. Compare cache keys before reading two
`history` rows with the same label as the same branch.

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
from ginkgo import Out, file, shell, task


@task("shell")
def normalize_seed_card(
    seed_card: file, output_path: Out[file], check_path: Out[file]
) -> list[file]:
    return shell(cmd=...)
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

Tuple unpacking — `a, b = normalize_seed_card(...)` — cannot work on a single
call, because the call returns one deferred expression rather than the tuple
the annotation describes, so ginkgo raises a `TypeError` naming the task and
this idiom. Unpacking an `ExprList` does succeed, but it means something else
entirely: it hands back one `Expr` per fan-out branch, not the elements of any
branch's result.

### Selecting One Output By Name: `.output["name"]`

A task with `Out[...]` parameters (see
[Reads vs. Writes: `file` vs. `Out[file]`](caching-and-provenance.md#reads-vs-writes-file-vs-outfile))
can select one of them by *name* instead of by position — no need to remember
which index a shell task's `output=[...]` put it at:

```python
from shlex import quote

from ginkgo import Out, file, shell, task


@task(kind="shell")
def normalize(src: file, dest: Out[file], check: Out[file]):
    return shell(
        cmd=f"tr a-z A-Z < {quote(src)} > {quote(dest)} && shasum {quote(dest)} > {quote(check)}"
    )


norm = normalize().map(src=seed_cards, dest=normalized_paths, check=check_paths)
normalized_cards = norm.output["dest"]
checksums = norm.output["check"]
```

`.output["name"]` resolves to that parameter's own resolved argument (coerced
to `file`/`folder`), not the task's return value — so it works whether the
task's return is explicit or inferred from its `Out[...]` parameters — and
creates a dependency edge on the producing node exactly like `.output[i]`
does, including on an `ExprList`. A name that is not one of the task's
`Out[...]` parameters is a `KeyError` at flow-construction time, naming the
task's actual `Out[...]` parameters. Reading one as an attribute, as in
`norm.dest` on a single call, is refused with a pointer to `.output["dest"]`.

## See Also

- [Resources and Scheduling](resources.md) &mdash; declaring CPU, memory, GPU,
  priority, retries, and what a failure stops (`on_failure`).
- [Environments](environments.md) &mdash; how shell, script, and notebook tasks
  resolve Pixi and container environments.
- [Caching and Provenance](caching-and-provenance.md) &mdash; how task
  boundaries become cache boundaries.
- [Reports](reports.md) &mdash; render a finished run as an HTML report.
- [Bioinformatics Workflow](../examples/bioinfo-workflow.md) &mdash; these
  patterns in a complete, runnable example.
