---
title: "Tasks And Flows"
---

This guide covers how to write tasks and flows: [fan-out](/ginkgo/guide/concepts/#glossary),
dynamic expansion, each kind of task, and project layout. Read
[Core Concepts](/ginkgo/guide/concepts/) first if you haven't yet.

## Your First Task And Flow

A task is a Python function decorated with `@task()`. A flow is a function
decorated with `@flow`. It calls tasks and returns what Ginkgo should run.

```python
from pathlib import Path

from ginkgo import file, flow, task


@task()
def write_summary(rows: list[str]) -> file:
    output = Path("results/summary.txt")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return file(str(output))


@flow
def main():
    return write_summary(rows=["sample_a", "sample_b"])
```

When you write a task:

- Define it at module scope, not inside another function.
- Give each input and the return value a type annotation.
- Return an ordinary Python value, or one of Ginkgo's path types such as
  `file`.
- Call it with keyword arguments: `write_summary(rows=rows)`, not
  `write_summary(rows)`. A positional call is an error.

Changing a task's code, or a local module it imports, makes the task run
again. See [Cache Identity](/ginkgo/guide/caching-and-provenance/#cache-identity).

### Type Checks

Ginkgo checks plain `int`, `float`, `str` and `bool` annotations on every
task's arguments and return value. For example, `n: int` given `"30"` or `3.0`
fails before the task runs. `float` accepts an `int`, and `str` accepts a
`Path`.

## Fan-Out With `.map()`

To run a task across many inputs, pass the arguments that stay **fixed** to
the task call, then `.map()` over the arguments that **vary**:

```python
filtered = filter_fastq(min_length=8).map(
    sample_id=samples["sample_id"],
    fastq=samples["fastq"],
)
```

`.map()` pairs the varying lists by position, so they must all be the same
length. Each run of the task is a [branch](/ginkgo/guide/concepts/#glossary). `.map()`
returns an `ExprList`, with one expression per branch.

Call `.map()` on a task call, not on the bare task: `filter_fastq.map(...)` is
an error. So is passing `.map()` a single value, as in `.map(min_length=8)`.
Put that argument in the call.

### `.product_map()`: Every Combination

Use `.product_map()` when you want every combination of the varying values,
rather than pairing them by position:

```python
models = train().product_map(
    sample_id=["sample_a", "sample_b"],
    lr=[0.01, 0.1],
)
```

This runs `train` four times: each `sample_id` with each `lr`.

### Naming Output Files For Each Branch

A fan-out often writes one file per branch. Ginkgo gives you two ways to name
those files:

- `per_branch()` builds each branch's path from that branch's own values. It
  works with `.map()` and `.product_map()`.
- `expand()` and `zip_expand()` build the whole list of paths up front. Pass
  the list to `.map()`. Don't pass it to `.product_map()`.

### Per-Cell Output Paths With `per_branch()`

Pass the output path as a `per_branch()` template. Its placeholders name the
call's own arguments. Ginkgo fills the template in once per branch, from that
branch's values:

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

This makes four branches, and each file's name matches the values that made
it. Placeholders can also name arguments fixed on the task call, but not
another task's result.

### Building The Varying Lists With `expand()`

The varying arguments are plain lists, so they can come from anywhere, such as
config or a sample sheet. For output paths that follow a pattern,
`ginkgo.expand()` builds them from a `str.format` template:

```python
from ginkgo import expand, flow


@flow
def main():
    items = ["alpha", "beta"]
    seed_paths = expand("results/seed/{item}.txt", item=items)
    # ["results/seed/alpha.txt", "results/seed/beta.txt"]

    return write_seed_card().map(item=items, output_path=seed_paths)
```

With several wildcards, `expand()` gives every combination, and `zip_expand()`
pairs them by position:

```python
expand("results/{item}/{rep}.txt", item=["a", "b"], rep=[1, 2])
# ["results/a/1.txt", "results/a/2.txt", "results/b/1.txt", "results/b/2.txt"]

zip_expand("results/{item}/{rep}.txt", item=["a", "b"], rep=[1, 2])
# ["results/a/1.txt", "results/b/2.txt"]
```

Don't pass an `expand()` list to `.product_map()`, which would combine the
paths again. Ginkgo rejects that call. Use `.product_map()` with
`per_branch()` instead.

### Fan-Out Over A Non-Rectangular Set Of Combinations

Sometimes you want only some of the combinations. For example, every unordered
pair of populations on each chromosome. `.product_map()` can't express that.
Build one row per combination in a DataFrame, and `.map()` over its columns.
`per_branch()` builds each output path from the row's own values:

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

This runs six branches (`results/2L_YRI_CEU.txt`, `results/2L_YRI_CHB.txt`,
...), one per row, with no hand-written paths.

### Chaining Fan-Out

You can chain fan-out calls. The result is always one flat `ExprList`. The
existing branches form the outer loop, and the new values form the inner loop.

### How Branches Are Labelled

Each branch of a fan-out is labelled with the values that tell it apart. The
label appears in the live table, `--dry-run`, `runs show`, `debug` and the HTML
report, and commands that take a task name accept it, as in
`ginkgo history 'align[sample_a]'` or `--rerun 'align[sample_a]'`.

- `.map()` uses the bare values, as in `align[sample_a]`.
- `.product_map()` names each value, as in `train[sample=sample_a,lr=0.01]`.
- Long values, and values that look like paths, are left out of the label.
- If two tasks would end up with the same name, the second gets a number, as
  in `align[2]`.

A label comes from the values, so changing the `.map()` arguments can move a
label onto different inputs. `ginkgo history` shows each row's cache key: check
it before comparing two rows with the same label.

## Dynamic Graphs: Returning Tasks From A Task

A task can return new task calls. Ginkgo adds them to the graph and runs them.
This is [dynamic expansion](/ginkgo/guide/concepts/#glossary). A task can return a plain
value, an expression, an `ExprList`, or lists, tuples or dicts that hold
expressions. A task that returns expressions finishes once they have run, and
its result is the value they produce.

When the parent task is a cache hit, Ginkgo still checks each task it
returned. So a change that reaches only a returned task, such as an edited
script, reruns that task and not the parent.

### Fanning Out Over Data A Task Writes

When the number of tasks depends on data, write the data in one task and
return one task for each piece. The writing task declares the folder as its
`Out[folder]`:

```python
@task()
def split(table: file, chunks: Out[folder]) -> list[file]:
    Path(chunks).mkdir(parents=True)
    for i, block in enumerate(Path(table).read_text().split("\n\n")):
        (Path(chunks) / f"chunk{i}.txt").write_text(block + "\n")
    return [
        sort_chunk(chunk=str(path), sorted_path=f"results/sorted/{path.name}")
        for path in sorted(Path(chunks).glob("chunk*.txt"))
    ]


@flow
def main():
    parts = split(table="data/table.txt", chunks="results/chunks")
    return combine(parts=parts, merged="results/merged.txt")
```

- Ginkgo removes a task's `Out[...]` paths before it runs, so the glob sees
  only the chunks this run wrote.
- `split` waits for the `sort_chunk` tasks it returns, and its result is the
  list of their results. `combine` takes that list like any `list[file]`.

A returned task can't read a path that a task after its parent writes. Ginkgo
stops with a cycle error that names the parent.

### Returned Tasks That Write A Path

Don't have a returned task write an `Out[...]` path that another task reads
by its literal path. If that reader has already started, Ginkgo stops with an
error that names both tasks. Pass the path through the graph as an argument
instead.

## Selecting One Output

A task that makes several files returns them together. To pass one of them to
another task, select it with `.output`.

### Selecting One Output By Name: `.output["name"]`

A task with `Out[...]` parameters can select one of them by name:

```python
@task()
def split_text(text: str, upper_path: Out[file], lower_path: Out[file]) -> tuple[file, file]:
    Path(upper_path).write_text(text.upper())
    Path(lower_path).write_text(text.lower())
    return file(upper_path), file(lower_path)


@flow
def main():
    parts = split_text(text="Hello", upper_path="upper.txt", lower_path="lower.txt")
    return show(path=parts.output["lower_path"])
```

`parts.output["lower_path"]` gives the path passed as `lower_path`, as a
`file`, and `show` waits for `split_text` to finish. It works whether the task
returns its outputs itself or Ginkgo infers them, and on the result of
`.map()`. A name that isn't an `Out[...]` parameter raises a `KeyError` that
lists the ones there are.

### Selecting One Output With `.output[i]`

`.output[i]` selects an output by position. It works on a task that returns a
tuple or list, and on a shell task that declares `output=[...]`. Above,
`parts.output[0]` is the upper-case file. On the result of `.map()` or
`.product_map()`, it selects element `i` from every branch, in branch order.

Don't unpack a task call, as in `a, b = split_text(...)`. The call returns one
expression, not a tuple, so Ginkgo raises a `TypeError` that suggests
`.output`.

## Task Kinds

All the tasks so far have been Python tasks. Ginkgo has these task kinds:

| Kind | Decorator | Body returns | Runs |
|---|---|---|---|
| Python | `@task()` | a Python value | in a separate worker process |
| Shell | `@task("shell")` | `shell(cmd=..., output=...)` | a shell command, in a declared environment if there is one |
| Script | `@task("script")` | `script(path, output=...)` | a `.py` or `.R` script file, with inputs passed as `--flags` |
| Notebook | `@task("notebook")` | `notebook(path, output=...)` | a marimo notebook, rendered to HTML (needs the `ginkgo-notebook` plugin) |
| Subworkflow | `@task("subworkflow")` | `subworkflow(path, params=...)` | another workflow, as its own `ginkgo run` |

All kinds share typed inputs and outputs, caching and resource declarations.
Pass the kind by position (`@task("shell")`) or by keyword
(`@task(kind="shell")`).

Shell, script and notebook tasks can each declare an `env`, a Pixi environment
or container to run in. Python tasks can't. They run in the same Python
environment as the `ginkgo` command. See
[Environments](/ginkgo/guide/environments/#python-tasks-run-in-the-clis-own-environment).

## Shell Tasks

Use `@task("shell")` when the real work is an external command. The task's
function runs in Ginkgo's own process and builds the command as a string.
Ginkgo then runs that string, in the task's `env` if it has one.

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

Ginkgo doesn't hash a script file that your command runs, so editing it
reruns nothing. Pass the script as a `file` argument instead, as in
`summarise(table=table, script="scripts/summarise.sh")`.

### How Ginkgo Runs Your Command

- **Quote every value.** The shell splits `cmd=` on spaces and expands `$`,
  `*` and `;`. Wrap each value in `shlex.quote`, as above.
- **No spaces in paths.** A shell task fails on a `file`, `folder` or
  `Out[...]` path that contains a space. Script and notebook tasks don't have
  this limit.
- **`-eo pipefail`.** Any failing line or pipeline stage fails the task, so
  `nosuchtool | cat > out` doesn't cache an empty file.
- **bash.** Commands run under `bash`. In a container, they run under
  `[container] shell` (default `bash`), which must accept `-o pipefail`.
  Debian's `dash` doesn't.

When a command fails, Ginkgo removes whatever it left at its declared output
paths.

### Shell Task Pitfalls

Add `|| true` to a command that can fail without being an error. `grep` exits 1
when it finds no match, and the stage before a `head` that stops early exits
141:

```bash
grep pattern in.txt > out.txt || true
(gzip -cd reads.fq.gz || true) | head -n 4000 > sample.fq
```

Redirect the output of a background process, or `wait` for it. Otherwise the
task waits until the process exits:

```bash
long_running_server > server.log 2>&1 &
```

### Declaring Outputs With `Out[...]` Instead Of `output=`

A shell, script or notebook task can declare its outputs as `Out[...]`
parameters and leave out `output=`. Ginkgo infers the outputs and the return
value from those parameters (see
[Declaring Outputs](/ginkgo/guide/outputs/#reads-and-writes)):

```python
@task(kind="shell")
def normalize(src: file, dest: Out[file], check: Out[file]):
    return shell(
        cmd=f"tr a-z A-Z < {quote(src)} > {quote(dest)} && shasum {quote(dest)} > {quote(check)}"
    )


norm = normalize().map(src=[...], dest=[...], check=[...])
normalized = norm.output["dest"]
```

If you give `output=` as well as `Out[...]` parameters, the paths must match
exactly, in any order. If you give neither, the task is an error.

### Outputs That May Not Be Written

Every declared output must exist once the task finishes, or the task fails.
When a command writes a file only sometimes, wrap that entry of `output=` in
`optional()`. A missing file becomes `None`, so annotate that element
`file | None`, and have the tasks that use it check for `None`:

```python
@task("shell")
def call_variants(sample: str) -> tuple[file, file | None]:
    return shell(
        cmd=f"caller --sample {quote(sample)} --out results/",
        output=(f"results/{sample}.vcf", optional(f"results/{sample}.warnings.txt")),
    )
```

`optional()` works the same way in `script()` and `notebook()`, and takes a
path string, not a `Path`. It applies only to `output=` entries: an
`Out[file | None]` parameter that is given a path must still write it.

## Script Tasks

Use `@task("script")` to run an existing Python (`.py`) or R (`.r` or `.R`)
script. The body returns `script(path)`. Ginkgo hashes the script, so editing
it makes the task run again.

```python
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@task("script", env="analysis_tools")
def build_brief(item: str, normalized_card: file, output_path: Out[file]) -> file:
    return script(_SCRIPTS_DIR / "build_brief.py")
```

Build the path from `Path(__file__)`, as here. A relative path resolves from
the [project](/ginkgo/guide/concepts/#glossary) root, not from the calling file. The same
applies to `notebook()`.

Ginkgo runs `python` for `.py` and `rscript` for `.r` or `.R`. Pass
`interpreter=...` to choose another. Each argument arrives as a command-line
option, with underscores turned into hyphens, so `normalized_card` arrives as
`--normalized-card`:

| Argument | The script receives | Declare it with |
|---|---|---|
| a string, number or path | `--name value` | `parser.add_argument("--name")` |
| `True` or `False` | `--name` when `True`, nothing when `False` | `action="store_true"` |
| a list or tuple of strings, numbers or paths | `--name a b c`, or a bare `--name` when the list is empty | `nargs="*"`, or `nargs="+"` to require one item |
| a dict, or a list holding anything else | one JSON string | `json.loads(args.name)` |

## Notebook Tasks

Use `@task("notebook")` to run a [marimo](https://marimo.io) notebook as a
task. It needs the `ginkgo-notebook` [plugin](/ginkgo/guide/concepts/#glossary) (see
[Plugins](/ginkgo/getting-started/installation/#plugins)). The body returns
`notebook(path)`, and Ginkgo records the rendered HTML page against the run.

```python
from ginkgo_notebook import notebook

_NOTEBOOKS_DIR = Path(__file__).resolve().parent / "notebooks"


@task("notebook")
def render_overview(summary_path: file, run_label: str) -> file:
    return notebook(_NOTEBOOKS_DIR / "overview.py")
```

The notebook must be a marimo `.py` file. Convert a Jupyter notebook with
`marimo convert notebook.ipynb -o notebook.py`.

Each argument reaches the notebook as a command-line option, read with
`mo.cli_args()`. As with script tasks, underscores become hyphens:

```python
@app.cell
def _():
    import marimo as mo

    args = mo.cli_args()
    summary_path = args.get("summary-path") or "results/summary.json"
    run_label = args.get("run-label") or "Draft"
    return run_label, summary_path
```

The values after `or` apply only when you open the notebook by hand with
`marimo edit`.

| Argument | The notebook receives | Read it with |
|---|---|---|
| a string, number or path | `--name value` | `args.get("name")` |
| `True` or `False` | `--name true` or `--name false` | `args.get("name")`, which gives a `bool` |
| a list or tuple of strings, numbers or paths | `--name a --name b`, or nothing when the list is empty | `args.get_all("name")`, which gives a list |
| a dict, or a list holding anything else | one JSON string | `json.loads(args.get("name"))` |

Always use `get_all` for lists. `get` can give a single string, and `get_all`
always gives a list.

`notebook()` also takes `output=`, the paths Ginkgo checks after the notebook
runs (without it, the result is the rendered HTML), and `log=`, a path for the
notebook's output.

Ginkgo hashes the notebook file, but not other files the notebook opens. So
pass every file the notebook reads as an argument.
[Notebook Task Details](/ginkgo/guide/notebooks/) covers how a notebook task runs, and what
to do when one fails.

### Figures From Another Task

When one task draws a figure and a notebook shows it, declare the figure at
both ends: an `Out[file]` on the task that draws it, and a `file` argument on
the notebook task that shows it:

```python
@task("notebook")
def region_report(region: str, cases: file, figure: Out[file]):
    """Render one region's report and draw its figure."""
    return notebook(_NOTEBOOKS_DIR / "region.py")


@task("notebook")
def overview(figures: list[file]) -> file:
    """Render the overview of every region's figure."""
    return notebook(_NOTEBOOKS_DIR / "overview.py")


@flow
def main():
    regions = ["north", "south"]
    reports = region_report(cases="data/cases.csv").map(
        region=regions,
        figure=expand("results/figures/{region}.png", region=regions),
    )
    return overview(figures=reports.output["figure"])
```

`region.py` saves its figure to `mo.cli_args().get("figure")`. `overview.py`
reads the paths with `get_all("figures")`, and shows each with
`mo.image(path)`.

So `overview` runs after the figures are drawn, and again when one changes. A
notebook that builds the path itself, as in
`mo.image(f"results/figures/{region}.png")`, gets neither: it may run before
the figure exists, and keep the old figure from the cache.

### Staging A Python Value For A Notebook

A notebook task's arguments reach the notebook as text, so a DataFrame, array
or fitted model can't be one. Ginkgo refuses it by name. Don't pass the path as
a plain `str` either: the notebook then won't wait for the file, or rerun when
it changes.

Add a small Python task that writes the value to a file and returns it as a
`file`. The notebook task takes that `file`:

```python
@task()
def stage_de_csv(de_table: pd.DataFrame, output_path: Out[file]) -> file:
    de_table.to_csv(Path(output_path), index=False)
    return file(output_path)


@flow
def main():
    de_table = differential_expression()
    de_csv = stage_de_csv(de_table=de_table, output_path="results/de.csv")
    return render_rnaseq_report(de_csv_path=de_csv, lfc_threshold=1.0)
```

Writing the file inside the notebook task's own function doesn't work, because
the value is still one of its arguments. If nothing else needs the value, have
the task that makes it write the file and return `file(...)` directly.

## Subworkflow Tasks

Use `@task("subworkflow")` to run a whole workflow as a single task of another
workflow. The body returns `subworkflow(path)`. The child runs as its own
`ginkgo run`, and the task's result is a `SubWorkflowResult` that holds the
child run's ID.

```python
from ginkgo import SubWorkflowResult, file, subworkflow, task


@task("subworkflow")
def run_child(dataset: file) -> SubWorkflowResult:
    return subworkflow("child/flow.py", params={"dataset": str(dataset)})
```

`params` sets the child's parameters, and `config` adds config files, as
`--config` does for `ginkgo run`. Editing any `.py` file in the child's
directory, or a `config` file, reruns the parent task. The child's own tasks
keep their cache, so only what the edit changed runs again.

## Keep Flows Thin

`ginkgo init` creates this layout:

```text
<project-root>/
├── pixi.toml          # Ginkgo and the packages Python tasks import
├── ginkgo.toml        # config
├── workflow/
│   ├── __init__.py
│   ├── flow.py        # the flow
│   ├── modules/       # task code
│   ├── envs/          # Pixi environments that tasks name with env=
│   ├── notebooks/     # notebook task files
│   └── scripts/       # script task files
```

With no file named, `ginkgo run` looks for `workflow/flow.py`.
`ginkgo run <entry.py>` runs any file.

Keep the flow thin. It should load config, call tasks, set up fan-out and
fan-in, and return the final expressions. Put heavy work in task bodies.

## See Also

- [Resources and Scheduling](/ginkgo/guide/resources/): declaring CPU, memory, GPU,
  priority, retries, and what a failure stops (`on_failure`).
- [Environments](/ginkgo/guide/environments/): how shell, script and notebook tasks use
  Pixi and container environments.
- [Caching and Provenance](/ginkgo/guide/caching-and-provenance/): how a task's code and
  inputs decide whether it runs again.
- [Reports](/ginkgo/guide/reports/): render a finished run as an HTML report.
- [Example: Bioinformatics Workflow](/ginkgo/examples/bioinfo-workflow/): these
  patterns in a complete example.
