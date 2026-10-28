# Tasks And Flows

This guide covers how to write tasks and flows. It starts with a first task.
Then it covers [fan-out](concepts.md#glossary), dynamic expansion, each kind
of task, and how to lay out a project. [Core Concepts](concepts.md) explains why calling a task returns an
[expression](concepts.md#glossary) instead of running the task. Read it first
if you haven't yet.

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

Call tasks with keyword arguments. Write `write_summary(rows=rows)`, not
`write_summary(rows)`. Ginkgo uses each argument's name for fan-out and for the
cache key. A positional call fails with an error that says so.

When you write a task:

- Define it at module scope, not inside another function.
- Give each input and the return value a type annotation.
- Return an ordinary Python value, or one of Ginkgo's path types such as
  `file`.

Changing a task's code, or a local module it imports, makes the task run
again. [Cache Identity](caching-and-provenance.md#cache-identity) gives the
exact rule.

### Type Checks

Ginkgo checks plain `int`, `float`, `str` and `bool` annotations. It checks
them on the arguments of every kind of task, and on the return value. For
example, `n: int` given `"30"` or `3.0` fails before the task runs.

A few conversions pass the check. `float` accepts an `int`, and `str` accepts a
`Path`. A NumPy scalar passes the matching Python annotation, and reaches the
task unchanged. Ginkgo doesn't check `None`.

## Fan-Out With `.map()`

A plain task call runs the task once. To run a task across many inputs, split
its arguments into two groups: the ones that stay **fixed** for every call, and
the ones that **vary** from call to call.

Pass the fixed arguments to the task call itself, then `.map()` over the
varying arguments:

```python
filtered = filter_fastq(min_length=8).map(
    sample_id=samples["sample_id"],
    fastq=samples["fastq"],
)
```

In this example:

- `min_length=8` is **fixed**. Every call receives the same value.
- `sample_id` and `fastq` are the **varying** arguments. Each is a list.
  `.map()` runs `filter_fastq` once per list position, taking one `sample_id`
  and one `fastq` from the same position each time.

`.map()` pairs the varying lists by position, so they must all be the same
length. Each run of the task is a [branch](concepts.md#glossary). `.map()`
returns an `ExprList`, with one expression per branch.

A call that has only some of its arguments doesn't run. It returns a partial
call, and `.map()` supplies the rest. So `.map()` gives an error if:

- the call already has every required argument
- you call `.map()` on the task itself, as in `filter_fastq.map(...)`
- you pass `.map()` a single value, as in `.map(min_length=8)`. Put that
  argument in the call instead.

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

This makes four branches. Each `output_path` comes from the values of the
branch that writes it, so a file's name always matches its contents.

Placeholders can also name arguments fixed on the task call. Ginkgo gives an
error for a template with no placeholders, because every branch would get the
same path. It also gives an error for a placeholder that names another task's
result, such as `t.output[1]`. That value doesn't exist until the other task
runs.

### Building The Varying Lists With `expand()`

The varying arguments are plain lists, so they can come from anywhere. For
example, a config value, a sample sheet or a directory listing. When they are
output paths that follow a pattern, `ginkgo.expand()` builds them from a
template in `str.format` style. Projects created by `ginkgo init` use it:

```python
from ginkgo import expand, flow


@flow
def main():
    items = ["alpha", "beta"]
    seed_paths = expand("results/seed/{item}.txt", item=items)
    # ["results/seed/alpha.txt", "results/seed/beta.txt"]

    return write_seed_card().map(item=items, output_path=seed_paths)
```

`expand(template, **wildcards)` gives every combination of the wildcard values
once, in a fixed order:

```python
expand("results/{item}/{rep}.txt", item=["a", "b"], rep=[1, 2])
# ["results/a/1.txt", "results/a/2.txt", "results/b/1.txt", "results/b/2.txt"]
```

`zip_expand(template, **wildcards)` pairs the wildcards by position instead,
giving one string per position. The lists must all be the same length, or it
raises `ValueError`:

```python
from ginkgo import zip_expand

zip_expand("results/{item}/{rep}.txt", item=["a", "b"], rep=[1, 2])
# ["results/a/1.txt", "results/b/2.txt"]
```

Pass every placeholder in the template as a keyword. A keyword that isn't in
the template is an error.

`.product_map()` treats every list it gets as a set of values to combine. An
`expand()` list already holds one path per combination, so `.product_map()`
would combine it again with the lists it came from. Ginkgo rejects that call
and points you to `per_branch()`. To get every combination of values with one
path each, use `.product_map()` with `per_branch()`.

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

Ginkgo labels each branch with its varying values, for example
`train[sample=sample_a,lr=0.01]`. The same label appears in the live table,
`--dry-run`, `runs show`, `debug` and the HTML report.
`ginkgo history "de[batch]"` finds that branch. `cache ls` shows the label from
the most recent run that recorded each cache entry.

The rules are:

- `.product_map()` labels every varying argument as `name=value`, as in
  `train[sample=sample_a,lr=0.01]`.
- `.map()` uses bare values. It takes the varying arguments in the order you
  pass them, and adds one at a time until the labels tell every branch apart.
  `.map(factor=["condition", "batch"])` gives `de[condition]` and `de[batch]`.
  `.map(factor=["c", "c", "b"], adjust_for=["batch", "", "batch"])` needs both
  arguments, and gives `de[c,batch]`, `de[c,]` and `de[b,batch]`. An empty
  string still counts as a value.
- `.map()` skips a value longer than 24 characters, or one that looks like a
  path because it contains `/` or ends in a file extension. If no varying
  argument has a usable value, the branch takes the label of the upstream
  branch it was mapped over. If there is none, it takes the first varying
  argument's value in full.
- Chained fan-out joins the labels, as in `train[sample_a,lr=0.01,epochs=10]`.
- Plain calls of one task, gathered in one list, are labelled as a `.map()`
  over the arguments that differ. `[score(name=n) for n in ["ann", "bo"]]`
  gives `score[ann]` and `score[bo]`. This works in a flow, and in the list a
  dynamic task returns.
- Any other call has no label, and is shown by its task name.

When two tasks would be shown under the same name or label, the second and
later ones take a number, in the order they run: `child`, `child[2]`,
`child[3]`. Each runs after its upstream calls. In a chain such as
`step(x=step(x=step(x=0)))`, the innermost call runs first and is `step`, and
the one the flow returns is `step[3]`.

A `.map()` label comes from the values. So if you change the `.map()`
arguments, a label can move onto different inputs. `ginkgo history` shows the
cache key of each row. Two rows with the same label and the same cache key ran
the same code on the same inputs.

## Dynamic Graphs: Returning Tasks From A Task

A task can return new task calls. Ginkgo adds them to the graph and runs them.
This is how a workflow's graph grows while it runs, which is called
[dynamic expansion](concepts.md#glossary).

A task can return:

- a plain Python value
- a `shell(...)` call, in a shell task
- an expression
- an `ExprList`
- lists, tuples or dicts that hold expressions

A task that returns expressions finishes once they have run. Its result is the
value they produce.

When the parent task is a cache hit, Ginkgo still checks each task it returned.
A returned task whose code, script, module or inputs changed runs again. An
unchanged one comes from the cache, with its outputs restored. The parent's
own cache key covers only its own code and inputs. So a change that reaches
only a returned task, such as an edited script, reruns that task and not the
parent.

### Fanning Out Over Data A Task Writes

Sometimes the number of tasks to run depends on data. Write the data in one
task, and return one task for each piece. The writing task declares the folder
as its `Out[folder]`, and returns the tasks that read the files inside it:

```python
from pathlib import Path

from ginkgo import Out, file, flow, folder, task


@task()
def sort_chunk(chunk: file, sorted_path: Out[file]) -> file:
    lines = sorted(Path(chunk).read_text().splitlines())
    Path(sorted_path).write_text("\n".join(lines) + "\n")
    return file(sorted_path)


@task()
def split(table: file, chunks: Out[folder]) -> list[file]:
    Path(chunks).mkdir(parents=True)
    for i, block in enumerate(Path(table).read_text().split("\n\n")):
        (Path(chunks) / f"chunk{i}.txt").write_text(block + "\n")
    return [
        sort_chunk(chunk=str(path), sorted_path=f"results/sorted/{path.name}")
        for path in sorted(Path(chunks).glob("chunk*.txt"))
    ]


@task()
def combine(parts: list[file], merged: Out[file]) -> file:
    Path(merged).write_text("".join(Path(part).read_text() for part in parts))
    return file(merged)


@flow
def main():
    parts = split(table="data/table.txt", chunks="results/chunks")
    return combine(parts=parts, merged="results/merged.txt")
```

Here is how this runs:

- Ginkgo removes a task's declared `Out[...]` paths before the task runs. So
  it removes `results/chunks` before `split` runs. The glob then sees only the
  chunks this run wrote, never ones left by an earlier run.
- The returned `sort_chunk` tasks read files that `split` has already written,
  so they don't wait for `split`. Instead, `split` waits for them. Its result
  is the list of their results.
- `combine` takes that list like any other `list[file]` input. It runs once
  every chunk is sorted.
- On a later run with an unchanged `table.txt`, `split` is a cache hit. Ginkgo
  checks each returned task, and each is a cache hit too.

A returned task can't read a path that a task after its parent writes. That
task waits for the parent, and the parent waits for the returned task. Ginkgo
stops the expansion with a cycle error that names the task that returned it.

### Returned Tasks That Write A Path

Ginkgo orders tasks by matching the literal paths they read and write. A
returned task that declares an `Out[...]` path can only hold back readers of
that path that haven't started yet. If a reader of that path has already run,
or is running, Ginkgo stops with an error that names both tasks. Pass the path
through the graph as an argument instead.

The task that returns the new tasks is the exception. It may take a folder and
return tasks that write inside it, as `split` does above.

## Selecting One Output

A task that makes several files returns them together. To pass one of them to
another task, select it with `.output`.

### Selecting One Output By Name: `.output["name"]`

A task with `Out[...]` parameters can select one of them by name:

```python
from pathlib import Path

from ginkgo import Out, file, flow, task


@task()
def split_text(text: str, upper_path: Out[file], lower_path: Out[file]) -> tuple[file, file]:
    Path(upper_path).write_text(text.upper())
    Path(lower_path).write_text(text.lower())
    return file(upper_path), file(lower_path)


@task()
def show(path: file) -> str:
    return Path(path).read_text()


@flow
def main():
    parts = split_text(text="Hello", upper_path="upper.txt", lower_path="lower.txt")
    return show(path=parts.output["lower_path"])
```

`parts.output["lower_path"]` gives the path passed as `lower_path`, as a
`file`. A task that uses it waits for `split_text` to finish.

`.output["name"]` gives that argument, not the task's return value. So it
works whether the task returns its outputs itself or Ginkgo infers them. It
also works on the result of `.map()`.

A name that isn't one of the task's `Out[...]` parameters raises a `KeyError`
when the flow builds the graph. The error lists the task's `Out[...]`
parameters. On a single call, reading an output as an attribute, as in
`parts.lower_path`, gives an error that points to `.output["lower_path"]`.

### Selecting One Output With `.output[i]`

`.output[i]` selects an output by position. It works on a task that returns a
tuple or list, and on a shell task that declares `output=[...]`. In the
example above, `parts.output[0]` is the upper-case file.

On a single call, `.output[i]` selects element `i` of the result, once the task
has run. On an `ExprList`, the result of `.map()` or `.product_map()`, it gives
a new `ExprList` that selects element `i` from every branch. The new list
stays in the same order as the branches. You can pass either result straight
to another task call or to `.map()`.

Don't unpack a task call, as in `a, b = split_text(...)`. The call returns one
expression, not the tuple its annotation describes. Ginkgo raises a
`TypeError` that names the task and suggests `.output`. Unpacking an `ExprList`
works, but it gives one expression per branch, not the elements of a result.

## Task Kinds

All the tasks so far have been Python tasks. Ginkgo has these task kinds:

| Kind | Decorator | Body returns | Runs |
|---|---|---|---|
| Python | `@task()` | a Python value | in a separate worker process |
| Shell | `@task("shell")` | `shell(cmd=..., output=...)` | a shell command, in a declared environment if there is one |
| Script | `@task("script")` | `script(path, output=...)` | a `.py` or `.R` script file, with inputs passed as `--flags` |
| Notebook | `@task("notebook")` | `notebook(path, output=...)` | a marimo notebook, rendered to HTML (needs the `ginkgo-notebook` plugin) |
| Subworkflow | `@task("subworkflow")` | `subworkflow(path, params=...)` | another workflow, as its own `ginkgo run` |

All kinds share the `@task` decorator, typed inputs and outputs, caching and
resource declarations. They differ in what the task body does.

Pass the kind by position (`@task("shell")`) or by keyword
(`@task(kind="shell")`).

Shell, script and notebook tasks can each declare an `env`, which is a Pixi
environment or container to run in. Python tasks can't. They run in the same
Python environment as the `ginkgo` command. See
[Environments](environments.md#python-tasks-run-in-the-clis-own-environment).

## Shell Tasks

Use `@task("shell")` when the real work is an external command.

A shell task's function doesn't run your tool. The function runs in Ginkgo's
own process and builds the command as a string. Ginkgo then runs that string,
in the environment the task declares, if any.

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

If your command runs a separate script file, Ginkgo doesn't hash that file. So
editing it alone reruns nothing. Pass the script to the task as a `file`
argument, and Ginkgo hashes it like any other input:

```python
@task(kind="shell")
def summarise(table: file, script: file) -> file:
    return shell(cmd=f"bash {quote(script)} {quote(table)} > summary.tsv", output="summary.tsv")
```

Call it as `summarise(table=table, script="scripts/summarise.sh")`.

### How Ginkgo Runs Your Command

`cmd=` is one string. The shell splits it on spaces, and expands `$`, `*`, `;`
and quotes in it. Ginkgo can't quote the values your function puts into that
string. So wrap each value in `shlex.quote`, as in the example above. A sample
name like `a;b` or `x*` then reaches the command as one word.

Shell tasks can't take `file`, `folder` or `Out[...]` paths that contain
spaces. The task fails before it runs. Script and notebook tasks don't have
this limit, because Ginkgo quotes their arguments.

Ginkgo runs every command with `-eo pipefail`. `-e` stops the command at the
first line that fails. `pipefail` makes a pipeline fail if any stage fails. So
`nosuchtool | cat > out` fails the task, rather than caching an empty file.

The shell depends on where the task runs:

- **No `env`, or a Pixi or Conda `env`.** Ginkgo uses `bash`, so bash syntax
  such as `<(...)` works.
- **A container `env`.** Ginkgo uses the shell named by `[container] shell` in
  `ginkgo.toml`, which is `bash` by default (see
  [Environments](environments.md)). The shell must accept `-o pipefail`.
  bash, zsh and Alpine's `sh` do. Debian's `dash` doesn't.

When a command fails, Ginkgo removes whatever it left at its declared output
paths. So a partial or empty file can't be mistaken for a result.

### Shell Task Pitfalls

Add `|| true` to a command that can fail without being an error. For example,
`grep` exits 1 when it finds no match. And when `head` stops reading early,
the stage before it is killed with exit code 141. `pipefail` reports both as
failures:

```bash
grep pattern in.txt > out.txt || true
(gzip -cd reads.fq.gz || true) | head -n 4000 > sample.fq
```

Redirect the output of a background process away from the task, or `wait` for
it. A shell task finishes only when its command has exited and everything it
started has closed the task's output. A process left running with `&` keeps
that output open, so the task waits until the process exits too:

```bash
long_running_server > server.log 2>&1 &
```

### Declaring Outputs With `Out[...]` Instead Of `output=`

A shell, script or notebook task can declare its outputs as `Out[...]`
parameters. Then you can leave out `output=`. Ginkgo infers it from those
parameters' paths, and infers the task's return value too (see
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

If you give `output=` as well as `Out[...]` parameters, the paths must match
exactly, in any order. If you give neither, the task is an error.

### Outputs That May Not Be Written

Every declared output must exist once the task finishes, or the task fails.
When a command writes a file only sometimes, wrap that entry of `output=` in
`optional()`. A written file is hashed, cached and restored like any other
output. A missing one becomes `None`, so annotate that element `file | None`,
and have the tasks that use it check for `None`:

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
takes a path string. Wrapping a `Path` is an error. It applies only to
`output=` entries. An `Out[file | None]` parameter doesn't make its path
optional. It lets the caller pass `None`, but a path that is passed must still
be written, whether or not it also appears in `optional()`.

## Script Tasks

Use `@task("script")` to run an existing Python (`.py`) or R (`.r` or `.R`)
script without rewriting it as a Python task. The body returns `script(path)`.
Ginkgo hashes the script file, so editing the script makes the task run again.

```python
from pathlib import Path

from ginkgo import Out, file, script, task

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@task("script", env="analysis_tools")
def build_brief(item: str, normalized_card: file, output_path: Out[file]) -> file:
    return script(_SCRIPTS_DIR / "build_brief.py")
```

Ginkgo picks the interpreter from the file extension: `python` for `.py`, and
`rscript` for `.r` or `.R`. Pass `interpreter=...` to choose another.

Ginkgo passes each argument to the script as a command-line option.
Underscores in the name become hyphens, so `normalized_card` arrives as
`--normalized-card`. Declare that hyphenated form in the script's argument
parser. The form of the option depends on the argument's type:

| Argument | The script receives | Declare it with |
|---|---|---|
| a string, number or path | `--name value` | `parser.add_argument("--name")` |
| `True` or `False` | `--name` when `True`, nothing when `False` | `action="store_true"` |
| a list or tuple of strings, numbers or paths | `--name a b c`, or a bare `--name` when the list is empty | `nargs="*"`, or `nargs="+"` to require one item |
| a dict, or a list holding anything else | one JSON string | `json.loads(args.name)` |

Build the script's path from `Path(__file__)`, as in the example. Ginkgo
doesn't resolve a relative path against the file that calls `script()`.
Before it runs a workflow, Ginkgo changes the working directory to the
[project](concepts.md#glossary) root, and resolves the path from there. So
`script("scripts/build_brief.py")` looks for
`<project-root>/scripts/build_brief.py`, not `workflow/scripts/`. The same
applies to `notebook()`.

## Notebook Tasks

Use `@task("notebook")` to run a [marimo](https://marimo.io) notebook as a
task. Notebook tasks need the `ginkgo-notebook`
[plugin](concepts.md#glossary) (see
[Plugins](../getting-started/installation.md#plugins)). Without it, a
`@task("notebook")` fails when Ginkgo builds the graph, and the error names
the plugin to install. The body returns `notebook(path)`, and Ginkgo records
the rendered HTML page against the run.

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

`notebook()` takes a marimo notebook, which is a `.py` file. Jupyter `.ipynb`
notebooks aren't supported. Convert one with
`marimo convert notebook.ipynb -o notebook.py`.

The task's function sets the notebook's parameters. Ginkgo passes each
argument to the notebook as a command-line option, and the notebook reads them
with `mo.cli_args()`. As with script tasks, underscores become hyphens, so the
notebook reads `summary_path` as `summary-path`:

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

The values after `or` apply only when you open the notebook by hand with
`marimo edit`. During a run, the task's arguments win.

The form of the option depends on the argument's type:

| Argument | The notebook receives | Read it with |
|---|---|---|
| a string, number or path | `--name value` | `args.get("name")` |
| `True` or `False` | `--name true` or `--name false` | `args.get("name")`, which gives a `bool` |
| a list or tuple of strings, numbers or paths | `--name a --name b`, or nothing when the list is empty | `args.get_all("name")`, which gives a list |
| a dict, or a list holding anything else | one JSON string | `json.loads(args.get("name"))` |

Use `get_all` for lists. `mo.cli_args()` collects a repeated option into a
list, but it joins values separated by spaces into one string. `get_all`
returns a list whatever the length, and `[]` when the list is empty.

`notebook()` takes two optional arguments beyond the path:

- `output` is a declared output path, or list of paths, that Ginkgo checks
  after the notebook runs. Without it, the task's result is the rendered HTML.
- `log` is a path that captures the notebook's output while it runs.

Ginkgo hashes the notebook file, so editing the notebook makes the task run
again. It doesn't hash other files the notebook opens. So pass every file the
notebook reads in as an argument.

[Notebook Task Details](notebooks.md) covers how a notebook task runs, what to
do when one fails, and how to show images in the page.

### Figures From Another Task

When one task draws a figure and a notebook shows it, the figure is a
dependency like any other file. Declare it at both ends. The task that draws
the figure declares it as an `Out[file]` output. The notebook task that shows
it takes the figure as a `file` argument:

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
reads the paths with `mo.cli_args().get_all("figures")`, and shows each with
`mo.image(path)`.

The figures are then inputs of `overview`, hashed by their contents. So a
redrawn figure makes `overview` run again, and Ginkgo runs `overview` after the
tasks that draw the figures. A notebook that builds the path itself, as in
`mo.image(f"results/figures/{region}.png")`, gets neither. It comes from the
cache with the old figure, and it may run before the figure exists.

### Staging A Python Value For A Notebook

Ginkgo passes a notebook task's arguments to another process as text. So a
Python value such as a DataFrame, an array or a fitted model can't be an
argument of a notebook task, because it has no text form. Ginkgo refuses it by
name when it prepares the notebook's arguments.

Don't pass the path as a plain `str` instead. A literal path is just a string
to Ginkgo. The notebook doesn't wait for the task that writes the file, so it
can run before the file exists. The path also stays out of the cache key, so
the notebook keeps an old page after the data changes.

Add a small Python task in between. It takes the value, annotated `object` or
the value's own type. It writes the file the notebook expects, and returns that
path as a `file`. The notebook task then takes a real `file`, so it waits for
the file and reruns when the file changes:

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

`output_path` tells the staging task where to write. The notebook depends on
`de_csv`, the file the staging task returns.

Don't write the CSV inside the notebook task's own function. Ginkgo passes
every argument to the notebook process, so the DataFrame still has to become
text, and Ginkgo refuses it. If the tasks after it only need the file, have the
task that makes the value write the file and return `file(...)` directly. Then
you don't need the staging task.

## Subworkflow Tasks

Use `@task("subworkflow")` to run a whole workflow as a single task of another
workflow. The body returns `subworkflow(path)`. The child workflow runs as its
own `ginkgo run`, in a separate process. The parent task's result is a
`SubWorkflowResult`, which holds the child run's ID.

```python
from ginkgo import SubWorkflowResult, file, subworkflow, task


@task("subworkflow")
def run_child(dataset: file) -> SubWorkflowResult:
    return subworkflow("child/flow.py", params={"dataset": str(dataset)})
```

`subworkflow()` takes two optional arguments:

- `params` sets the child's parameters. Ginkgo passes them to the child in a
  config file.
- `config` adds config files, as `--config` does for `ginkgo run`.

Subworkflows let you build a large pipeline from smaller workflows that each
run on their own.

The parent task's cache key covers every `.py` file in the child workflow's
directory, and the `config` files. So editing the child, or a module beside
it, reruns the parent task. The child's own tasks keep their cache, so that
rerun repeats only what the edit changed.

## Keep Flows Thin

A Ginkgo project usually has this layout. The package directory is always
named `workflow`, whatever the project is called:

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

- `pixi.toml` is the project's own Pixi environment. Ginkgo and the packages
  that Python tasks import are installed there.
- `ginkgo.toml` is the project's config file.
- `workflow/flow.py` holds the flow.
- `workflow/modules/` holds the task code.
- `workflow/envs/` holds the Pixi environments that tasks name with `env=`.
- `workflow/notebooks/` and `workflow/scripts/` hold the files for notebook
  and script tasks.

`ginkgo init` creates this layout. With no file named, `ginkgo run` looks for
`workflow/flow.py`. `ginkgo run <entry.py>` runs any file, whatever the layout
around it.

Keep the flow thin. It should:

- load config or top-level metadata
- call tasks
- set up fan-out and fan-in
- return the final expression or expressions

Put heavy work in task bodies, not in the flow.

## See Also

- [Resources and Scheduling](resources.md): declaring CPU, memory, GPU,
  priority, retries, and what a failure stops (`on_failure`).
- [Environments](environments.md): how shell, script and notebook tasks use
  Pixi and container environments.
- [Caching and Provenance](caching-and-provenance.md): how a task's code and
  inputs decide whether it runs again.
- [Reports](reports.md): render a finished run as an HTML report.
- [Example: Bioinformatics Workflow](../examples/bioinfo-workflow.md): these
  patterns in a complete example.
