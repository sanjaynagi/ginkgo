# Workflow patterns

Use normal Python tasks for orchestration and Python logic:

```python
from ginkgo import Out, file, task

@task()
def summarize(input_path: file, output_path: Out[file]) -> file:
    ...
```

Use shell tasks when the real unit of work is a command with declared outputs:

```python
from shlex import quote

from ginkgo import Out, file, shell, task

@task(kind="shell")
def normalize(input_path: file, output_path: Out[file]):
    return shell(cmd=f"tr a-z A-Z < {quote(input_path)} > {quote(output_path)}")
```

Quote every value formatted into `cmd=` with `shlex.quote`; a shell task's
paths must not contain spaces.

Use script tasks when a standalone script should run in a task-local Pixi env:

```python
from pathlib import Path

from ginkgo import Out, file, script, task

# This file lives in workflow/modules/, so scripts/ is one level up.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"

@task("script", env="analysis_tools")
def build_report(output_path: Out[file]):
    return script(path=_SCRIPTS_DIR / "build_report.py")
```

A relative path given to `script()` or `notebook()` resolves against the
project root (the directory holding `ginkgo.toml`), not against the file that
contains the call, so anchor it with `Path(__file__)` as above.

Use notebook tasks when a marimo notebook is part of the workflow output
(they come from the `ginkgo-notebook` plugin). The task's arguments reach the
notebook as `mo.cli_args()`, with `_` spelled `-` (`sample-id`), and the
rendered HTML is the task's result unless it declares outputs:

```python
from pathlib import Path

from ginkgo import file, task
from ginkgo_notebook import notebook

_NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent / "notebooks"

@task("notebook")
def render_report(sample_id: str) -> file:
    return notebook(_NOTEBOOKS_DIR / "report.py")
```

## Ginkgo types and cache correctness

Every path a task touches is either read or written, and the annotation says
which:

- A path the task **reads** — whether it comes from an upstream task's
  return value or is written down as a raw input path in the flow — should
  be `file` or `folder`. Ginkgo hashes its **contents** into the cache key,
  so the task reruns when the file changes, not only when the path string
  does. Prefer this over a bare `str`: it is explicit, type-checked, and
  validated to exist before the task runs. A `str` value naming an existing
  *file* is content-hashed automatically too (since it reads as a path — a
  separator or a file extension), but a `str` naming an existing *directory*
  is not — a directory is never auto-hashed, so it stays cached by path
  string only unless annotated `folder`.
- A path the task **writes** is `Out[file]` or `Out[folder]`. It need not
  exist before the task runs — Ginkgo creates its parent directory
  automatically — and it is cached by path only, since its contents *are*
  this run's output. Ginkgo checks the path exists (with the right kind)
  after the task runs, and registers it as produced by this task so a
  downstream task that reads the same path gets a real dependency edge.
- A path that is neither a tracked read nor a declared write — a shared log
  file, a staging path that is not itself the artifact — should be
  `ginkgo.untracked`: it keys by path string only, deliberately, and is
  never mistaken for the silent-staleness trap `ginkgo doctor` flags.

```python
from ginkgo import Out, file, folder, task

@task()
def analyse(manifest: file, output_dir: Out[folder]) -> file:
    ...
```

Prefer `Out[...]` over a bare `str` for every write — a file written
through a `str` path is content-hashed as an input on the next run, so the
task invalidates its own cache entry. `ginkgo doctor` and `--dry-run` warn on
a `str` parameter whose name looks path-like (`path`, `output_dir`,
`report_files`, ...), suggesting `file`/`folder` for a read or
`Out[file]`/`Out[folder]` for a write.

A data file opened by a literal path inside a task body is invisible to the
cache. Pass it in as a `file` argument instead. The same goes for a file a
task writes to a path that is neither an `Out[...]` parameter nor returned:
it is not tracked or restored, and Ginkgo says nothing about it. Declare it
`Out[file]` or return it. A value read from `os.environ` in a task body is
not part of the cache key either; pass it in as an argument or a
`ginkgo.param`. Every task in a module re-runs when any of the module's text
changes, comments included.

A shell, script or notebook output that may legitimately not be written is
wrapped in `optional()` in `output=`; when absent it resolves to `None`, so
annotate that element `file | None`. `Out[file | None]` is different: it
means the argument may be `None`, and a path that is passed must still be
written.

```python
from ginkgo import file, optional, shell, task

@task("shell")
def call_variants(sample: str) -> tuple[file, file | None]:
    return shell(
        cmd=f"mkdir -p results && echo {sample} > results/{sample}.vcf",
        output=(f"results/{sample}.vcf", optional(f"results/{sample}.warnings.txt")),
    )
```

A task with `Out[...]` parameters and no return annotation has its return
value inferred from them — one `Out[...]` parameter becomes the return
value, several become a tuple in declaration order — so a task that only
writes its declared outputs and returns nothing else needs no `return`
statement at all:

```python
@task()
def summarize(input_path: file, output_path: Out[file]):
    Path(output_path).write_text(summarize_contents(input_path))
    # No return: output_path is inferred as this task's `file` result.
```

Call `.output["name"]` on a task call (or a `.map()`/`.product_map()`
result) to select one `Out[...]` parameter by name, independent of what the
task returns:

```python
results = normalize().map(input_path=inputs, output_path=out_paths, check_path=check_paths)
normalized = results.output["output_path"]
checks = results.output["check_path"]
```

The type annotation on the *return value* matters too — a task returning a
`file`-typed path will have its output stored as an artifact; a task
returning `str` will not.

Remote-backed inputs such as `s3://bucket/data.csv` or `gs://bucket/reads.bam`
should flow through Ginkgo task inputs (each scheme needs its object-store
plugin: ginkgo-aws, ginkgo-gcp or ginkgo-oci). Let the runtime stage them locally;
avoid manual download code inside tasks.

Wrap the URI in `remote_file(...)` / `remote_folder(...)` when you want to
control *how* the input reaches the worker:

```python
from ginkgo import remote_file, task

# Stream via FUSE for sparse/random access — no whole-file download.
bam = remote_file("gs://bucket/sample.bam", access="fuse")

# Force staged download (the default).
ref = remote_file("gs://bucket/ref.fa", access="stage")
```

Fuse mode requires a worker image with FUSE drivers and `fuse_image` /
`fuse_privileged` set on the executor's `[remote.executors.<name>]` table
(a `k8s` or `batch` executor, from the `ginkgo-k8s` / `ginkgo-gcp` plugins).
If a mount
fails the worker falls back to staging and the CLI surfaces a warning;
cache keys are stable across modes so switching is free.

## Workflow parameters

Workflow inputs that should be settable from the command line are declared with
`ginkgo.param(...)`, and each becomes a `--flag`:

```python
import ginkgo

n_replicates = ginkgo.param("n_replicates", type=int, default=12)
```

Pass a parameter into a task as an argument; one read from a module global
inside a task body is not part of the cache key.

## Fan-out

Use `.map()` for zip-style fan-out across aligned inputs:

```python
reports = build_report(type='html').map(
    sample_id=["s1", "s2", "s3"],
    output_path=[
        "results/s1.txt",
        "results/s2.txt",
        "results/s3.txt",
    ],
)
```

Use `.product_map()` for Cartesian fan-out when every value on one axis should
pair with every value on another:

```python
comparisons = compare_thresholds(metrics=['accuracy', 'f1']).product_map(
    sample_id=["s1", "s2"],
    threshold=[0.1, 0.2, 0.3],
)
```

Choose `.map()` when lists are meant to line up positionally. Choose
`.product_map()` when you want all combinations. For combinations that are not
a grid, build one row per combination in a DataFrame and map over its columns:
`.map(**rows.to_dict("list"), out=per_branch("results/{a}_{b}.txt"))`.

## Data-dependent fan-out (checkpoints)

When the number of branches depends on data a task writes, write the pieces
into the task's own `Out[folder]` and return one task per piece; the returned
tasks run after it, and it completes with their results:

```python
@task()
def split(table: file, chunks: Out[folder]) -> list:
    Path(chunks).mkdir(parents=True, exist_ok=True)
    paths = write_chunks(table, chunks)  # your own helper
    return [count_lines(chunk=str(path)) for path in paths]
```

On a cache hit the returned tasks are replayed and each is looked up on its
own, so editing a child's script or module re-runs that child only. A returned
task must not read a path written by a task downstream of its parent: that is
a cycle, and the run fails naming it.

## Shell commands are strict

Every shell task runs under `bash -eo pipefail -c` (a container task under
`[container] shell`, `bash` by default, with the same flags), so a failing
line or a failing stage of a pipeline fails the task. Append `|| true` to a
step that may legitimately fail: a `grep` that can match nothing, or the
stage before `| head`, which `SIGPIPE` kills (`(gzip -cd x.gz || true) | head`).
