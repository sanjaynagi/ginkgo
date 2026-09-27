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
from ginkgo import Out, file, shell, task

@task(kind="shell")
def normalize(input_path: file, output_path: Out[file]):
    return shell(cmd=f"tr a-z A-Z < {input_path} > {output_path}")
```

Use script tasks when a standalone script should run in a task-local Pixi env:

```python
from ginkgo import Out, file, script, task

@task("script", env="analysis_tools")
def build_report(output_path: Out[file]):
    return script(path="scripts/build_report.py")
```

Use notebook tasks when the notebook is part of the workflow output:

```python
from ginkgo import Out, file, notebook, task

@task("notebook")
def render_report(sample_id: str, output_path: Out[file]):
    return notebook(path=f"notebooks/report_{sample_id}.ipynb")
```

## Ginkgo types and cache correctness

Every path a task touches is either read or written, and the annotation says
which:

- A path the task **reads** — whether it comes from an upstream task's
  return value or is written down as a raw input path in the flow — is
  `file` or `folder`. Ginkgo hashes its **contents** into the cache key, so
  the task reruns when the file changes, not only when the path string does.
- A path the task **writes** is `Out[file]` or `Out[folder]`. It need not
  exist before the task runs — Ginkgo creates its parent directory
  automatically — and it is cached by path only, since its contents *are*
  this run's output. Ginkgo checks the path exists (with the right kind)
  after the task runs, and registers it as produced by this task so a
  downstream task that reads the same path gets a real dependency edge.

```python
from ginkgo import Out, file, folder, task

@task()
def analyse(manifest: file, output_dir: Out[folder]) -> file:
    ...
```

Never annotate a path parameter `str`, on either side — `str` carries no
content tracking and no dependency edge, so `ginkgo doctor` and
`--dry-run` warn on a `str` parameter whose name looks path-like (`path`,
`output_dir`, `report_files`, ...).

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

Remote-backed inputs such as `s3://bucket/data.csv` or `oci://registry/path:tag`
should flow through Ginkgo task inputs. Let the runtime stage them locally;
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
`fuse_privileged` set in `[remote.k8s]` or `[remote.batch]`. If a mount
fails the worker falls back to staging and the CLI surfaces a warning;
cache keys are stable across modes so switching is free.

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
`.product_map()` when you want all combinations.
