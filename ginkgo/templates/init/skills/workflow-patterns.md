W# Workflow patterns

Use normal Python tasks for orchestration and Python logic:

```python
from ginkgo import file, task

@task()
def summarize(input_path: file, output_path: str) -> file:
    ...
```

Use shell tasks when the real unit of work is a command with declared outputs:

```python
from ginkgo import shell, task

@task(kind="shell")
def normalize(input_path: str, output_path: str):
    return shell(cmd=f"tr a-z A-Z < {input_path} > {output_path}", output=output_path)
```

Use script tasks when a standalone script should run in a task-local Pixi env:

```python
from pathlib import Path
from ginkgo import script, task

@task("script", env="analysis_tools")
def build_report(output_path: str):
    return script(path="scripts/build_report.py", outputs=output_path)
```

Use notebook tasks when the notebook is part of the workflow output:

```python
from pathlib import Path
from ginkgo import notebook, task

@task("notebook")
def render_report(sample_id: str):
    output=f"report_summary_{sample_id}.csv"
    return notebook(path=f"notebooks/report_{sample_id}.ipynb", output=output)
```

## Ginkgo types and cache correctness

Annotate file and folder parameters with `file` / `folder` instead of `str` so
that ginkgo hashes the **contents** of those paths when building the cache key.
A plain `str` annotation hashes only the path string — if an upstream task
overwrites the file at the same path with new results, any downstream task whose
inputs are typed `str` will see a spurious cache hit and silently return stale
output.

```python
from ginkgo import file, folder, task

# CORRECT — cache invalidates when file content changes
@task()
def analyse(manifest: file, output_dir: folder) -> file:
    ...

# WRONG — cache key depends only on the path string, not the file contents
@task()
def analyse(manifest: str, output_dir: str) -> str:
    ...
```

Use `file` for any single-file path that flows between tasks. Use `folder` for
directory outputs. Ginkgo uses these types to:

1. Hash file/folder contents into the cache key, so the cache correctly
   invalidates when upstream outputs change.
2. Copy outputs into the artifact store for provenance and remote caching.

The type annotation on the *return value* matters too — a task returning a
`file`-typed path will have its output stored as an artifact; a task returning
`str` will not.

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
