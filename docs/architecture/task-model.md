# Task Model

## Python Tasks

`@task()` supports:

- `version=...`
- `retries=...`
- `threads=...`, `memory=...`, `gpu=...` for resource declarations
- `remote=True` for explicit remote dispatch

Python tasks always execute in the scheduler's own Python environment. If a
task needs a different Pixi or container environment, it must be declared with
`kind="shell"` and invoke the desired script or command explicitly.

Python task bodies must be top-level importable functions for worker execution. Supported task inputs and outputs include:

- scalars and nested containers
- `file`, `folder`, `tmp_dir`
- `numpy.ndarray`
- `pandas.DataFrame`
- other values supported by the codec registry

## Shell Tasks

Shell execution is expressed by declaring `@task(kind="shell")` and returning `shell(...)` from the task body. The Python wrapper runs on the scheduler, constructs the concrete shell command from resolved values, and the runtime executes only that shell payload while validating the declared outputs.

For Pixi-backed shell tasks, the foreign environment does not import the task's
defining `workflow.py` module. The scheduler evaluates the wrapper locally and
dispatches only the shell payload through Pixi.

Shell tasks can also run inside Docker or Podman containers by declaring a container env:

```python
@task(kind="shell", env="docker://biocontainers/samtools:1.17")
def sort_bam(input_bam: file, output_bam: file) -> file:
    return shell(cmd=f"samtools sort {input_bam} -o {output_bam}", output=output_bam)
```

Graph construction remains scheduler-local and foreign environments are entered
only for executable shell payloads.

## Notebook Tasks

Notebook execution is expressed by declaring `@task("notebook")` and returning
a `notebook(...)` sentinel from the task body. The task decorator defines the
typed parameter schema, while the notebook file itself is treated as the
executable source artifact.

Task body pattern:

```python
@task("notebook")
def analyze_data(*, input_file: file) -> file:
    return notebook(
        path="notebooks/analysis.ipynb",
        outputs="output.html"
    )
```

Implemented notebook behavior includes:

- `.ipynb` execution through Papermill with standard parameters-cell injection
- managed Ginkgo kernelspecs under `.ginkgo/jupyter/` for `.ipynb` execution,
  with explicit `ipykernel` validation and deterministic kernel naming derived
  from the selected execution environment
- marimo notebook execution through a CLI/script invocation with resolved task arguments forwarded as CLI parameters
- stable run-scoped notebook artifacts under `.ginkgo/runs/<run_id>/notebooks/`
- HTML export recorded in provenance as explicit task metadata rather than inferred from filenames
- notebook source hashing folded into cache identity so notebook edits invalidate cache even when the task wrapper is unchanged
- explicit `outputs=` parameter for declaring and validating post-execution outputs (optional; runtime-managed artifacts are still recorded even when `outputs` is omitted)

For Papermill-backed notebooks, Ginkgo prefers the runtime-selected task
environment over embedded notebook kernelspec metadata. When a notebook task
declares `env=...`, the managed kernelspec is prepared from that environment;
otherwise the current interpreter environment is used.

Notebook tasks run on the same driver-side execution path as shell tasks,
preserving scheduler semantics for dependency resolution, retries, environment
dispatch, cache recording, and provenance.

## Script Tasks

Script execution is expressed by declaring `@task("script")` and returning a
`script(...)` sentinel from the task body. Scripts support Python and R languages
with automatic interpreter detection based on file extension.

Task body pattern:

```python
@task("script")
def process_data(*, input_file: file, threshold: float) -> file:
    return script(
        path="scripts/analyze.py",
        outputs="results.csv"
    )
```

Implemented script behavior includes:

- automatic interpreter detection: `.py` → `python`, `.R` or `.r` → `rscript`
- optional explicit interpreter override via `interpreter=` parameter
- resolved task inputs forwarded as CLI arguments (`--arg-name value`)
- explicit `outputs=` parameter for declaring and validating post-execution outputs (optional)
- source file hashing folded into cache identity so script edits invalidate cache

Script tasks, like notebook tasks, run on the driver-side execution path and
preserve full scheduler semantics.

## Special Types

Ginkgo currently ships three path-oriented marker types:

- `file`
- `folder`
- `tmp_dir`

These drive validation, caching, and scratch-directory lifecycle management.
