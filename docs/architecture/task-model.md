# Task Model

## Python Tasks

`@task()` supports:

- `version=...`
- `retries=...`
- `threads=...`, `memory=...`, `gpu=...`, `gpu_type=...`,
  `memory_retry_multiplier=...` for resource declarations, stored as a single
  `Resources` value (`core/resources.py`) on the `TaskDef`. Resources state
  what a task *needs* and never imply placement. The evaluator resolves an
  *effective* `Resources` per task by merging the `[resources.overrides]`
  runtime-config table (selectors: exact or fnmatch-glob task names; exact
  beats glob) over the declaration — `ResourceOverrides` in
  `core/resources.py` owns the matching rules. `memory_retry_multiplier`
  escalates the memory footprint per retry attempt, capped at the local
  `--memory` budget for locally-placed tasks.
- `remote=True` for explicit remote dispatch. Placement is otherwise decided
  by the evaluator: a `gpu` requirement is satisfied from the local `--gpus`
  budget when it fits, dispatched to the remote executor when one is
  configured, and a build error otherwise (see
  [remote execution](remote-execution.md)).

Python tasks execute in a spawned subprocess worker pool (`ProcessPoolExecutor`,
spawn context). A `ThreadPoolExecutor` fallback is used when the OS disallows
process spawning (e.g. certain container environments). Python tasks cannot
declare an `env`; to run code in a specific environment, use `kind="shell"`,
`kind="script"`, or `kind="notebook"` instead.

Task parameters use regular positional-or-keyword signatures — do not add a
`*,` separator to force keyword-only. The runtime always binds task arguments
by name (`fn(**resolved_args)`) at dispatch, and `.map()` / `.product_map()`
accept keyword axes, so the `*,` form adds friction without changing
behaviour.

Python task bodies must be top-level importable functions for worker execution. Supported task inputs and outputs include:

- scalars and nested containers
- `file`, `folder`, `tmp_dir`
- `numpy.ndarray`
- `pandas.DataFrame`
- other values supported by the codec registry

## Shell Tasks

Shell execution is expressed by declaring `@task(kind="shell")` and returning `shell(...)` from the task body. The Python wrapper runs on the scheduler, constructs the concrete shell command from resolved values, and the runtime executes only that shell payload while validating the declared outputs.

For Pixi-backed shell tasks, the foreign environment does not import the task's
defining module. The scheduler evaluates the wrapper locally and dispatches only
the shell payload through Pixi.

Shell, notebook, and script tasks can all run inside Docker or Podman containers by declaring a container env:

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
def analyze_data(input_file: file) -> file:
    return notebook(
        path="notebooks/analysis.ipynb",
        output="output.html"
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
- explicit `output=` parameter for declaring and validating post-execution outputs (optional; runtime-managed artifacts are still recorded even when `output` is omitted)

Both Jupyter subprocesses — Papermill execution and the nbconvert HTML export —
run under `build_jupyter_env_prefix`. Every such subprocess walks
`jupyter_core.paths.jupyter_path()`, which always ends with
`SYSTEM_JUPYTER_PATH`, so an unreadable `conf.json` under a host system
directory can fail a render before any HTML is written. The prefix sets three
variables to put that search path under Ginkgo's control:

- `JUPYTER_PATH` — Ginkgo's managed kernel prefix, so Papermill finds the
  kernelspec Ginkgo installed. Purely additive, so it cannot remove anything.
- `JUPYTER_PLATFORM_DIRS=1` — makes `SYSTEM_JUPYTER_PATH` come from
  `platformdirs.site_data_dir` rather than the hardcoded `/usr/local/share/jupyter`
  and `/usr/share/jupyter`.
- `XDG_DATA_DIRS` — what `site_data_dir` reads on Linux and macOS alike, pointed
  at Ginkgo's own prefix.

The last two are both required. `JUPYTER_PLATFORM_DIRS=1` on its own relocates
the system directories on macOS but not on Linux, where the platform-appropriate
data directories *are* `/usr/local/share` and `/usr/share`. The user-level and
environment-level search entries are left alone: they are the user's own home
directory and Ginkgo's interpreter prefix, where nbconvert's templates live.

A notebook task records `notebook_artifact_run_id` alongside its artifact
pointers, naming the run that rendered them. The value travels into the cache
entry with the pointers, so a later run that replays them on a cache hit can
report a reused artifact — and its recorded render status — as belonging to the
run that produced it rather than to itself.

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
def process_data(input_file: file, threshold: float) -> file:
    return script(
        path="scripts/analyze.py",
        output="results.csv"
    )
```

Implemented script behavior includes:

- automatic interpreter detection: `.py` → `python`, `.R` or `.r` → `rscript`
- optional explicit interpreter override via `interpreter=` parameter
- resolved task inputs forwarded as CLI arguments (`--arg-name value`)
- explicit `output=` parameter for declaring and validating post-execution outputs (optional)
- source file hashing folded into cache identity so script edits invalidate cache

Script tasks, like notebook tasks, run on the driver-side execution path and
preserve full scheduler semantics.

## What a Script or Notebook Argument May Be

Script and notebook tasks are the two kinds whose runners forward resolved
arguments to another process — a script task as `--arg-name value` options, a
notebook task through a parameter file. Both go through
`serialize_cli_argument_value` in `task_runners/shell.py`, which is the single
home for what that boundary accepts: `None`, booleans, numbers, strings, path
types, an `AssetRef` whose artifact holds readable bytes (see
[Assets](assets.md)), and lists, tuples, and dicts of those.

A live Python payload has no text form there, so it is refused by name rather
than reaching `json.dumps` or `yaml.safe_dump` and failing with only its type
named. The refusal names the parameter, the type received, and the task kind,
and points at writing the payload to a file in a Python task first. It is
reachable by following the "annotate it `object`" advice for a consumer of a
semantic asset in a task kind that cannot receive one.

`TaskValidator.validate_driver_arguments`, called from
`validate_task_contract`, runs the serializer over the node's
`execution_args` before the task is dispatched, so the refusal lands ahead of
environment preparation rather than mid-run; the serializer keeps the same
refusal for callers that reach it directly. It runs at the execution-args
stage rather than at prepare time because remote inputs are only resolved to
local paths by then.

Shell tasks are deliberately not covered: a shell task's body is Python and
runs before the command is built, so taking a live payload and writing the
format the command expects is the sanctioned route rather than an error.

## Special Types

Ginkgo currently ships three path-oriented marker types:

- `file`
- `folder`
- `tmp_dir`

These drive validation, caching, and scratch-directory lifecycle management.

## Optional Outputs

Shell, script, and notebook tasks validate every declared output after
execution. Some tools legitimately emit different file sets under different
configuration modes, so a declared path can be wrapped in `optional()`:

```python
@task(kind="shell")
def filter_bam(bam: file, mode: str) -> tuple[file, file | None]:
    return shell(
        cmd=f"filter --mode {mode} {bam}",
        output=("results/filtered.bam", optional("results/unmapped.fastq.gz")),
    )
```

`OptionalOutput` (`core/optional.py`) wraps a *declaration*, which is why it
lives apart from the `file` / `folder` / `tmp_dir` markers above — those
describe a *value*. It is accepted wherever a path may be declared: as a bare
output, or as an item of the list and tuple forms. There is no dict/named
output form, so issue #98's `output={"unmapped_fastq": optional(...)}` sketch
is not the implemented spelling.

The contract:

- A present optional path is hashed, stored, restored, and validated exactly
  like a required file output.
- An absent one resolves to `None` and does not fail the task. A missing
  *required* output fails as before.
- Absence is a cacheable result, not a suppressed error:
  `CacheStore._hash_value` encodes it as a distinct `{"type": "absent"}` token,
  so present and absent are different cache keys and cannot serve each other.
- Consumers annotate `file | None` and branch explicitly.
  `unwrap_optional_annotation` (`core/types.py`) is the single home for
  splitting `X | None` into its inner type and a nullability flag; validation,
  coercion, cache hashing, and the output index all share it.

A heterogeneous tuple such as `tuple[file, file | None]` governs each element
with its own annotation. Every container walk previously applied only the
first (`inner_args[0]`) to all elements, which handed an absent optional the
annotation `file` and lost its nullability — crashing cache-key hashing,
dropping the element from the manifest, and, for `tuple[file, folder | None]`,
storing nothing so the task never cached. `pair_elements_with_annotations`
(`core/types.py`) is the one home for that pairing, shared by
`validate_annotated_value`, `CacheStore._hash_value`,
`_collect_output_artifacts`, `_validate_output_value`, and `output_summary`.
Anything else walking a container annotation should use it rather than
reaching for `get_args(...)[0]`.

Three walks over a declared output serve different needs, all in
`runtime/task_runners/shell.py`: `iter_output_values` returns every path
(pre-execution cleanup must remove a stale optional file too, or it would be
mistaken for this run's output), `iter_required_output_values` returns only
paths that must exist, and `resolve_output_value` rebuilds the declared
scalar/list/tuple shape with absent optionals replaced by `None`.

Manifests carry presence explicitly: `output_summary` emits `optional` and
`present` keys rather than dropping an absent output, so `ginkgo inspect run`
shows which optional outputs materialised.

Two limits are deliberate. Dry-run cannot report optionality, because a driver
task's output paths are computed inside the task body at execution time and the
plan does not know them. And a cache hit recording absence does not delete a
file that exists at that path from another source — the result value is `None`
either way, and deleting files Ginkgo did not create would be worse.
