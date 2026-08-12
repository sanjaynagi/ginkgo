# Configuration, Parameters, and Secrets

## Configuration

`ginkgo.config(path)` loads a TOML or YAML file and returns a plain dict. Files
given with `--config` layer over the file the workflow asks for, so supplying one
value does not require restating every other value. Later `--config` paths win
over earlier ones, and all of them win over the workflow's own file. The base
file is optional when overrides are given: overrides alone may define the whole
config.

## Parameters

`ginkgo.param(name, ...)` declares an input the workflow accepts and returns its
resolved value:

```python
import ginkgo

n_replicates = ginkgo.param("n_replicates", type=int, default=12, help="Replicates per item")
region = ginkgo.param("region", help="Genome region")   # no default: required
```

```bash
ginkgo run workflow.py --n-replicates 24 --region 2L:1-100000
```

A parameter resolves from three sources, in order:

1. the command line — `--n-replicates 24`, `--n-replicates=24`
2. the `[params]` table of the project config
3. the declared `default`; a declaration without one is required

```toml
[params]
n_replicates = 24
region = "2L:1-100000"
```

The flag is the dashed form of the name, so `n_replicates` is supplied as
`--n-replicates`, while the config key and the Python variable keep the
underscored form.

`type` follows `argparse`'s convention and defaults to `str`. `bool` is handled
separately from other callables, because `bool("false")` is `True`: a boolean
parameter accepts a bare `--flag` or an explicit `true`/`false`/`1`/`0`/`yes`/
`no`/`on`/`off`. `multiple=True` makes a flag repeatable and resolves to a
tuple. `choices` is checked after type conversion.

Declarations are registered on the active config session, which is why the
runtime config is loaded *before* the workflow module is imported: resolution
must not depend on whether a workflow calls `config()` before or after
`param()`. Unrecognised flags are reported once the flow has been built, so
parameters declared inside a flow body still claim their flag.

Every command that imports a workflow accepts parameter flags: `run`, `doctor`,
`secrets`, and `inspect workflow`. All but `run` treat an unsupplied required
parameter as absent rather than an error, so a workflow can still be described
without its inputs. `ginkgo run <workflow> --help` lists the declared
parameters, and `ginkgo inspect workflow` reports them as structured JSON —
name, flag, type, default, whether required, help, and choices — so a consumer
can enumerate a workflow's inputs without running it.

Resolved values are written to `params.yaml` alongside the loaded config, and
each parameter's source (`cli`, `config`, or `default`) is recorded under
`param_sources` in `manifest.yaml`.

The `[params]` table layers key by key across config sources, unlike top-level
keys which replace wholesale. An override that sets one parameter therefore
leaves the rest of the base table intact. This is why the runtime config is
loaded as separate layers rather than merged first: merging would take the last
source's table whole and silently drop parameters the override did not mention.

The table is read from the discovered project config (`ginkgo.toml`,
`ginkgo.yaml`, or `ginkgo.yml`) and any `--config` overlay — not from a
differently named file the workflow happens to load itself. Parameters must
resolve before the workflow module is imported, so the name it will pass to
`config()` is not yet known. Pass such a file with `--config` to have its
`[params]` table count.

A sub-workflow's `params=` is delivered the same way: `subworkflow(path,
params={...})` writes a `[params]` table into a temporary `--config` file, so the
child resolves them through `ginkgo.param` like any other source, and a parameter
the parent does not pass keeps whatever the child's own table gives it.

### Parameters must be passed as task arguments

**A parameter reaches a task as an argument.** Task cache keys hash task
arguments, so a parameter passed as one correctly invalidates the tasks that
received it:

```python
n_reps = ginkgo.param("n_reps", type=int, default=3)

@flow
def main():
    return simulate(n=n_reps)          # changing --n-reps re-runs simulate
```

A parameter read from a module global inside a task body breaks that rule. The
read is invisible to the task's cache key, so a changed value silently reuses the
previous result:

```python
tag = ginkgo.param("tag", default="a")

@task()
def write_it(output_path: str) -> file:
    Path(output_path).write_text(tag)  # WRONG: changing --tag will not re-run this
    return output_path
```

Using a parameter in the flow body to shape the graph — output paths, `.map()`
lists, which tasks exist at all — is fine, because those become task arguments
and paths.

`ginkgo run` and `ginkgo doctor` both warn when a task body reads a declared
parameter as a global (`param_read_from_global`). Detection disassembles each
task function, matching `LOAD_GLOBAL` reads against the globals of the module
that declared the parameter, so an unrelated global of the same name elsewhere is
not reported. It is deliberately best-effort: a read made by a helper the task
calls is not found. That is why it warns rather than failing the run — a check
that cannot see every violation should not present itself as authoritative.

The same cache gap applies to a value read from `config()` at module level, so
this is not new behaviour; parameters make the pattern easier to reach, which is
why it is checked.

Worker processes do receive the run's real parameter values — they re-import the
workflow module and re-resolve against the same inputs — so a value is correct
whenever it is computed. Only cache reuse is affected.

## Secrets

Workflows can declare runtime-only secret dependencies via `secret(...)`
references, which are resolved at execution time through a pluggable resolver
layer with environment-variable lookup and optional `.env` support. Secret
references remain identifiers during graph construction and cache-keying, so
rotating a credential value does not invalidate cache entries that are
otherwise still valid.

Secret-bearing inputs are redacted before they reach persisted provenance or
cache metadata, and task log capture redacts resolved secret values before they
are written to per-task stdout/stderr logs.
