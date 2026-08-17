# CLI

The `ginkgo` CLI is the main operator surface for authoring, validating,
running, and inspecting workflows. Every command operates on the project rooted
at the nearest `ginkgo.toml`.

## Command Overview

`ginkgo init`
: Scaffold a new project &mdash; `ginkgo.toml`, a starter `workflow/flow.py`, the
  canonical layout, and a `skills/` directory for coding agents. See
  [Working with Coding Agents](coding-agents.md).

`ginkgo run`
: Build the expression tree, validate the workflow, evaluate ready tasks, and
  record the run. The command you reach for most.

`ginkgo test`
: Validate a workflow without executing task bodies. Use it in CI or before a
  long run to catch wiring errors early.

`ginkgo inspect`
: Inspect the resolved task graph (`inspect workflow`) or the structure of a
  recorded run (`inspect run <run_id>`).

`ginkgo debug`
: Inspect a finished run &mdash; task status, timing, logs, and cache decisions
  &mdash; from its recorded run directory.

`ginkgo doctor`
: Check a workflow and its environment for problems: missing environments,
  unresolved secrets, malformed config.

`ginkgo report`
: Render a finished run as a self-contained HTML report. See
  [Assets and Reports](assets.md).

`ginkgo cache`
: List, clear, and prune cached task results. See
  [Caching and Provenance](caching-and-provenance.md).

`ginkgo asset`
: List and inspect typed, versioned task outputs. See
  [Assets and Reports](assets.md).

`ginkgo models`
: List model assets together with their recorded metrics.

`ginkgo notebooks`
: List the rendered notebook HTML artifacts produced by runs.

`ginkgo env`
: List and reset the Pixi and container environments backing shell tasks. See
  [Environments](environments.md).

`ginkgo secrets`
: List and validate the secret references a workflow resolves at run time.

Run `ginkgo <command> --help` for the full flag set of any command.

## Running Workflows

```bash
ginkgo run flow.py
ginkgo run flow.py --jobs 8 --cores 32 --memory 64
ginkgo run flow.py --dry-run
```

`ginkgo run` builds the expression tree, validates the workflow, evaluates ready
tasks subject to the `--jobs`, `--cores`, `--memory`, and `--gpus` budgets,
and writes run history under `.ginkgo/runs/`. Run it from a project root with
no path argument and Ginkgo discovers the canonical `workflow/flow.py`
entrypoint.

Repeated `--resource name=value` flags budget any custom resource dimensions
tasks declare (e.g. `--resource api_calls=10`) — see
[Custom Resource Dimensions](resources.md#custom-resource-dimensions).

`--dry-run` resolves the graph and computes cache keys without executing any
task body &mdash; the fastest way to confirm a workflow is wired correctly.

`--agent-output` swaps the live terminal UI for a stream of newline-delimited JSON
events, for programmatic use by AI coding agents &mdash; see
[Working with Coding Agents](coding-agents.md).

## Workflow Parameters

A workflow declares the inputs it accepts with `ginkgo.param(...)`, and each one
becomes a command-line flag:

```python
import ginkgo

n_replicates = ginkgo.param("n_replicates", type=int, default=12, help="Replicates per item")
region = ginkgo.param("region", help="Genome region")   # no default: required
```

```bash
ginkgo run flow.py --n-replicates 24 --region 2L:1-100000
```

The flag is the dashed form of the name. A value resolves from the command line
first, then the `[params]` table of `ginkgo.toml`, then the declared default:

```toml
[params]
n_replicates = 24
region = "2L:1-100000"
```

`ginkgo run flow.py --help` lists the parameters that workflow declares,
with their types and defaults. A flag the workflow does not declare is rejected
before anything runs, and the error names the parameters it does declare. A
required parameter that is not supplied fails the same way.

`type` follows `argparse`'s convention, so `type=int`, `type=float`, and
`type=Path` all work. Booleans accept a bare `--flag` or an explicit
`--flag false`, and `multiple=True` makes a flag repeatable:

```bash
ginkgo run flow.py --item alpha --item beta --verbose
```

Resolved values are recorded in the run's `params.yaml`, and where each came
from &mdash; the CLI, config, or the default &mdash; in `manifest.yaml`.

The `[params]` table layers across config files, so `--config extra.toml` setting
one parameter leaves the others in `ginkgo.toml` alone.

```{important}
**Pass a parameter into a task as an argument.** Cache keys hash task arguments,
so a parameter passed as one correctly re-runs the tasks that used it. A
parameter read from a module global inside a task body is not part of the key, so
changing it would silently reuse the previous result. Both `ginkgo run` and
`ginkgo doctor` warn when they spot this.
```

## Validation And Diagnostics

Use these commands to inspect a workflow without committing to the full
workload:

```bash
ginkgo test --dry-run
ginkgo doctor flow.py
ginkgo debug <run_id>
```

`ginkgo doctor` catches environment and configuration problems before a run.
Pass `--json` for structured output suitable for programmatic use:

```bash
ginkgo doctor flow.py --json
```

`ginkgo debug` is most useful after the fact: once a run directory exists, it
surfaces recorded task status, logs, and cache behavior without manually
navigating `.ginkgo/runs/`.

`ginkgo cache` has several subcommands beyond listing:

```bash
ginkgo cache ls                          # list cached task results
ginkgo cache explain --run <run_id>      # explain cache decisions for a run
ginkgo cache prune --older-than 7d       # remove entries older than a duration
ginkgo cache prune --max-size 10GB       # remove entries to stay under a size limit
ginkgo cache prune --max-entries 500     # remove entries to stay under an entry count
ginkgo cache clear <cache_key>           # remove a specific cache entry
```

`cache prune` requires at least one of `--older-than`, `--max-size`, or
`--max-entries`. Add `--dry-run` to preview what would be removed.

## A Typical Loop

For local development, a practical cycle looks like this:

1. author and adjust tasks in code
2. check the wiring with `ginkgo run --dry-run` (or `ginkgo test`)
3. run with `ginkgo run`
4. inspect failures or cache reuse with `ginkgo debug`

Because Ginkgo caches completed tasks, iterating on a later stage of a workflow
re-executes only that stage &mdash; earlier tasks serve straight from cache.
