# CLI

The `ginkgo` CLI is the main operator surface for authoring, validating,
running, and inspecting workflows. Every command operates on the project rooted
at the nearest `ginkgo.toml`.

## Command Overview

`ginkgo init`
: Scaffold a new project &mdash; `ginkgo.toml`, a starter `workflow.py`, the
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
ginkgo run workflow.py
ginkgo run workflow.py --jobs 8 --cores 32 --memory 64
ginkgo run workflow.py --dry-run
```

`ginkgo run` builds the expression tree, validates the workflow, evaluates ready
tasks subject to the `--jobs`, `--cores`, and `--memory` budgets, and writes run
history under `.ginkgo/runs/`. Run it from a project root with no path argument
and Ginkgo discovers the canonical `workflow.py` entrypoint.

`--dry-run` resolves the graph and computes cache keys without executing any
task body &mdash; the fastest way to confirm a workflow is wired correctly.

`--agent` swaps the live terminal UI for a stream of newline-delimited JSON
events, for programmatic use by AI coding agents &mdash; see
[Working with Coding Agents](coding-agents.md).

## Validation And Diagnostics

Use these commands to inspect a workflow without committing to the full
workload:

```bash
ginkgo test --dry-run
ginkgo doctor workflow.py
ginkgo debug <run_id>
```

`ginkgo doctor` catches environment and configuration problems before a run.
`ginkgo debug` is most useful after the fact: once a run directory exists, it
surfaces recorded task status, logs, and cache behavior without manually
navigating `.ginkgo/runs/`.

## A Typical Loop

For local development, a practical cycle looks like this:

1. author and adjust tasks in code
2. check the wiring with `ginkgo run --dry-run` (or `ginkgo test`)
3. run with `ginkgo run`
4. inspect failures or cache reuse with `ginkgo debug`

Because Ginkgo caches completed tasks, iterating on a later stage of a workflow
re-executes only that stage &mdash; earlier tasks serve straight from cache.
