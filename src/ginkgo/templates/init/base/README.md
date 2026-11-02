# ginkgo-init-template

This starter project is the canonical `ginkgo init` scaffold.

See `skills/index.md` for concise contributor and agent guidance tailored to
this project layout.

## Getting Started

`pixi.toml` declares the environment the `ginkgo` CLI runs from, including
Ginkgo itself. Install it once, then use the declared tasks:

```bash
pixi install
pixi run check   # ginkgo run --dry-run
pixi run run     # ginkgo run
```

## Commit `pixi.lock`

`pixi.toml` pins ginkgo and its plugins to the release that created this
project. `pixi install` resolves everything else and writes the result into
`pixi.lock`, so the lock records the exact environment this project runs.

That makes `pixi.lock` a source file here. Commit it, and commit it again
whenever `pixi install` or `pixi update` changes it. The scaffolded `.gitignore`
ignores the generated directories (`.pixi/`, `.ginkgo/`, `results/`, `logs/`)
and deliberately does not ignore the lock. A clone without the lock re-resolves
the other dependencies and may get different versions from the ones your
results came from.

To upgrade ginkgo, change the version on all three ginkgo entries in
`pixi.toml` together (the plugins pin the exact ginkgo version they ship with),
then run `pixi install`.

Python tasks cannot declare `env=` — they execute in the same interpreter as the
CLI — so any library a Python or notebook task imports belongs in the
`[dependencies]` or `[pypi-dependencies]` table of this `pixi.toml`. Only shell,
script, and notebook tasks can point at a task-local environment such as
`workflow/envs/analysis_tools/`.

## What Ginkgo Is

Ginkgo is a Python workflow orchestrator for building reproducible, analytical
pipelines. You define work as typed tasks and connect those tasks inside a flow.
Ginkgo then evaluates the dependency graph, executes tasks concurrently,
reuses cached results when inputs have not changed, and records provenance that
you can inspect from the CLI, or as an HTML report with the `ginkgo-report`
plugin.

## Core Concepts

- A `@task()` does not run immediately. It builds a deferred computation node.
- A `@flow` wires task nodes together into a workflow graph.
- `.map()` provides zip-style fan-out across many independent task invocations.
- `.product_map()` provides Cartesian fan-out across parameter combinations.
- Python tasks are useful for orchestration, data transformation, and analytics.
- Shell tasks let you call CLI tools with explicit declared outputs.
- Notebook tasks (from the `ginkgo-notebook` plugin) run marimo notebooks as part of the workflow and render them to HTML.
- Script tasks let you execute standalone scripts with task-managed inputs and outputs.
- `expand(...)` builds a deterministic list of paths to `.map()` over, one per row.
- `per_branch(...)` derives one output path per fan-out branch from that branch's
  own arguments — the way to give each cell of a `.product_map()` grid its own file.

- ginkgo.toml is the canonical configuration file for a Ginkgo project, however, it is optional.

## What This Starter Demonstrates

This project is intentionally domain-neutral and demonstrates the main Ginkgo
patterns in one small workflow:

- Python tasks
- local shell tasks
- Pixi-backed script execution
- a shell task that a single `env="docker://..."` argument moves into a
  container (see `package_brief` in `workflow/modules/analysis.py`)
- notebook rendering
- fan-out and fan-in
- `expand(...)` for deterministic output-path columns

## Project Layout

- `workflow/flow.py` contains the top-level flow wiring.
- `workflow/modules/` contains the reusable task implementations, split into
  `prep.py`, `analysis.py` and `reporting.py`. That split is a caching
  decision, not a tidiness one: a task's cache identity covers the whole module
  it is defined in, so tasks that share a file are invalidated together when
  any of them is edited. Keep an expensive task in a module of its own.
- `workflow/envs/analysis_tools/` contains a task-local Pixi env.
- `workflow/scripts/` contains the script used by the script task.
- `workflow/notebooks/` contains the report notebook.
- `tests/workflows/smoke.py` is the validation workflow. It re-exports `main`
  from `workflow/flow.py`, so running it covers the same flow `ginkgo run`
  executes: `ginkgo run tests/workflows/smoke.py`.

## What The Workflow Produces

The default run creates a small set of synthetic work items, normalizes them
with a local shell task, builds Markdown briefs with a Pixi-backed script task,
packages those briefs with a local shell task, renders a notebook
overview, and then writes a final delivery manifest.

Outputs are written under `results/`.

## CLI Usage

```bash
# Preview the plan for the default workflow without executing any task body.
# The quickest way to confirm a workflow is wired correctly.
ginkgo run --dry-run

# Run the validation workflow. Task bodies execute unless --dry-run is passed.
ginkgo run tests/workflows/smoke.py
ginkgo run tests/workflows/smoke.py --dry-run

# Execute the default workflow with Rich terminal output.
ginkgo run --cores 8

# Execute the workflow and stream machine-readable JSONL events.
ginkgo run --agent-output
ginkgo run --cores 8 --agent-output

# Inspect the static workflow graph without running any tasks.
ginkgo inspect workflow

# List recorded runs, then inspect one from its stored provenance.
ginkgo runs ls
ginkgo runs show <run_id>

# Show a human-friendly debug report for failed tasks in a run.
ginkgo debug <run_id>

# Emit machine-readable failure diagnostics for a run.
ginkgo debug <run_id> --json

# Validate workflow structure, environments, and configuration.
ginkgo doctor

# Emit machine-readable validation diagnostics.
ginkgo doctor --json

# List cache entries stored in the local workspace.
ginkgo cache ls

# Explain why tasks in a run reused cache or reran.
ginkgo cache explain <run_id>
```
