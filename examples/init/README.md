# ginkgo-init-template

This starter project is the canonical `ginkgo init` scaffold.

## What Ginkgo Is

Ginkgo is a Python workflow orchestrator for building reproducible, analytical
pipelines. You define work as typed tasks and connect those tasks inside a flow.
Ginkgo then evaluates the dependency graph, executes tasks concurrently,
reuses cached results when inputs have not changed, and records provenance that
you can inspect from the CLI or UI.

## Core Concepts

- A `@task()` does not run immediately. It builds a deferred computation node.
- A `@flow` wires task nodes together into a workflow graph.
- `.map()` provides fan-out across many independent task invocations.
- Python tasks are useful for orchestration, data transformation, and analytics.
- Shell tasks let you call CLI tools with explicit declared outputs.
- Notebook tasks let you execute notebooks as part of the workflow and render them to html, which are then displayed in the UI.
- Script tasks let you execute standalone scripts with task-managed inputs and outputs.
- `expand(...)` helps build deterministic output paths from small parameter grids.

- ginkgo.toml is the canonical configuration file for a Ginkgo project, however, it is optional.

## What This Starter Demonstrates

This project is intentionally domain-neutral and demonstrates the main Ginkgo
patterns in one small workflow:

- Python tasks
- local shell tasks
- Pixi-backed script execution
- Docker-backed shell execution
- notebook rendering
- fan-out and fan-in
- `expand(...)` for deterministic output paths

## Project Layout

- `ginkgo_init_template/workflow.py` keeps the CLI entrypoint thin.
- `ginkgo_init_template/modules/` contains the actual task implementations.
- `ginkgo_init_template/envs/analysis_tools/` contains a task-local Pixi env.
- `ginkgo_init_template/scripts/` contains the script used by the script task.
- `ginkgo_init_template/notebooks/` contains the report notebook.
- `tests/workflows/smoke.py` is the validation workflow used by `ginkgo test`.

## What The Workflow Produces

The default run creates a small set of synthetic work items, normalizes them
with a local shell task, builds Markdown briefs with a Pixi-backed script task,
packages those briefs with a Docker-backed shell task, renders a notebook
overview, and then writes a final delivery manifest.

Outputs are written under `results/`.

## CLI Usage

```bash
# Validate the starter workflow definitions without executing tasks.
ginkgo test --dry-run

# Execute the default workflow with Rich terminal output.
ginkgo run --cores 8

# Execute the workflow and stream machine-readable JSONL events.
ginkgo run --agent
ginkgo run --cores 8 --agent

# Inspect the static workflow graph without running any tasks.
ginkgo inspect workflow

# Inspect a completed run from its stored provenance.
ginkgo inspect run <run_id>

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
ginkgo cache explain --run <run_id>

# Open the local UI for browsing runs, tasks, and logs.
ginkgo ui
```
