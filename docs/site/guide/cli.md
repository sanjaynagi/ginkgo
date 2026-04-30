# CLI

The `ginkgo` CLI is the main operator surface for authoring, validating,
running, and inspecting workflows.

## Core CLI Commands

The top-level CLI currently includes:

- `ginkgo run`
- `ginkgo test`
- `ginkgo debug`
- `ginkgo doctor`
- `ginkgo cache`
- `ginkgo env`
- `ginkgo init`
- `ginkgo secrets`

## Running Workflows

```bash
ginkgo run workflow.py
ginkgo run workflow.py --jobs 8 --cores 32 --memory 64
ginkgo run workflow.py --dry-run
```

`ginkgo run` builds the expression tree, validates the workflow, evaluates ready
tasks, and writes run history under `.ginkgo/runs/`.

## Validation And Diagnostics

Use these commands when you want to inspect a workflow without immediately
running the full workload:

```bash
ginkgo test --dry-run
ginkgo doctor workflow.py
ginkgo debug <run_id>
```

`ginkgo debug` is particularly useful once you already have a run directory and
want to inspect recorded information after the fact.

## A Good Working Pattern

For local development, a practical loop looks like this:

1. author and adjust tasks in code
2. run with `ginkgo run`
3. inspect failures or cache behavior with `ginkgo debug`
