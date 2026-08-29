# Commands

Start here:

- `ginkgo run --dry-run`
  Previews the plan for `{{ workflow_relpath }}` without executing any task body.
- `ginkgo run {{ tests_relpath }}/smoke.py --dry-run`
  Validates a workflow in `{{ tests_relpath }}` without executing tasks.
- `ginkgo run --agent-output`
  Executes and emits machine-readable JSONL runtime events on stdout for programmatic
  consumers or agents.
- `ginkgo run`
  Executes the default workflow from `{{ workflow_relpath }}`.
- `ginkgo inspect workflow`
  Shows the static workflow graph without running anything.
- `ginkgo doctor`
  Validates workflow loading, configuration, and environment setup.

When you already have a run id:

- `ginkgo runs show <run_id>`
  Reconstructs a completed run from stored provenance.
- `ginkgo debug <run_id>`
  Shows a human-readable failure summary.
- `ginkgo debug <run_id> --json`
  Emits structured diagnostics for programmatic consumers.

Testing guidance:

- put workflow validation files under `{{ tests_relpath }}`
- use `ginkgo run --dry-run` for static validation of your own flow
- run a validation workflow by path when you want it to execute
