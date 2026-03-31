# Commands

Start here:

- `ginkgo test --dry-run`
  Validates test workflows in `{{ tests_relpath }}` without executing tasks.
- `ginkgo doctor`
  Validates workflow loading, configuration, and environment setup.
- `ginkgo run`
  Executes the default workflow from `{{ workflow_relpath }}`.
- `ginkgo run --agent`
  Emits machine-readable JSONL runtime events on stdout for programmatic
  consumers.
- `ginkgo inspect workflow`
  Shows the static workflow graph without running anything.
- `ginkgo ui`
  Browse runs, tasks, and artifacts when you need a run id or visual context.

When you already have a run id:

- `ginkgo inspect run <run_id>`
  Reconstructs a completed run from stored provenance.
- `ginkgo debug <run_id>`
  Shows a human-readable failure summary.
- `ginkgo debug <run_id> --json`
  Emits structured diagnostics for programmatic consumers.

Testing guidance:

- put workflow validation files under `{{ tests_relpath }}`
- use `ginkgo test --dry-run` for static validation
- use `ginkgo test` when you want those test workflows to execute
