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

- `ginkgo runs ls`
  Lists recorded runs, newest first.
- `ginkgo runs show <run_id>`
  Reconstructs a completed run from stored provenance; `--json` for the full
  snapshot.
- `ginkgo history <task-name>`
  Shows every run of one task, with status, duration, and cache key.
- `ginkgo query "<sql>"`
  Runs one read-only SQL statement against the provenance database.
- `ginkgo export events <run_id>` / `ginkgo export manifest <run_id>`
  Writes a run's ledger events as JSONL, or its manifest as YAML.
- `ginkgo debug <run_id>`
  Shows a human-readable failure summary.
- `ginkgo debug <run_id> --json`
  Emits structured diagnostics for programmatic consumers.
- `ginkgo report <run_id>`
  Renders a run as an HTML report; needs the `ginkgo-report` plugin.
  `--out <dir>` writes the bundle to a directory, with rendered notebook
  pages under `<dir>/pages/`. `--single-file` writes one HTML file instead, by
  default `.ginkgo/reports/<run_id>.html` or the `--out` path ending in
  `.html`; it links rendered pages but does not include them.
- `ginkgo cache ls`
  Lists cache entries with their full keys.
- `ginkgo cache explain <run_id>`
  Shows each task's cache key beside its branch label, and why it reused the
  cache or re-ran.
- `ginkgo cache clear <full-cache-key>`
  Removes one entry; a key prefix is not accepted. `ginkgo cache prune
  --max-entries 0` removes every entry.

Provenance:

- `ginkgo why <path>`
  Names the task and run that produced a file (or an artifact id), its cache
  key, and the inputs that task recorded.

Testing guidance:

- put workflow validation files under `{{ tests_relpath }}`
- use `ginkgo run --dry-run` for static validation of your own flow
- run a validation workflow by path when you want it to execute
