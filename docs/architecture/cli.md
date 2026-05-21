# CLI

The current CLI supports:

- `ginkgo run`
- `ginkgo test`
- `ginkgo debug`
- `ginkgo doctor`
- `ginkgo inspect`
- `ginkgo secrets`
- `ginkgo init`
- `ginkgo asset ls`
- `ginkgo asset versions`
- `ginkgo asset inspect`
- `ginkgo models`
- `ginkgo cache ls`
- `ginkgo cache explain`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls`
- `ginkgo env clear`

Implemented CLI features include the dry-run execution-plan preview, merged
config overrides, human-readable run summaries, structured inspection and
diagnostics, secret discovery and validation, cache inspection and eviction,
failed-task debugging, and asset catalog inspection for local workspaces.

`ginkgo run --dry-run` validates the workflow and prints a static execution
plan instead of running it: tasks grouped into dependency waves, each
annotated `[cached]`, `[will run]`, or `[unknown]`, with static `.map()`
fan-out fully expanded and a peak-resource summary. Cache status is resolved
by a leaf-anchored cascade — a task is checkable only while every upstream
dependency is a confirmed cache hit — so a fully warm rerun previews as all
`[cached]`. The plan builder (`runtime/dry_run.py`) is read-only: no task
runs, no environment is prepared, and no cached output is materialised. Large
fan-out groups collapse unless `--verbose` is passed. `ginkgo test --dry-run`
keeps its terse per-workflow validation line rather than printing a full plan
for each discovered workflow.

`ginkgo cache prune` accepts `--older-than <duration>`, `--max-size <size>`,
and `--max-entries <N>`. At least one of the three is required; multiple
may be combined, and eviction always proceeds oldest-first with orphan
artifact garbage collection at the end. `--dry-run` previews what would be
removed without touching disk.

Run-time failure diagnostics classify each task failure into one of a small
set of categories — `env_mismatch`, `import_error`, `invalid_path`,
`missing_input`, `shell_command_error`, `serialization_error`,
`user_code_error`, `output_validation_error`, `cache_error`,
`cycle_detected`, and `scheduler_error` — and the end-of-run renderer
groups failures by category so that common root causes stand out without
digging through individual panels.
