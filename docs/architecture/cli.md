# CLI

The current CLI supports:

- `ginkgo run`
- `ginkgo test`
- `ginkgo debug`
- `ginkgo doctor`
- `ginkgo inspect`
- `ginkgo secrets`
- `ginkgo ui`
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

Implemented CLI features include dry-run validation, merged config overrides,
human-readable run summaries, structured inspection and diagnostics, secret
discovery and validation, cache inspection and eviction, failed-task
debugging, and asset catalog inspection for local workspaces.

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
