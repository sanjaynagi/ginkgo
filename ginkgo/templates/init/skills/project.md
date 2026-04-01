# Project

Key files and directories:

- `ginkgo.toml`: project config loaded by `ginkgo.config(...)`
- `pixi.toml`: project development environment
- `{{ workflow_relpath }}`: flow wiring, config loading, and task composition
- `{{ modules_relpath }}`: reusable task implementations
- `{{ envs_relpath }}`: task-local Pixi environments
- `{{ tests_relpath }}`: test workflows used by `ginkgo test`
- `.ginkgo/`: runtime state for runs, cache, artifacts, and provenance

Keep `workflow.py` thin:

- good: flow definitions, config lookups, task composition, `.map()`,
  `.product_map()`, `expand(...)`
- move to `modules/`: task bodies, shell command construction, parsing, heavy
  transformation logic

Task-local Pixi environments live under `{{ envs_relpath }}` and are referenced
by name from tasks, for example `@task("script", env="analysis_tools")`.

Prefer `ginkgo inspect` and `ginkgo debug` for run state before looking inside
`.ginkgo/` directly.
