# Working With Coding Agents

Ginkgo is built to be authored and operated by AI coding agents as well as by
people. Two features make that practical: a generated skills directory that
gives an agent project-specific guidance, and a structured run mode that emits
machine-readable events.

## The `skills/` Directory

`ginkgo init` scaffolds a new project *and* writes a `skills/` directory
alongside it:

```bash
ginkgo init my-project
```

The `skills/` directory is concise operational guidance for contributors and
coding agents working in the repository:

- `index.md` — entry point and a map of the other files
- `project.md` — canonical layout and where code should live
- `config.md` — config loading and CLI overlay patterns
- `commands.md` — validation, execution, and inspection commands
- `workflow-patterns.md` — task-kind syntax, environments, notebooks, and
  remote staging
- `local.md` — repository-specific conventions that refine the defaults

Point an agent at `skills/index.md` and it has enough context to author and run
workflows correctly without rediscovering the project's conventions each time.

Two flags control what `init` writes:

```bash
ginkgo init my-project --no-skills   # project scaffold only, no skills/
ginkgo init --skills-only            # add skills/ to an existing project
```

Pass `--force` to overwrite existing files.

## Agent Run Mode

`ginkgo run --agent` replaces the live terminal UI with a stream of
newline-delimited JSON events on stdout:

```bash
ginkgo run workflow.py --agent
```

Each line is one JSON event — graph registration, task started, cache hit, task
completed, run completed, and so on. An agent can parse the stream to follow run
progress, detect failures, and read task results without scraping a rendered
terminal UI.

Add `--verbose` (`ginkgo run --agent --verbose`) to also include task log output
in the event stream.

## Inspection Commands

These read-only commands give an agent — or a person — a stable view of a
workflow and its runs:

- `ginkgo inspect workflow` — the resolved task graph, before running anything
- `ginkgo inspect run <run_id>` — the structure of a recorded run
- `ginkgo debug <run_id>` — task status, logs, and failure detail
- `ginkgo report <run_id>` — a full HTML report of a run (see
  [Assets and Reports](assets.md))

## See Also

- [CLI](cli.md) — the full command surface.
- [Assets and Reports](assets.md) — inspecting workflow outputs.
