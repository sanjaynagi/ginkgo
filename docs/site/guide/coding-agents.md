# Working With Coding Agents

Ginkgo is built to be written and run by AI coding agents as well as by
people. It gives agents a `skills/` folder of project guidance, and a run mode
that prints machine-readable events.

## The `skills/` Folder

`ginkgo init` creates a new project, with a `skills/` folder in it:

```bash
ginkgo init my-project
```

`skills/` is a folder of plain Markdown files. They tell an agent how your
project is organised and which `ginkgo` commands to run. Any agent that can
read files can use them.

- `index.md` is the entry point, with a map of the other files.
- `project.md` covers the project layout and where code should live.
- `config.md` covers loading config, and overriding it with `--config` on the
  command line.
- `commands.md` covers the commands that check, run and inspect a workflow.
- `workflow-patterns.md` covers task kinds, environments, notebooks, workflow
  parameters, and staging inputs for remote tasks.
- `local.md` is yours to edit. Add your project's own conventions here. Where
  it conflicts with the other files, an agent follows `local.md`.

### Pointing An Agent At The Skills

`skills/index.md` is the entry point. Tell your agent to read it first.

`ginkgo init` also writes a short `AGENTS.md` at the project root, which points
to `skills/index.md`. Many agents read `AGENTS.md` first. If the project
already has an `AGENTS.md`, `init` keeps it and writes none.

Claude Code reads `CLAUDE.md`, not `AGENTS.md`. Add the line `@AGENTS.md` to
`CLAUDE.md` to include it.

### Choosing What `init` Writes

```bash
ginkgo init my-project --no-skills   # the project only, with no skills/ or AGENTS.md
ginkgo init --skills-only            # skills/ and AGENTS.md, in the current directory
```

`init` refuses to overwrite files that already exist. Pass `--force` to
overwrite them. Don't pass `--force` if you have edited `skills/local.md` or
the other `skills/` files, because it replaces them with the defaults. It
never replaces an existing `AGENTS.md`.

## Agent Run Mode

`ginkgo run --agent-output` replaces the live terminal display with a stream
of events on stdout. The events are JSON Lines (JSONL): one JSON object per
line.

```bash
ginkgo run flow.py --agent-output
```

An agent can read the stream to follow progress, spot failures and read task
results. It doesn't need to read a terminal display. Each event names its type
in its `event` field. The ones an agent most often needs are these:

- `run_started`: the run began.
- `task_cache_hit`: a task's result came from the cache.
- `task_completed`: a task finished.
- `task_failed`: a task failed.
- `run_completed`: the run ended. Its `status` is `success`, `failed` or
  `cancelled`.

To see every event type, run `ginkgo export events <run_id>` on a finished
run. It prints the run's events in the same shape.

If a run can't start, Ginkgo prints one `run_rejected` event. A missing
workflow file, an unknown parameter, or a graph that does not validate all
cause this. The event has an `error` message and an `error_type`. It has no
`run_id`, because no run exists. The command exits non-zero, and writes the
same message to stderr.

Add `--verbose` (`ginkgo run --agent-output --verbose`) to include each task's
log output in the stream.

## Inspection Commands

These commands let an agent, or a person, look at a workflow and its runs.
They do not change the workflow or its results.

- `ginkgo doctor --json` checks the workflow and its setup for problems.
- `ginkgo run --dry-run` shows which tasks would run, and why, without running
  any.
- `ginkgo inspect workflow` shows the task graph, before anything runs.
- `ginkgo runs show <run_id> --json` shows a recorded run.
- `ginkgo history <task-name> --json` shows every run of one task.
- `ginkgo cache explain <run_id> --json` shows why each task ran or came from
  the cache.
- `ginkgo debug <run_id>` shows task status, logs and failure detail.
- `ginkgo why <path>` names the task and run that produced a file, and its
  inputs.
- `ginkgo export events <run_id>` prints a finished run's events as JSONL.
- `ginkgo query "<sql>" --json` runs one read-only SQL statement over the
  [provenance](concepts.md#glossary) database. The tables can change between releases (see
  [Querying Provenance](querying-provenance.md)).
- `ginkgo report <run_id>` builds an HTML report of a run (see
  [Reports](reports.md)). It writes the report files under
  `.ginkgo/reports/`.

## See Also

- [CLI](cli.md): every command.
- [Reports](reports.md): rendering a run as HTML.
