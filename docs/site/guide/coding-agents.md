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
- `workflow-patterns.md` — task-kind syntax, environments, notebooks,
  workflow parameters, and remote staging
- `local.md` — repository-specific conventions that refine the defaults

Point an agent at `skills/index.md` and it has enough context to author and run
workflows correctly without rediscovering the project's conventions each time.
`init` also writes a short `AGENTS.md` at the project root that does this,
since many agents read that file first; a project that already has an
`AGENTS.md` keeps its own. Claude Code reads `CLAUDE.md` instead: add a line
`@AGENTS.md` to it to pull the pointer in.

Two flags control what `init` writes:

```bash
ginkgo init my-project --no-skills   # project scaffold only, no skills/ or AGENTS.md
ginkgo init --skills-only            # add skills/ to an existing project
```

Pass `--force` to overwrite existing files.

## Agent Run Mode

`ginkgo run --agent-output` replaces the live terminal UI with a stream of
newline-delimited JSON events on stdout:

```bash
ginkgo run flow.py --agent-output
```

Each line is one JSON event — graph registration, task started, cache hit, task
completed, run completed, and so on. An agent can parse the stream to follow run
progress, detect failures, and read task results without scraping a rendered
terminal UI.

A run rejected before it starts — a missing workflow file, an unknown parameter,
a graph that does not validate — prints a single `run_rejected` event, with the
`error` message and its `error_type`, and no `run_id`: there is no run. The
command exits non-zero, and the same message is on stderr.

Add `--verbose` (`ginkgo run --agent-output --verbose`) to also include task log output
in the event stream.

The stream's last line is a `run_summary` event with the whole outcome, so an
agent does not have to rebuild it from the events before: the `status` and
`run_dir`; `counts` of tasks executed, cached, failed, ignored and skipped; each
failure's task, category, exit code, last log lines (ANSI codes stripped) and
log path; each skipped task with the failure that blocked it; and the files the
run produced, with the task that produced each.

## Long Runs

An agent's tool call usually times out after a couple of minutes, so a long
workflow should not run in the foreground. Detach it, then wait:

```bash
ginkgo run flow.py --detach --agent-output   # {"event": "run_detached", "run_id": ..., "log": ...}
ginkgo runs wait <run_id> --timeout 100 --json
```

`--detach` starts the run in the background and returns once it has started,
with its run ID and the log its output goes to (`.ginkgo/detached/<run_id>.log`).
`ginkgo runs wait` polls the ledger until the run finishes, then prints the same
payload as the `run_summary` event and exits 0 if the run succeeded, 1 if it
failed, and 130 if it was cancelled. At the timeout it prints the payload of the
run so far and exits 124, so call it again to keep waiting.
`ginkgo runs status <run_id> --json` prints that payload once, without waiting.

The ledger is the only record of a run, so a run killed outright (`kill -9`, a
lost machine) stays `running` there. The `--timeout` is what guards against
waiting on it forever.

## Inspection Commands

These read-only commands give an agent — or a person — a stable view of a
workflow and its runs:

- `ginkgo inspect workflow` — the resolved task graph, before running anything
- `ginkgo runs show <run_id> --json` — the structure of a recorded run
- `ginkgo history <task-name> --json` — every run of one task, across runs
- `ginkgo query "<sql>" --json` — one read-only SQL statement over the ledger
- `ginkgo export events <run_id>` — a finished run's events as JSONL
- `ginkgo debug <run_id>` — task status, logs, and failure detail
- `ginkgo why <path>` — the task and run that produced a file, and its inputs
- `ginkgo report <run_id>` — a full HTML report of a run (see
  [Reports](reports.md))

## See Also

- [CLI](cli.md) — the full command surface.
- [Reports](reports.md) — rendering a run as HTML.
