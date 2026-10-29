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
of JSONL events on stdout, one JSON object per line. Add `--verbose` to
include each task's log output.

```bash
ginkgo run flow.py --agent-output
```

Each event names its type in its `event` field. The ones an agent most often
needs:

- `run_started`: the run began.
- `task_cache_hit`, `task_completed`, `task_failed`: a task came from the
  cache, finished or failed.
- `run_completed`: the run ended. Its `status` is `success`, `failed` or
  `cancelled`.
- `run_summary`: the last line, with the whole outcome. It holds the
  run's `status` and `run_dir`, task counts, each failure with its error and
  last log lines, each skipped task and what blocked it, and the files the run
  produced.

The `run_summary` status matches `ginkgo runs ls`, so a successful run is
`succeeded` there, where `run_completed` says `success`.

If a run can't start, as with a missing workflow file or an unknown
parameter, Ginkgo prints one `run_rejected` event, with an `error` and an
`error_type` but no `run_id`. The command exits non-zero.

## Long Runs

An agent's tool call usually times out after a couple of minutes, so run a
long workflow with `--detach`. It starts the run in the background, prints its
run ID and log path, and returns. Then follow the run with
`ginkgo runs wait`:

```bash
ginkgo run flow.py --detach --agent-output
# {"event": "run_detached", "run_id": "20261028_101500_123456_0a1b2c3d", "log": "/.../.ginkgo/detached/20261028_101500_123456_0a1b2c3d.log", ...}
ginkgo runs wait 20261028_101500_123456_0a1b2c3d --timeout 100 --json
# exit 124: still running; run the same command again
# exit 0: succeeded; 1: failed or rejected; 3: failed only where the policy allows; 130: cancelled
```

`runs wait` prints the same payload as the `run_summary` event, or the run so
far at the timeout. `ginkgo runs status <run_id> --json` prints it once,
without waiting. Until the run is recorded, the payload's `status` is
`starting`. If the run never started, it is `rejected`, with the tail of the
run's log.

A run killed outright (`kill -9`, a lost machine) reads as `running` for good,
so always pass `--timeout`.

## Inspection Commands

These commands look at a workflow and its runs, without changing either:

- `ginkgo doctor --json` and `ginkgo run --dry-run` check a workflow before it
  runs.
- `ginkgo inspect workflow` shows the task graph.
- `ginkgo runs show <run_id> --json` shows a recorded run.
- `ginkgo cache explain <run_id> --json` shows why each task ran or came from
  the cache.
- `ginkgo debug <run_id>` shows task status, logs and failure detail.
- `ginkgo history <task-name> --json` shows every run of one task.
- `ginkgo why <path>` names the task and run that produced a file.
- `ginkgo query "<sql>" --json` runs read-only SQL over the provenance
  database. See [Querying Provenance](querying-provenance.md).
- `ginkgo export events <run_id>` prints a finished run's events, in the same
  shape as `--agent-output`.

## See Also

- [CLI](cli.md): every command.
- [Reports](reports.md): rendering a run as HTML.
