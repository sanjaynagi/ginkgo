---
title: "Agent Tasks"
---

An agent task runs a coding agent, such as Codex or Claude Code, on a brief. It
needs the `ginkgo-agent` plugin (`pip install "ginkgo-workflow[agent]"`). Ginkgo
calls the agent's own command-line tool, which signs in with its own login.
Ginkgo itself holds no API key and makes no model calls.

```python
from ginkgo import file, folder, task
from ginkgo_agent import agent

@task("agent", resources={"agents": 1})
def interpret(packet: folder, brief: file, schema: file) -> folder:
    return agent(brief=brief, reads=[packet], output_schema=schema)
```

The task's value is the agent's workspace: a folder holding what it wrote. With
an `output_schema`, its final message is there too, as `final_message.json`.
Without one, it's `final_message.txt`.

## Choosing The Agent

`ginkgo.toml` names the agent, so the same workflow runs under either:

```toml
[agents]
runner = "codex"        # or "claude", or "stub"
model = "gpt-5-codex"   # optional; the tool's default otherwise
command = "codex"       # optional; the executable, by default the runner's name
                        # a relative path is relative to the project

[resources.budgets]
agents = 3              # at most three agents at once
```

The budget is a [custom resource](/ginkgo/guide/resources/), so agents are
limited apart from cores and memory. A task asks for one with
`resources={"agents": 1}`.

## What `agent()` Takes

| Argument | Meaning |
|---|---|
| `brief` | The prompt file. |
| `reads` | Paths the agent reads. They are listed in its prompt, and the task fails if one changes while it runs. |
| `output_schema` | A JSON Schema file. The task fails unless the final message is JSON that matches it. Codex accepts only OpenAI's strict schemas: every object sets `additionalProperties: false` and lists all its properties as `required`. |
| `network` | Whether the agent may use the network. Off by default. |
| `timeout_minutes` | Stop the agent and fail the task after this long. 60 by default; `None` for no limit. |

Pass each path in `reads` as a `file` or `folder` argument of the task too, so
a change to it re-runs the task. Ginkgo warns about a read that isn't in one.
A path passed as a plain `str` doesn't count: only its text is hashed.

## Each Attempt

Each attempt runs in a new directory in the run directory, so a retry never
overwrites an earlier one:

```
.ginkgo/runs/<run>/agents/task_0007_attempt_1/
    prompt.md            the brief, with the workspace and reads appended
    transcript.jsonl     the agent's event stream
    final_message.txt    its final message
    workspace/           its working directory, and the task's value
```

The attempt fails on a timeout, a non-zero exit, a final message that doesn't
match the schema, or a change to a path in `reads`. `retry_on=AgentTaskError`
retries any of these.

Ginkgo records each attempt's runner, model, tool version, token usage and
time against the task, as `agent_*` fields of its `extra` column, which
`ginkgo query` can read. A cache hit carries the fields of the attempt it
reuses, so add up usage over attempts, not over cache hits.

## Caching

The cache key holds the task's arguments, the brief, the schema, the runner,
the model and the tool's version. A cache hit means this exact attempt already
ran, and costs no model call. Changing any of them runs the agent again, and
so does `ginkgo run --rerun <task>`.

Set `model`. Without it the tool picks its own default, from its own
configuration, which Ginkgo can't see: changing that default would not run
the agent again.

## What The Agent Can See

Codex runs in its `workspace-write` sandbox: it can write only in its workspace,
and has no network unless `network=True`. Claude Code may edit files, but
not its reads, and with `network=False` its web tools are refused. Each read's
directory is added so Claude Code can open it, so for a read file, the files
beside it can be edited. Whatever the runner, the task fails if a read changes.

Neither stops the agent from reading other files the user running Ginkgo can
read, such as credentials in your home directory. Run agents as a user that
can't read anything secret.

## Testing A Workflow Without A Model

`runner = "stub"` runs `command` as a script instead of an agent. It reads the
prompt on stdin and finds its paths in these variables:

| Variable | Holds |
|---|---|
| `GINKGO_AGENT_WORKSPACE` | the workspace, also its working directory |
| `GINKGO_AGENT_FINAL_MESSAGE` | where to write the final message |
| `GINKGO_AGENT_READS` | the reads, one per line |
| `GINKGO_AGENT_OUTPUT_SCHEMA` | the schema, or empty |
| `GINKGO_AGENT_MODEL` | the model, or empty |
| `GINKGO_AGENT_NETWORK` | `1` or `0` |

What it prints to stdout is kept as the transcript. A line of JSON with a
`usage` object counts towards the attempt's token usage.
