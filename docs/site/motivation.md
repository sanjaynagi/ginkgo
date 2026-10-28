# Why Ginkgo

Ginkgo is built on these founding principles:

1. **Scientific workflows belong in Python.**
2. **A workflow's graph should be able to grow as it runs.**
3. **Workflows will be written and run by AI agents as well as by people.**

## Scientific Workflows Belong In Python

Data scientists and bioinformaticians already write their analyses in Python.
Many workflow tools ask them to describe the workflow again in a second
language, such as YAML or a custom syntax. The analysis then lives in two
languages, and the two can drift apart.

In Ginkgo, a workflow is plain Python functions marked with `@flow` and
`@task()`. It sits in the same language as the analysis itself.

## A Workflow's Graph Should Be Able To Grow As It Runs

Real analyses rarely know their full shape before they start. Take a filtering
task that decides how many samples pass a quality check. The next task needs to
run once for each sample that passed, and nobody knows that number until the
filter has run.

In Ginkgo, a task can look at its inputs and return new tasks. Ginkgo adds them
to the graph and runs them. The workflow grows to match the data.
[Core Concepts](guide/concepts.md#dynamic-expansion-a-task-can-add-tasks) shows
an example.

## Workflows Will Be Written And Run By AI Agents As Well As By People

More and more analysis is written, run and debugged by AI coding agents working
alongside scientists. A workflow tool should be as easy for an agent to read and
operate as it is for a person.

The first two principles already help. Agents write plain Python well. A graph
that can grow lets an agent write what the data needs, without working around
the tool.

An agent also needs to see what happened in a run. A person can watch a live
table in the terminal, but an agent works best from structured facts. So Ginkgo
reports runs as JSON. It also records every run in the
[provenance](guide/concepts.md#glossary) database, which an agent can query. An
agent can then answer "which task made this file?" or "why did this task run
again?" from the record, rather than from a guess.

[Working With Coding Agents](guide/coding-agents.md) lists the commands that
put this into practice.

## See Also

- [Quickstart](getting-started/quickstart.md): run your first workflow.
- [Core Concepts](guide/concepts.md): how task calls build a graph, and how a
  task adds to it.
- [Working With Coding Agents](guide/coding-agents.md): skills, agent run mode
  and the inspection commands.
