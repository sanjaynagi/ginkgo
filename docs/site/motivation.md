# Why Ginkgo

Ginkgo was built on three founding principles:

1. **Scientific workflows belong in Python.**
2. **An orchestrator should handle a changing graph natively.**
3. **Workflows will be written and run by AI agents as well as by people.**

## Scientific Workflows Belong In Python

Data scientists and bioinformaticians already work in Python. They should not
have to drop into a separate YAML or DSL layer to make an analysis reproducible
— a workflow should be plain functions with `@flow` and `@task()`, in the same
language as the analysis itself.

## An Orchestrator Should Handle A Changing Graph Natively

Real analyses rarely know their full shape up front: the number of samples,
which branches to take, the follow-up work — all depend on intermediate
results.

Ginkgo handles dynamic DAG expansion directly. A task can inspect its resolved
inputs and return new expressions, and the runtime folds them into the running
graph, so the workflow grows as the data comes in.

## Built For The Era Of Agentic AI

More and more analysis is written, run and debugged by AI coding agents working
alongside scientists. A workflow tool for that era has to be as easy for an
agent to read and operate as it is for a person.

The first two principles already help: agents write plain Python well, and a
graph that can change shape lets an agent express what the data calls for
without working around the tool. Ginkgo builds on that:

- `ginkgo init` writes a `skills/` directory and an `AGENTS.md`, so an agent
  starts with the project's conventions instead of rediscovering them.
- `ginkgo run --agent-output` streams the run as JSON events, and most
  inspection commands take `--json`, so an agent reads results rather than
  scraping a terminal UI.
- Every run is recorded. An agent can ask which task made a file
  (`ginkgo why`), why a step ran again (`ginkgo cache explain`), or query the
  record with SQL (`ginkgo query`), and answer from facts rather than guesses.
- Content-addressed caching makes iterating cheap: when an agent changes one
  step, only that step and what depends on it run again.

See [Working With Coding Agents](guide/coding-agents.md) for the details.

## See Also

- [Quickstart](getting-started/quickstart.md) — run a workflow in a few minutes.
- [Core Concepts](guide/concepts.md) — the deferred-expression model behind
  dynamic expansion.
- [Working With Coding Agents](guide/coding-agents.md) — skills, agent run mode
  and the inspection commands.
