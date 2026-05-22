# Why Ginkgo

Ginkgo was built around two ideas about scientific computing.

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

## See Also

- [Quickstart](getting-started/quickstart.md) — run a workflow in a few minutes.
- [Core Concepts](guide/concepts.md) — the deferred-expression model behind
  dynamic expansion.
