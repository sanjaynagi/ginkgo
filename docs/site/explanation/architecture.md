# Architecture Overview

This page describes the parts of Ginkgo's architecture that affect how
workflows behave. It is not an internal roadmap.

## Package Shape

The current source tree is organized around three major layers:

- `ginkgo/core/` for the user-facing DSL
- `ginkgo/runtime/` for evaluation, scheduling, caching, provenance, and value transport
- `ginkgo/cli/` and `ginkgo/ui/` for operator-facing surfaces

## Execution Boundary

Ginkgo builds the graph locally, schedules locally, and records provenance
locally.

Foreign environments are used only for executable shell payloads and notebook
execution. This keeps orchestration behavior deterministic and easy to inspect.

## Runtime Flow

A run moves through five stages:

1. **Discovery.** The CLI locates the flow entrypoint &mdash; an explicit path,
   or the canonical `workflow.py` when run from a project root.
2. **Graph construction.** The flow body executes as ordinary Python, but each
   task call returns a deferred expression rather than a result. The flow
   assembles those expressions into an expression tree.
3. **Registration.** The evaluator walks the tree, registering every task and
   the dependency edges between them. This is where fan-out from `.map()` is
   expanded into concrete task instances.
4. **Dispatch.** Tasks whose inputs are all resolved become *ready*. The
   scheduler dispatches ready tasks subject to the `--jobs`, `--cores`, and
   `--memory` budgets, resolving each task's inputs from upstream results.
5. **Recording.** Completed results are written to the cache, and task status,
   timing, logs, and provenance are recorded into the run directory.

Because the graph is built before anything executes, Ginkgo can validate
wiring, compute cache keys, and report a dry-run plan without running a single
task body.

## Cache, Artifacts, And Provenance

Three ideas are worth separating:

- cache identity decides whether prior work can be reused
- artifacts store the durable bytes for file and folder outputs
- provenance records what happened in a specific run

Separating the three keeps cache reuse independent of any single run's
provenance record.

## See Also

- [Core Concepts](../guide/concepts.md) &mdash; the authoring model these
  layers support.
- [Caching and Provenance](../guide/caching-and-provenance.md) &mdash; cache
  identity, artifact storage, and run directories in depth.
