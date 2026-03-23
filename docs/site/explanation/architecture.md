# Architecture Overview

This page explains just enough architecture to help end users reason about
workflow behavior. It is not an internal roadmap.

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

At a high level:

1. the CLI discovers a flow
2. the flow builds an expression tree
3. the evaluator registers tasks and dependencies
4. ready tasks are dispatched subject to resource limits
5. results are cached and recorded into the run directory

## Cache, Artifacts, And Provenance

Three ideas are worth separating:

- cache identity decides whether prior work can be reused
- artifacts store the durable bytes for file and folder outputs
- provenance records what happened in a specific run

That split is why reruns remain understandable instead of collapsing into a
single opaque state directory.
