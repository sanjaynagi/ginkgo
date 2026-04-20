# Ginkgo Architecture

Ginkgo is a Python-based workflow orchestrator for scientific and data workflows. The current implementation is local-first and centered on a lazy expression-tree DSL, content-addressed caching, reproducible task environments via Pixi, and run provenance that is inspectable from both the CLI and a local web UI.

This directory is the internal architecture reference. It is split into topic files so agents and humans can read only the sections relevant to a given task rather than the full document. For end-user-facing documentation see `docs/site/`.

## Current Status

The repository currently implements:

- A lazy DSL built around `@flow`, `@task`, `Expr`, and `ExprList` for
  declarative workflow construction
- Concurrent local execution with dynamic DAG expansion, resource-aware
  scheduling, and explicit task kinds for Python, shell, notebook, and script
  work
- Content-addressed caching, artifact storage, and value transport for common
  Python values and path-based outputs
- Early cache completion for warm runs, including prepare-phase cache hits that
  avoid environment preparation for cached tasks and allow version-pinned
  remote inputs to skip staging on warm reruns
- Reproducible environment dispatch through Pixi for local shell execution and
  container-backed execution for shell tasks
- Provenance capture, logs, machine-readable runtime events, and structured
  inspection and diagnostics through the CLI, with append-only hot-path
  provenance updates in `events.jsonl` and a reconstructed/finalized
  `manifest.yaml`
- Remote task execution via Kubernetes (GKE, EKS, OKE) and GCP Batch, with
  per-task GPU/memory/CPU resource declarations, code-sync packaging, and
  full provenance integration
- Remote-input access policy with staged download or FUSE streaming
  (gcsfuse / mountpoint-s3 / rclone), per-ref / per-task / pattern / config
  resolution, cache-stable mode switching, and graceful fallback
- A local-first web UI for runs, cache inspection, graphs, notebook artifacts,
  embedded notebook viewing, and multi-workspace browsing
- A canonical package-oriented project layout with workflow autodiscovery and
  scaffolded project initialization
- An example-driven benchmark harness with generated benchmark inputs, checked-
  in baselines, and a separate CI lane for slowdown detection
- Selective retry policies with exponential backoff, size- and count-based
  cache eviction, task-level scheduling priority as a strict tiebreaker, and
  end-of-run failure classification that groups diagnostics by category
- Sub-workflow composition via `@task(kind="subworkflow")` returning a
  `subworkflow(path, params=..., config=...)` descriptor, running the
  child workflow as an opaque `ginkgo run` subprocess with child run-id
  stitched into the parent manifest
- Static HTML report export (`ginkgo report <run-id>`) that bundles run
  summary, parameters, task graph, task ledger, failure diagnostics,
  asset previews, and rendered notebooks into a self-contained document
  with bundled fonts, progressive-enhancement islands, and optional
  single-file mode

## Topic Map

Each topic file below is self-contained. Load only the pages relevant to your task.

- [Canonical Workflow Project Layout](project-layout.md) — expected structure of a user-authored workflow repository.
- [Package Layout](package-layout.md) — the `ginkgo/` source tree.
- [Execution Model](execution-model.md) — flow construction, dynamic DAG expansion, scheduling, remote references, worker-affine staging, execution backends.
- [Task Model](task-model.md) — Python, shell, notebook, and script task kinds; path-oriented special types.
- [Caching](caching.md) — cache keying, hashing, artifact store, and cache maintenance.
- [Assets](assets.md) — asset catalog, wrapped asset sentinels (`table`/`array`/`fig`/`text`/`model`), and live-payload rehydration.
- [Value Transport](value-transport.md) — codec layer for cross-process task inputs/outputs.
- [Configuration and Secrets](config-secrets.md) — secret references, resolvers, and redaction.
- [Provenance and Run State](provenance.md) — on-disk run layout and manifest contents.
- [CLI](cli.md) — available commands and capabilities.
- [Web UI](web-ui.md) — local UI server, multi-workspace, live updates.
- [Remote Execution](remote-execution.md) — `RemoteExecutor` protocol, Kubernetes and GCP Batch executors, remote worker, code sync, GCS backend, infrastructure scripts.
- [Remote Input Access](remote-input-access.md) — staged vs FUSE-mounted access strategies, per-input policy resolution, mount lifecycle, pod security, fallback semantics.
- [Agent Operability](agent-operability.md) — runtime event protocol, agent-mode output, structured inspection/diagnostics, Slack notifications.
- [Documentation Stack](documentation-stack.md) — Sphinx + MyST site under `docs/site/`.
- [Benchmarking](benchmarking.md) — benchmark harness and input provenance.
- [Reporting](reporting.md) — static HTML report export for completed runs.
- [Validation Workflows](validation.md) — canonical workflow families and example corpus.
- [Current Constraints](constraints.md) — active runtime boundaries and tradeoffs.
