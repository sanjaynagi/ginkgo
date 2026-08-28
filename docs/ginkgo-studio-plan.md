# Ginkgo Studio — product and implementation plan

## Status

Proposed plan for discussion. This is a phase-specific planning document and
must not be committed. The intended implementation lives in a separate
`ginkgo-studio` repository, with small compatibility improvements made in the
`ginkgo` repository where the control-plane contract requires them.

## Problem

Ginkgo is currently operated one project and one terminal at a time. Its CLI,
runtime event stream, run provenance, asset store, notebook outputs, cache
metadata, diagnostics, and static reports expose most of the facts needed for
a graphical control plane, but there is no machine-level service that can:

- register and monitor multiple Ginkgo projects;
- configure, validate, and launch workflows;
- supervise concurrent runs and recover their state after restart;
- present live task state and logs without terminal parsing;
- search and compare run history across projects; or
- browse scientific assets and their previews in one responsive interface.

The current asset model also has an important boundary: an asset becomes known
when a task returns an `AssetResult`. Ginkgo can catalogue materialised asset
versions and their observed lineage, but it cannot reliably show an asset that
has never been materialised, calculate general staleness, or launch a run for a
selected asset. Those capabilities require additions to Ginkgo's programming
and execution models, not only UI work.

## Proposed solution

Build **Ginkgo Studio** as a separate, local-first product containing:

1. a machine-local Python service for project registration, run supervision,
   indexing, preview generation, and the browser API; and
2. a React and TypeScript web application for operating projects, runs, logs,
   assets, environments, caches, notebooks, and diagnostics.

Keep Ginkgo as the headless workflow engine. Studio invokes the copy of Ginkgo
configured for each project through a stable, versioned JSON/JSONL control
protocol. It must not import workflow modules or run user code in the Studio
service process.

The first release is local-only and single-user. Multi-user hosting,
authentication, permissions, and a shared remote control plane are explicit
non-goals until the local product is mature.

## Product principles

- **Ginkgo remains authoritative.** Project `.ginkgo/` state, artifacts, and
  provenance are the scientific record. Studio's database is a disposable
  read index and can be rebuilt.
- **The CLI remains complete.** Studio enhances operation but is never required
  to run, inspect, debug, or recover a workflow.
- **User code is isolated.** Every inspection and run occurs in a subprocess
  using the project's own environment and working directory.
- **Capabilities are honest.** Studio distinguishes declared, previously
  observed, materialised, stale, unknown, and unsupported states rather than
  inferring certainty.
- **Progressive disclosure.** Common operation is fast and calm; provenance,
  logs, resources, and raw metadata remain available in contextual inspectors.
- **Performance is contractual.** Pagination, incremental indexing,
  virtualization, bounded previews, and reconnectable event streams are part
  of the architecture rather than later optimisations.
- **Local does not mean unsafe.** Path access, subprocess invocation, HTML
  previews, secrets, state-changing requests, and network binding all have
  explicit security boundaries.

## Scope and prioritisation

Effort is relative and includes backend, frontend, tests, and documentation:

- **S** — a few focused days after foundations exist;
- **M** — roughly one to two engineer-weeks;
- **L** — several engineer-weeks or a cross-repository change; and
- **XL** — a new product/runtime capability that should have its own plan.

### Feature decision matrix

| Capability | Value | Effort | Recommendation | Why |
| --- | --- | --- | --- | --- |
| Explicit project registration | High | S | First release | Establishes the machine-level workspace without unsafe filesystem scanning. |
| Project and workflow health | High | S | First release | Existing discovery, inspection, doctor, and secret validation provide most inputs. |
| Completed run history | High | S | First release | Manifests and `RunSummary` already provide a strong snapshot. |
| Completed run detail and graph | High | S–M | First release | Existing structured task dependencies, timings, failures, notebooks, and assets are sufficient. |
| Configure and launch runs | High | M | First release | Requires a run supervisor, config workspace, and stable launch acknowledgement. |
| Dry-run plan and cache forecast | High | S–M | First release | The plan already exists; Ginkgo needs a stable structured form. |
| Live task state | High | M | First release | Typed runtime events exist; Studio needs reconnect, replay, and projection logic. |
| Live stdout/stderr and structured events | High | M | First release | Existing agent-verbose output is usable; global cursors and virtualised rendering are needed. |
| Graceful cancellation | High | M–L | First release if contract work lands | Needs explicit cancellation status and reliable local/remote process cleanup. |
| Materialised asset catalog | High | M | First release | Existing asset keys, versions, metadata, checks, and lineage are directly useful. |
| Table, figure, text, array, model previews | High | M–L | First release, bounded | Much preview logic exists in reports but must become reusable and safe. |
| Notebook browsing | Medium | S | First release | Existing rendered notebook paths can be exposed in a sandboxed viewer or opened externally. |
| Static report access/export | Medium | S | Easy win | Reuse the existing report command and surface completed bundles. |
| Cache and storage visibility | Medium | S–M | Early follow-up | Existing cache listing and prune preview make a useful operational page. |
| Environment and reproducibility view | High | M | Early follow-up | Environment locks, config, executor, Git identity, and rerun command are high-value scientific context. |
| Global search | High | M | Early follow-up | Valuable once the SQLite projection covers runs, tasks, assets, and failures. |
| Run comparison | High | M | Early follow-up | Parameters, timings, cache outcomes, failures, and asset metrics are already comparable. |
| Saved launch presets | Medium | S | Early follow-up | Studio-owned state with little runtime impact. |
| Desktop notifications | Medium | S | Optional easy win | Useful local polish; browser permission and platform behaviour need care. |
| Experiment groups and metric comparison | High for DS/ML | M–L | Later, after run comparison | Requires an explicit grouping model and stable metric extraction. |
| Parameter sweeps | High for experiments | L | Optional later | Introduces run generation, queueing, concurrency, and partial failure semantics. |
| Declarative pre-materialisation asset catalog | High | XL | Separate Ginkgo phase | Requires `AssetSpec` or equivalent definitions in the programming model. |
| Asset staleness/freshness | High | XL | Defer | Correctness depends on declared assets, code/config/input identity, and dynamic graph semantics. |
| Materialise selected assets | High | XL | Defer | Requires asset-to-task mapping, subgraph selection, and dynamic fan-out rules. |
| Asset partition/backfill views | Situational | XL | Do not plan yet | Ginkgo has no partition model; avoid copying Dagster features without a Ginkgo use case. |
| Schedules and sensors | High for production | XL | Separate daemon/runtime phase | Requires durable ticking, missed-run policy, leases, retries, and lifecycle management. |
| Multi-user remote hosting | Potentially high | XL | Explicit non-goal | Changes security, storage, identity, deployment, audit, and concurrency assumptions. |
| Collaborative annotations/ownership | Low initially | L | Defer | Adds governance state before the single-user operating model is proven. |

### Easy wins to preserve

Once the read-only service and project adapter exist, several features offer
substantial value without changing Ginkgo's execution model:

- registered-project and workflow health;
- completed run history, task graphs, failures, notebooks, and reports;
- dry-run/cache/resource-plan visualisation from a structured command;
- materialised asset metadata, checks, versions, and observed lineage;
- saved launch presets and exact command previews;
- cache/storage summaries and prune previews;
- desktop completion/failure notifications; and
- initial run comparison over parameters, timings, failures, and metrics.

These should not be delayed behind a perfect live graph, declarative assets,
scheduling, or a general experiment-management system.

### Difficult features to isolate

The following are not ordinary UI increments and should receive independent
go/no-go decisions:

- restart-safe run supervision and cancellation across local and remote work;
- bounded previews for arbitrary or potentially hostile scientific artifacts;
- million-line logs and very large dynamically expanding graphs;
- pre-materialisation asset definitions and definition/result reconciliation;
- correct asset freshness and asset-selected partial execution;
- parameter sweeps with durable queue and concurrency semantics;
- schedules, sensors, partitions, and backfills; and
- remote multi-user deployment, authentication, permissions, and audit.

The first three are justified by the core Studio experience. The remainder can
be omitted indefinitely without compromising a high-quality local control
plane.

## User experience and information architecture

### Visual thesis

Ginkgo Studio should feel like a precise scientific instrument: calm,
information-dense, tactile, and trustworthy. Use warm neutral surfaces,
graphite text, and one restrained Ginkgo-green accent for primary action and
live state. Prefer typography, alignment, and dividers over dashboard-card
mosaics. Monospace is reserved for logs, identifiers, paths, and numeric
measurements.

### Application frame

- A narrow left rail switches between the machine overview and registered
  projects.
- The centre is the primary operational workspace: run table, graph, timeline,
  log stream, or asset browser.
- A right inspector opens for task, run, asset, version, or diagnostic detail
  without losing the user's place.
- Global command/search access supports keyboard-first navigation.
- Dense views use persistent filters and URL-addressable selections.

### Intentional interaction

- Task nodes transition between queued, preparing, running, retrying, cached,
  succeeded, failed, and cancelled states without moving unrelated content.
- The contextual inspector uses a shared-layout transition between graph,
  table, timeline, and log selections.
- Live logs follow appended output only while the user is at the end; reading
  history shows a clear "Jump to latest" control.
- All motion is fast, functional, and disabled or reduced under
  `prefers-reduced-motion`.

### Primary surfaces

#### Machine overview

- registered, missing, incompatible, and unhealthy projects;
- active and queued runs across projects;
- recent failures and retries;
- aggregate local CPU/memory pressure when available;
- cache and artifact storage by project; and
- service/index health and last refresh.

This is an operational list and timeline, not a grid of summary cards.

#### Project overview

- discovered workflows and validation status;
- recent and active runs;
- known/materialised assets;
- environment and secret readiness without secret values;
- Git branch/revision/dirty state;
- cache and artifact footprint; and
- project invocation command and detected Ginkgo capabilities.

#### Workflow and launch

- static graph and task inspector;
- task kind, environment, resources, retry policy, execution mode, and declared
  outputs where available;
- TOML/YAML config editor with syntax validation;
- config file/preset selection and difference from project defaults;
- executor, jobs, cores, memory, trust-workspace, and profile controls;
- secret availability checks without retrieval;
- dry-run dependency waves, cache forecast, and peak resource forecast;
- exact command preview; and
- launch acknowledgement with immediate navigation to the run.

A schema-driven form builder is deliberately deferred until Ginkgo has an
explicit configuration schema. The first release uses a good structured text
editor rather than guessing types from arbitrary config files.

#### Runs

- server-paginated history filtered by project, workflow, status, time,
  executor, tags/group, and free text;
- active-run state, elapsed time, task counts, resource use, and cancellation;
- static and dynamically expanded task graph;
- task timeline/Gantt with staging, execution, retries, and terminal state;
- structured event stream and raw stdout/stderr views;
- task inputs, outputs, cache key/outcome, environment, logs, failure category,
  remote job identity, and produced assets;
- failure summaries and log tails;
- rerun with previous config; and
- links to related reruns or experiment groups.

#### Asset catalog

For the first release, the catalog contains materialised assets and explicitly
declared outputs only. It provides:

- asset key, kind, group, caption, latest version, producing task/run, checks,
  and last materialisation time;
- search and filters by project, workflow, kind, group, check result, and age;
- version history and aliases;
- observed upstream and downstream lineage;
- metadata-only summaries that do not load artifact bytes;
- bounded previews loaded on demand;
- version comparison for schema, dimensions, text, model metrics, and metadata;
  and
- safe artifact download/open actions.

Previously observed outputs may be labelled "observed in prior runs" but must
not be presented as declared assets. Assets that have never existed require
the optional declarative-assets phase described below.

#### Scientific previews

- **Tables:** schema, row/column counts, bounded server-side row samples,
  column selection, sort/filter over the sample, and explicit truncation.
- **Figures:** thumbnail/gallery mode for images and sandboxed iframes for
  interactive HTML figures.
- **Text:** capped plain text, Markdown, and JSON with search and copy actions.
- **Arrays:** shape, dtype, chunks, coordinates, and small bounded slices;
  never load an entire large array for a preview.
- **Models:** framework, byte size, metrics, version comparison, and associated
  metadata; never deserialize arbitrary model code in the Studio process.
- **Files:** metadata and download/open actions with conservative text/image
  detection; unknown files are not executed or embedded.
- **Notebooks:** sandboxed rendered HTML where available, plus paths to source
  and executed notebooks.

#### Operations and reproducibility

- environment manifests and captured locks;
- Ginkgo, Python, executor, and remote backend identity;
- Git revision and dirty status at launch time once recorded;
- resolved non-secret parameters and exact rerun command;
- cache entries, hit/miss explanation, prune preview, and deliberate cleanup;
- artifact sizes and orphan/lifecycle diagnostics;
- `doctor`, structured failure categories, and secret availability; and
- remote job IDs, staging/FUSE statistics, and links where a provider URL can
  be generated safely.

#### Comparison and experiments

Run comparison should precede a full experiment manager. It compares two or
more runs across:

- config and parameters;
- source/Git and environment identity;
- task additions/removals and state;
- duration, resource, staging, and cache differences;
- failures and retries; and
- asset versions, schemas, dimensions, checks, and model metrics.

Experiment groups later add a Studio-owned label and optional parameter axes.
They must not rewrite Ginkgo provenance. Parameter sweeps are a separate
feature built on launch requests and a durable queue, not a client-side loop.

## Repository and component architecture

### Repository ownership

```text
ginkgo/
  Headless execution, provenance, assets, public protocol and schemas

ginkgo-studio/
  server/ginkgo_studio/    Machine service and browser API
  web/                     React and TypeScript application
  schemas/                 Generated/pinned Ginkgo protocol artefacts
  tests/contract/          Cross-version protocol fixtures
  tests/integration/       Real subprocess and recovery tests
  tests/browser/           Playwright user journeys
```

Studio should use Pixi for the development toolchain, Python environment, and
Node runtime, with pnpm for frontend package resolution and locking. The
initial stack should remain small:

- FastAPI plus Uvicorn for the local service;
- standard-library SQLite with explicit repository queries and WAL mode;
- React, TypeScript, and Vite;
- TanStack Query for browser server-state;
- TanStack Virtual for long tables and streaming logs;
- an accessible headless primitive library for dialogs, menus, tabs, and
  popovers rather than a visually prescriptive component framework; and
- a graph-library spike before choosing between React Flow, Cytoscape, or a
  smaller custom SVG/canvas layer.

The service and web application are one deployable product. A separate Node
server, SSR framework, GraphQL layer, ORM, message broker, or plugin framework
is not justified for the local first release.

### Service components

1. **Project registry** — records explicit project roots, labels, invocation
   commands, capability handshakes, and health. It never scans a home directory
   recursively by default.
2. **Ginkgo adapter** — invokes the configured project command, negotiates the
   protocol version, validates JSON, and translates supported versions into
   Studio's internal models.
3. **Run supervisor** — creates launch requests, starts process groups, captures
   acknowledgement, tracks stdout/stderr/events, requests cancellation, and
   records terminal state.
4. **Indexer** — incrementally projects project provenance and asset metadata
   into SQLite and can rebuild from source after database loss.
5. **Event projector** — turns ordered Ginkgo events into current run/task
   state and publishes reconnectable Server-Sent Events to browsers.
6. **Preview service** — produces bounded, cacheable, content-type-specific
   previews without executing user artifacts.
7. **Browser API** — provides paginated query endpoints and explicit command
   endpoints with validation, idempotency, and local security controls.

### Data flow

```text
Browser --HTTP command--> Studio run supervisor
                              |
                              +--project cwd--> project Ginkgo subprocess
                                                    |
Browser <--SSE events---- Studio event projector <--+--JSONL events
                              |
Browser <--HTTP queries-- SQLite read index <--------+--manifest/assets/logs
```

Studio invokes the project's own executable, for example:

- `pixi run ginkgo`;
- `uv run ginkgo`;
- `.venv/bin/ginkgo`; or
- an explicit user-configured command.

Automatic detection may suggest a command, but registration must show and
persist the resolved command. Shell strings are not accepted; commands are
stored and executed as argument arrays.

### Source of truth and identity

- A project receives a Studio UUID; its canonical path is mutable metadata,
  not its primary key.
- Run identity is `(project_id, run_id)` because run IDs are project-local.
- Asset identity is `(project_id, namespace, name)`.
- Ginkgo files remain authoritative for runs and assets.
- Studio owns only project registration, launch requests, saved presets,
  experiment grouping, UI preferences, and its rebuildable projections.
- Index records carry source path, modification identity, and ingestion cursor
  so reconciliation is deterministic.

### SQLite projection

The initial schema should cover:

- `projects`, `project_capabilities`, and `workflows`;
- `launch_requests` and `run_relationships`;
- `runs`, `tasks`, and compact searchable event metadata;
- `assets`, `asset_versions`, and `asset_lineage`;
- `launch_presets` and later `experiment_groups`; and
- `index_cursors` and migration metadata.

Raw logs and artifact bytes do not belong in SQLite. Keep paths and offsets,
serve paginated chunks from their authoritative files, and index only bounded
search metadata where the value is clear.

### Browser API shape

Prefer a small versioned REST API plus SSE:

```text
GET    /api/v1/projects
POST   /api/v1/projects
GET    /api/v1/projects/{project_id}
POST   /api/v1/projects/{project_id}/refresh
GET    /api/v1/projects/{project_id}/workflows
GET    /api/v1/workflows/{workflow_id}/plan
POST   /api/v1/workflows/{workflow_id}/launch
GET    /api/v1/runs
GET    /api/v1/runs/{project_id}/{run_id}
POST   /api/v1/runs/{project_id}/{run_id}/cancel
GET    /api/v1/runs/{project_id}/{run_id}/events
GET    /api/v1/runs/{project_id}/{run_id}/stream
GET    /api/v1/tasks/{project_id}/{run_id}/{task_id}/logs
GET    /api/v1/assets
GET    /api/v1/assets/{project_id}/{namespace}/{name}
GET    /api/v1/assets/{...}/versions/{version_id}/preview
```

State-changing requests use an idempotency key. List endpoints use cursor
pagination and stable sorting. API DTOs are Studio contracts and do not expose
raw internal dataclasses or arbitrary manifest mappings.

## Ginkgo compatibility work

### Required control protocol

Add a public protocol version independent of the Ginkgo package version. A
handshake such as `ginkgo info --json` should return:

- Ginkgo and protocol versions;
- project root, workflow candidates, and state roots;
- supported commands/event types; and
- capability flags such as `structured_plan`, `cancelled_status`,
  `asset_preview`, and `declarative_assets`.

Within one protocol major version, changes are additive. Unknown event fields
and event types are tolerated. Breaking semantic changes increment the
protocol major version.

### Required first-release changes in Ginkgo

1. **Structured project handshake.** Add project identity, paths, executable
   version, protocol version, and capability reporting.
2. **Stable workflow inspection schema.** Formalise the current workflow graph
   output and include task resources, retries, environment, execution mode,
   and declared outputs where known.
3. **Structured dry-run plan.** Expose dependency waves, cache status, and
   resource summary without parsing Rich output.
4. **Launch acknowledgement.** Accept an externally supplied run ID or emit a
   guaranteed first acknowledgement containing the final run ID before lengthy
   imports and validation. An explicit run ID is preferable for idempotency.
5. **Globally ordered events.** Add a monotonic sequence/cursor for replay and
   deduplication. Per-task log sequence alone is insufficient for a merged
   reconnectable stream.
6. **Cancellation semantics.** Record `cancelled` distinctly at run and task
   level, emit terminal cancellation events, and specify graceful/forced
   signal behaviour for local and remote work.
7. **Stable run and asset JSON.** Formalise run summary, failure, asset list,
   asset version, lineage, and bounded-preview schemas.
8. **Reproducibility metadata.** Record Ginkgo/Python version, executor,
   invocation, Git revision/dirty state where available, and relevant
   environment identity without secrets.
9. **Published fixtures.** Check in representative protocol fixtures for
   success, failure, cache hit, retry, dynamic expansion, cancellation,
   semantic assets, notebooks, and remote execution.

These should be general headless-operability improvements. Ginkgo must not
gain React-specific fields or Studio-specific storage assumptions.

### Optional declarative-assets phase

Introduce an immutable `AssetSpec` or equivalent attached to a task definition,
separate from the runtime `AssetResult`. A useful definition may include:

- stable key and kind;
- description, group, tags, and optional owner/contact metadata;
- producing task/output position;
- declared upstream assets when statically known; and
- preview/materialisation hints that do not affect identity.

The phase must define:

- validation between declared specifications and returned assets;
- dynamically named and mapped outputs;
- asset selection to producing subgraphs;
- staleness across source, config, external inputs, and upstream versions;
- cache interaction and partial execution;
- missing, failed, skipped, and never-materialised states; and
- compatibility for tasks that continue to return undeclared assets.

Do not bundle this work into the first Studio release. The materialised catalog
is useful without it, and rushing the model would create misleading lineage
and scheduling semantics.

## Live execution and recovery

### Run lifecycle

Studio-owned launch requests move through:

```text
created -> starting -> acknowledged -> running -> terminal
                    \-> start_failed
```

Ginkgo-owned terminal status is one of `succeeded`, `failed`, or `cancelled`.
Studio may additionally report `lost` when the service cannot find a process
or terminal provenance after restart; `lost` must not overwrite Ginkgo files.

### Restart recovery

On service startup Studio should:

1. load non-terminal launch requests;
2. inspect recorded process identity without assuming a reused PID is the same
   process;
3. reconcile each request with its run directory and event cursor;
4. resume file ingestion for living external processes;
5. finalise from terminal provenance where available; and
6. mark an irreconcilable request `lost` with a diagnostic and recovery action.

The first release does not need to re-parent or regain signal control of every
orphaned platform process. It does need to recover read visibility correctly
and avoid launching a duplicate run after an ambiguous acknowledgement.

## Performance and responsiveness

### Design rules

- Never rescan every manifest on a list request.
- Watch or periodically reconcile registered project state, then incrementally
  ingest changed files.
- Use event cursors rather than replacing a full run snapshot per update.
- Paginate and filter in SQLite before serialisation.
- Route-split graph, editor, and rich-preview dependencies.
- Virtualise run, event, task, asset, and log collections.
- Fetch previews only when visible or selected.
- Put explicit row, byte, line, dimension, and execution-time caps on previews.
- Summarise or cluster very large DAGs before rendering every node.
- Move indexing and preview work off the request/event loop.

### Initial performance budgets

Measured on a representative modern laptop after a warm service start:

- machine/project/run list API: p95 below 200 ms with 25 projects and 10,000
  indexed runs;
- visible live event latency: p95 below 250 ms from Studio receipt to browser
  presentation;
- main application route interactive within 1.5 seconds on localhost;
- smooth log interaction for at least one million persisted lines without
  creating a DOM node or retaining a parsed object for every line;
- ordinary graph interaction remains responsive around 1,000 visible nodes;
  larger graphs automatically enter a summarised/filtered mode; and
- no preview endpoint reads an unbounded artifact into memory.

Budgets should be tested with generated fixtures in CI where deterministic and
profiled manually on release candidates.

## Security model

- Bind to loopback only by default. Remote binding is unsupported in the first
  release and requires an explicit future threat model.
- Use a per-install local token or equivalent origin-bound protection, strict
  origin checking, and CSRF protection for commands.
- Store subprocess commands as argument arrays; never interpolate them through
  a shell.
- Canonicalise and allow-list every project and artifact path before access.
- Do not expose environment values, resolved secret values, `.env` contents,
  or arbitrary process environments.
- Sandbox notebook and interactive-figure HTML in iframes with a restrictive
  Content Security Policy. Do not grant same-origin privileges unnecessarily.
- Do not deserialize models or import arbitrary Python merely to preview an
  asset.
- Apply content-type, range, and size controls to downloads and previews.
- Require confirmation for cancellation and destructive cache/artifact
  actions; show the resolved project and estimated impact.
- Record Studio command actions locally for diagnosis, without recording
  secrets or full sensitive config values.

## Delivery phases

Phases 0–4 are roughly 13–21 focused engineer-weeks when delivered
sequentially, excluding pauses for design decisions and release feedback. This
is a sizing aid rather than a deadline: cross-platform process recovery,
remote cancellation, preview hardening, and visual polish are the largest
sources of variance. Two engineers can overlap Ginkgo protocol work with
Studio read-only/frontend work after Phase 0, but the protocol remains the
integration critical path.

### Phase 0 — product and protocol alignment

**Effort:** S–M

Deliverables:

- agree first-release scope and explicit non-goals;
- define visual thesis, information architecture, and low-fidelity flows;
- define protocol schemas, capability negotiation, and compatibility policy;
- create representative Ginkgo JSON/JSONL fixtures;
- spike project command invocation, event ingestion, SSE, and one large log;
- choose the graph library using a 1,000-node representative graph; and
- create both repository plans and success criteria before implementation.

Success criteria:

- one throwaway Studio process launches a fixture workflow through the proposed
  protocol and streams its events to a minimal browser page;
- no Ginkgo internals are imported by Studio; and
- hard features are not prerequisites for the vertical slice.

### Phase 1 — Ginkgo headless control contract

**Effort:** M

Ginkgo deliverables:

- project/protocol handshake;
- stable workflow and dry-run JSON;
- deterministic launch acknowledgement/external run ID;
- globally ordered events;
- cancellation status and signal semantics;
- stable run/asset JSON schemas; and
- contract fixtures and compatibility tests.

Success criteria:

- a standalone test harness can inspect, launch, follow, cancel, reconnect to,
  and inspect a run using only documented subprocess contracts.

### Phase 2 — Studio read-only foundation

**Effort:** M–L

Studio deliverables:

- repository/tooling, packaged frontend, local service, migrations, and API;
- explicit project registration and invocation configuration;
- capability/health checks;
- rebuildable SQLite indexing;
- machine and project views;
- completed run history, run detail, static graph, tasks, failures, assets, and
  notebooks;
- static report links; and
- initial accessibility and visual-token system.

Success criteria:

- register at least two projects using different invocation commands;
- rebuild the database entirely from project state;
- browse 10,000 generated runs within performance budgets; and
- deleting Studio state loses no Ginkgo run or artifact data.

### Phase 3 — launch and live control

**Effort:** L

Deliverables:

- config editor, options, secret readiness, command preview, and dry-run plan;
- launch request lifecycle and idempotency;
- live graph, timeline, structured events, and stdout/stderr;
- reconnect and replay;
- graceful cancellation with forced fallback;
- restart reconciliation and lost-run diagnostics; and
- rerun with previous configuration.

Success criteria:

- launch from either registered project and reach the run page immediately;
- live state agrees with final provenance;
- disconnect/reconnect introduces no duplicate or missing visible events;
- Studio restart recovers visibility of an active run; and
- cancellation cleans up local workers and remote handles according to the
  Ginkgo contract.

This phase completes the minimum compelling product.

### Phase 4 — scientific asset experience

**Effort:** M–L

Deliverables:

- materialised asset catalog and filters;
- version history, aliases, checks, and observed lineage;
- safe bounded previews for tables, figures, text, arrays, models, files, and
  notebooks;
- artifact downloads/opening; and
- asset version comparison.

Success criteria:

- previews never execute model/user code or read an unbounded artifact;
- every truncation/cap is visible;
- lineage and producing-run links are correct for fixtures; and
- a malformed or hostile preview fails in isolation without destabilising the
  service.

### Phase 5 — operations, comparison, and polish

**Effort:** M–L, selectable by feature

Candidate deliverables:

- global search;
- run comparison;
- reproducibility and Git/environment panels;
- cache/storage management and prune preview;
- saved launch presets;
- project diagnostics and remote-execution detail;
- desktop notifications;
- experiment grouping; and
- responsive/mobile viewing, keyboard workflows, theming, and further visual
  polish.

Treat these as individually selectable increments. Run comparison and
reproducibility are recommended; desktop notifications and experiment groups
can be omitted without weakening the core product.

### Optional Phase 6 — declarative assets

**Effort:** XL; separate Ginkgo feature plan required

Deliverables may include:

- `AssetSpec` definitions;
- never-materialised assets in Studio;
- definition/materialisation reconciliation;
- asset-level selection and execution;
- correct stale/fresh/unknown semantics; and
- impact-aware lineage views.

Proceed only after materialised asset workflows demonstrate concrete demand.

### Optional Phase 7 — durable automation

**Effort:** XL; separate control-plane architecture required

Schedules, sensors, backfills, and sweep queues require a durable always-on
daemon with leases, clock/missed-tick semantics, retry policy, concurrency,
upgrade recovery, and operational alerts. Do not grow the foreground local
service into this role implicitly.

## Testing strategy

### Ginkgo

- schema tests for every public command and event;
- golden protocol fixtures with additive-compatibility checks;
- cancellation tests for Python, shell, notebook, remote, retry, and dynamic
  work;
- replay and sequence invariants; and
- explicit tests preventing secret leakage into protocol payloads.

### Studio service

- unit tests for projections, cursors, pagination, path validation, launch
  transitions, and capability adapters;
- migration and full-rebuild tests;
- integration tests using real fixture projects and subprocesses;
- crash/restart tests at pre-acknowledgement, running, cancelling, and terminal
  boundaries;
- hostile/malformed event, manifest, log, and artifact tests; and
- performance tests over generated run/log/asset corpora.

### Frontend

- component tests for stateful controls and renderers;
- Playwright journeys for registration, inspection, launch, reconnect,
  cancellation, failure diagnosis, and asset browsing;
- accessibility checks for keyboard use, focus, semantics, contrast, status
  not conveyed by colour alone, and reduced motion;
- screenshot regression for a small set of high-value surfaces; and
- browser profiling for initial load, large logs, live updates, and graphs.

### Cross-repository compatibility

- Studio CI against the oldest and newest supported Ginkgo protocol fixtures;
- periodic integration against released Ginkgo packages;
- clear unsupported/incompatible UI states; and
- read-only degradation where safe instead of an all-or-nothing version gate.

## Risks and tradeoffs

### Cross-repository version drift

**Risk:** Studio relies on undocumented Ginkgo details or requires exact
release lockstep.

**Mitigation:** versioned protocol, capability negotiation, fixtures, additive
schema policy, and CI across a support window.

### Project environment diversity

**Risk:** projects use different Pixi/uv/venv installations or broken commands.

**Mitigation:** explicit argument-array invocation per project, handshake
diagnostics, no Studio-process imports, and clear health/remediation output.

### Filesystem indexing and partial writes

**Risk:** Studio observes changing event/manifest files, missed watcher events,
or removed projects.

**Mitigation:** append cursors, atomic/terminal snapshots, periodic
reconciliation, tolerant projections, and a rebuildable index.

### Run supervision after restart

**Risk:** detached processes, reused PIDs, or lost launch acknowledgement lead
to duplicate or incorrectly terminal runs.

**Mitigation:** explicit run/request IDs, process identity beyond PID where
possible, provenance reconciliation, idempotency keys, and a visible `lost`
state rather than guessing.

### Large graphs and logs

**Risk:** a visually rich page becomes unusable for bioinformatics fan-out.

**Mitigation:** virtualised logs, chunk APIs, incremental graph projection,
grouping, filtering, viewport rendering, and explicit performance fixtures.

### Unsafe scientific artifacts

**Risk:** notebook/HTML/model/file previews execute code, escape paths, or
consume unbounded resources.

**Mitigation:** sandboxing, allow-listed paths, content caps, no model
deserialisation, strict MIME handling, and isolated preview failure.

### Premature Dagster parity

**Risk:** schedules, sensors, partitions, asset execution, governance, and
multi-user hosting expand the project before the core local workflow is good.

**Mitigation:** treat Dagster as a functional reference, not a feature
checklist. Ship the Ginkgo-specific scientific control plane first.

## First-release success criteria

Ginkgo Studio v1 is successful when a user can:

1. install and start Studio locally without changing an existing project;
2. explicitly register multiple projects and see compatibility/health;
3. inspect workflows and their task graphs;
4. edit/select configuration, validate it, and inspect a dry-run plan;
5. launch concurrent runs through each project's own Ginkgo environment;
6. watch task state, retries, cache outcomes, timeline, resources, and logs live;
7. disconnect or restart Studio and recover complete run visibility;
8. cancel a run with an explicit terminal state and correct cleanup;
9. browse completed runs, failures, notebooks, and materialised scientific
   assets with safe bounded previews;
10. search/filter ordinary operational collections without visible lag;
11. rebuild Studio's database without losing or changing Ginkgo state; and
12. continue performing all core operations through the Ginkgo CLI when Studio
    is not running.

Quality gates:

- documented and fixture-tested Ginkgo protocol;
- no known secret leakage or arbitrary artifact execution path;
- performance budgets met on representative corpora;
- keyboard-accessible primary journeys and reduced-motion support;
- supported-version compatibility matrix passing; and
- installation, upgrade, migration, backup/rebuild, and troubleshooting docs.

## Recommended minimum and stopping points

The recommended minimum product is Phases 0–4: project registration,
completed history, workflow inspection, configuration and launch, live runs
and logs, cancellation/recovery, and the materialised scientific asset
experience.

Phase 5 is modular. Prioritise global search, run comparison,
reproducibility, and storage visibility. Stop before experiment grouping if
those features do not have active users.

Do not commit to declarative assets, staleness, selected-asset execution,
schedules/sensors, partitions/backfills, or multi-user hosting as part of the
Studio v1 promise. Each is valuable, but each changes Ginkgo's product model
or operational responsibility enough to warrant its own evidence and plan.

## Decisions required before implementation

1. Confirm local-only, single-user scope for v1.
2. Confirm that a materialised/observed asset catalog is sufficient for v1.
3. Choose whether the initial process is foreground-only (`ginkgo-studio`) or
   also installs a background OS service; foreground-only is recommended.
4. Choose the supported Ginkgo protocol/version window.
5. Decide whether Ginkgo exposes bounded previews itself or publishes a public
   artifact-preview library used by Studio; a stable Ginkgo-owned preview API
   is recommended.
6. Decide whether v1 records Git identity and invocation metadata in Ginkgo
   provenance; recommended for reproducibility and comparison.
7. Approve the Phase 0 vertical-slice spike before either repository begins
   broad implementation.
