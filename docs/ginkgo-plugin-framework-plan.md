# Ginkgo Plugin Framework — Exploration / Plan

## Problem

All cloud and infrastructure integrations are hard-wired into the `ginkgo`
package: Kubernetes and GCP Batch executors, the S3/GCS/OCI object-store
backends, the gcsfuse/mountpoint-s3/rclone FUSE drivers, and Slack
notifications. Consequences:

- **Dependency weight.** `kubernetes`, `google-cloud-batch`, `gcsfs`,
  `ocifs`, `s3fs` are pulled in (or gated behind an ad-hoc `cloud` extra)
  even for users who run purely locally or on one provider.
- **Closed extension surface.** Adding AWS Batch, Slurm, Azure, or a lab's
  in-house scheduler means a PR against the main repo. Snakemake and
  Nextflow both grew large ecosystems precisely because executors and
  storage live outside the core.
- **Release coupling.** A fix to the OCI rclone driver requires a full
  ginkgo release.

## Prior art

- **Snakemake** discovers plugins via Python entry points. Naming
  convention `snakemake-executor-plugin-*` / `snakemake-storage-plugin-*`;
  each plugin depends on a small, semver'd *interface package*
  (`snakemake-interface-executor-plugins`) rather than on snakemake
  itself. Plugins declare a settings dataclass that snakemake maps to
  CLI/config options automatically.
- **Nextflow** uses pf4j (JVM) plugins with a central registry; config
  `plugins { id 'nf-amazon@2.0.0' }` triggers download at run start. A
  set of *core plugins* (`nf-amazon`, `nf-google`, `nf-azure`, `nf-wave`)
  ships with defined default versions and auto-installs on first use.

The Snakemake model maps directly onto Python packaging and is the one to
copy: entry points for discovery, a thin stable interface, a settings
contract, and a naming convention. The Nextflow idea worth stealing is the
explicit *core plugin set* with versions managed by the main project.

## Existing seams in ginkgo

The protocols already exist; only construction/dispatch is hard-wired:

| Extension point | Protocol (today) | Hard-wired dispatch (today) |
|---|---|---|
| Remote executor | `RemoteExecutor` / `RemoteJobHandle` (`runtime/remote_executor.py`) | `cli/commands/run.py` — `if executor == "k8s": ... elif executor == "batch": ...` |
| Object storage | `ObjectStore` (`remote/backend.py`) | `remote/resolve.py::resolve_backend` — `if scheme == "s3": ...` |
| FUSE mount driver | `MountDriver` (`remote/access/drivers/base.py`) | `resolve_driver` scheme map |
| Notifications | `notifications/slack.py` | direct import |
| Env backends | `ExecutionEnvironment` (`runtime/backend.py`) — pixi, container | direct construction |

## Alternatives considered

Two independent axes: discovery mechanism and contract shape.

**Discovery.** Entry points (Snakemake, pytest, Airflow) vs explicit
config declaration (Sphinx, mkdocs) vs naming-convention scan (old
Flask), namespace packages (Airflow 1.x), runtime registry fetch
(Nextflow, Terraform), and out-of-process RPC plugins (HashiCorp
go-plugin). Naming scans and namespace packages are strictly worse
entry points; runtime fetch conflicts with pixi-lockfile
reproducibility; out-of-process plugins buy language-agnosticism and
dependency isolation ginkgo doesn't need yet (the remote worker and
FUSE subprocesses are already process boundaries) at the cost of an RPC
protocol — revisit only if a plugin ever has irreconcilable Python
dependency conflicts. Entry points win, with one correction borrowed
from the Sphinx model: an optional `[plugins] enabled = [...]`
allowlist in `ginkgo.toml` restricts loading to named plugins when
present (determinism / supply-chain hygiene); absent means
auto-discover.

**Contract shape.** Typed protocol registries (one per kind) vs a hook
framework (pluggy: many plugins respond to lifecycle hookpoints) vs a
Sphinx-style untyped extension object. Registries are the right shape
for driver-style backends where exactly one implementation serves a
request (executor, store, mount driver). Hooks are the right shape for
observers where many plugins react to one event (notifications,
metrics, provenance sinks) — and ginkgo already has an internal
`EventBus`, so the observer surface is EventBus subscription, not a
driver registry. Hybrid: registries for backends, event subscription
for observers. The untyped extension object ages badly; discarded.

## Proposed design

### 1. Discovery: entry points, one group per kind

```toml
# in a plugin's pyproject.toml
[project.entry-points."ginkgo.executor"]
k8s = "ginkgo_k8s:KubernetesExecutorPlugin"

[project.entry-points."ginkgo.object_store"]
gs = "ginkgo_gcp:GCSStorePlugin"

[project.entry-points."ginkgo.mount_driver"]
gs = "ginkgo_gcp:GcsfuseDriverPlugin"

[project.entry-points."ginkgo.observer"]
slack = "ginkgo_slack:SlackObserverPlugin"

[project.entry-points."ginkgo.secret_resolver"]
gcp-sm = "ginkgo_gcp:SecretManagerResolverPlugin"
```

Backend kinds (`executor`, `object_store`, `mount_driver`,
`secret_resolver`) are registry lookups: one implementation serves a
request, keyed by name or scheme. **Observers** are different: they
subscribe to the existing runtime `EventBus` and many may react to the
same event (Slack today; metrics exporters, provenance sinks, report
post-processors later). An observer plugin's `build(settings)` returns
an event subscriber, replacing the hard-wired Slack integration.

A new `ginkgo/plugins/` package owns:

- `registry.py` — enumerate entry points per group lazily
  (`importlib.metadata.entry_points`), load on first use, cache. Loading
  a plugin must import only the plugin's registration module, not its
  heavy dependencies; heavy imports happen inside the factory call.
- `interface.py` — the plugin-facing contracts. Re-exports the existing
  protocols (`RemoteExecutor`, `ObjectStore`, `MountDriver`) plus a small
  `Plugin` base: `name`, `kind`, `settings_cls`, `build(settings)`,
  `doctor_probes()`. This module carries an explicit
  `PLUGIN_API_VERSION` and a semver compatibility check at load time.
- `errors.py` — `PluginNotFoundError` with an install hint
  ("executor 'slurm' not found; try `pip install ginkgo-slurm`").

Keys are namespaced by kind, so `executor = "k8s"` in `ginkgo.toml` and
`s3://` URIs resolve through the registry instead of if/elif chains.

### 2. Settings contract

Each plugin declares a `@dataclass(kw_only=True)` settings class. Core
maps the plugin's config namespace onto it:

```toml
[remote.k8s]            # existing namespace, unchanged for users
namespace = "ginkgo"
fuse_privileged = false
```

Core reads `[remote.<plugin-name>]`, constructs `settings_cls(**table)`,
and calls `plugin.build(settings=...)`. Unknown keys error with the
plugin name attached. Existing config files keep working verbatim.

### 3. Core plugins: monorepo, separate distributions

Keep core plugins in the ginkgo repo (one CI, atomic interface changes)
but as separate installable distributions:

```text
ginkgo/                      # core: DSL, evaluator, caching, provenance,
                             #   local exec, staged access, CLI, plugin registry
plugins/
  ginkgo-k8s/                # KubernetesExecutor (+ fuse pod annotations)
  ginkgo-gcp/                # GCPBatchExecutor, GCS store, gcsfuse driver
  ginkgo-aws/                # S3 store, mountpoint-s3 driver
  ginkgo-oci/                # OCI store, rclone driver
  ginkgo-slack/              # Slack notifier
```

Grouping is **by provider, not by kind** — a user targeting GCP installs
one package and gets executor + store + fuse driver together, mirroring
Nextflow's `nf-google`. `pip install ginkgo` stays lean;
`ginkgo[cloud]` (or `ginkgo[gcp]`, `ginkgo[aws]`, …) becomes a
meta-extra depending on the core plugin distributions, so the "installed
by default" experience is an extras choice, not a hard dependency.

Staged access, the local process-pool path, and the `RemoteExecutor` /
`ObjectStore` / `MountDriver` protocols stay in core. FUSE *coordination*
(`MountedAccess`, hydration, policy resolver) stays in core; only the
per-provider drivers move out.

### 4. Worker-side symmetry (the hard part)

Remote workers hydrate fuse markers and download from object stores, so
**the plugin must be importable inside the worker image/environment**,
not just on the driver host. Design consequences:

- The task payload records which plugins (name + version) the driver used
  for refs in that payload. `run_task` verifies availability before
  hydration and fails with a clear message naming the missing
  distribution.
- Worker images for core plugins: `Dockerfile.worker*` installs the
  relevant plugin distributions. Third-party executor plugins document
  their own image requirements.
- `ginkgo doctor` gains a plugin section: installed plugins + versions,
  interface-version compatibility, and each plugin's own contributed
  probes (replacing the hard-wired driver probes in `access/doctor.py`).

### 5. CLI

- `ginkgo plugins list` — kind, key, distribution, version, source
  (core / third-party), load status.
- Errors on unknown executor/scheme names suggest an install command.

### 6. Conformance test kit

`ginkgo.plugins.testing` ships reusable interface test suites (an
executor suite, a store suite, a driver suite) that plugin authors run
in their own CI against their implementation. This — more than the
registry — is what made Snakemake's ecosystem reliable, and it doubles
as the version-skew early-warning system. First-class deliverable, not
an afterthought.

## Candidate extension points beyond this phase

Surfaces that fit the same framework later, roughly in order of pull:

- **Secret resolvers** (`runtime/environment/secrets.py`) — Vault, GCP
  Secret Manager, AWS SM. Cheap to include in phase 1 scope if desired.
- **Remote artifact / CAS backends** — currently rides on `ObjectStore`;
  may need its own kind if non-object-store CAS appears.
- **CLI subcommands** — plugins adding `ginkgo <cmd>`; low priority.
- **Env backends** (pixi/container) and **task kinds** — deferred, below.

## Explicitly out of scope (this phase)

- **Task kinds as plugins** (notebook/script/shell) — deeply entwined with
  the scheduler's driver-task lifecycle; revisit later.
- **Env backends as plugins** (pixi/container) — same reason.
- **Remote plugin fetching at run start** (Nextflow-style auto-install) —
  Python environments are user-managed; auto-`pip install` at run time is
  surprising and breaks reproducibility. An install *hint* is enough.
- **A separate interface distribution** (`ginkgo-plugin-interface`).
  Start with `ginkgo.plugins.interface` inside core + `PLUGIN_API_VERSION`;
  split out only if third-party authors need to avoid depending on ginkgo.

## Risks / tradeoffs

- **Version skew.** A plugin built against an older interface must fail
  loudly at load, not misbehave at runtime. Mitigation: interface version
  check + a small compatibility test suite plugins can run in their CI
  (`ginkgo.plugins.testing`).
- **Cache identity.** Ref access policy is already excluded from cache
  identity; moving backends into plugins must not change hashing inputs.
  Plugin name/version must never enter cache keys.
- **Import cost.** Entry-point *enumeration* is cheap; plugin *import* is
  not (kubernetes ~0.5 s+). Registry must stay lazy — nothing imports
  until config/URIs actually demand that plugin.
- **Repo mechanics.** Multiple distributions in one repo complicates
  versioning/release. Mitigation: lockstep versions for core plugins
  (release together with ginkgo), hatch workspaces or per-dir builds.
- **Churn for contributors.** Open PRs touching `remote/` will conflict
  with the move. Sequence the extraction after the current PR queue
  drains.

## Phasing

1. **Registry + interfaces (in-core).** Add `ginkgo/plugins/`; convert the
   three dispatch sites (executor if/elif, `resolve_backend`,
   `resolve_driver`) to registry lookups; register the existing built-ins
   via ginkgo's own entry points in `pyproject.toml`. Pure refactor — no
   packaging change, behaviour identical, proves the discovery mechanism.
2. **Settings + doctor + CLI.** Settings dataclasses per plugin,
   plugin-contributed doctor probes, `ginkgo plugins list`, payload
   plugin-requirements check in `run_task`.
3. **Extraction.** Move provider code to `plugins/` distributions;
   `cloud` extra becomes a meta-extra; worker Dockerfiles install plugin
   dists; CI matrix covers core-only and core+plugins installs.
4. **Third-party authoring kit.** `ginkgo-plugin-template` repo
   (scaffold, CI, compatibility tests), authoring docs page, naming
   convention `ginkgo-<provider>`.

## Success criteria

- `pip install ginkgo` has no cloud SDK dependencies; a purely local
  workflow runs with nothing else installed.
- Existing users see no change: `ginkgo[cloud]` still installs everything,
  `executor = "k8s"` and `[remote.*]` config keep working verbatim, and
  all existing tests pass against the extracted plugins.
- A third-party executor plugin can be authored, installed from PyPI, and
  selected via `executor = "<name>"` with zero edits to the ginkgo repo.
- A missing plugin produces an actionable error naming the distribution,
  on both the driver and the worker side.
- `ginkgo plugins list` and `ginkgo doctor` report plugin state.
