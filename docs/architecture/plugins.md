# Plugins

Ginkgo's optional integrations — remote executors, object stores, FUSE
drivers, secret backends, run notifications — are contributed through one
plugin registry, `ginkgo/plugins/`. Core looks an implementation up by the
key config or user code selects it with, and never names a concrete class.

## Contract

`ginkgo/plugins/api.py` defines a `Plugin` — a name and a tuple of
contributions — and one frozen spec dataclass per extension point. Each spec is
registered under its `keys`, and its `build` takes a single argument: a frozen
context object wherever a point needs several inputs, so a field can be added
without breaking plugins.

| Spec | Keys | `build` |
|---|---|---|
| `ExecutorType` | the `type` of a `[remote.executors.<name>]` table | `build(settings) -> RemoteExecutor`; also carries `label` and `required_settings` |
| `ObjectStoreType` | URI scheme | `build(ObjectStoreContext) -> ObjectStore` |
| `MountDriverType` | URI scheme | `build() -> MountDriver` |
| `SecretBackendType` | the `backend` of `secret(name, backend=...)` | `build(SecretBackendContext) -> tuple[SecretBackend, ...]`, tried in order |
| `EventSubscriberType` | a name, e.g. `slack` | `build(EventSubscriberContext) -> EventSubscriber \| None`; `None` means disabled by config |
| `TaskKind` | the `kind` of `@task(kind=...)` | `runner(RunnerContext) -> TaskRunner`; also carries the kind's `directive` type, `call_form`, `execution_mode` (`driver` or `worker`) and capabilities `allows_env`, `remote_capable`, `eager_source_hash`, `exit_code_retries`. `directive`, `runner` and `call_form` are set together or not at all |
| `EnvironmentType` | every scheme in `schemes` (`docker`, `oci`), with `""` for a bare name | `build(EnvironmentContext) -> ExecutionEnvironment`; also carries a `name` and a `label` for run tables and provenance |

The set of extension points is fixed: adding one is a change to core, so each
dispatch site stays typed and greppable. There is no generic hook mechanism.
Each spec declares a `point_name` used in messages.

The API is provisional until 1.0. First-party plugins pin the exact core
version.

## Discovery

`PluginRegistry.discover` loads core's own plugin (`ginkgo/plugins/builtin.py`)
first, directly rather than through an entry point, so it never depends on
package metadata being installed. It then loads every entry point in the
`ginkgo.plugins` group, sorted by name; each must name a module attribute that
is a `Plugin`. `plugin_registry()` discovers once per process, on first use.

A plugin is registered in its distribution's `pyproject.toml`:

```toml
[project.entry-points."ginkgo.plugins"]
k8s = "ginkgo_k8s:PLUGIN"
```

An entry point that fails to import, or is not a `Plugin`, is skipped and
recorded as a `PluginLoadFailure` rather than raised: one broken third-party
plugin must not stop core or the other plugins from working, on a laptop or in
a worker pod. The failure is reported where it matters — `ginkgo plugins`
lists it and exits 1, and a lookup that finds nothing names it.

The other errors are explicit:

- `PluginConflictError` — two plugins contribute the same key to one extension
  point, or two task kinds return the same directive type; the message names
  both.
- `PluginNotFoundError` (a `LookupError`, not a `ValueError`, so code catching
  bad values never swallows it) — nothing provides the key asked for. The
  message lists what is available, names the distribution to install when a
  first-party plugin provides the key (`_FIRST_PARTY_PROVIDERS` in
  `registry.py`: `k8s` → ginkgo-k8s, `batch` → ginkgo-gcp), and lists any
  plugin that failed to load.

`use_plugin_registry(registry)` swaps the process's registry for a block, for
tests and embedders that need a known set of plugins.

## Dispatch sites

- **Executors.** `ExecutorRegistry` (`runtime/executor_registry.py`) reads
  `[remote.executors.<name>]` tables only, and looks each table's `type` up
  while parsing config, so an uninstalled type fails before anything runs
  (`[remote.executors.<name>]: No installed plugin provides executor type
  'k8s'. ...`). It checks `required_settings` before the run starts, labels
  run headers with `label`, and builds lazily through `build`. `ginkgo doctor`
  probes the same tables. Core contributes no executor type.
- **Object stores.** `remote.resolve.resolve_backend(scheme)` looks the scheme
  up and passes an `ObjectStoreContext` of the merged runtime config and any
  region override; each store reads its own provider settings.
- **FUSE drivers.** `remote.access.drivers.base.resolve_driver(scheme)`. A
  missing driver raises `DriverUnavailableError`, which callers already treat
  as a reason to fall back to staging.
- **Secrets.** `SecretResolver` builds a backend's chain from its plugin the
  first time a ref names it, so a run resolving only `env` secrets never builds
  (or authenticates) another backend, and a ref naming an uninstalled backend
  raises `PluginNotFoundError` rather than reading as a missing secret.
- **Environments.** `EnvironmentRouter` (`runtime/backend.py`) routes an
  `env=` value by scheme and builds each environment type on first use, one
  instance per type `name` shared by all its schemes. The run table labels a
  bare name with the bare-name environment's `label` (`pixi:analysis`), and
  provenance records the same label as `backend`.
- **Task kinds.** `@task` only checks that `kind` is a non-empty string, so
  decorating a task never loads plugins; the evaluator looks the kind up when
  it registers the task's node, before anything runs, and refuses an unknown
  kind or `retry_on_exit_codes` on a kind without `exit_code_retries`. A task
  must return its own kind's directive, which runs on its own kind's runner, so
  capabilities are always checked against what actually runs; returning another
  kind's directive is an error naming the kind to use. Runners are built on first use from
  a `RunnerContext` of evaluator services (backend, validator, the shared
  `ShellRunner`, cache store, run directory, `annotate`, `notice`). A runner's
  `replay_cached` re-applies anything it stored on `node.runner_extras`, which
  the evaluator saves in the cache entry's `extra` as `runner_extras`.
  `runtime/task_kinds.task_kind(task_def)` is the one lookup every caller uses
  (dispatch, validation, dry run, remote-input staging, `inspect`).
- **Event subscribers.** `ginkgo run` builds each registered subscriber and
  attaches it to the store recorder's `on_committed` hook, not the bus, so it
  only sees events already in the ledger.

`ginkgo plugins [--json]` lists the loaded plugins, each contribution's keys
(the empty bare-name key shows as `(bare name)`), and any plugin that failed to
load, exiting 1 if one did.

## First-party plugins

The Kubernetes and GCP Batch executors are not in core. They live in separate
distributions in this repository, under `plugins/`:

| Distribution | Package | Entry point | Contributes | Depends on |
|---|---|---|---|---|
| `plugins/ginkgo-k8s` | `ginkgo_k8s` | `k8s = "ginkgo_k8s:PLUGIN"` | `ExecutorType` `k8s` (label `Kubernetes`, requires `image`) | `kubernetes` |
| `plugins/ginkgo-gcp` | `ginkgo_gcp` | `gcp = "ginkgo_gcp:PLUGIN"` | `ExecutorType` `batch` (label `GCP Batch`, requires `project`, `image`) | `google-cloud-batch`, `google-cloud-logging` |

Each pins the exact core version (`ginkgo==0.1.0`). They are not on PyPI: core's
`all` extra installs both through git direct references at the same ref as
core.

An executor plugin implements `RemoteExecutor` / `RemoteJobHandle`
(`runtime/remote_executor.py`) and runs tasks by starting
`python -m ginkgo.remote.worker` in a ginkgo worker image. The public helpers in
`remote/executor_support.py` — `encode_payload`, `generate_job_name`,
`payload_requires_fuse`, `parse_worker_output` — speak that worker protocol, so
a plugin never reimplements the payload or result format. Worker images install
`ginkgo[cloud]` only: a worker runs tasks and needs no executor plugin.

### Writing one

1. Build a `Plugin` in a module of your distribution — a name and a tuple of
   contributions, each a spec from `ginkgo.plugins.api`:

   ```python
   from ginkgo.plugins.api import ExecutorType, Plugin

   PLUGIN = Plugin(
       name="ginkgo-k8s",
       contributions=(
           ExecutorType(
               name="k8s",
               label="Kubernetes",
               required_settings=("image",),
               build=build_kubernetes_executor,
           ),
       ),
   )
   ```

2. Register it under the `ginkgo.plugins` entry-point group in the
   distribution's `pyproject.toml` (see [Discovery](#discovery)).
3. Pin the exact core version you were built against.

`build` receives the executor table's settings (without `type` and `code`) and
is called only on first dispatch, so importing the plugin stays cheap.

### Testing

The pixi `default` environment installs both plugins editable (feature
`plugins`); `pixi run test` runs `pytest tests/ plugins/`. Plugin tests live in
`plugins/<name>/tests`.

Core's own tests must pass with no plugin installed. The `core` environment has
none, and `pixi run -e core test-core` runs `tests/` there; CI runs it as a
separate `core-without-plugins` job. A core test that configures `k8s` or
`batch` executors uses the `stub_executor_types` fixture (`tests/conftest.py`),
which swaps in a registry of core's plugin plus stub executor types that mirror
the real labels and required settings and refuse to build.

## Not yet extension points

CLI commands and value codecs are still wired directly. URI parsing for `remote_file` /
`remote_folder` (`core/remote.py`) still knows the `s3`, `oci` and `gs`
schemes by name.

Two parts of the task-kind contract are still loose, and are to be tightened
before a task kind moves out of core: a runner receives the evaluator's own
`NodeRun` (typed `Any`), and `ShellRunner` is both the shared subprocess
service in `RunnerContext` and the `shell` kind's runner.

## Failure classification

Failure categories and doctor diagnostics are declared on the exception
classes that need them, not by core naming those classes: `failure_kind:
ClassVar[FailureKind]` is read by `classify_failure`, and `diagnostic_code` /
`diagnostic_suggestion` by `ginkgo doctor` (for example `EnvNotAllowedError`
declares `INVALID_ENV_KIND`). `FailureKind` (`errors.py`) is a `Literal` of
the categories reports know; a declared value outside it is ignored. An
exception a plugin raises classifies itself the same way.
