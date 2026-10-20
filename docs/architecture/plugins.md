# Plugins

Ginkgo's optional integrations — remote executors, object stores, FUSE
drivers, secret backends, environments, task kinds, run notifications, extra
CLI commands — are contributed through one
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
| `ObjectStoreType` | URI scheme | `build(ObjectStoreContext) -> ObjectStore`; also carries `parse_uri(uri) -> RemoteLocation`, which splits the scheme's URIs into bucket and key (`core.remote.parse_bucket_key_uri` for `scheme://bucket/key` layouts) |
| `MountDriverType` | URI scheme | `build() -> MountDriver` |
| `SecretBackendType` | the `backend` of `secret(name, backend=...)` | `build(SecretBackendContext) -> tuple[SecretBackend, ...]`, tried in order |
| `EventSubscriberType` | a name, e.g. `slack` | `build(EventSubscriberContext) -> EventSubscriber \| None`; `None` means disabled by config |
| `TaskKind` | the `kind` of `@task(kind=...)` | `runner(RunnerContext) -> TaskRunner`; also carries the kind's `directive` type, `call_form`, `execution_mode` (`driver` or `worker`) and capabilities `allows_env`, `remote_capable`, `eager_source_hash`, `exit_code_retries`. `directive`, `runner` and `call_form` are set together or not at all |
| `CliCommand` | a `ginkgo <name>` subcommand | no `build`: `add_arguments(parser)` declares the command's arguments when the CLI builds its parser, and `run(args) -> int` executes it; also carries `help` |
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

An entry point that fails to import, is not a `Plugin`, or claims a key a
plugin loaded before it already holds, is skipped and recorded as a `PluginLoadFailure` rather than raised: one broken third-party
plugin must not stop core or the other plugins from working, on a laptop or in
a worker pod. The failure is reported where it matters — `ginkgo plugins`
lists it and exits 1, and a lookup that finds nothing names it.

The other errors are explicit:

- `PluginConflictError` — two plugins contribute the same key to one extension
  point, or two task kinds return the same directive type; the message names
  both. Discovery records it as the later plugin's load failure; constructing a
  `PluginRegistry` directly raises it.
- `PluginNotFoundError` (a `LookupError`, not a `ValueError`, so code catching
  bad values never swallows it) — nothing provides the key asked for. The
  message lists what is available, names the distribution to install when a
  first-party plugin provides the key (`_FIRST_PARTY_PROVIDERS` in
  `registry.py`: executor types `k8s` → ginkgo-k8s and `batch` → ginkgo-gcp,
  object stores and FUSE drivers `s3` → ginkgo-aws, `gs` → ginkgo-gcp and
  `oci` → ginkgo-oci, the bare-name environment and CLI command `env` →
  ginkgo-pixi, event subscriber `slack` → ginkgo-slack, CLI command `report` →
  ginkgo-report, task kind `notebook` → ginkgo-notebook), and lists any plugin that failed to load. The empty
  bare-name key reads as `(bare name)` in messages (`describe_key`).

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
- **Object stores.** `core.remote.parse_remote_uri(uri)` — behind
  `remote_file()`, `remote_folder()` and the configured `[remote]` stores —
  parses a URI with its scheme's `parse_uri`, so core knows no scheme by name.
  `is_remote_uri` counts a scheme an installed store serves, or one a
  first-party plugin would: `s3://` without ginkgo-aws is still a remote URI,
  and using it names the plugin to install instead of being read as a local
  path. `remote.resolve.resolve_backend(scheme)` builds the store from an
  `ObjectStoreContext` of the merged runtime config and any region override;
  each store reads its own provider settings, with the public
  `config_table`/`config_string`/`first_defined` helpers. The fsspec-based
  stores share `FsspecBackend` (`remote/fsspec_backends.py`).
- **FUSE drivers.** `remote.access.drivers.base.resolve_driver(scheme)`. A
  missing driver raises `DriverUnavailableError`, which callers already treat
  as a reason to fall back to staging. Every driver unmounts with
  `drivers.base.unmount_fuse`.
- **Secrets.** `SecretResolver` builds a backend's chain from its plugin the
  first time a ref names it, so a run resolving only `env` secrets never builds
  (or authenticates) another backend, and a ref naming an uninstalled backend
  raises `PluginNotFoundError` rather than reading as a missing secret.
- **Environments.** `EnvironmentRouter` (`runtime/backend.py`) routes an
  `env=` value by scheme and builds each environment type on first use, one
  instance per type `name` shared by all its schemes. The run table labels a
  bare name with the bare-name environment's `label` (`pixi:analysis`), and
  provenance records the same label as `backend`. Core contributes only the
  container environment (`docker://`, `oci://`); bare names come from
  ginkgo-pixi, so without it a bare `env=` fails at validation naming the
  plugin. A lock file an environment reports is copied into the run directory
  as `envs/<env>.<lockfile name>`. Core keeps `envs/manifest.py`
  (`find_pixi_manifest`) for the project's *own* environment: the interpreter
  check compares the running interpreter with the project's Pixi manifest, and
  `ginkgo init` scaffolds a Pixi project.
- **Task kinds.** `@task` only checks that `kind` is a non-empty string, so
  decorating a task never loads plugins; the evaluator looks the kind up when
  it registers the task's node, before anything runs, and refuses an unknown
  kind or `retry_on_exit_codes` on a kind without `exit_code_retries`. A task
  must return its own kind's directive, which runs on its own kind's runner, so
  capabilities are always checked against what actually runs; returning another
  kind's directive is an error naming the kind to use. Runners are built on first use from
  a `RunnerContext` of evaluator services (the validator, the shared
  `ShellRunner`, the run directory, `annotate`, and for sub-workflows the run
  id and database). A runner handles no cache hits: anything it puts in
  `node.runner_extras` while running, the evaluator saves in the cache entry's
  `extra` as `runner_extras` and records against the task again on a hit.
  `runtime/task_kinds.task_kind(task_def)` is the one lookup every caller uses
  (dispatch, validation, dry run, remote-input staging, `inspect`). Without
  ginkgo-notebook, a `@task("notebook")` fails at graph build naming the plugin
  to install.

  A runner may record a `rendered_html` annotation — a page for people to
  read — with `rendered_html_run_id`, the run that rendered it. Core knows
  nothing more about such pages: the run summary exposes both on
  `TaskSummary`, the end-of-run CLI lists them as "Rendered pages", and
  ginkgo-report links them. On a cache hit the replayed `runner_extras` point
  the annotation at the producing run's page, so `rendered_html_run_id` names
  that earlier run.
- **Event subscribers.** `ginkgo run` builds each registered subscriber and
  attaches it to the store recorder's `on_committed` hook, not the bus, so it
  only sees events already in the ledger. A `[notifications.<name>]` table
  that no installed subscriber is named for draws a warning built from the
  registry's not-found message, so `[notifications.slack]` without
  ginkgo-slack names the plugin to install instead of silently sending
  nothing.
- **CLI commands.** Core's own commands are wired directly in `cli/app.py`;
  they are the CLI, not optional integrations. `_build_parser` then adds a
  subparser for every `CliCommand`, ignoring with a warning one whose name a
  core command already has, and `main` dispatches any command core does not
  handle to its plugin's `run`. Building the parser therefore imports every
  plugin module on every invocation, so a plugin module should import
  cheaply; a failure building the parser is reported like any other command
  failure. A first-party command that is not installed (`ginkgo report`
  without ginkgo-report) is answered with the registry's not-found message —
  the plugin to install, or the plugin that failed to load — rather than
  argparse's "invalid choice". `run` executes at the project root: only the
  `workflow`, `out` and `config` arguments are resolved against the directory
  the user ran `ginkgo` from.

  Core's commands are not `CliCommand` contributions, so a clash with one is
  caught in `app.py` rather than by the registry, and `ginkgo plugins` does not
  show it. Moving core's commands onto `CliCommand` would put every clash on
  the registry's path and replace `main`'s if-chain; it is deferred.

`ginkgo plugins [--json]` lists the loaded plugins, each contribution's keys
(the empty bare-name key shows as `(bare name)`), and any plugin that failed to
load, exiting 1 if one did.

## First-party plugins

Remote executors, object stores, FUSE drivers, Pixi environments, notebook
tasks, Slack notifications and HTML reports are not in core. They live in separate distributions in this repository, under
`plugins/`:

| Distribution | Package | Entry point | Contributes | Depends on |
|---|---|---|---|---|
| `plugins/ginkgo-aws` | `ginkgo_aws` | `aws = "ginkgo_aws:PLUGIN"` | `ObjectStoreType` `s3`, `MountDriverType` `s3` (mountpoint-s3) | `s3fs` |
| `plugins/ginkgo-k8s` | `ginkgo_k8s` | `k8s = "ginkgo_k8s:PLUGIN"` | `ExecutorType` `k8s` (label `Kubernetes`, requires `image`) | `kubernetes` |
| `plugins/ginkgo-gcp` | `ginkgo_gcp` | `gcp = "ginkgo_gcp:PLUGIN"` | `ObjectStoreType` `gs`, `MountDriverType` `gs` (gcsfuse), `ExecutorType` `batch` (label `GCP Batch`, requires `project`, `image`) | `gcsfs`, `google-auth`, `google-cloud-batch`, `google-cloud-logging` |
| `plugins/ginkgo-oci` | `ginkgo_oci` | `oci = "ginkgo_oci:PLUGIN"` | `ObjectStoreType` `oci` (with its `oci://namespace/bucket/key` parser), `MountDriverType` `oci` (rclone) | `ocifs` |
| `plugins/ginkgo-notebook` | `ginkgo_notebook` | `notebook = "ginkgo_notebook:PLUGIN"` | `TaskKind` `notebook` (marimo notebooks; driver mode, allows `env=`, not remote-capable, eager source hash) | `marimo` |
| `plugins/ginkgo-pixi` | `ginkgo_pixi` | `pixi = "ginkgo_pixi:PLUGIN"` | `EnvironmentType` `pixi` (bare names, label `pixi`), `CliCommand` `env` | none (needs the `pixi` binary) |
| `plugins/ginkgo-report` | `ginkgo_report` | `report = "ginkgo_report:PLUGIN"` | `CliCommand` `report` | `jinja2` |
| `plugins/ginkgo-slack` | `ginkgo_slack` | `slack = "ginkgo_slack:PLUGIN"` | `EventSubscriberType` `slack` | none |

Each pins the exact core version (`ginkgo==0.1.0`). They are not on PyPI: core's
`all` extra installs every one through git direct references at the same ref as
core.

An executor plugin implements `RemoteExecutor` / `RemoteJobHandle`
(`runtime/remote_executor.py`) and runs tasks by starting
`python -m ginkgo.remote.worker` in a ginkgo worker image. The public helpers in
`remote/executor_support.py` — `encode_payload`, `generate_job_name`,
`payload_requires_fuse`, `parse_worker_output` — speak that worker protocol, so
a plugin never reimplements the payload or result format. Worker images install
core and the object-store plugins (ginkgo-aws, ginkgo-gcp, ginkgo-oci): a worker
stages and publishes but never submits, so it needs no executor plugin.

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

The pixi `default` environment installs every first-party plugin editable (feature
`plugins`); `pixi run test` runs `pytest tests/ plugins/`. Plugin tests live in
`plugins/<name>/tests`.

Core's own tests must pass with no plugin installed. The `core` environment has
none, and `pixi run -e core test-core` runs `tests/` there; CI runs it as a
separate `core-without-plugins` job. A core test that configures `k8s` or
`batch` executors uses the `stub_executor_types` fixture (`tests/conftest.py`),
which swaps in a registry of core's plugin plus stub executor types that mirror
the real labels and required settings and refuse to build. Core's tests use
`s3://` and `gs://` URIs as examples of any object store: an autouse fixture,
`stub_object_stores`, gives every test core's plugin plus stores for those two
schemes that parse but refuse to build, so core's tests behave the same whether
or not the store plugins are installed. The real stores are tested in their
plugins.

## Not yet extension points

Value codecs are still wired directly.

Two parts of the task-kind contract are still loose: a runner receives the
evaluator's own `NodeRun` (typed `Any`), and `ShellRunner` is both the shared
subprocess service in `RunnerContext` and the `shell` kind's runner.
ginkgo-notebook is the first task kind outside core, and it relies on these
`NodeRun` attributes: `task_def`, `node_id`, `resolved_args`,
`execution_args` and `runner_extras`. Changing them breaks it.

## Failure classification

Failure categories and doctor diagnostics are declared on the exception
classes that need them, not by core naming those classes: `failure_kind:
ClassVar[FailureKind]` is read by `classify_failure`, and `diagnostic_code` /
`diagnostic_suggestion` by `ginkgo doctor` (for example `EnvNotAllowedError`
declares `INVALID_ENV_KIND`). `FailureKind` (`errors.py`) is a `Literal` of
the categories reports know; a declared value outside it is ignored. An
exception a plugin raises classifies itself the same way.
