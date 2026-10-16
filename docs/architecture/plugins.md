# Plugins

Ginkgo's optional integrations — remote executors, object stores, FUSE
drivers, secret backends, run notifications — are contributed through one
plugin registry, `ginkgo/plugins/`. Core looks an implementation up by the
key config or user code selects it with, and never names a concrete class.

## Contract

`ginkgo/plugins/api.py` defines a `Plugin` — a name and a tuple of
contributions — and one frozen spec dataclass per extension point:

| Spec | Key | `build` |
|---|---|---|
| `ExecutorType` | the `type` of a `[remote.executors.<name>]` table | `build(settings) -> RemoteExecutor`; also carries `label` and `required_settings` |
| `ObjectStoreType` | URI scheme | `build(*, config, region) -> ObjectStore` |
| `MountDriverType` | URI scheme | `build() -> MountDriver` |
| `SecretBackendType` | the `backend` of `secret(name, backend=...)` | `build(*, project_root, config, environ) -> tuple[SecretBackend, ...]`, tried in order |
| `EventSubscriberType` | a name, e.g. `slack` | `build(EventSubscriberContext) -> EventSubscriber \| None`; `None` means disabled by config |

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

Errors are explicit and raised at discovery or lookup:

- `PluginLoadError` — an entry point failed to import, or is not a `Plugin`.
- `PluginConflictError` — two plugins contribute the same key to one extension
  point; the message names both.
- `PluginNotFoundError` (a `ValueError`) — nothing provides the key asked for;
  the message lists what is available.

## Dispatch sites

- **Executors.** `ExecutorRegistry` (`runtime/executor_registry.py`) accepts any
  registered `type`, reads the legacy `[remote.<type>]` section for each,
  checks `required_settings` before the run starts, labels run headers with
  `label`, and builds lazily through `build`. `ginkgo doctor` probes the same
  sections.
- **Object stores.** `remote.resolve.resolve_backend(scheme)` looks the scheme
  up and passes the merged runtime config; each store reads its own provider
  settings.
- **FUSE drivers.** `remote.access.drivers.base.resolve_driver(scheme)`. A
  missing driver raises `DriverUnavailableError`, which callers already treat
  as a reason to fall back to staging.
- **Secrets.** `build_secret_resolver` builds every registered backend chain,
  keyed by backend name.
- **Event subscribers.** `ginkgo run` builds each registered subscriber and
  attaches it to the store recorder's `on_committed` hook, not the bus, so it
  only sees events already in the ledger.

`ginkgo plugins [--json]` lists the loaded plugins and their contributions.

## Not yet extension points

Environment backends (Pixi, containers), task kinds, CLI commands and value
codecs are still wired directly. URI parsing for `remote_file` /
`remote_folder` (`core/remote.py`) still knows the `s3`, `oci` and `gs`
schemes by name.
