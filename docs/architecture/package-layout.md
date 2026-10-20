# Package Layout

The source tree is organized around the user-facing DSL, the execution engine,
and environment backends. Top-level modules hold what a workflow author touches
directly, or what every layer needs: configuration, parameters, wildcards,
project and workspace location, the error taxonomy.

```text
ginkgo/
├── __init__.py           # lazy re-exports; the authoring surface
├── config.py             # TOML/YAML loading, layering, config sessions
├── params.py             # ginkgo.param declarations and resolution
├── wildcards.py          # expand / zip_expand / per_branch / slug
├── project.py            # project-root discovery (ginkgo.project_root)
├── workspace_layout.py   # the .ginkgo/ directory convention
├── errors.py             # error taxonomy and user-code failure location
├── formatting.py         # value formatters shared by read-only presenters
├── plugins/
│   ├── api.py            # Plugin and the extension-point specs
│   ├── registry.py       # entry-point discovery and lookup
│   └── builtin.py        # core's own contributions
├── core/
│   ├── directive.py      # ExecutionDirective base type
│   ├── expr.py
│   ├── flow.py
│   ├── hashing.py        # BLAKE3 content-hash helpers (dependency-free)
│   ├── optional.py       # optional() output declarations for driver tasks
│   ├── remote.py         # remote_file / remote_folder input references
│   ├── resources.py      # declarative per-task resource requirements
│   ├── script.py
│   ├── secret.py
│   ├── shell.py
│   ├── source_hash.py    # task-body hashing for cache keys
│   ├── subworkflow.py    # subworkflow() invocation primitive
│   ├── task.py
│   └── types.py          # file / folder / tmp_dir path-oriented types
├── runtime/
│   ├── backend.py        # ExecutionEnvironment protocol and EnvironmentRouter
│   ├── evaluator.py      # _ConcurrentEvaluator scheduler/lifecycle loop
│   ├── executors.py      # the evaluator's heterogeneous executor pool
│   ├── scheduler.py
│   ├── worker.py
│   ├── module_loader.py
│   ├── events.py
│   ├── log_drain.py      # worker log chunks to TaskLog events
│   ├── dry_run.py        # static execution-plan preview for --dry-run
│   ├── run_summary.py    # RunSummary, the one read model, loaded from the ledger
│   ├── rundir.py         # RunDir: a run's logs, env locks and manifest on disk;
│   │                     # also run ids and log tails
│   ├── store_recorder.py # bus subscriber: events -> ledger rows, and the manifest
│   ├── event_values.py   # rendering user values into a form an event can carry
│   ├── executor_registry.py    # named executors: config, lookup, lazy build
│   ├── remote_executor.py      # RemoteExecutor / RemoteJobHandle protocols
│   ├── remote_dispatch.py      # code bundles, job handles, polling
│   ├── remote_input_resolver.py  # RemoteStager for the evaluator
│   ├── diagnostics.py
│   ├── task_validation.py      # TaskValidator: contracts, inputs, coercion
│   ├── task_runners/
│   │   ├── driver.py           # shared base for out-of-process runners
│   │   ├── shell.py            # ShellRunner: subprocess + shell driver tasks
│   │   ├── script.py           # ScriptRunner
│   │   └── subworkflow.py      # child `ginkgo run` subprocess
│   ├── caching/
│   │   ├── cache.py            # CacheStore: cache keys and the entry's bytes
│   │   ├── index.py            # CacheIndex: the cache's rows in the ledger
│   │   ├── node_cache.py       # NodeCache: is there a result for this node?
│   │   ├── hash_memo.py        # content digests, memoised in digest_memo
│   │   └── digest_registry.py  # known digests for this run's outputs
│   ├── artifacts/
│   │   ├── artifact_store.py   # content-addressed artifact storage
│   │   ├── artifact_model.py
│   │   ├── remote_artifact_store.py
│   │   ├── remote_arg_transfer.py  # argument staging for remote execution
│   │   ├── fs_share.py         # filesystem-shared copies into the CAS
│   │   ├── output_index.py     # compact typed index of task outputs
│   │   └── value_codec.py      # cross-process value serialization
│   └── environment/
│       ├── secrets.py          # SecretResolver and redaction
│       └── resources.py
├── remote/
│   ├── backend.py           # ObjectStore protocol
│   ├── code_bundle.py       # code packaging for remote workers
│   ├── executor_support.py  # worker-protocol helpers for executor plugins
│   ├── fsspec_backends.py   # FsspecBackend, the base the store plugins extend
│   ├── publisher.py         # remote output publishing
│   ├── resolve.py           # backend factory
│   ├── staging.py           # remote input staging; StagingIndex owns staging_entries
│   ├── worker.py            # remote worker entry point
│   └── access/              # FUSE / staged remote input access
│       ├── doctor.py        # access-layer diagnostics
│       ├── mounted.py       # FUSE-mount coordination
│       ├── protocol.py      # wire encoding for fuse refs
│       ├── resolver.py      # RemoteInputResolver
│       ├── staged.py        # staged (download) access path
│       ├── worker_hydration.py  # worker-side input hydration
│       └── drivers/         # MountDriver protocol, dispatch and unmount; drivers are plugins
├── query.py                 # ginkgo.query: the public read API over the ledger
├── store/                   # rows and SQL; imports nothing from runtime/
│   ├── __init__.py          # open_store, ProvenanceStore
│   ├── protocol.py          # ProvenanceStore: the ledger's write and read surface
│   ├── sqlite.py            # SqliteStore: connection, pragmas, transactions
│   ├── schema.py            # versioned DDL steps and migrate()
│   ├── direct_index.py      # DirectIndex: a connection, a lock, a transaction
│   ├── maintenance.py       # prune_events / prune_digest_memo / vacuum
│   ├── writer.py            # StoreWriter: queued, batched appends of stored rows
│   ├── projector.py         # stored row -> projection rows, one function per type
│   ├── fs.py                # network-filesystem detection for the SQLite warning
│   └── errors.py            # StoreError, SchemaVersionError, StoreLockedError
├── envs/
│   ├── container.py         # ContainerBackend (Docker/Podman)
│   ├── interpreter.py       # what the running interpreter cannot import
│   ├── manifest.py          # find_pixi_manifest: the project's own Pixi manifest
│   └── mounts.py            # bind-mount model for container execution
├── cli/
│   ├── app.py               # parser tree and dispatch
│   ├── common.py            # the shared console, table style, and run opener
│   ├── errors.py            # how a top-level failure is reported
│   ├── workspace.py         # canonical workflow discovery
│   ├── workflow_params.py   # CLI side of ginkgo.param
│   ├── commands/            # one module per core command: run, inspect,
│   │                        # cache, db, debug, doctor, export, history, init,
│   │                        # plugins, query, runs, secrets, why
│   └── renderers/           # rich live output, JSONL agent output, dry-run
│                            # and debug renderers, shared formatting
└── templates/
    └── init/                # the only copy of the starter project scaffold
```

The repository also holds the first-party plugin distributions, each its own
package with its own `pyproject.toml` and tests (see
[Plugins](plugins.md#first-party-plugins)):

```text
plugins/
├── ginkgo-aws/
│   ├── pyproject.toml       # entry point aws = "ginkgo_aws:PLUGIN"
│   ├── src/ginkgo_aws/
│   │   ├── __init__.py      # PLUGIN: the s3 object store and FUSE driver
│   │   ├── store.py         # S3FileSystemBackend
│   │   └── mountpoint.py    # MountpointS3Driver
│   └── tests/
├── ginkgo-k8s/
│   ├── pyproject.toml       # entry point k8s = "ginkgo_k8s:PLUGIN"
│   ├── src/ginkgo_k8s/
│   │   ├── __init__.py      # PLUGIN: the k8s executor type
│   │   └── executor.py      # KubernetesExecutor
│   └── tests/
├── ginkgo-gcp/
│   ├── pyproject.toml       # entry point gcp = "ginkgo_gcp:PLUGIN"
│   ├── src/ginkgo_gcp/
│   │   ├── __init__.py      # PLUGIN: the gs object store, FUSE driver, batch executor
│   │   ├── store.py         # GCSFileSystemBackend
│   │   ├── gcsfuse.py       # GcsFuseDriver
│   │   └── batch.py         # GCPBatchExecutor
│   └── tests/
├── ginkgo-oci/
│   ├── pyproject.toml       # entry point oci = "ginkgo_oci:PLUGIN"
│   ├── src/ginkgo_oci/
│   │   ├── __init__.py      # PLUGIN: the oci object store and FUSE driver
│   │   ├── store.py         # OCIFileSystemBackend, parse_oci_uri
│   │   └── rclone.py        # RcloneDriver
│   └── tests/
├── ginkgo-notebook/
│   ├── pyproject.toml       # entry point notebook = "ginkgo_notebook:PLUGIN"
│   ├── src/ginkgo_notebook/
│   │   ├── __init__.py      # PLUGIN: the notebook task kind
│   │   ├── directive.py     # notebook() and NotebookDirective
│   │   └── runner.py        # NotebookRunner: one `marimo export html` per task
│   └── tests/
├── ginkgo-pixi/
│   ├── pyproject.toml       # entry point pixi = "ginkgo_pixi:PLUGIN"
│   ├── src/ginkgo_pixi/
│   │   ├── __init__.py      # PLUGIN: bare-name environments and `ginkgo env`
│   │   ├── environment.py   # PixiRegistry, PixiEnvironment
│   │   └── command.py       # `ginkgo env ls` / `clear`
│   └── tests/
├── ginkgo-report/
│   ├── pyproject.toml       # entry point report = "ginkgo_report:PLUGIN"
│   ├── src/ginkgo_report/
│   │   ├── __init__.py      # PLUGIN: the `report` CLI command
│   │   ├── command.py       # arguments and command_report
│   │   ├── model.py         # ReportData built from the run summary
│   │   ├── render.py        # Jinja renderer producing an HTML bundle
│   │   ├── sizing.py        # log-tail caps
│   │   ├── templates/
│   │   └── static/
│   └── tests/
└── ginkgo-slack/
    ├── pyproject.toml       # entry point slack = "ginkgo_slack:PLUGIN"
    ├── src/ginkgo_slack/
    │   ├── __init__.py      # PLUGIN: the slack event subscriber
    │   ├── subscriber.py    # config parsing and SlackSubscriber
    │   └── webhook.py       # webhook payloads and delivery
    └── tests/
```
