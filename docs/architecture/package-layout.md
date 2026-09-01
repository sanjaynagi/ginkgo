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
├── core/
│   ├── asset.py          # asset identity, table/array/fig/text/model wrappers
│   ├── directive.py      # ExecutionDirective base type
│   ├── expr.py
│   ├── flow.py
│   ├── hashing.py        # BLAKE3 content-hash helpers (dependency-free)
│   ├── notebook.py
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
│   ├── backend.py        # ExecutionEnvironment protocol, Local, Composite
│   ├── evaluator.py      # _ConcurrentEvaluator scheduler/lifecycle loop
│   ├── executors.py      # the evaluator's heterogeneous executor pool
│   ├── scheduler.py
│   ├── worker.py
│   ├── module_loader.py
│   ├── notebook_kernels.py
│   ├── events.py
│   ├── log_drain.py      # worker log chunks to TaskLog events
│   ├── dry_run.py        # static execution-plan preview for --dry-run
│   ├── profiling.py      # phase-timer aggregation for --profile
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
│   │   ├── notebook.py         # NotebookRunner
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
│   │   ├── value_codec.py      # cross-process value serialization
│   │   ├── asset_store.py      # asset catalog metadata
│   │   ├── asset_kinds.py      # kind registry for the asset model
│   │   ├── asset_loaders.py    # per-kind artifact loaders
│   │   ├── asset_serialization.py
│   │   ├── asset_registration.py   # cache-to-catalog glue
│   │   └── live_payloads.py    # in-memory wrapped-asset payloads
│   ├── notifications/
│   │   ├── notifications.py
│   │   └── slack.py
│   └── environment/
│       ├── secrets.py          # SecretResolver and redaction
│       └── resources.py
├── remote/
│   ├── backend.py           # ObjectStore protocol
│   ├── code_bundle.py       # code packaging for remote workers
│   ├── fsspec_backends.py   # S3, OCI, GCS backends
│   ├── gcp_batch.py         # GCP Batch executor
│   ├── kubernetes.py        # Kubernetes executor
│   ├── _executor_common.py  # helpers shared by both remote executors
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
│       └── drivers/         # per-provider FUSE drivers (s3, gcsfuse, rclone)
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
├── reporting/
│   ├── model.py             # ReportData built from provenance and the catalog
│   ├── render.py            # Jinja renderer producing an HTML bundle
│   └── sizing.py            # per-kind preview caps and size formatting
├── envs/
│   ├── container.py         # ContainerBackend (Docker/Podman)
│   ├── mounts.py            # bind-mount model for container execution
│   └── pixi.py
├── cli/
│   ├── app.py               # parser tree and dispatch
│   ├── common.py            # the shared console, table style, and run opener
│   ├── errors.py            # how a top-level failure is reported
│   ├── workspace.py         # canonical workflow discovery
│   ├── workflow_params.py   # CLI side of ginkgo.param
│   ├── commands/            # one module per command: run, inspect, cache,
│   │                        # asset, db, debug, doctor, env, export, history,
│   │                        # init, lineage, models, notebooks, query, report,
│   │                        # runs, secrets
│   └── renderers/           # rich live output, JSONL agent output, dry-run
│                            # and debug renderers, shared formatting
└── templates/
    └── init/                # the only copy of the starter project scaffold
```
