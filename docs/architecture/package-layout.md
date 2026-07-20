# Package Layout

The current source tree is organized around the user-facing DSL, the execution engine, and environment backends:

```text
ginkgo/
├── __init__.py
├── config.py
├── wildcards.py
├── core/
│   ├── expr.py
│   ├── flow.py
│   ├── hashing.py        # BLAKE3 content-hash helpers (dependency-free)
│   ├── notebook.py
│   ├── script.py
│   ├── shell.py
│   ├── task.py
│   └── types.py
├── runtime/
│   ├── backend.py        # ExecutionEnvironment protocol, LocalEnvironment, CompositeEnvironment
│   ├── evaluator.py      # _ConcurrentEvaluator scheduler/lifecycle loop
│   ├── module_loader.py
│   ├── notebook_kernels.py
│   ├── scheduler.py
│   ├── worker.py
│   ├── events.py
│   ├── remote_executor.py   # RemoteExecutor / RemoteJobHandle protocols
│   ├── diagnostics.py
│   ├── task_validation.py     # TaskValidator: contracts, inputs, coercion
│   ├── task_runners/
│   │   ├── shell.py           # ShellRunner: subprocess + shell driver tasks
│   │   └── notebook.py        # NotebookRunner: notebook + script driver tasks
│   ├── caching/
│   │   ├── cache.py           # CacheStore (content-addressed)
│   │   ├── provenance.py      # RunProvenanceRecorder
│   │   ├── hash_memo.py
│   │   └── materialization_log.py
│   ├── artifacts/
│   │   ├── artifact_store.py  # content-addressed artifact storage
│   │   ├── artifact_model.py
│   │   ├── asset_store.py     # asset catalog metadata
│   │   └── value_codec.py     # cross-process value serialization
│   ├── notifications/
│   │   ├── notifications.py
│   │   └── slack.py
│   └── environment/
│       ├── secrets.py         # SecretResolver and redaction
│       └── resources.py
├── remote/
│   ├── backend.py           # ObjectStore protocol
│   ├── code_bundle.py       # code packaging for remote workers
│   ├── fsspec_backends.py   # S3, OCI, GCS backends
│   ├── gcp_batch.py         # GCP Batch executor
│   ├── kubernetes.py        # Kubernetes executor
│   ├── publisher.py         # remote output publishing
│   ├── resolve.py           # backend factory
│   ├── staging.py           # remote input staging
│   ├── worker.py            # remote worker entry point
│   └── access/              # FUSE / staged remote input access
│       ├── doctor.py        # access-layer diagnostics
│       ├── mounted.py       # FUSE-mount coordination
│       ├── protocol.py      # wire encoding for fuse refs
│       ├── resolver.py      # RemoteInputResolver
│       ├── staged.py        # staged (download) access path
│       ├── worker_hydration.py  # worker-side input hydration
│       └── drivers/         # per-provider FUSE drivers (s3, gcsfuse, rclone)
├── envs/
│   ├── container.py      # ContainerBackend (Docker/Podman)
│   └── pixi.py
└── cli/
    ├── app.py
    └── commands/
```
