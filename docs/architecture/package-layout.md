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
│   ├── notebook.py
│   ├── script.py
│   ├── shell.py
│   ├── task.py
│   └── types.py
├── runtime/
│   ├── backend.py        # TaskBackend protocol, LocalBackend, CompositeBackend
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
│   │   ├── hashing.py
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
│   ├── backend.py           # RemoteStorageBackend protocol
│   ├── code_bundle.py       # code packaging for remote workers
│   ├── fsspec_backends.py   # S3, OCI, GCS backends
│   ├── gcp_batch.py         # GCP Batch executor
│   ├── kubernetes.py        # Kubernetes executor
│   ├── publisher.py         # remote output publishing
│   ├── resolve.py           # backend factory
│   ├── staging.py           # remote input staging
│   └── worker.py            # remote worker entry point
├── envs/
│   ├── container.py      # ContainerBackend (Docker/Podman)
│   └── pixi.py
├── cli/
│   ├── app.py
│   └── commands/
└── ui/
    ├── server/
    │   ├── __init__.py      # re-exports create_ui_server
    │   ├── app.py           # HTTP/WebSocket handler and route wiring
    │   ├── live.py          # live-state capture and diffing
    │   ├── payloads.py      # run/task/workspace/cache payload builders
    │   ├── utils.py         # shared formatting helpers
    │   ├── websocket.py     # WebSocket framing
    │   └── workspaces.py    # WorkspaceRecord, WorkspaceRegistry, discovery
    └── static/
```
