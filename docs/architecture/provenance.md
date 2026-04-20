# Provenance and Run State

Each run records provenance under `.ginkgo/runs/<run_id>/`:

```text
.ginkgo/runs/<run_id>/
├── manifest.yaml
├── params.yaml
├── envs/
└── logs/
```

The manifest records:

- run metadata and status
- resolved task inputs
- input hashes
- cache keys
- task dependencies and dynamic dependency ids
- retries and attempts
- outputs
- asset versions and metadata for asset-producing tasks
- notebook artifact metadata including rendered HTML paths, executed notebook paths where applicable, and render status
- exit codes and errors
- run-level CPU and RSS summaries
