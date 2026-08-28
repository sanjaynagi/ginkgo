# Current Constraints

The current runtime still has important boundaries and tradeoffs:

- worker-executed Python tasks must be importable by module path
- the scheduler's authoritative live execution state is still in-memory; what
  happened is persisted to the ledger at `.ginkgo/ginkgo.db` as it happens, and
  exported once per run to `runs/<id>/manifest.yaml`
- pre-ledger workspaces are not migrated: runs recorded before `.ginkgo/ginkgo.db`
  existed are invisible to the CLI

Those constraints drive several of the future roadmap items in the implementation plan.
