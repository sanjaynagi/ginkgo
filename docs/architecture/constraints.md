# Current Constraints

The current runtime still has important boundaries and tradeoffs:

- worker-executed Python tasks must be importable by module path
- the scheduler's authoritative live execution state is still in-memory; what
  happened is persisted to the ledger at `.ginkgo/ginkgo.db` as it happens, and
  exported once per run to `runs/<id>/manifest.yaml`
- pre-ledger workspaces are not migrated: runs, cache entries and assets
  recorded before `.ginkgo/ginkgo.db` existed lived in files ginkgo no longer
  reads, so they are invisible to the CLI. There is no import path; delete
  `.ginkgo/` and re-run
- a lost `ginkgo.db` colds the cache and loses the run history. `ginkgo db
  check` finds the bytes it stranded; back the file up as you would `.git`

Those constraints drive several of the future roadmap items in the implementation plan.
