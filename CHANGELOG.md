# Changelog

Ginkgo and its first-party plugins release together at one version. Each
release's GitHub notes start from its entry here. Breaking changes are allowed
before 1.0.

## Unreleased

## 0.4.0 (2026-10-29)

The first release on PyPI, as `ginkgo-workflow`. The import and the command are
both still `ginkgo`, and each plugin is published under its own name at the same
version as core.

### Breaking changes

- Delete `.ginkgo/` before the first run. The ledger schema and the cache key
  format have both changed, and 0.4.0 refuses a workspace created by an older
  version (#393, #411).
- Assets are removed, and with them `asset()`, `AssetRef` and `file | AssetRef`
  parameters (#357).
- Cloud backends, environments, notebooks, reports and notifications move into
  plugins: `ginkgo-k8s`, `ginkgo-gcp`, `ginkgo-aws`, `ginkgo-oci`, `ginkgo-pixi`,
  `ginkgo-notebook`, `ginkgo-report` and `ginkgo-slack` (#358–#363). Jupyter
  support is removed.
- A task sees only its arguments. Ginkgo no longer scans for environment
  variables or literal paths (#394).
- Shell tasks run in strict mode, and scalar annotations are checked (#371).
- `--profile` is removed (#364).

### New

- `Out[...]` parameters declare the paths a task writes (#320–#330).
- `ginkgo run --detach`, `ginkgo runs status` and `ginkgo runs wait`, and a final
  `run_summary` event for `--agent-output` runs (#423).
- `ginkgo why` names a dynamic task's child and resolves paths inside output
  folders, and `cache explain` shows changed scalar values (#405, #414, #421).
- Editing one task or the flow no longer invalidates the other tasks in its
  module (#411). Cached dynamic tasks replay their children, and `--rerun` and
  `cache clear --task` / `--all` are new (#381). Cache and ledger maintenance
  commands (#391).
- `on_failure="ignore"`, `--keep-going`, and a skipped state for tasks that
  can't run (#276). Interrupted runs are recorded as cancelled, and Ctrl-C stops
  a run promptly (#300, #379, #384).
- Remote workers download inputs directly, inputs are keyed by store metadata
  and staged concurrently, and in-flight jobs are cancelled when a run stops
  (#366, #396, #397, #406). A reference OKE deployment, and a kind cluster test
  in CI (#398, #400, #402).
- More checks while the graph is built and before a run (#316, #351, #352, #377,
  #422).
- Resource use is recorded across runs (#279).

### Docs and internals

- The docs site moves to Astro Starlight, with a new home page and a walkthrough
  video (#415, #424–#426).
- The evaluator is split into `TaskGraph`, `Placement`, `ResourceLedger` and a
  typed `TaskInvocation` (#364–#370). The test suite is a third smaller and 4x
  faster (#401).
