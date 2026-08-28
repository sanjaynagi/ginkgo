# Provenance and Cache Store: Scoping Plan

Status: scoping draft for discussion. Nothing here is implemented.
Implementation epic with phased tasks: https://github.com/sanjaynagi/ginkgo/issues/247

## 1. Problem definition

Ginkgo records a lot of provenance, but it records it into nine
independent file-based stores, each with its own format, its own reader,
and no shared identity model. Every question a user, the CLI, the report,
or any future UI/API consumer asks has to be answered by locating files and replaying
them.

### 1.1 Inventory of what exists today

Observed by running a four-task workflow cold and warm in a scratch
workspace, plus the sources in `runtime/caching/`, `runtime/artifacts/`,
`remote/staging.py`.

| Store | Location | Format | Written by | Read by |
|---|---|---|---|---|
| Run manifest | `.ginkgo/runs/<run>/manifest.yaml` | YAML snapshot + `_provenance_event_offset` | `RunProvenanceRecorder._write_manifest` | `load_manifest` — 7 src call sites (`run_summary`, `inspect`, `debug`, `notebooks`, `notifications`, benchmarks) |
| Provenance patch log | `.ginkgo/runs/<run>/events.jsonl` | JSONL, two interleaved streams (see 1.2) | `RunProvenanceRecorder._append_event`, run renderer | `_replay_provenance_events` on every `load_manifest` |
| Params | `.ginkgo/runs/<run>/params.yaml` | YAML | recorder | report, inspect |
| Cache entries | `.ginkgo/cache/<key>/{meta.json,output.json}` | one dir per key; `meta.json` carries the key components | `CacheStore.save` | `CacheStore.load/has_entry`, `cache ls/explain/prune` (directory scans) |
| Stat index | `.ginkgo/cache/stat_index.json` | flat JSON map stat-fingerprint → cache key | `CacheStore.save_stat_index` | `--trust-mtimes` path |
| Artifact records | `.ginkgo/artifacts/refs/<id>.json`, `blobs/`, `trees/` | JSON per artifact + CAS bytes | `LocalArtifactStore` | cache, assets, prune |
| Materialization log | `.ginkgo/artifacts/materializations.json` | JSON map path → (artifact, size, mtime) | `MaterializationLog` | cache-hit restore check |
| Asset catalog | `.ginkgo/assets/<ns>/<name>/{index.yaml, versions/<v>/meta.yaml}` | YAML per key + per version; lineage inside `index.yaml` | `AssetStore` | `asset ls/show`, report, evaluator rehydration |
| Staging cache | `.ginkgo/staging/…` + `staging_cache_file` | JSON entries per URI | `StagingCache` | remote-input hydration |

Plus two in-memory-only structures that lose useful state at exit:
`HashMemo` (issue #245 — a 100 GB reference folder is re-hashed every
run) and `DigestRegistry`.

### 1.2 Concrete pains

- **Two event streams in one file.** `events.jsonl` interleaves the typed
  runtime stream (`run_started`, `task_cache_miss`, … with `ts`/`v`) and
  the recorder's untyped patch stream (`provenance_task_updated` with a
  free-form `fields` dict, no timestamp, no version). In the toy run 71
  lines split 41/30 between them. The patch stream is the source of
  truth for `manifest.yaml`, but it is a change-log of a mutable dict,
  not a log of things that happened.
- **Every read is a replay.** `load_manifest` re-reads and re-applies the
  patch log tail on every call; `RunSummary`, `inspect run`, `debug`,
  notifications and the report each do it independently.
- **No cross-run queries.** "Show me every run of `annotate_compounds`
  in the last month and how long each took", "which runs consumed asset
  `table:rows@v3`", "which cache entries have never been hit" — none are
  answerable without a directory walk and N YAML parses.
- **`cache explain` scans siblings.** It compares `meta.json` against the
  newest earlier sibling *by task name*, which the docs already admit
  mis-pairs fanned-out tasks. It has to, because there is no index from
  task → entries → key components.
- **Asset lineage is shallow and one-directional.** `index.yaml` records
  parents per version; there is no child index, no run → asset → run
  traversal, and no task-level provenance (which cache key produced
  which asset version).
- **Sub-workflow stitching parses stdout.** `_extract_child_run_id`
  regexes the child's console output to find its run id, then points at
  its `manifest.yaml` by path.
- **Cache policy is blind.** Prune is oldest-first by `timestamp`; there
  is no hit count or last-hit time, so a hot entry from March is evicted
  before a cold one from yesterday.
- **Per-file catalogs do not scale.** Snakemake hit filesystem inode and
  latency limits with one JSON per output and added an SQLAlchemy
  backend in 9.x for exactly this reason. Ginkgo's `cache/` and
  `assets/` trees have the same shape.
- **No concurrency story.** Two `ginkgo run`s in one workspace race on
  `stat_index.json`, `materializations.json` and the asset `index.yaml`
  (atomic replace, last writer wins).
- **Any future UI or API would have to re-index everything.** The
  (unimplemented) Studio plan sketches an "Indexer" that parses
  manifests, events, and asset YAML into its own SQLite projection. Built
  as drafted, it would be a second implementation of every reader above,
  in a second repo. Nothing exists yet, so this is avoidable.
- **Hash naming drift.** `input_hashes` label digests `sha256` while the
  algorithm is BLAKE3 (`caching.md`). Cosmetic, but it shows the schema
  is implicit.

### 1.3 What is already right and must be preserved

- Content-addressed identity everywhere: cache keys, `artifact_id`,
  `AssetVersion.version_id`, `source_hash`, env identity. These become
  primary keys.
- Bytes are already separated from metadata (CAS under `artifacts/`).
- The typed runtime event vocabulary (`runtime/events.py`) is good and
  already has renderers; it is the right seed for the event log.
- Deferred env materialisation, prepare-phase cache probes, the
  `env_materialized_digest` drift check — none of this changes.
- Run directories with logs and copied lockfiles stay as files. Logs
  and bytes do not belong in SQL.

## 2. What to borrow, and from whom

Three research sweeps (redun/Nextflow/Snakemake/Cromwell;
Dagster/Prefect/Flyte/Metaflow/Parsl; OpenLineage/PROV/RO-Crate/Bazel/
Nix/DVC/Temporal/Pachyderm) converge on a small set of ideas.

| Idea | Source | Take for ginkgo |
|---|---|---|
| Append-only event log is the single source of truth; every "current state" table is a projection recomputable from it | Dagster `event_logs` → `asset_keys`; Cromwell `METADATA_ENTRY` → summary tables; Temporal history; Marquez `lineage_events` | Yes — this is the spine of the design |
| Store normalised columns for what you query (type, run, task, attempt, asset key, cache key), JSON only for the long tail | Dagster pitfall: whole-event blob forces deserialising every row | Yes |
| Call graph: `Task(hash) × Args(hash) → Value(hash)`, with `CallEdge` parent/child; `Execution`/`Job` are attempts over it | redun | Yes — this is what turns "run history" into "provenance". Ginkgo's cache entry *is* a call node already |
| Two-tier cache: cheap shallow memo (`eval_hash`) plus deep validation that walks descendants (`call_hash`) | redun; Snakemake rerun-triggers vs `--cache`; Cromwell base-aggregation then file hashes | Partly — ginkgo has the shallow tier and the `--trust-mtimes` tier; the store makes a deep tier possible later |
| `code_version` + `data_version` recorded on each materialisation; staleness computed at query time by diffing against upstream | Dagster | Yes — ginkgo already has `source_hash` and `content_hash`; recording them per asset version enables `stale/fresh/unknown` without a scheduler change |
| Attempt is a first-class entity distinct from task | Parsl `try`; Cromwell `METADATA_CALL_ATTEMPT` | Yes — retries currently overwrite `error`/`exit_code` on the task dict |
| Metadata in a small relational DB; payloads in a blob store referenced by URI/digest | Flyte, Metaflow, Prefect, redun `value_store` | Already true; make it a stated invariant |
| Cache index separate from cache value, with hit metadata; explicit isolation for concurrent lookups | Prefect `task_run_state_cache`, `SERIALIZABLE` | Yes — `hit_count`, `last_hit_at`; SQLite transactions replace atomic-rename races |
| Edge table `(referrer, reference)` as the lineage graph; `why-depends` | Nix `Refs`; Pachyderm commit provenance | Yes — one `edges` table for task→task, task→artifact, asset→asset |
| Replay one log into many views: `what-ran`, `what-failed`, `what-up` | Buck2; Bazel BEP self-describing event graph | Yes — CLI verbs become SQL views |
| Persist the stat→digest memo across runs | issue #245, redun `File.is_valid()` (path,size,mtime) | Yes — trivial once there is a DB |
| Resolvable content-addressed URIs for outputs (`lid://…`) | Nextflow lineage | Later — `ginkgo://artifact/<id>`, useful for links from any UI |
| `push`/`pull` of provenance records between repos | redun | Not now; keep the schema portable so it is possible |
| OpenLineage events, Workflow Run RO-Crate, W3C PROV as **export serialisers** over the store, not as the store | Marquez, nf-prov, CWLProv | Yes, cheap once the entities exist; do not model the schema around them |
| Per-run history growth needs a retention story; irreversible archival is a trap | Cromwell carboniting removal | Design retention in: events are prunable per run once projections exist |
| SQLite is fine locally but single-writer and opaque; not portable across machines | Nextflow LevelDB pain; redun BLOB-in-SQLite | Use SQLite with WAL, keep bytes out, keep a backend protocol so Postgres can follow |

Things deliberately **not** borrowed: Dagster's declarative asset graph and
partitions (the Studio plan draft already rules this out without demand), Flyte's
manual `cache_version` (ginkgo's `source_hash` is strictly better),
Prefect's server-first architecture, Metaflow's "no invalidation, just
dedup" stance.

## 3. Proposed solution

### 3.1 Principles

1. **One ledger.** A single append-only `events` table per workspace is
   the source of truth for what happened. The runtime event bus and the
   provenance recorder become one typed stream.
2. **Projections, not mutation.** `runs`, `tasks`, `attempts`,
   `asset_versions`, `cache_entries` are read models maintained in the
   same transaction as the event append, and fully rebuildable from the
   ledger (`ginkgo db rebuild`).
3. **Content-addressed identity is the primary key.** `cache_key`,
   `artifact_id`, `version_id`, `source_hash`, `env_hash` are the join
   columns. Nothing is renumbered.
4. **Bytes stay out.** SQL holds metadata, digests, paths and offsets.
   CAS blobs, logs, notebooks, lockfiles stay files.
5. **Rebuildable, but canonical.** The DB is the canonical *index*; the
   run directory keeps a `manifest.yaml` snapshot exported at finalize so
   a run directory remains self-describing and archivable. Deleting
   `ginkgo.db` and rebuilding from run directories must be lossless for
   everything the CLI shows today.
6. **Backend protocol, SQLite first.** Stdlib `sqlite3`, WAL mode,
   hand-written SQL (the Studio plan draft also assumes no ORM). A
   `ProvenanceStore` protocol so a Postgres implementation can serve a
   team later without touching the evaluator.

### 3.2 Location

`.ginkgo/ginkgo.db` in the workspace, next to the directories it indexes.
`WorkspaceLayout` gains `db`. One DB per workspace keeps the current
single-user, local-first model; a shared-team DB is a backend question
(3.7), not a layout question.

### 3.3 Schema (SQLite dialect, first cut)

```sql
-- versioning
CREATE TABLE schema_version (version INTEGER NOT NULL, applied_at TEXT NOT NULL);

-- the ledger
CREATE TABLE events (
  seq        INTEGER PRIMARY KEY,          -- global monotonic order
  run_id     TEXT NOT NULL,
  ts         TEXT NOT NULL,
  type       TEXT NOT NULL,                -- run_started, task_cache_hit, asset_materialized, ...
  v          INTEGER NOT NULL DEFAULT 1,   -- payload schema version per type
  task_id    TEXT,                         -- task_0007 (node-scoped id); NULL for run events
  attempt    INTEGER,
  cache_key  TEXT,
  asset_key  TEXT,                         -- "table:rows"
  payload    TEXT NOT NULL                 -- JSON, everything not in a column
);
CREATE INDEX events_run ON events(run_id, seq);
CREATE INDEX events_type_ts ON events(type, ts);
CREATE INDEX events_asset ON events(asset_key, seq) WHERE asset_key IS NOT NULL;

-- projections
CREATE TABLE runs (
  run_id TEXT PRIMARY KEY, workflow TEXT, status TEXT, started_at TEXT, finished_at TEXT,
  jobs INTEGER, cores INTEGER, memory INTEGER, error TEXT,
  params TEXT, param_sources TEXT, resources TEXT, timings TEXT,   -- JSON
  parent_run_id TEXT, parent_task_id TEXT,                          -- subworkflow stitching
  ginkgo_version TEXT, snapshot_written INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE tasks (
  run_id TEXT NOT NULL, task_id TEXT NOT NULL, node_id INTEGER NOT NULL,
  name TEXT NOT NULL, display_label TEXT, kind TEXT, execution_mode TEXT, env TEXT,
  status TEXT, cached INTEGER, cache_key TEXT, source_hash TEXT, version INTEGER,
  env_hash TEXT, extra_source_hash TEXT,
  started_at TEXT, finished_at TEXT, attempts INTEGER, max_attempts INTEGER,
  exit_code INTEGER, failure TEXT, output_summary TEXT, resource_usage TEXT,  -- JSON
  stdout_log TEXT, stderr_log TEXT, execution_backend TEXT, remote_job_id TEXT,
  PRIMARY KEY (run_id, task_id)
);
CREATE INDEX tasks_name ON tasks(name, started_at);
CREATE INDEX tasks_cache_key ON tasks(cache_key);
CREATE TABLE attempts (
  run_id TEXT, task_id TEXT, attempt INTEGER, started_at TEXT, finished_at TEXT,
  status TEXT, exit_code INTEGER, failure TEXT, retry_delay_s REAL, execution_backend TEXT,
  PRIMARY KEY (run_id, task_id, attempt)
);
CREATE TABLE task_inputs (            -- redun Argument
  run_id TEXT, task_id TEXT, param TEXT, position INTEGER,
  value_type TEXT, value_summary TEXT, digest TEXT,       -- digest = the hash folded into the key
  artifact_id TEXT, asset_key TEXT, asset_version_id TEXT, remote_uri TEXT,
  PRIMARY KEY (run_id, task_id, param, position)
);
CREATE TABLE task_outputs (
  run_id TEXT, task_id TEXT, position INTEGER, name TEXT, value_type TEXT,
  path TEXT, artifact_id TEXT, asset_key TEXT, asset_version_id TEXT,
  PRIMARY KEY (run_id, task_id, position)
);
CREATE TABLE edges (                  -- Nix Refs / redun CallEdge, one table
  run_id TEXT, src_kind TEXT, src_id TEXT, dst_kind TEXT, dst_id TEXT, edge TEXT,
  -- (task,task,'depends_on' | 'dynamic'), (task,artifact,'produced'), (task,asset_version,'consumed'),
  -- (asset_version,asset_version,'derived_from'), (run,run,'child_of')
  PRIMARY KEY (run_id, src_kind, src_id, dst_kind, dst_id, edge)
);
CREATE INDEX edges_dst ON edges(dst_kind, dst_id);

-- cache (redun call node + Prefect cache index)
CREATE TABLE cache_entries (
  cache_key TEXT PRIMARY KEY, function TEXT NOT NULL, version INTEGER, source_hash TEXT,
  extra_source_hash TEXT, env TEXT, env_hash TEXT, env_materialized_digest TEXT,
  inputs TEXT, input_hashes TEXT,                          -- JSON, as meta.json today
  output_codec TEXT, output_size INTEGER,
  created_run_id TEXT, created_at TEXT, hit_count INTEGER NOT NULL DEFAULT 0, last_hit_at TEXT,
  extra TEXT, size_bytes INTEGER
);
CREATE INDEX cache_function ON cache_entries(function, created_at);
CREATE TABLE cache_key_components (   -- what `cache explain` diffs; one row per component
  cache_key TEXT, component TEXT, value TEXT, PRIMARY KEY (cache_key, component)
);
CREATE TABLE cache_artifacts (cache_key TEXT, path TEXT, artifact_id TEXT, PRIMARY KEY (cache_key, path));
CREATE TABLE stat_index (stat_key TEXT PRIMARY KEY, cache_key TEXT NOT NULL);
CREATE TABLE digest_memo (            -- HashMemo persisted (#245)
  kind TEXT, device INTEGER, inode INTEGER, size INTEGER, mtime_ns INTEGER, fingerprint TEXT,
  digest TEXT NOT NULL, path TEXT, last_seen TEXT, PRIMARY KEY (kind, fingerprint)
);

-- artifacts and assets
CREATE TABLE artifacts (
  artifact_id TEXT PRIMARY KEY, kind TEXT, digest_algorithm TEXT, extension TEXT, size INTEGER,
  created_at TEXT, storage_backend TEXT, remote_uri TEXT, refcount_hint INTEGER
);
CREATE TABLE materializations (path TEXT PRIMARY KEY, artifact_id TEXT, size INTEGER, mtime_ns INTEGER);
CREATE TABLE asset_keys (asset_key TEXT PRIMARY KEY, namespace TEXT, name TEXT,
  latest_version_id TEXT, version_count INTEGER, last_materialized_at TEXT, group_name TEXT, caption TEXT);
CREATE TABLE asset_versions (
  asset_key TEXT, version_id TEXT, kind TEXT, sub_kind TEXT, artifact_id TEXT, content_hash TEXT,
  run_id TEXT, task_id TEXT, producer_task TEXT, cache_key TEXT, created_at TEXT,
  code_version TEXT,            -- producer source_hash (Dagster code_version)
  data_version TEXT,            -- hash(code_version, upstream data_versions) (Dagster data_version)
  metadata TEXT, metrics TEXT, checks TEXT,
  PRIMARY KEY (asset_key, version_id)
);
CREATE TABLE asset_aliases (asset_key TEXT, alias TEXT, version_id TEXT, PRIMARY KEY (asset_key, alias));

-- remote inputs
CREATE TABLE staging_entries (uri TEXT PRIMARY KEY, digest TEXT, etag TEXT, version_id TEXT,
  size INTEGER, staged_at TEXT, blob_path TEXT, last_used_at TEXT);
CREATE TABLE env_materializations (env_hash TEXT, host TEXT, materialized_digest TEXT, seen_at TEXT,
  PRIMARY KEY (env_hash, host));
```

Notes on the shape:

- `events.payload` stays JSON so new event types need no migration;
  columns exist only for what is filtered or joined on (Dagster's lesson).
- `edges` is one table on purpose: "what did this run touch", "who
  consumed this asset version", "which runs are children of this run"
  are all the same query shape.
- `cache_key_components` makes `cache explain` a two-row diff on an
  index instead of a sibling scan, and lets it compare against *the
  entry the same node hit last time* (`tasks.cache_key` for the previous
  run of that `display_label`) rather than the nearest sibling by name.
- `asset_versions.code_version / data_version` are recorded, not yet
  acted upon. Staleness is a later query, not a scheduler change.

### 3.4 Event vocabulary

Unify the two streams. The typed `GinkgoEvent` classes in
`runtime/events.py` are the vocabulary; the recorder's `provenance_*`
patches are replaced by new typed events carrying the facts they encode:

| Today (patch stream) | Becomes |
|---|---|
| `provenance_task_created` | `graph_node_registered` (exists) + `task_planned` with inputs, input hashes, cache key, dependency ids |
| `provenance_task_updated {status: running}` | `task_started` (exists) |
| `provenance_task_updated {cached, output, outputs, assets}` | `task_completed` (exists; grows `assets`, `output_summary`) and `asset_materialized` (new, one per asset) |
| `provenance_task_updated {failure}` | `task_failed` (exists) |
| `provenance_task_updated {retry…}` | `task_retrying` (exists) |
| `provenance_task_timing` / `provenance_run_timing` | `phase_timed` (new) |
| `provenance_run_update {status, finished_at, resources}` | `run_completed` (exists; grows `resources`) |
| `update_task_extra(**fields)` — env lock, container digest, access stats, notebook artefacts | `task_annotated {fields}` (new, the one deliberately open-ended event) |

Every event gets `seq`, `ts`, `v`. `--agent-output` JSONL becomes a
rendering of the same rows. `RunProvenanceRecorder` collapses to a thin
subscriber on the `EventBus` that appends to the store; the evaluator
stops calling two things.

### 3.5 Write path

- One writer per process: a `StoreWriter` thread drains a queue, appends
  events and updates projections in one transaction per batch (flush on
  every terminal event and at most every ~50 ms). SQLite WAL,
  `busy_timeout` 5 s, `synchronous=NORMAL`.
- Current cost baseline: `provenance_write_seconds` was 3 ms for a
  four-task run. Batched SQLite inserts are well within that.
- Cache save becomes: write `output.json` and artifacts to disk as now,
  then insert `cache_entries` + components + artifacts in one
  transaction. The entry directory keeps `meta.json` during the
  transition (3.8) so `db rebuild` can re-index it.
- Concurrent runs in one workspace now serialise on the DB instead of
  racing on JSON replace. Cache lookup-then-save is a transaction.
- Remote workers keep writing nothing; the driver owns provenance, as
  today. The Kubernetes/Batch envelope stats land via `task_annotated`.

### 3.6 Read path

A `ginkgo.query` module (public, typed) is the one reader; CLI, report, notifications, and any future UI all go through it.

```python
store = ProvenanceStore.open(layout)
store.runs(workflow=..., status=..., since=..., limit=...)
store.run(run_id)                      # RunSummary, built from tasks/attempts rows
store.task_history(name, limit=50)     # every attempt of a task across runs, with durations and cache outcome
store.explain_rerun(run_id, task_id)   # component diff against the key this node hit last time
store.lineage(asset_key, version_id, direction="upstream"|"downstream", depth=None)
store.why(path_or_artifact_id)         # which task in which run produced these bytes, from what
store.cache_stats()                    # entries, bytes, hit_count distribution, never-hit entries
store.events(run_id, after_seq=0)      # incremental reads (live tailing)
store.sql(query, params)               # read-only escape hatch
```

CLI additions that fall out: `ginkgo runs ls|show|diff`, `ginkgo history
<task>`, `ginkgo lineage <asset|path>`, `ginkgo cache stats`,
`ginkgo db rebuild|check|vacuum`, `ginkgo query "<sql>"`, `ginkgo export
--openlineage|--ro-crate <run>`. Existing `inspect run`, `debug`,
`cache explain`, `asset *` re-implement on the store with unchanged
output.

### 3.7 Consumers

No consumer exists today beyond the CLI and the report; Studio is a plan,
not software. Ginkgo ships the **store and the Python query API**, and a
JSON schema for every event type. There is **no HTTP API in this plan**.
`ginkgo.query` is not an extra surface: it is the one reader the CLI and
report are rebuilt on, and any later consumer — a notebook, a script, a
future UI — opens the workspace DB read-only through it; SQLite WAL
permits concurrent readers with one writer.

If Studio is built later, its draft plan should be revised to drop the
"Indexer" and consume `ginkgo.query` directly; its own tables (projects,
launch requests, presets) would live in its own DB and join on
`(project_id, run_id)`. Its "rebuild from source" requirement is met by
`ginkgo db rebuild`.

If an HTTP API is ever wanted, it would be a thin layer over
`ginkgo.query` and `store.events(after_seq)`. It is deliberately out of
scope here.

### 3.8 Migration and compatibility

- Dual-write phase: the store is written alongside today's files; all
  readers switch to the store; `ginkgo db rebuild` can construct the
  store from files in existing workspaces (this is also the test that the
  projections are complete).
- `manifest.yaml` is kept, but changes meaning: from "source of truth
  reconstructed on read" to "snapshot exported at finalize". Its keys do
  not change, so external scripts keep working. `events.jsonl` is
  replaced by `ginkgo export --events <run>` on demand; the JSONL format
  is unchanged (it is the `events` table).
- Cache `meta.json`/`output.json` stay on disk (the DB indexes them);
  `stat_index.json`, `materializations.json`, asset YAML,
  `staging_cache_file` are removed once readers are migrated.
- A workspace with no DB and existing runs gets one built lazily on the
  first command that needs it.

### 3.9 What the nine stores become

| Today | After |
|---|---|
| `manifest.yaml` | Kept, demoted to an export written once at finalize |
| `events.jsonl` | Removed → `events` |
| `params.yaml` | Removed — its content is `manifest.params` |
| `cache/<key>/meta.json` | Removed → `cache_entries` + `cache_key_components` (decision: DB is the only cache index) |
| `cache/<key>/output.json` | Kept — it is the cached value, not metadata |
| `stat_index.json` | Removed → `stat_index` |
| `artifacts/refs/*.json` | Removed → `artifacts`, rebuildable from the CAS itself; blobs and trees stay |
| `materializations.json` | Removed → `materializations` |
| asset `index.yaml` + `versions/*/meta.yaml` | Removed → `asset_keys`, `asset_versions`, `asset_aliases`, `edges` |
| staging cache file | Removed → `staging_entries`; staged bytes stay on disk |
| `HashMemo` (in-memory) | Persisted as `digest_memo` |

Nine metadata stores become one database plus bytes on disk (CAS blobs, cached `output.json`) and one exported `manifest.yaml` per run. `db rebuild` recovers runs, assets, lineage and artifacts from those; cache entries are recovered for every key a snapshot references, and entries no run ever touched are the one thing a lost DB does not bring back (reported by `db check`, removed only by an explicit `cache clear --orphans`). Every reader that exists today collapses into `ginkgo.query`; the
evaluator calls one `EventBus` subscriber instead of recorder and bus.
During dual-write there are temporarily *more* stores, not fewer — the
price of proving `db rebuild` lossless before deleting anything.

## 4. Risks and tradeoffs

- **SQLite on network filesystems.** HPC users with `.ginkgo/` on NFS or
  Lustre hit SQLite's well-known locking problems. Mitigation: detect
  (`statfs`) and warn; document a `GINKGO_DB` override to a local path;
  the store protocol is the long-term answer (Postgres or a per-user
  daemon). This is the single biggest deployment risk and should be
  tested early on a real cluster.
- **Migration surface.** `RunProvenanceRecorder` has ~45 references
  across `evaluator.py` and eight test modules; `load_manifest` has 26.
  The evaluator work is mechanical (one emit replaces recorder + bus
  pairs) but touches the hot path of a 2,363-line module.
- **Two sources of truth during dual-write.** Bounded by making
  `db rebuild` + a golden comparison against `load_manifest` a CI test
  for the whole transition.
- **Unbounded growth.** `events` grows with every task; a workspace with
  10,000 runs of 1,000 tasks is ~10⁷ rows, fine for SQLite but not free.
  Retention: `ginkgo db prune --events-older-than` drops event rows for
  runs whose projections and snapshot exist. Never drop projections.
- **Schema churn.** Hand-written migrations versioned in
  `schema_version`; every event payload carries `v`. Additive changes
  only within a major version.
- **Over-reach.** It is tempting to build staleness, asset selection, a
  deep "call-hash" cache tier at the same time. The store
  *enables* those; this plan only records what they need
  (`code_version`, `data_version`, `edges`) and stops.
- **Scope of `store.sql`.** A raw SQL escape hatch commits to the schema
  as a public surface. Ship it read-only and label the schema versioned
  but not stable until v1.

Alternatives considered: DuckDB (better analytics, worse for many small
transactional writes and not stdlib — Parquet export covers analytics);
keep files and add only a derived SQLite index (the shape the Studio plan draft assumed; it leaves every write-side pain in 1.2 unsolved
and duplicates readers); Postgres-first (wrong for a local-first tool).

## 5. Phasing

Each phase is shippable on its own and leaves the CLI behaviour
unchanged unless stated.

0. **Schema and protocol** (S). Land `ProvenanceStore` protocol, SQLite
   implementation, migrations, `WorkspaceLayout.db`, schema tests.
   Unify event vocabulary on paper (3.4) and add the new event types to
   `runtime/events.py` with JSON schemas.
1. **Ledger + run projections, dual-write** (M). Recorder becomes an
   `EventBus` subscriber writing `events`, `runs`, `tasks`, `attempts`,
   `task_inputs/outputs`, `edges`. `ginkgo db rebuild` from run dirs.
   `RunSummary`, `inspect run`, `debug`, notifications, report read from
   the store. Golden test: store-derived `RunSummary` == manifest-derived.
   Sub-workflow child linked via `parent_run_id` (child receives it by
   env var) instead of stdout parsing.
2. **Cache and digest index** (M). `cache_entries`, components,
   `cache_artifacts`, `stat_index`, `digest_memo` (closes #245),
   `materializations`, `artifacts`. `cache ls/explain/prune` on the
   store; prune gains `--least-recently-hit`; `cache stats`. Cache
   lookup by index instead of `is_dir`.
3. **Asset catalog and lineage** (M). `asset_keys/versions/aliases` and
   `edges` replace the YAML catalog; `asset ls/show/versions` unchanged;
   new `ginkgo lineage`, `store.why`. Record `code_version` /
   `data_version` per asset version.
4. **Query surface** (S–M). `ginkgo runs`, `ginkgo history`,
   `ginkgo query`, `ginkgo export --openlineage|--ro-crate`, public
   `ginkgo.query` docs. Studio plan draft revised to consume it.
5. **Retire file catalogs** (S). Remove `events.jsonl` writes and the
   replay path, the JSON/YAML side stores; `manifest.yaml` becomes an
   export; `db prune`. Update `docs/architecture/provenance.md`,
   `caching.md`, `assets.md`, `constraints.md`.

Later, enabled but not planned here: asset staleness (`stale/fresh/
unknown` from `data_version`), deep call-hash validation, any HTTP API,
Postgres backend, `push`/`pull` of provenance between workspaces.

## 6. Success criteria

- Every CLI command that reads provenance today produces identical
  output from the store, verified against manifest-derived output on the
  example corpus and the benchmark harness.
- `rm .ginkgo/ginkgo.db && ginkgo db rebuild` restores everything the
  CLI shows for existing workspaces.
- Warm-run overhead from provenance writes stays at or below today's
  `provenance_write_seconds` on the benchmark baselines; cold-run cache
  saves do not regress.
- A 100 GB unchanged `folder` input is stat-walked, not re-hashed, on
  the second run (#245).
- `cache explain` names the component that moved for a fanned-out task
  by comparing against the entry *that node* hit previously.
- `ginkgo lineage table:rows` traverses asset → producing task → run →
  consumed inputs → their producing runs, across runs.
- Two concurrent `ginkgo run`s in one workspace complete without
  corrupting cache, asset, or run state.
- Any later consumer could be built against `ginkgo.query` alone, with
  no file parsing and no indexer of its own.

## 7. Decisions (settled 2026-08-28)

All five were put to the maintainer. Settled as: (1) DB canonical, manifest exported; (2) per-workspace `.ginkgo/ginkgo.db`; (3) NFS: warn once and continue; (4) keep `manifest.yaml`, drop `params.yaml`; (5) switch `AssetRef` resolution with the CLI readers. Additionally: `meta.json`/`refs/*.json` are dropped — the DB is the only cache index; a read-only raw-SQL escape hatch is in scope; and **no backwards compatibility is required** — pre-ledger workspaces are not migrated, and no legacy readers are written. Tracked in issue #247. The original framing follows for the record.

1. **DB is canonical index + exported snapshot** (recommended) versus
   files canonical + DB as a disposable cache. The former solves 1.2;
   the latter is what the Studio plan draft assumed.
2. **Location**: per-workspace `.ginkgo/ginkgo.db` (recommended) versus
   per-user `~/.ginkgo/` shared across workspaces (helps env/staging
   sharing, #176, but muddies rebuild semantics).
3. **NFS stance**: warn-and-continue versus refuse without an override.
4. **Keep `manifest.yaml` as an export indefinitely**, or deprecate after
   a release. Recommended: keep; it is cheap and makes a run directory
   archivable on its own.
5. Whether Phase 3 should also switch `AssetRef` resolution
   (`resolve_version`, `latest`) to the store immediately, or leave the
   evaluator on YAML until the CLI readers are proven. Recommended:
   switch together — two catalogs is worse than one migration.
