"""The ledger schema and its migrations.

The store is one append-only ``events`` table plus projection tables derived
from it. Every table exists from version 1 even though later phases are what
populate most of them: one migration is easier to reason about than eight, and
an empty table costs nothing.

Before 1.0 the schema is edited in place rather than migrated. Ginkgo has a
handful of users and a workspace is derived data — the events it holds describe
runs that can be re-run — so carrying migration steps for every column change
buys less than it costs in schema that has to be read as archaeology. A database
written by an older schema is refused with an error telling the user to delete
the workspace, which is the whole upgrade procedure.

The machinery for stepped migrations is still here, and this becomes the usual
"never edit a shipped step; add another one" once there is history worth
preserving.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from sqlite3 import Connection

from ginkgo.store.errors import SchemaVersionError, StaleWorkspaceError

__all__ = ["MIGRATIONS", "SCHEMA_VERSION", "migrate", "schema_version"]


_SCHEMA = """
CREATE TABLE schema_version (version INTEGER NOT NULL, applied_at TEXT NOT NULL);

-- ledger
CREATE TABLE events (
  seq       INTEGER PRIMARY KEY,
  run_id    TEXT NOT NULL,
  ts        TEXT NOT NULL,             -- ISO-8601 UTC, from GinkgoEvent.ts
  type      TEXT NOT NULL,             -- GinkgoEvent.event
  v         INTEGER NOT NULL DEFAULT 1,
  task_id   TEXT,                      -- 'task_0007'; NULL for run-level events
  attempt   INTEGER,
  cache_key TEXT,
  asset_key TEXT,                      -- 'table:rows'
  payload   TEXT NOT NULL              -- JSON of the remaining event fields
);
CREATE INDEX events_run      ON events(run_id, seq);
CREATE INDEX events_type_ts  ON events(type, ts);
CREATE INDEX events_asset    ON events(asset_key, seq) WHERE asset_key IS NOT NULL;

-- run projections
CREATE TABLE runs (
  run_id TEXT PRIMARY KEY, workflow TEXT NOT NULL, status TEXT NOT NULL,
  started_at TEXT NOT NULL, finished_at TEXT, error TEXT,
  jobs INTEGER, cores INTEGER, memory INTEGER,
  params TEXT, param_sources TEXT, resources TEXT, timings TEXT,   -- JSON
  parent_run_id TEXT, parent_task_id TEXT,
  ginkgo_version TEXT, snapshot_written INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX runs_started ON runs(started_at);
CREATE INDEX runs_parent  ON runs(parent_run_id) WHERE parent_run_id IS NOT NULL;

CREATE TABLE tasks (
  run_id TEXT NOT NULL, task_id TEXT NOT NULL, node_id INTEGER NOT NULL,
  name TEXT NOT NULL, display_label TEXT, kind TEXT NOT NULL, execution_mode TEXT NOT NULL, env TEXT,
  status TEXT NOT NULL, cached INTEGER, cache_key TEXT, source_hash TEXT, version INTEGER,
  env_hash TEXT, extra_source_hash TEXT,
  started_at TEXT, finished_at TEXT, attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER,
  exit_code INTEGER, failure TEXT, output_summary TEXT, resource_usage TEXT, timings TEXT, extra TEXT, -- JSON
  stdout_log TEXT, stderr_log TEXT, execution_backend TEXT, remote_job_id TEXT,
  -- Projected out of resource_usage so asking what a task typically needs is a
  -- query rather than JSON unpacking. Null for anything unmeasured: a cache
  -- hit, or a task that never started. Declared and effective memory differ
  -- when memory_retry_multiplier escalated a retry, and comparing a measured
  -- peak against the declaration alone would call that attempt an overrun.
  peak_rss_bytes INTEGER, cpu_seconds REAL,
  declared_threads INTEGER, declared_memory_gb REAL, effective_memory_gb REAL,
  PRIMARY KEY (run_id, task_id)
);
CREATE INDEX tasks_name      ON tasks(name, started_at);
CREATE INDEX tasks_cache_key ON tasks(cache_key) WHERE cache_key IS NOT NULL;
-- history --resources reads every measured execution of one task name.
CREATE INDEX tasks_measured  ON tasks(name) WHERE peak_rss_bytes IS NOT NULL;

CREATE TABLE attempts (
  run_id TEXT NOT NULL, task_id TEXT NOT NULL, attempt INTEGER NOT NULL,
  started_at TEXT, finished_at TEXT, status TEXT NOT NULL,
  exit_code INTEGER, failure TEXT, retry_delay_s REAL, execution_backend TEXT, remote_job_id TEXT,
  PRIMARY KEY (run_id, task_id, attempt)
);

CREATE TABLE task_inputs (
  run_id TEXT NOT NULL, task_id TEXT NOT NULL, param TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0,
  value_type TEXT, value_summary TEXT, digest TEXT,
  artifact_id TEXT, asset_key TEXT, asset_version_id TEXT, remote_uri TEXT,
  PRIMARY KEY (run_id, task_id, param, position)
);
CREATE TABLE task_outputs (
  run_id TEXT NOT NULL, task_id TEXT NOT NULL, position INTEGER NOT NULL, name TEXT,
  value_type TEXT, path TEXT, artifact_id TEXT, asset_key TEXT, asset_version_id TEXT,
  PRIMARY KEY (run_id, task_id, position)
);

-- one edge table for every graph
CREATE TABLE edges (
  run_id TEXT NOT NULL, src_kind TEXT NOT NULL, src_id TEXT NOT NULL,
  dst_kind TEXT NOT NULL, dst_id TEXT NOT NULL, edge TEXT NOT NULL,
  PRIMARY KEY (run_id, src_kind, src_id, dst_kind, dst_id, edge)
);
CREATE INDEX edges_dst ON edges(dst_kind, dst_id);
-- `lineage --downstream` walks the other end of the same rows.
CREATE INDEX edges_src ON edges(src_kind, src_id);
-- kinds: task | artifact | asset_version | run
-- edges: depends_on | dynamic_depends_on | produced | consumed | derived_from | child_of

-- cache index (Phase 2)
CREATE TABLE cache_entries (
  cache_key TEXT PRIMARY KEY, function TEXT NOT NULL, version INTEGER, source_hash TEXT,
  extra_source_hash TEXT, env TEXT, env_hash TEXT, env_materialized_digest TEXT,
  inputs TEXT, input_hashes TEXT, extra TEXT,               -- JSON
  output_codec TEXT, size_bytes INTEGER,
  created_run_id TEXT, created_at TEXT NOT NULL,
  hit_count INTEGER NOT NULL DEFAULT 0, last_hit_at TEXT
);
CREATE INDEX cache_function ON cache_entries(function, created_at);
CREATE TABLE cache_artifacts (cache_key TEXT NOT NULL, path TEXT NOT NULL, artifact_id TEXT NOT NULL,
  PRIMARY KEY (cache_key, path));
CREATE INDEX cache_artifacts_artifact ON cache_artifacts(artifact_id);
CREATE TABLE stat_index (stat_key TEXT PRIMARY KEY, cache_key TEXT NOT NULL);
CREATE TABLE digest_memo (
  kind TEXT NOT NULL, fingerprint TEXT NOT NULL, digest TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  PRIMARY KEY (kind, fingerprint)
);

-- artifacts, assets, remote inputs (Phases 2-3)
CREATE TABLE artifacts (
  artifact_id TEXT PRIMARY KEY, kind TEXT NOT NULL, digest_algorithm TEXT NOT NULL,
  digest_hex TEXT NOT NULL DEFAULT '',
  extension TEXT, size INTEGER, created_at TEXT NOT NULL, storage_backend TEXT, remote_uri TEXT
);
CREATE TABLE materializations (path TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, size INTEGER, mtime_ns INTEGER);
CREATE TABLE asset_versions (
  asset_key TEXT NOT NULL, version_id TEXT NOT NULL, kind TEXT NOT NULL,
  artifact_id TEXT NOT NULL, content_hash TEXT NOT NULL,
  run_id TEXT, task_id TEXT, producer_task TEXT, cache_key TEXT, created_at TEXT NOT NULL,
  code_version TEXT, data_version TEXT,
  metadata TEXT,                                            -- JSON
  PRIMARY KEY (asset_key, version_id)
);
CREATE INDEX asset_versions_created ON asset_versions(asset_key, created_at);
-- Lineage edges name a version and not the asset it belongs to, so resolving
-- one back to its row is a lookup by version id alone.
CREATE INDEX asset_versions_version ON asset_versions(version_id);
CREATE TABLE asset_aliases (asset_key TEXT NOT NULL, alias TEXT NOT NULL, version_id TEXT NOT NULL,
  PRIMARY KEY (asset_key, alias));
CREATE TABLE staging_entries (uri TEXT PRIMARY KEY, digest TEXT, etag TEXT, version_id TEXT,
  size INTEGER, staged_at TEXT, blob_path TEXT, last_used_at TEXT);
CREATE TABLE env_materializations (env_hash TEXT NOT NULL, host TEXT NOT NULL,
  materialized_digest TEXT, seen_at TEXT NOT NULL, PRIMARY KEY (env_hash, host));
"""


MIGRATIONS: list[tuple[int, str | Callable[[Connection], None]]] = [
    (1, _SCHEMA),
]
"""Every schema step, in the order they are applied, keyed by resulting version.

One step, while the schema is still edited in place. A database written by a
different version of the schema is not migrated to this one: it is refused, and
the user deletes the workspace.
"""

SCHEMA_VERSION = MIGRATIONS[-1][0]
"""The version a freshly migrated database ends up at."""


def schema_version(conn: Connection) -> int:
    """Return the schema version recorded in *conn*, or ``0`` if unmigrated.

    Parameters
    ----------
    conn : Connection
        Any connection to the database, read-only or not.

    Returns
    -------
    int
        The highest applied version, ``0`` for a database with no
        ``schema_version`` table.

    Raises
    ------
    SchemaVersionError
        If the table exists but holds no usable version — a database that was
        truncated or hand-edited mid-migration.
    """
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchall()
    if not tables:
        return 0
    row = conn.execute("SELECT max(version) FROM schema_version").fetchone()
    version = None if row is None else row[0]
    if not isinstance(version, int):
        raise SchemaVersionError(path=_database_path(conn), found=0, expected=SCHEMA_VERSION)
    return version


def migrate(conn: Connection) -> int:
    """Apply every migration step *conn* is missing and return the new version.

    Applying nothing is the common case and costs one query. All outstanding
    steps share a single transaction, so an interrupted migration leaves the
    database at the version it started from rather than part-way through.

    Two processes can reach an unmigrated workspace at the same moment, so the
    version is read again under the write lock: whichever loses the race sees
    the other's work and applies nothing, rather than re-running the DDL and
    failing on a table that now exists.

    Parameters
    ----------
    conn : Connection
        A write-mode connection.

    Returns
    -------
    int
        The schema version after migrating.

    Raises
    ------
    StaleWorkspaceError
        If the database is at a version this schema has no step for. Pre-1.0
        the schema is edited in place, so an older database cannot be brought
        forward: without this check it would open cleanly and then fail on the
        first query naming a column it does not have.
    """
    current = schema_version(conn)
    if current not in (0, SCHEMA_VERSION):
        raise StaleWorkspaceError(
            path=_database_path(conn), found=current, expected=SCHEMA_VERSION
        )
    if not [version for version, _ in MIGRATIONS if version > current]:
        return current

    # Statements are executed one at a time rather than through
    # ``executescript``, which commits any open transaction before it runs and
    # so would break a partly applied migration into separately durable pieces.
    conn.execute("BEGIN IMMEDIATE")
    try:
        current = schema_version(conn)
        pending = [(version, step) for version, step in MIGRATIONS if version > current]
        if not pending:
            conn.execute("COMMIT")
            return current
        for version, step in pending:
            if isinstance(step, str):
                for statement in _statements(step):
                    conn.execute(statement)
            else:
                step(conn)
            conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                (version, datetime.now(UTC).isoformat()),
            )
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
    return pending[-1][0]


def _statements(script: str) -> Iterator[str]:
    """Split a DDL script into executable statements.

    Comments are removed before splitting because the schema's comments contain
    semicolons of their own. Neither they nor any string literal in the schema
    contains ``--``, so cutting each line at the first one is exact here — and
    it lets the DDL above stay byte-identical to the design it came from.
    """
    stripped = "\n".join(line.partition("--")[0].rstrip() for line in script.splitlines())
    for fragment in stripped.split(";"):
        if fragment.strip():
            yield fragment.strip()


def _database_path(conn: Connection) -> Path:
    """Return the file backing *conn*, for error messages."""
    row = conn.execute("PRAGMA database_list").fetchone()
    return Path(row[2]) if row is not None and row[2] else Path(":memory:")
