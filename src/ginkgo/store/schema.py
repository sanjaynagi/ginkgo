"""The ledger schema.

The store is one append-only ``events`` table plus projection tables derived
from it. Every table exists from the start even though later phases are what
populate most of them: an empty table costs nothing.

Before 1.0 there are no migrations. Any schema change edits ``_SCHEMA`` in
place and bumps :data:`SCHEMA_VERSION`. A workspace written at any other
version is refused with an error telling the user to delete it: it holds
derived data, and the runs it describes can be re-run.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from sqlite3 import Connection

from ginkgo.store.errors import CorruptStoreError, StaleWorkspaceError

__all__ = ["SCHEMA_VERSION", "create_schema", "schema_version"]


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
  payload   TEXT NOT NULL              -- JSON of the remaining event fields
);
CREATE INDEX events_run      ON events(run_id, seq);

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

-- tracking: how the cache key tracks the input, content|version|path|value|output|untracked
-- (issue #307). Metadata only: nothing reads it to decide a cache hit.
CREATE TABLE task_inputs (
  run_id TEXT NOT NULL, task_id TEXT NOT NULL, param TEXT NOT NULL,
  value_type TEXT, value_summary TEXT, digest TEXT,
  remote_uri TEXT, tracking TEXT,
  PRIMARY KEY (run_id, task_id, param)
);
CREATE TABLE task_outputs (
  run_id TEXT NOT NULL, task_id TEXT NOT NULL, position INTEGER NOT NULL, name TEXT,
  value_type TEXT, path TEXT, artifact_id TEXT,
  PRIMARY KEY (run_id, task_id, position)
);

-- one edge table for every graph
CREATE TABLE edges (
  run_id TEXT NOT NULL, src_kind TEXT NOT NULL, src_id TEXT NOT NULL,
  dst_kind TEXT NOT NULL, dst_id TEXT NOT NULL, edge TEXT NOT NULL,
  PRIMARY KEY (run_id, src_kind, src_id, dst_kind, dst_id, edge)
);
CREATE INDEX edges_dst ON edges(dst_kind, dst_id);
-- kinds: task | artifact | run
-- edges: depends_on | dynamic_depends_on | produced | child_of

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

-- artifacts and remote inputs (Phases 2-3)
CREATE TABLE artifacts (
  artifact_id TEXT PRIMARY KEY, kind TEXT NOT NULL, digest_algorithm TEXT NOT NULL,
  digest_hex TEXT NOT NULL DEFAULT '',
  extension TEXT, size INTEGER, created_at TEXT NOT NULL, storage_backend TEXT, remote_uri TEXT
);
CREATE TABLE materializations (path TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, size INTEGER, mtime_ns INTEGER);
CREATE TABLE staging_entries (uri TEXT PRIMARY KEY, digest TEXT, etag TEXT, version_id TEXT,
  size INTEGER, staged_at TEXT, blob_path TEXT, last_used_at TEXT);
CREATE TABLE env_materializations (env_hash TEXT NOT NULL, host TEXT NOT NULL,
  materialized_digest TEXT, seen_at TEXT NOT NULL, PRIMARY KEY (env_hash, host));
"""


SCHEMA_VERSION = 7
"""The version :func:`create_schema` writes; bump it with every edit to ``_SCHEMA``."""


def schema_version(conn: Connection) -> int:
    """Return the schema version recorded in *conn*, or ``0`` if it has none.

    Parameters
    ----------
    conn : Connection
        Any connection to the database, read-only or not.

    Returns
    -------
    int
        The recorded version, ``0`` for a database with no
        ``schema_version`` table.

    Raises
    ------
    CorruptStoreError
        If the table exists but holds no usable version — a database that was
        truncated or hand-edited.
    """
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchall()
    if not tables:
        return 0
    row = conn.execute("SELECT max(version) FROM schema_version").fetchone()
    version = None if row is None else row[0]
    if not isinstance(version, int):
        raise CorruptStoreError(
            path=_database_path(conn), detail="its schema_version table holds no version"
        )
    return version


def create_schema(conn: Connection) -> None:
    """Create the schema on an empty database, and check it on any other.

    A database already at :data:`SCHEMA_VERSION` is left alone, which is the
    common case and costs one query. The DDL runs in a single transaction, so
    an interrupted creation leaves an empty database rather than half a schema.

    Two processes can reach an empty workspace at the same moment, so the
    version is read again under the write lock: whichever loses the race sees
    the other's schema and creates nothing, rather than re-running the DDL and
    failing on a table that now exists.

    Parameters
    ----------
    conn : Connection
        A write-mode connection.

    Raises
    ------
    StaleWorkspaceError
        If the database is at any other version. Without this check it would
        open cleanly and then fail on the first query naming a column it does
        not have.
    """
    if _check_version(conn):
        return

    # Statements are executed one at a time rather than through
    # ``executescript``, which commits any open transaction before it runs and
    # so would break the schema into separately durable pieces.
    conn.execute("BEGIN IMMEDIATE")
    try:
        if not _check_version(conn):
            for statement in _statements(_SCHEMA):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                (SCHEMA_VERSION, datetime.now(UTC).isoformat()),
            )
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def _check_version(conn: Connection) -> bool:
    """Return whether *conn* has the current schema, ``False`` if it has none.

    Raises
    ------
    StaleWorkspaceError
        If the database is at any other version.
    """
    current = schema_version(conn)
    if current not in {0, SCHEMA_VERSION}:
        raise StaleWorkspaceError(
            path=_database_path(conn), found=current, expected=SCHEMA_VERSION
        )
    return current == SCHEMA_VERSION


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
