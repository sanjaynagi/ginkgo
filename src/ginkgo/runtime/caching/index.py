"""The cache's rows in the provenance ledger.

Everything ginkgo knows about a cache entry — which function wrote it, the
hashes its key was built from, the artifacts it owns, whether the environment
it was written against still matches — lives in the database, and this is the
module that reads and writes those rows. The bytes stay on disk: ``output.json``
under ``cache/<key>/`` and the CAS blobs under ``artifacts/``.

The index sits in ``runtime/`` because the shape of a cache entry is a runtime
concept; ``store/`` below it knows only tables and transactions.

Unlike the run tables, the cache tables are **not** projections of the event
ledger. They are a direct-write index: `CacheIndex` is the only thing that
writes them, synchronously, on its own connection, because a cache save has to
be visible to the `load` that may follow it microseconds later and the
recorder's connection belongs to its writer thread. Two connections over one
WAL database is what WAL is for, and every write here is either an
``INSERT OR IGNORE`` on a content-addressed key or an idempotent upsert, so two
runs saving the same entry at once agree by construction.

Nothing constructs an index as a side effect. Opening one is a decision — to
write (`open`), to read (`for_reading`), or to keep the rows to this process
(`in_memory`) — because the first of those creates and migrates a database.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ginkgo.formatting import now_iso
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.store.jsonio import dumps_or_none, loads
from ginkgo.store.protocol import ProjectionOp, ProvenanceStore
from ginkgo.store.sqlite import MEMORY, open_store

__all__ = ["ENTRY_COLUMNS", "CacheEntry", "CacheIndex"]


ENTRY_COLUMNS = (
    "cache_key",
    "function",
    "version",
    "source_hash",
    "extra_source_hash",
    "env",
    "env_hash",
    "env_materialized_digest",
    "inputs",
    "input_hashes",
    "extra",
    "size_bytes",
    "created_run_id",
    "created_at",
)
"""What a reader selects to describe one entry, in row order."""

_JSON_COLUMNS = frozenset({"env_hash", "inputs", "input_hashes", "extra"})
"""Entry columns whose text is JSON, decoded on the way out."""

_INSERT_ENTRY = """
INSERT OR IGNORE INTO cache_entries (
  cache_key, function, version, source_hash, extra_source_hash,
  env, env_hash, env_materialized_digest, inputs, input_hashes,
  extra, size_bytes, created_run_id, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_ARTIFACT = """
INSERT INTO artifacts (
  artifact_id, kind, digest_algorithm, digest_hex, extension, size,
  created_at, storage_backend, remote_uri
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (artifact_id) DO UPDATE SET
  kind=excluded.kind, digest_algorithm=excluded.digest_algorithm,
  digest_hex=excluded.digest_hex, extension=excluded.extension,
  size=excluded.size, storage_backend=excluded.storage_backend,
  remote_uri=coalesce(excluded.remote_uri, artifacts.remote_uri)
"""

_ARTIFACT_COLUMNS = (
    "artifact_id, kind, digest_algorithm, digest_hex, extension, size, "
    "created_at, storage_backend, remote_uri"
)


class CacheEntry:
    """One ``cache_entries`` row.

    Columns are read by name — ``entry["source_hash"]`` — and the ones holding
    JSON come back as the objects they encode, so every column is reached the
    same way whatever its storage type.
    """

    __slots__ = ("_row",)

    def __init__(self, row: Mapping[str, Any]) -> None:
        self._row = row

    @classmethod
    def from_row(cls, row: sqlite3.Row | Mapping[str, Any]) -> CacheEntry:
        """Return the entry a ``cache_entries`` row describes.

        For a reader that selected :data:`ENTRY_COLUMNS` and did not join the
        entry's artifacts — asking what an entry was keyed on does not need
        them.
        """
        return cls(dict(row) | {"artifact_ids": {}})

    def __getitem__(self, column: str) -> Any:
        value = self._row[column]
        return loads(value) if column in _JSON_COLUMNS else value

    @property
    def artifact_ids(self) -> dict[str, str]:
        """Output path to artifact id, as recorded by the run that saved it."""
        return dict(self._row["artifact_ids"])

    @property
    def extra(self) -> dict[str, Any] | None:
        """Task-kind-specific metadata, or ``None`` when the entry recorded none."""
        parsed = self["extra"]
        return parsed if isinstance(parsed, dict) else None

    @property
    def env_materialized_digest(self) -> str | None:
        """The environment digest measured where the entry was written."""
        value = self._row["env_materialized_digest"]
        return str(value) if value is not None else None

    def as_meta(self) -> dict[str, Any]:
        """Return the entry as the flat mapping the cache records it from.

        The inverse of what :meth:`~ginkgo.runtime.caching.cache.CacheStore.save`
        passes in, so :func:`~ginkgo.runtime.caching.cache.key_components` can
        label an entry read back out exactly as it labelled the one saved.
        """
        return {column: self[column] for column in ENTRY_COLUMNS}


class CacheIndex:
    """The cache's view of the provenance database.

    Parameters
    ----------
    store : ProvenanceStore
        An open store. The index owns it and closes it with :meth:`close`.

    Notes
    -----
    Every method takes the index's lock, so the evaluator's threads can share
    one index and one connection.
    """

    def __init__(self, *, store: ProvenanceStore) -> None:
        self._store = store
        self._lock = threading.RLock()
        self._seen_digests: set[tuple[str, str]] = set()

    @classmethod
    def open(cls, *, path: Path, readonly: bool = False) -> CacheIndex:
        """Open the index for writing, creating and migrating the database.

        Parameters
        ----------
        path : Path
            The database file, normally ``WorkspaceLayout.db``.
        readonly : bool, optional
            Open a reader instead: it never creates the file and never
            migrates it, and fails if there is nothing there to read.

        Returns
        -------
        CacheIndex
        """
        return cls(store=open_store(Path(path), readonly=readonly, thread_shared=True))

    @classmethod
    def in_memory(cls) -> CacheIndex:
        """Return an index over a private database that touches no filesystem.

        What a remote worker gets: it has the artifacts it staged but not the
        workspace that indexes them, and creating a database in a pod's scratch
        directory would record rows nothing will ever read.
        """
        return cls(store=open_store(MEMORY, thread_shared=True))

    @classmethod
    def for_reading(cls, path: Path) -> CacheIndex:
        """Return a read-only index over *path*, or an empty one if it is absent.

        A workspace nobody has run anything in has no database and no cache,
        which is an answer rather than an error — and a read path must never be
        the thing that creates one.
        """
        if not Path(path).is_file():
            return cls.in_memory()
        return cls.open(path=path, readonly=True)

    def close(self) -> None:
        """Flush what was deferred and release the connection.

        Safe to call more than once.
        """
        with self._lock:
            self._flush_seen_digests()
            self._store.close()

    def __enter__(self) -> CacheIndex:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- cache entries -------------------------------------------------------

    def record_entry(
        self,
        *,
        cache_key: str,
        meta: Mapping[str, Any],
        artifact_ids: Mapping[str, str],
        size_bytes: int,
        run_id: str | None,
    ) -> None:
        """Record one cache entry and the artifacts it owns.

        Both land in one transaction: an entry whose artifacts were lost would
        have its blobs collected as orphans.

        Parameters
        ----------
        cache_key : str
            The content-addressed key.
        meta : Mapping[str, Any]
            The entry's facts, in the flat shape
            :func:`~ginkgo.runtime.caching.cache.key_components` reads.
        artifact_ids : Mapping[str, str]
            Output path to artifact id.
        size_bytes : int
            Size of the entry's ``output.json``.
        run_id : str | None
            The run that wrote it.
        """
        ops = [
            ProjectionOp(
                sql=_INSERT_ENTRY,
                params=(
                    cache_key,
                    meta.get("function"),
                    meta.get("version"),
                    meta.get("source_hash"),
                    meta.get("extra_source_hash"),
                    meta.get("env"),
                    dumps_or_none(meta.get("env_hash")),
                    meta.get("env_materialized_digest"),
                    dumps_or_none(meta.get("inputs")),
                    dumps_or_none(meta.get("input_hashes")),
                    dumps_or_none(meta.get("extra")),
                    size_bytes,
                    run_id,
                    meta.get("created_at") or now_iso(),
                ),
            )
        ]
        ops += [
            ProjectionOp(
                sql="INSERT OR IGNORE INTO cache_artifacts (cache_key, path, artifact_id) "
                "VALUES (?, ?, ?)",
                params=(cache_key, str(path), str(artifact_id)),
            )
            for path, artifact_id in artifact_ids.items()
        ]
        self._write(*ops)

    def entry(self, cache_key: str) -> CacheEntry | None:
        """Return one cache entry, or ``None`` when the index has no such row."""
        with self._lock:
            rows = self._store.query(
                f"SELECT {', '.join(ENTRY_COLUMNS)} FROM cache_entries WHERE cache_key = ?",  # noqa: S608
                (cache_key,),
            )
            if not rows:
                return None
            artifacts = self._store.query(
                "SELECT path, artifact_id FROM cache_artifacts WHERE cache_key = ?",
                (cache_key,),
            )
        row = dict(rows[0])
        row["artifact_ids"] = {str(a["path"]): str(a["artifact_id"]) for a in artifacts}
        return CacheEntry(row)

    def cache_keys(self) -> list[str]:
        """Return every cache key the index holds, sorted."""
        with self._lock:
            rows = self._store.query("SELECT cache_key FROM cache_entries ORDER BY cache_key")
        return [str(row["cache_key"]) for row in rows]

    def cache_artifact_ids(self) -> list[str]:
        """Return every artifact id a cache entry names, sorted."""
        with self._lock:
            rows = self._store.query(
                "SELECT DISTINCT artifact_id FROM cache_artifacts ORDER BY artifact_id"
            )
        return [str(row["artifact_id"]) for row in rows]

    def referenced_artifact_ids(self) -> set[str]:
        """Return every artifact id a cache entry or asset version still points at.

        Both halves are one query so a garbage collector cannot see a
        half-updated picture and delete bytes the other half still wants.
        """
        with self._lock:
            rows = self._store.query(
                "SELECT artifact_id FROM cache_artifacts "
                "UNION SELECT artifact_id FROM asset_versions"
            )
        return {str(row["artifact_id"]) for row in rows}

    def record_hit(self, cache_key: str, *, at: str | None = None) -> None:
        """Count one hit against an entry.

        Written here rather than projected from ``TaskCacheHit`` so that every
        ``cache_entries`` column has one writer on one connection: a hit landing
        through the ledger's writer while another process held the write lock
        for a save would have updated no rows at all.
        """
        self._write(
            ProjectionOp(
                sql="UPDATE cache_entries SET hit_count = hit_count + 1, last_hit_at = ? "
                "WHERE cache_key = ?",
                params=(at or now_iso(), cache_key),
            )
        )

    def forget_entries(self, cache_keys: Sequence[str]) -> None:
        """Drop the rows for entries whose bytes have been removed."""
        if not cache_keys:
            return
        placeholders = ", ".join("?" for _ in cache_keys)
        self._write(
            *(
                ProjectionOp(
                    sql=f"DELETE FROM {table} WHERE cache_key IN ({placeholders})",  # noqa: S608
                    params=tuple(cache_keys),
                )
                for table in ("cache_entries", "cache_artifacts", "stat_index")
            )
        )

    # -- artifacts -----------------------------------------------------------

    def record_artifact(self, record: ArtifactRecord) -> None:
        """Record one artifact, replacing what is there.

        Artifact ids are content digests, so a second write of the same id
        describes the same bytes; the row is updated rather than ignored so
        that a publisher filling in ``remote_uri`` is not silently dropped.
        """
        self._write(
            ProjectionOp(
                sql=_INSERT_ARTIFACT,
                params=(
                    record.artifact_id,
                    record.kind,
                    record.digest_algorithm,
                    record.digest_hex,
                    record.extension,
                    record.size,
                    record.created_at,
                    record.storage_backend,
                    record.remote_uri,
                ),
            )
        )

    def artifact(self, artifact_id: str) -> ArtifactRecord | None:
        """Return one artifact record, or ``None`` when the index has no such row."""
        with self._lock:
            rows = self._store.query(
                f"SELECT {_ARTIFACT_COLUMNS} FROM artifacts WHERE artifact_id = ?",  # noqa: S608
                (artifact_id,),
            )
        if not rows:
            return None
        row = rows[0]
        return ArtifactRecord(
            artifact_id=str(row["artifact_id"]),
            kind=str(row["kind"]),
            digest_algorithm=str(row["digest_algorithm"]),
            digest_hex=str(row["digest_hex"]),
            extension=str(row["extension"] or ""),
            size=int(row["size"] or 0),
            created_at=str(row["created_at"]),
            storage_backend=str(row["storage_backend"] or "local"),
            remote_uri=row["remote_uri"],
        )

    def artifact_ids(self) -> list[str]:
        """Return every artifact id the index holds, sorted."""
        with self._lock:
            rows = self._store.query("SELECT artifact_id FROM artifacts ORDER BY artifact_id")
        return [str(row["artifact_id"]) for row in rows]

    def forget_artifact(self, artifact_id: str) -> None:
        """Drop one artifact's row, and any materialization pointing at it."""
        self._write(
            ProjectionOp(sql="DELETE FROM artifacts WHERE artifact_id = ?", params=(artifact_id,)),
            ProjectionOp(
                sql="DELETE FROM materializations WHERE artifact_id = ?", params=(artifact_id,)
            ),
        )

    # -- stat index ----------------------------------------------------------

    def stat_index_lookup(self, stat_key: str) -> str | None:
        """Return the content key a stat fingerprint last resolved to."""
        with self._lock:
            rows = self._store.query(
                "SELECT cache_key FROM stat_index WHERE stat_key = ?", (stat_key,)
            )
        return str(rows[0]["cache_key"]) if rows else None

    def record_stat_index(self, *, stat_key: str, cache_key: str) -> None:
        """Point a stat fingerprint at the content key it resolved to."""
        self._write(
            ProjectionOp(
                sql="INSERT INTO stat_index (stat_key, cache_key) VALUES (?, ?) "
                "ON CONFLICT (stat_key) DO UPDATE SET cache_key=excluded.cache_key",
                params=(stat_key, cache_key),
            )
        )

    # -- materializations ----------------------------------------------------

    def record_materialization(self, *, path: Path, artifact_id: str) -> None:
        """Record the stat of a just-materialized artifact path.

        A path that does not exist records nothing: there is no stat to take,
        and an absent row simply means the next check hashes the content.
        """
        resolved = path.resolve()
        try:
            st = resolved.stat()
        except OSError:
            return
        self._write(
            ProjectionOp(
                sql="INSERT INTO materializations (path, artifact_id, size, mtime_ns) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (path) DO UPDATE SET "
                "artifact_id=excluded.artifact_id, size=excluded.size, "
                "mtime_ns=excluded.mtime_ns",
                params=(str(resolved), artifact_id, st.st_size, st.st_mtime_ns),
            )
        )

    def materialization_matches(self, *, path: Path, artifact_id: str) -> bool:
        """Return whether *path* still has the stat it had when materialized.

        The answer is only trusted for files: a directory's mtime does not move
        when a child's contents change, so callers ask about files.
        """
        resolved = path.resolve()
        with self._lock:
            rows = self._store.query(
                "SELECT artifact_id, size, mtime_ns FROM materializations WHERE path = ?",
                (str(resolved),),
            )
        if not rows or str(rows[0]["artifact_id"]) != artifact_id:
            return False
        try:
            st = resolved.stat()
        except OSError:
            return False
        return st.st_size == rows[0]["size"] and st.st_mtime_ns == rows[0]["mtime_ns"]

    # -- digest memo ---------------------------------------------------------

    def digest(self, *, kind: str, fingerprint: str) -> str | None:
        """Return a remembered content digest, if this content has been hashed.

        A hit is noted in memory and written to ``last_seen`` once, at
        :meth:`close`. A write transaction per hashed file would put the memo's
        cost back on the warm run it exists to make cheap.

        Parameters
        ----------
        kind : str
            ``"file"`` or ``"directory"``.
        fingerprint : str
            The stat identity the digest was computed for.

        Returns
        -------
        str | None
            The digest, or ``None`` when content with this identity has not
            been hashed before.
        """
        with self._lock:
            rows = self._store.query(
                "SELECT digest FROM digest_memo WHERE kind = ? AND fingerprint = ?",
                (kind, fingerprint),
            )
            if not rows:
                return None
            self._seen_digests.add((kind, fingerprint))
        return str(rows[0]["digest"])

    def record_digest(self, *, kind: str, fingerprint: str, digest: str) -> None:
        """Remember the digest of content with this stat identity."""
        self._write(
            ProjectionOp(
                sql="INSERT INTO digest_memo (kind, fingerprint, digest, last_seen) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (kind, fingerprint) DO UPDATE SET "
                "digest=excluded.digest, last_seen=excluded.last_seen",
                params=(kind, fingerprint, digest, now_iso()),
            )
        )

    # -- internals -----------------------------------------------------------

    def _flush_seen_digests(self) -> None:
        """Stamp every memo entry this process used, in one transaction."""
        if not self._seen_digests or self._store.readonly:
            return
        seen = sorted(self._seen_digests)
        self._seen_digests.clear()
        stamp = now_iso()
        self._write(
            *(
                ProjectionOp(
                    sql="UPDATE digest_memo SET last_seen = ? WHERE kind = ? AND fingerprint = ?",
                    params=(stamp, kind, fingerprint),
                )
                for kind, fingerprint in seen
            )
        )

    def _write(self, *ops: ProjectionOp) -> None:
        """Run *ops* in one transaction, holding the index's lock."""
        with self._lock, self._store.transaction():
            self._store.apply(ops)
