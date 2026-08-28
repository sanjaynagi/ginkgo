"""The asset catalog's rows in the provenance ledger.

Everything ginkgo knows about an asset version — which run and task produced
it, which artifact holds its bytes, which versions it was derived from, the
code and data versions it was built at — lives in the database. The bytes stay
in the content-addressed artifact store, exactly as before; only the catalog
metadata moved.

Like the cache index, the catalog is a
:class:`~ginkgo.runtime.direct_index.DirectIndex` rather than a projection of
the event ledger: registering a version has to read the parents registered
moments earlier — possibly by a sibling task on another thread in the same run
— to derive the child's ``data_version``, and the recorder's writer thread is
asynchronous. ``AssetMaterialized`` still records in the ledger *that* a
version was materialized; this module is what makes it findable.

There is no separate ``asset_keys`` table. An asset key is the set of versions
carrying it: its latest version, its version count and the kinds it has taken
are all questions about ``asset_versions``, and a summary row would only be a
second copy that could disagree.
"""

from __future__ import annotations

from typing import Any

from ginkgo.core.asset import AssetKey, AssetRef, AssetVersion
from ginkgo.core.hashing import hash_str
from ginkgo.runtime.direct_index import DirectIndex
from ginkgo.store.jsonio import dumps, loads
from ginkgo.store.protocol import ProjectionOp

__all__ = ["AssetStore"]


_VERSION_COLUMNS = (
    "asset_key, version_id, kind, artifact_id, content_hash, "
    "run_id, producer_task, created_at, metadata"
)
"""What a reader selects to rebuild an :class:`AssetVersion`, in row order."""

_INSERT_VERSION = """
INSERT INTO asset_versions (
  asset_key, version_id, kind, artifact_id, content_hash,
  run_id, task_id, producer_task, cache_key, created_at,
  code_version, data_version, metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (asset_key, version_id) DO UPDATE SET
  task_id=coalesce(excluded.task_id, asset_versions.task_id),
  cache_key=coalesce(excluded.cache_key, asset_versions.cache_key),
  code_version=coalesce(excluded.code_version, asset_versions.code_version),
  data_version=coalesce(excluded.data_version, asset_versions.data_version),
  metadata=excluded.metadata
"""


class AssetStore(DirectIndex):
    """The asset catalog's view of the provenance database.

    Parameters
    ----------
    store : ProvenanceStore
        An open store. The catalog owns it and closes it with
        :meth:`~ginkgo.runtime.direct_index.DirectIndex.close`, unless it was
        attached to another index's connection.

    Notes
    -----
    Every method takes the index's lock, so the evaluator's task threads can
    share one catalog and one connection with the cache index.
    """

    # -- writing -------------------------------------------------------------

    def register_version(
        self,
        *,
        version: AssetVersion,
        parents: tuple[AssetRef, ...] | list[AssetRef] = (),
        code_version: str | None = None,
        task_id: str | None = None,
        cache_key: str | None = None,
    ) -> AssetVersion:
        """Persist one immutable asset version and the lineage that produced it.

        The version row and its ``derived_from`` edges land in one
        transaction: a version whose parents were lost would be a version whose
        ``data_version`` cannot be explained.

        Parameters
        ----------
        version : AssetVersion
            Version metadata to persist.
        parents : tuple[AssetRef, ...] | list[AssetRef]
            The asset references the producing task consumed. Each becomes a
            ``derived_from`` edge and feeds the child's ``data_version``.
        code_version : str | None
            The producing task's source hash — for a driver kind, its source
            hash folded together with the driver file's. The identity of the
            code that produced this version.
        task_id : str | None
            The producing task within the run, ``'task_0007'``.
        cache_key : str | None
            The cache key of the producing task's entry, if it had one.

        Returns
        -------
        AssetVersion
            The version as given, so callers can register and use in one line.
        """
        parent_ids = [parent.version_id for parent in parents]
        data_version = self._data_version(code_version=code_version, parents=parents)
        ops = [
            ProjectionOp(
                sql=_INSERT_VERSION,
                params=(
                    str(version.key),
                    version.version_id,
                    version.kind,
                    version.artifact_id,
                    version.content_hash,
                    version.run_id,
                    task_id,
                    version.producer_task,
                    cache_key,
                    version.created_at,
                    code_version,
                    data_version,
                    dumps(version.metadata),
                ),
            )
        ]
        ops += [
            ProjectionOp(
                sql="INSERT INTO edges (run_id, src_kind, src_id, dst_kind, dst_id, edge) "
                "VALUES (?, 'asset_version', ?, 'asset_version', ?, 'derived_from') "
                "ON CONFLICT DO NOTHING",
                params=(version.run_id, parent_id, version.version_id),
            )
            for parent_id in parent_ids
        ]
        self._write(*ops)
        return version

    def set_alias(self, *, key: AssetKey, alias: str, version_id: str) -> None:
        """Point one alias at a specific asset version.

        Parameters
        ----------
        key : AssetKey
            Asset identity.
        alias : str
            Alias label.
        version_id : str
            Target version identifier, which must already be registered.
        """
        self.get_version(key=key, version_id=version_id)
        self._write(
            ProjectionOp(
                sql="INSERT INTO asset_aliases (asset_key, alias, version_id) VALUES (?, ?, ?) "
                "ON CONFLICT (asset_key, alias) DO UPDATE SET version_id=excluded.version_id",
                params=(str(key), alias, version_id),
            )
        )

    # -- reading -------------------------------------------------------------

    def get_version(self, *, key: AssetKey, version_id: str) -> AssetVersion:
        """Load one specific asset version.

        Parameters
        ----------
        key : AssetKey
            Asset identity.
        version_id : str
            Immutable version identifier.

        Returns
        -------
        AssetVersion

        Raises
        ------
        FileNotFoundError
            If the catalog has no such version.
        """
        rows = self._query(
            f"SELECT {_VERSION_COLUMNS} FROM asset_versions "  # noqa: S608
            "WHERE asset_key = ? AND version_id = ?",
            (str(key), version_id),
        )
        if not rows:
            raise FileNotFoundError(f"Unknown asset version {key}@{version_id}")
        return _version_from_row(rows[0])

    def version_by_id(self, version_id: str) -> AssetVersion | None:
        """Return the version with this id, whatever asset it belongs to.

        Version ids are content-addressed over key, content and run, so one id
        names one version. Lineage edges carry ids alone, and this is how they
        are resolved back to versions.
        """
        rows = self._query(
            f"SELECT {_VERSION_COLUMNS} FROM asset_versions WHERE version_id = ?",  # noqa: S608
            (version_id,),
        )
        return _version_from_row(rows[0]) if rows else None

    def get_latest_version(self, *, key: AssetKey) -> AssetVersion | None:
        """Return the most recently created version of an asset, if any."""
        rows = self._query(
            f"SELECT {_VERSION_COLUMNS} FROM asset_versions WHERE asset_key = ? "  # noqa: S608
            "ORDER BY created_at DESC, version_id DESC LIMIT 1",
            (str(key),),
        )
        return _version_from_row(rows[0]) if rows else None

    def list_versions(self, *, key: AssetKey) -> list[AssetVersion]:
        """Return every version of one asset, oldest first."""
        rows = self._query(
            f"SELECT {_VERSION_COLUMNS} FROM asset_versions WHERE asset_key = ? "  # noqa: S608
            "ORDER BY created_at, version_id",
            (str(key),),
        )
        return [_version_from_row(row) for row in rows]

    def list_aliases(self, *, key: AssetKey) -> dict[str, str]:
        """Return the alias mapping for one asset key.

        Parameters
        ----------
        key : AssetKey
            Asset identity.

        Returns
        -------
        dict[str, str]
            Mapping of alias label to the version id it points at. Empty when
            the asset has no aliases or is unknown.
        """
        rows = self._query(
            "SELECT alias, version_id FROM asset_aliases WHERE asset_key = ? ORDER BY alias",
            (str(key),),
        )
        return {str(row["alias"]): str(row["version_id"]) for row in rows}

    def list_asset_keys(self) -> list[AssetKey]:
        """Return every asset key the catalog holds, sorted."""
        rows = self._query("SELECT DISTINCT asset_key FROM asset_versions ORDER BY asset_key")
        return [AssetKey.parse(str(row["asset_key"])) for row in rows]

    def referenced_artifact_ids(self) -> set[str]:
        """Return artifact IDs referenced by every catalogued asset version.

        The asset catalog and the cache share a single content-addressed
        artifact store, and asset versions are meant to outlive the ephemeral
        cache, so a garbage collector must treat these IDs as live even when no
        cache entry still references them.
        """
        rows = self._query("SELECT DISTINCT artifact_id FROM asset_versions")
        return {str(row["artifact_id"]) for row in rows}

    def resolve_version(self, *, key: AssetKey, selector: str | None = None) -> AssetVersion:
        """Resolve a version selector to one concrete version.

        Parameters
        ----------
        key : AssetKey
            Asset identity.
        selector : str | None
            An alias, an explicit version id, or ``None`` for the latest.

        Returns
        -------
        AssetVersion

        Raises
        ------
        FileNotFoundError
            If the asset has no versions, or the selector names none.
        """
        if selector is None:
            latest = self.get_latest_version(key=key)
            if latest is None:
                raise FileNotFoundError(f"No versions registered for asset {key}")
            return latest
        aliases = self.list_aliases(key=key)
        return self.get_version(key=key, version_id=aliases.get(selector, selector))

    def parents_of(self, version_id: str) -> list[str]:
        """Return the version ids this version was derived from."""
        rows = self._query(
            "SELECT DISTINCT src_id FROM edges "
            "WHERE edge = 'derived_from' AND dst_kind = 'asset_version' AND dst_id = ? "
            "ORDER BY src_id",
            (version_id,),
        )
        return [str(row["src_id"]) for row in rows]

    def children_of(self, version_id: str) -> list[str]:
        """Return the version ids derived from this version."""
        rows = self._query(
            "SELECT DISTINCT dst_id FROM edges "
            "WHERE edge = 'derived_from' AND src_kind = 'asset_version' AND src_id = ? "
            "ORDER BY dst_id",
            (version_id,),
        )
        return [str(row["dst_id"]) for row in rows]

    # -- internals -----------------------------------------------------------

    def _data_version(
        self,
        *,
        code_version: str | None,
        parents: tuple[AssetRef, ...] | list[AssetRef],
    ) -> str | None:
        """Return the identity of the inputs and code this version came from.

        ``blake3(code_version || sorted parent data versions)``, where a parent
        whose own ``data_version`` was never recorded contributes its content
        hash instead. Two versions agreeing here were built by the same code
        from the same upstream data; nothing consumes that yet, and staleness
        detection is the reason it is recorded.
        """
        if code_version is None:
            return None
        parent_versions = sorted(
            self._data_version_of(parent) for parent in {p.version_id: p for p in parents}.values()
        )
        return hash_str("\n".join([code_version, *parent_versions]))

    def _data_version_of(self, parent: AssetRef) -> str:
        """Return a parent's recorded ``data_version``, or its content hash."""
        rows = self._query(
            "SELECT data_version FROM asset_versions WHERE version_id = ?",
            (parent.version_id,),
        )
        recorded = rows[0]["data_version"] if rows else None
        return str(recorded) if recorded else parent.content_hash


def _version_from_row(row: Any) -> AssetVersion:
    """Rebuild an :class:`AssetVersion` from an ``asset_versions`` row."""
    return AssetVersion(
        key=AssetKey.parse(str(row["asset_key"])),
        version_id=str(row["version_id"]),
        kind=str(row["kind"]),
        artifact_id=str(row["artifact_id"]),
        content_hash=str(row["content_hash"]),
        run_id=str(row["run_id"] or ""),
        producer_task=str(row["producer_task"] or ""),
        created_at=str(row["created_at"]),
        metadata=dict(loads(row["metadata"]) or {}),
    )
