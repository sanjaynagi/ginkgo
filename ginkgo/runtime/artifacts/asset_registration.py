"""Asset registration glue between the cache and asset stores.

When a task returns ``AssetResult`` sentinels, the evaluator stores the
file content into the artifact store, registers an immutable
``AssetVersion`` in the local asset catalog, and replaces each sentinel
with an ``AssetRef`` so downstream tasks see the resolved reference.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ginkgo.core.asset import (
    AssetKey,
    AssetRef,
    AssetResult,
    AssetVersion,
    asset_ref_from_version,
    collect_asset_refs,
    make_asset_version,
)
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.caching.cache import CacheStore


def asset_key_for_result(*, name: str, kind: str) -> AssetKey:
    """Build one asset key for a supported asset result."""
    if kind != "file":
        raise ValueError(f"Unsupported asset kind: {kind!r}")
    return AssetKey(namespace="file", name=name)


def render_asset_ref(*, asset_ref: AssetRef) -> dict[str, Any]:
    """Render one asset reference for provenance and event payloads."""
    return {
        "artifact_id": asset_ref.artifact_id,
        "artifact_path": asset_ref.artifact_path,
        "asset_key": str(asset_ref.key),
        "content_hash": asset_ref.content_hash,
        "kind": asset_ref.kind,
        "metadata": dict(asset_ref.metadata),
        "name": asset_ref.name,
        "namespace": asset_ref.namespace,
        "version_id": asset_ref.version_id,
    }


def asset_index_for(*, value: Any) -> list[dict[str, Any]]:
    """Return rendered asset summaries for one task result value."""
    return [render_asset_ref(asset_ref=asset_ref) for asset_ref in collect_asset_refs(value)]


@dataclass(kw_only=True)
class AssetRegistrar:
    """Materialise asset sentinels in a task result into asset references.

    Parameters
    ----------
    cache_store : CacheStore
        Provides access to the underlying artifact store for content storage.
    asset_store : AssetStore
        Local asset catalog where new versions and lineage edges are recorded.
    run_id_provider : Callable[[], str]
        Returns the active run id at registration time.
    """

    cache_store: CacheStore
    asset_store: AssetStore
    run_id_provider: Callable[[], str]

    def materialize_results(self, *, node: Any, value: Any) -> Any:
        """Register nested asset sentinels and replace them with asset refs.

        Mutates ``node.asset_versions`` to record every newly registered
        version so the scheduler can later persist them.
        """
        node.asset_versions = []
        parent_refs = self._parent_asset_refs(node=node)
        return self._replace_asset_results(node=node, value=value, parent_refs=parent_refs)

    def _replace_asset_results(
        self,
        *,
        node: Any,
        value: Any,
        parent_refs: list[AssetRef],
    ) -> Any:
        """Recursively replace nested asset sentinels with asset refs."""
        if isinstance(value, AssetResult):
            asset_ref, asset_version = self._register_asset_result(
                node=node,
                asset_result=value,
                parent_refs=parent_refs,
            )
            node.asset_versions.append(asset_version)
            return asset_ref

        if isinstance(value, list):
            return [
                self._replace_asset_results(node=node, value=item, parent_refs=parent_refs)
                for item in value
            ]

        if isinstance(value, tuple):
            return tuple(
                self._replace_asset_results(node=node, value=item, parent_refs=parent_refs)
                for item in value
            )

        return value

    def _register_asset_result(
        self,
        *,
        node: Any,
        asset_result: AssetResult,
        parent_refs: list[AssetRef],
    ) -> tuple[AssetRef, AssetVersion]:
        """Store one file asset and register its immutable catalog version."""
        source_path = asset_result.path
        if not source_path.is_file():
            raise FileNotFoundError(
                f"{node.task_def.name}.return asset file must exist: {str(source_path)!r}"
            )

        record = self.cache_store._artifact_store.store(src_path=source_path)

        asset_name = asset_result.name or node.task_def.fn.__name__
        version = make_asset_version(
            key=asset_key_for_result(name=asset_name, kind=asset_result.kind),
            kind=asset_result.kind,
            artifact_id=record.artifact_id,
            content_hash=record.digest_hex,
            run_id=self.run_id_provider(),
            producer_task=node.task_def.name,
            metadata=asset_result.metadata,
        )
        self.asset_store.register_version(version=version)
        asset_ref = asset_ref_from_version(
            version=version,
            artifact_path=self.cache_store._artifact_store.artifact_path(
                artifact_id=record.artifact_id
            ),
        )
        if parent_refs:
            self.asset_store.record_lineage(child=asset_ref, parents=parent_refs)
        return asset_ref, version

    def _parent_asset_refs(self, *, node: Any) -> list[AssetRef]:
        """Collect unique upstream asset references consumed by one task."""
        if node.resolved_args is None:
            return []
        unique: dict[tuple[str, str, str], AssetRef] = {}
        for asset_ref in collect_asset_refs(node.resolved_args):
            unique[(asset_ref.namespace, asset_ref.name, asset_ref.version_id)] = asset_ref
        return list(unique.values())
