"""Regression tests: cache GC must not delete catalogued asset artifacts.

The cache and the asset catalog share a single content-addressed artifact
store. ``ginkgo cache prune`` / ``cache clear`` run ``_gc_orphan_artifacts``,
which previously collected referenced artifact IDs from cache entries only —
deleting artifacts that asset versions still referenced.
"""

from pathlib import Path

from ginkgo.cli.commands.cache import _gc_orphan_artifacts
from ginkgo.core.asset import AssetKey, make_asset_version
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.asset_store import AssetStore


def _ginkgo_dirs(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Return (cache_root, artifacts_root, assets_root) under a fresh tree."""
    base = tmp_path / ".ginkgo"
    cache_root = base / "cache"
    cache_root.mkdir(parents=True)
    return cache_root, base / "artifacts", base / "assets"


def test_gc_keeps_artifact_referenced_only_by_an_asset(tmp_path):
    """An artifact reachable only from the asset catalog must survive GC."""
    cache_root, artifacts_root, assets_root = _ginkgo_dirs(tmp_path)

    # Store a blob and register an asset version pointing at it. No cache
    # entry references the blob.
    store = LocalArtifactStore(root=artifacts_root)
    record = store.store_bytes(data=b"asset payload", extension="parquet")

    asset_store = AssetStore(root=assets_root)
    asset_store.register_version(
        version=make_asset_version(
            key=AssetKey(namespace="default", name="demo_table"),
            kind="table",
            artifact_id=record.artifact_id,
            content_hash=record.digest_hex,
            run_id="run-1",
            producer_task="pkg.workflow.build_table",
            metadata=None,
        )
    )

    _gc_orphan_artifacts(cache_root)

    assert store.exists(artifact_id=record.artifact_id), (
        "GC deleted an artifact still referenced by a catalogued asset"
    )


def test_gc_deletes_artifact_referenced_by_neither_cache_nor_asset(tmp_path):
    """A genuinely orphaned artifact is still collected — GC stays effective."""
    cache_root, artifacts_root, _ = _ginkgo_dirs(tmp_path)

    store = LocalArtifactStore(root=artifacts_root)
    record = store.store_bytes(data=b"truly orphaned", extension="bin")

    _gc_orphan_artifacts(cache_root)

    assert not store.exists(artifact_id=record.artifact_id), (
        "GC failed to delete a genuinely orphaned artifact"
    )
