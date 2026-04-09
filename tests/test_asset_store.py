"""Unit tests for the asset metadata store."""

from __future__ import annotations

from pathlib import Path

from ginkgo.core.asset import AssetKey, AssetRef, make_asset_version
from ginkgo.runtime.artifacts.asset_store import AssetStore


def _asset_ref(*, key: AssetKey, version_id: str) -> AssetRef:
    return AssetRef(
        key=key,
        version_id=version_id,
        kind="file",
        artifact_id=f"artifact-{version_id}",
        content_hash=f"hash-{version_id}",
        artifact_path=f"/artifacts/{version_id}",
        metadata={},
    )


class TestAssetStore:
    def test_register_alias_and_lineage(self, tmp_path: Path) -> None:
        store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
        key = AssetKey(namespace="file", name="prepared_data")
        version = make_asset_version(
            key=key,
            kind="file",
            artifact_id="artifact-1",
            content_hash="hash-1",
            run_id="run-1",
            producer_task="tests.writer",
        )

        store.register_version(version=version)
        store.set_alias(key=key, alias="latest", version_id=version.version_id)

        resolved = store.resolve_version(key=key, selector="latest")
        latest = store.get_latest_version(key=key)

        assert resolved.version_id == version.version_id
        assert latest is not None
        assert latest.version_id == version.version_id
        assert store.list_asset_keys() == [key]

        child_key = AssetKey(namespace="file", name="transformed_data")
        child_version = make_asset_version(
            key=child_key,
            kind="file",
            artifact_id="artifact-2",
            content_hash="hash-2",
            run_id="run-2",
            producer_task="tests.transformer",
        )
        store.register_version(version=child_version)
        child_ref = _asset_ref(key=child_key, version_id=child_version.version_id)
        parent_ref = _asset_ref(key=key, version_id=version.version_id)

        store.record_lineage(child=child_ref, parents=[parent_ref])

        lineage = store.lineage_for(key=child_key, version_id=child_version.version_id)
        assert lineage is not None
        assert [parent.version_id for parent in lineage.parents] == [version.version_id]
