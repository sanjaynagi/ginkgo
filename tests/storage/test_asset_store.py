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


def _make_version(*, name: str, suffix: str, run_id: str, producer: str):
    key = AssetKey(namespace="file", name=name)
    version = make_asset_version(
        key=key,
        kind="file",
        artifact_id=f"artifact-{suffix}",
        content_hash=f"hash-{suffix}",
        run_id=run_id,
        producer_task=producer,
    )
    return key, version


class TestAssetStore:
    def test_register_and_resolve_alias(self, tmp_path: Path) -> None:
        store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
        key, version = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )

        store.register_version(version=version)
        store.set_alias(key=key, alias="latest", version_id=version.version_id)

        resolved = store.resolve_version(key=key, selector="latest")
        latest = store.get_latest_version(key=key)

        assert resolved.version_id == version.version_id
        assert latest is not None
        assert latest.version_id == version.version_id
        assert store.list_asset_keys() == [key]

    def test_list_aliases(self, tmp_path: Path) -> None:
        store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
        key, version = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        store.register_version(version=version)

        assert store.list_aliases(key=key) == {}

        store.set_alias(key=key, alias="latest", version_id=version.version_id)
        assert store.list_aliases(key=key) == {"latest": version.version_id}

    def test_referenced_artifact_ids(self, tmp_path: Path) -> None:
        store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
        _, first = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        _, second = _make_version(
            name="transformed_data", suffix="2", run_id="run-2", producer="tests.transformer"
        )
        store.register_version(version=first)
        store.register_version(version=second)

        assert store.referenced_artifact_ids() == {"artifact-1", "artifact-2"}

    def test_record_lineage(self, tmp_path: Path) -> None:
        store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
        parent_key, parent_version = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        child_key, child_version = _make_version(
            name="transformed_data", suffix="2", run_id="run-2", producer="tests.transformer"
        )
        store.register_version(version=parent_version)
        store.register_version(version=child_version)

        store.record_lineage(
            child=_asset_ref(key=child_key, version_id=child_version.version_id),
            parents=[_asset_ref(key=parent_key, version_id=parent_version.version_id)],
        )

        lineage = store.lineage_for(key=child_key, version_id=child_version.version_id)
        assert lineage is not None
        assert [parent.version_id for parent in lineage.parents] == [parent_version.version_id]
