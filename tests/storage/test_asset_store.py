"""Unit tests for the asset catalog's rows in the ledger."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

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


@pytest.fixture
def store() -> AssetStore:
    with AssetStore.in_memory() as catalog:
        yield catalog


class TestAssetStore:
    def test_register_and_resolve_alias(self, store: AssetStore) -> None:
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

    def test_round_trips_a_version(self, store: AssetStore) -> None:
        key, version = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        registered = store.register_version(version=version)

        assert registered is version
        assert store.get_version(key=key, version_id=version.version_id) == version

    def test_unknown_version_is_not_found(self, store: AssetStore) -> None:
        key = AssetKey(namespace="file", name="absent")
        with pytest.raises(FileNotFoundError):
            store.get_version(key=key, version_id="nope")
        with pytest.raises(FileNotFoundError):
            store.resolve_version(key=key)

    def test_list_aliases(self, store: AssetStore) -> None:
        key, version = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        store.register_version(version=version)

        assert store.list_aliases(key=key) == {}

        store.set_alias(key=key, alias="latest", version_id=version.version_id)
        assert store.list_aliases(key=key) == {"latest": version.version_id}

    def test_referenced_artifact_ids(self, store: AssetStore) -> None:
        _, first = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        _, second = _make_version(
            name="transformed_data", suffix="2", run_id="run-2", producer="tests.transformer"
        )
        store.register_version(version=first)
        store.register_version(version=second)

        assert store.referenced_artifact_ids() == {"artifact-1", "artifact-2"}

    def test_registers_lineage_with_the_version(self, store: AssetStore) -> None:
        parent_key, parent = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        _, child = _make_version(
            name="transformed_data", suffix="2", run_id="run-2", producer="tests.transformer"
        )
        store.register_version(version=parent)
        store.register_version(
            version=child,
            parents=[_asset_ref(key=parent_key, version_id=parent.version_id)],
        )

        assert store.parents_of(child.version_id) == [parent.version_id]
        assert store.children_of(parent.version_id) == [child.version_id]
        assert store.version_by_id(parent.version_id) == parent

    def test_data_version_follows_code_and_parents(self, store: AssetStore) -> None:
        """Same code and same upstream data agree; either changing does not."""
        parent_key, parent = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        store.register_version(version=parent, code_version="source-a")
        parent_ref = _asset_ref(key=parent_key, version_id=parent.version_id)

        _, child = _make_version(
            name="transformed_data", suffix="2", run_id="run-2", producer="tests.transformer"
        )
        store.register_version(version=child, parents=[parent_ref], code_version="source-b")

        _, twin = _make_version(
            name="twin", suffix="3", run_id="run-3", producer="tests.transformer"
        )
        store.register_version(version=twin, parents=[parent_ref], code_version="source-b")

        _, other_code = _make_version(
            name="other", suffix="4", run_id="run-4", producer="tests.transformer"
        )
        store.register_version(version=other_code, parents=[parent_ref], code_version="source-c")

        versions = {
            row["version_id"]: row["data_version"]
            for row in store.store.query("SELECT version_id, data_version FROM asset_versions")
        }
        assert versions[child.version_id] == versions[twin.version_id]
        assert versions[child.version_id] != versions[other_code.version_id]
        assert versions[parent.version_id] is not None

    def test_a_version_without_a_code_version_has_no_data_version(self, store: AssetStore) -> None:
        _, version = _make_version(
            name="prepared_data", suffix="1", run_id="run-1", producer="tests.writer"
        )
        store.register_version(version=version)

        rows = store.store.query("SELECT data_version FROM asset_versions")
        assert rows[0]["data_version"] is None

    def test_metadata_keeps_the_order_it_was_written_in(self, store: AssetStore) -> None:
        """A model's metrics render in the order the author listed them."""
        key = AssetKey(namespace="model", name="classifier")
        metrics = {"accuracy": 0.9, "precision": 0.8, "recall": 0.7, "f1": 0.75}
        version = make_asset_version(
            key=key,
            kind="model",
            artifact_id="artifact-m",
            content_hash="hash-m",
            run_id="run-1",
            producer_task="tests.fit",
            metadata={"framework": "sklearn", "metrics": metrics},
        )
        store.register_version(version=version)

        read_back = store.get_version(key=key, version_id=version.version_id)

        assert list(read_back.metadata) == ["framework", "metrics"]
        assert list(read_back.metadata["metrics"]) == list(metrics)

    def test_registering_a_version_is_one_critical_section(self, store: AssetStore) -> None:
        """Read-parents-then-write must not be interruptible by another thread.

        Registering concurrently from several threads, each deriving its
        `data_version` from the same parent, must produce one row per version
        and agreeing digests — which it cannot if the read and the write are
        two separate holds of the lock.
        """
        parent_key, parent = _make_version(
            name="prepared_data", suffix="0", run_id="run-0", producer="tests.writer"
        )
        store.register_version(version=parent, code_version="source-parent")
        parent_ref = _asset_ref(key=parent_key, version_id=parent.version_id)

        def register(index: int) -> None:
            _, version = _make_version(
                name=f"child_{index}",
                suffix=f"c{index}",
                run_id=f"run-{index}",
                producer="tests.transformer",
            )
            store.register_version(
                version=version, parents=[parent_ref], code_version="source-child"
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(register, range(8)))

        rows = store.store.query(
            "SELECT data_version FROM asset_versions WHERE asset_key LIKE 'file:child_%'"
        )
        assert len(rows) == 8
        assert len({row["data_version"] for row in rows}) == 1

    def test_for_reading_an_absent_workspace_is_empty(self, tmp_path: Path) -> None:
        with AssetStore.for_reading(tmp_path / "ginkgo.db") as catalog:
            assert catalog.list_asset_keys() == []
        assert not (tmp_path / "ginkgo.db").exists()
