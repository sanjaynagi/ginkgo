"""Tests for the cache's rows in the ledger."""

from __future__ import annotations

import json
import socket
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from ginkgo import task
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.query import Query
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.errors import StoreError
from ginkgo.store.projector import projection_ops
from ginkgo.store.sqlite import open_store
from ginkgo.store.protocol import StoredEvent
from ginkgo.workspace_layout import WorkspaceLayout


@pytest.fixture
def index(tmp_path: Path):
    with CacheIndex.open(path=tmp_path / "ginkgo.db") as opened:
        yield opened


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "ginkgo.db"


def _components(path: Path, cache_key: str) -> dict:
    """Read one entry's key components the way `cache explain` does."""
    with Query(open_store(path, readonly=True), layout=WorkspaceLayout.relative()) as reader:
        return reader._cache_key_components(cache_key)


def _record(index: CacheIndex, cache_key: str, **overrides) -> None:
    """Record one entry with a plausible set of facts."""
    artifact_ids = overrides.pop("artifact_ids", {"/out/a.txt": "artifact-1"})
    meta = {
        "function": "pkg.produce",
        "version": 1,
        "source_hash": "src-1",
        "extra_source_hash": None,
        "env": "bio",
        "env_hash": {"env": "bio", "pixi_lock": "lock-1"},
        "env_materialized_digest": "sha-1",
        "inputs": {"n": 3},
        "input_hashes": {"n": {"digest": "h", "type": "int"}},
        "extra": None,
        "created_at": "2026-08-28T10:00:00+00:00",
    }
    meta.update(overrides)
    index.record_entry(
        cache_key=cache_key,
        meta=meta,
        artifact_ids=artifact_ids,
        size_bytes=42,
        run_id="run-1",
    )


class TestOpening:
    """Which database an index touches is always the caller's decision."""

    def test_constructing_or_reading_creates_no_database(self, tmp_path: Path) -> None:
        """A read path must not create a database by being constructed."""
        from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore

        with CacheIndex.in_memory() as index:
            CacheStore(index=index, root=tmp_path / ".ginkgo" / "cache")
            LocalArtifactStore(root=tmp_path / ".ginkgo" / "artifacts", index=index)
        with CacheIndex.for_reading(tmp_path / "ginkgo.db") as index:
            assert index.cache_keys() == []
            assert index.entry("key-1") is None

        assert not list(tmp_path.glob("**/ginkgo.db*"))

    def test_a_read_only_index_never_writes(self, tmp_path: Path) -> None:
        """It reads the rows that are there and refuses to change them."""
        path = tmp_path / "ginkgo.db"
        with CacheIndex.open(path=path) as writer:
            _record(writer, "key-1")

        with CacheIndex.open(path=path, readonly=True) as reader:
            entry = reader.entry("key-1")
            assert entry is not None
            with pytest.raises(StoreError):
                reader.record_hit("key-1")


class TestEntries:
    def test_an_entry_round_trips(self, index: CacheIndex, db_path: Path) -> None:
        _record(index, "key-1", extra={"notebook_extras": {"html": "a.html"}})

        entry = index.entry("key-1")
        assert entry is not None
        assert entry["function"] == "pkg.produce"
        assert entry.env_materialized_digest == "sha-1"
        assert entry.artifact_ids == {"/out/a.txt": "artifact-1"}
        assert entry["input_hashes"] == {"n": {"digest": "h", "type": "int"}}
        assert entry.extra == {"notebook_extras": {"html": "a.html"}}
        assert _components(db_path, "key-1")["source_hash"] == "src-1"
        assert index.referenced_artifact_ids() == {"artifact-1"}
        assert index.entry("nothing") is None
        assert _components(db_path, "nothing") == {}

    def test_saving_a_key_again_keeps_the_first_even_from_many_threads(
        self, index: CacheIndex
    ) -> None:
        """Entries are content-addressed: INSERT OR IGNORE is what concurrent runs rely on."""
        _record(index, "key-1")
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: _record(index, "key-1", source_hash="src-2"), range(8)))

        entry = index.entry("key-1")
        assert entry is not None and entry["source_hash"] == "src-1"
        assert index.cache_keys() == ["key-1"]

    def test_forgetting_an_entry_removes_its_rows(self, index: CacheIndex, db_path: Path) -> None:
        _record(index, "key-1")
        index.record_stat_index(stat_key="stat-1", cache_key="key-1")

        index.forget_entries(["key-1"])

        assert index.entry("key-1") is None
        assert _components(db_path, "key-1") == {}
        assert index.stat_index_lookup("stat-1") is None
        assert index.referenced_artifact_ids() == set()


class TestArtifacts:
    def _artifact(self, artifact_id: str = "artifact-1") -> ArtifactRecord:
        return ArtifactRecord(
            artifact_id=artifact_id,
            kind="blob",
            digest_algorithm="blake3",
            digest_hex=artifact_id,
            extension=".txt",
            size=12,
            created_at="2026-08-28T10:00:00+00:00",
            storage_backend="local",
        )

    def test_a_record_round_trips_and_publishing_fills_in_its_uri(self, index: CacheIndex) -> None:
        record = self._artifact()
        index.record_artifact(record)
        assert index.artifact("artifact-1") == record
        assert index.artifact_ids() == ["artifact-1"]

        index.record_artifact(replace(record, remote_uri="gs://b/a"))

        stored = index.artifact("artifact-1")
        assert stored is not None and stored.remote_uri == "gs://b/a"

    def test_forgetting_an_artifact_drops_its_materializations(
        self, index: CacheIndex, tmp_path: Path
    ) -> None:
        path = tmp_path / "out.txt"
        path.write_text("data", encoding="utf-8")
        index.record_artifact(self._artifact())
        index.record_materialization(path=path, artifact_id="artifact-1")

        index.forget_artifact("artifact-1")

        assert index.artifact("artifact-1") is None
        assert index.materialization_matches(path=path, artifact_id="artifact-1") is False


class TestFingerprints:
    def test_a_stat_fingerprint_maps_to_the_latest_content_key(self, index: CacheIndex) -> None:
        assert index.stat_index_lookup("stat-1") is None

        index.record_stat_index(stat_key="stat-1", cache_key="content-1")
        index.record_stat_index(stat_key="stat-1", cache_key="content-2")
        assert index.stat_index_lookup("stat-1") == "content-2"

    def test_a_digest_is_remembered_by_fingerprint(self, index: CacheIndex) -> None:
        assert index.digest(kind="file", fingerprint="1:2:3:4") is None

        index.record_digest(kind="file", fingerprint="1:2:3:4", digest="abc")
        assert index.digest(kind="file", fingerprint="1:2:3:4") == "abc"
        assert index.digest(kind="directory", fingerprint="1:2:3:4") is None

    def test_a_hit_stamps_last_seen_once_the_index_closes(
        self, tmp_path: Path, db_path: Path
    ) -> None:
        """The stamp is deferred: a warm run must not pay a write per file."""
        with CacheIndex.open(path=db_path) as index:
            index.record_digest(kind="file", fingerprint="f", digest="abc")

        def last_seen() -> str:
            with open_store(db_path, readonly=True) as store:
                return str(store.query("SELECT last_seen FROM digest_memo")[0]["last_seen"])

        before = last_seen()
        with CacheIndex.open(path=db_path) as index:
            assert index.digest(kind="file", fingerprint="f") == "abc"
            assert last_seen() == before

        assert last_seen() > before


class TestHitAccounting:
    def test_a_hit_is_counted_against_the_entry(self, index: CacheIndex, db_path: Path) -> None:
        _record(index, "key-1")

        index.record_hit("key-1", at="2026-08-28T11:00:00+00:00")

        with open_store(db_path, readonly=True) as store:
            row = store.query(
                "SELECT hit_count, last_hit_at FROM cache_entries WHERE cache_key = 'key-1'"
            )[0]
        assert row["hit_count"] == 1
        assert row["last_hit_at"] == "2026-08-28T11:00:00+00:00"

    def test_the_projector_leaves_the_cache_tables_alone(self) -> None:
        """The cache tables are a direct-write index, not a projection."""
        event = StoredEvent(
            run_id="run-2",
            ts="2026-08-28T11:00:00+00:00",
            type="task_cache_hit",
            task_id="task_0000",
            cache_key="key-1",
            payload=json.dumps({"task_id": "task_0000", "cache_key": "key-1"}),
        )

        assert not [op for op in projection_ops(event) if "cache_entries" in op.sql]


class TestEnvMaterializationIsWrittenOnlyBySave:
    """Every lookup path asks for the digest; a lookup must not open a write."""

    class _Backend:
        """The two questions a cache asks an execution environment."""

        def env_identity(self, *, env: str) -> str:
            return f"lock-for-{env}"

        def materialized_digest(self, *, env: str) -> str:
            return f"digest-for-{env}"

    def _store(self, path: Path, tmp_path: Path, *, index: CacheIndex) -> CacheStore:
        return CacheStore(index=index, root=tmp_path / "cache", backend=self._Backend())

    def test_a_lookup_over_a_read_only_index_neither_raises_nor_writes(
        self, db_path: Path, tmp_path: Path
    ) -> None:
        with CacheIndex.open(path=db_path) as writable:
            _record(writable, "key-1")

        @task(env="bio")
        def produce() -> int:
            return 1

        with CacheIndex.open(path=db_path, readonly=True) as index:
            store = self._store(db_path, tmp_path, index=index)

            assert store.has_entry(cache_key="key-1", task_def=produce) is False
            assert index.env_materializations() == []

    def test_save_records_the_digest_against_this_host(
        self, db_path: Path, tmp_path: Path
    ) -> None:
        @task(env="bio")
        def produce() -> int:
            return 1

        with CacheIndex.open(path=db_path) as index:
            store = self._store(db_path, tmp_path, index=index)
            store.save(
                cache_key="key-1",
                result=1,
                task_def=produce,
                resolved_args={},
                input_hashes={},
            )
            rows = index.env_materializations()

        assert [row["materialized_digest"] for row in rows] == ["digest-for-bio"]
        assert rows[0]["host"] == socket.gethostname()
