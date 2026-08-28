"""Tests for the cache's rows in the ledger."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.projector import projection_ops
from ginkgo.store.protocol import ProjectionOp, StoredEvent


@pytest.fixture
def index(tmp_path: Path):
    with CacheIndex.open(path=tmp_path / "ginkgo.db") as opened:
        yield opened


def _record(index: CacheIndex, cache_key: str, **overrides) -> None:
    """Record one entry with a plausible set of facts."""
    meta = {
        "function": "pkg.produce",
        "version": 1,
        "source_hash": "src-1",
        "extra_source_hash": None,
        "env": "bio",
        "env_hash": {"env": "bio", "pixi_lock": "lock-1"},
        "env_materialized_digest": "sha-1",
        "inputs": {"n": 3},
        "input_hashes": {"n": {"sha256": "h", "type": "int"}},
        "extra": None,
        "created_at": "2026-08-28T10:00:00+00:00",
    }
    meta.update(overrides)
    index.record_entry(
        cache_key=cache_key,
        meta=meta,
        components={"task": meta["function"], "source_hash": meta["source_hash"]},
        artifact_ids=overrides.pop("artifact_ids", {"/out/a.txt": "artifact-1"}),
        size_bytes=42,
        run_id="run-1",
    )


class TestEntries:
    def test_an_entry_round_trips(self, index: CacheIndex) -> None:
        _record(index, "key-1")

        entry = index.entry("key-1")
        assert entry is not None
        assert entry["function"] == "pkg.produce"
        assert entry.env_materialized_digest == "sha-1"
        assert entry.artifact_ids == {"/out/a.txt": "artifact-1"}
        assert json.loads(entry["input_hashes"]) == {"n": {"sha256": "h", "type": "int"}}
        assert index.key_components("key-1")["source_hash"] == "src-1"

    def test_a_missing_entry_is_none(self, index: CacheIndex) -> None:
        assert index.entry("nothing") is None
        assert index.key_components("nothing") == {}

    def test_extra_metadata_is_returned_when_recorded(self, index: CacheIndex) -> None:
        _record(index, "key-1", extra={"notebook_extras": {"html": "a.html"}})
        entry = index.entry("key-1")
        assert entry is not None
        assert entry.extra == {"notebook_extras": {"html": "a.html"}}

    def test_a_second_save_of_the_same_key_keeps_the_first(self, index: CacheIndex) -> None:
        """Entries are content-addressed, so the write is idempotent."""
        _record(index, "key-1")
        _record(index, "key-1", source_hash="src-2")

        entry = index.entry("key-1")
        assert entry is not None
        assert entry["source_hash"] == "src-1"

    def test_two_threads_saving_one_key_both_succeed(self, index: CacheIndex) -> None:
        """The contract two concurrent runs rely on: INSERT OR IGNORE."""
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: _record(index, "key-1"), range(8)))

        assert index.entry("key-1") is not None
        rows = index.store.query("SELECT count(*) AS n FROM cache_entries")
        assert rows[0]["n"] == 1

    def test_forgetting_an_entry_removes_its_rows(self, index: CacheIndex) -> None:
        _record(index, "key-1")
        index.record_stat(stat_key="stat-1", cache_key="key-1")

        index.forget_entries(["key-1"])

        assert index.entry("key-1") is None
        assert index.key_components("key-1") == {}
        assert index.stat_lookup("stat-1") is None
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

    def test_a_record_round_trips(self, index: CacheIndex) -> None:
        record = self._artifact()
        index.record_artifact(record)

        assert index.artifact("artifact-1") == record
        assert index.artifact_ids() == ["artifact-1"]

    def test_publishing_fills_in_the_remote_uri(self, index: CacheIndex) -> None:
        record = self._artifact()
        index.record_artifact(record)

        published = replace(record, remote_uri="gs://b/a")
        index.record_artifact(published)

        stored = index.artifact("artifact-1")
        assert stored is not None
        assert stored.remote_uri == "gs://b/a"

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

    def test_referenced_ids_cover_cache_entries_and_asset_versions(
        self, index: CacheIndex
    ) -> None:
        _record(index, "key-1", artifact_ids={"/out/a.txt": "artifact-1"})
        with index.store.transaction():
            index.store.apply(
                [
                    ProjectionOp(
                        sql="INSERT INTO asset_versions (asset_key, version_id, kind, "
                        "artifact_id, content_hash, created_at) "
                        "VALUES ('table:rows', 'v1', 'table', 'artifact-2', 'h', '2026-08-28')",
                    )
                ]
            )

        assert index.referenced_artifact_ids() == {"artifact-1", "artifact-2"}


class TestStatIndex:
    def test_a_fingerprint_maps_to_the_content_key(self, index: CacheIndex) -> None:
        assert index.stat_lookup("stat-1") is None

        index.record_stat(stat_key="stat-1", cache_key="content-1")
        assert index.stat_lookup("stat-1") == "content-1"

        index.record_stat(stat_key="stat-1", cache_key="content-2")
        assert index.stat_lookup("stat-1") == "content-2"


class TestDigestMemo:
    def test_a_digest_is_remembered_by_fingerprint(self, index: CacheIndex) -> None:
        assert index.digest(kind="file", fingerprint="1:2:3:4") is None

        index.record_digest(kind="file", fingerprint="1:2:3:4", digest="abc")
        assert index.digest(kind="file", fingerprint="1:2:3:4") == "abc"
        assert index.digest(kind="directory", fingerprint="1:2:3:4") is None

    def test_a_hit_moves_last_seen(self, index: CacheIndex) -> None:
        index.record_digest(kind="file", fingerprint="f", digest="abc")
        before = index.store.query("SELECT last_seen FROM digest_memo")[0]["last_seen"]

        index.digest(kind="file", fingerprint="f")

        after = index.store.query("SELECT last_seen FROM digest_memo")[0]["last_seen"]
        assert after >= before


class TestHitAccounting:
    def test_a_cache_hit_counts_against_the_entry(self, index: CacheIndex) -> None:
        _record(index, "key-1")
        event = StoredEvent(
            run_id="run-2",
            ts="2026-08-28T11:00:00+00:00",
            type="task_cache_hit",
            task_id="task_0000",
            cache_key="key-1",
            payload=json.dumps({"task_id": "task_0000", "cache_key": "key-1"}),
        )

        with index.store.transaction():
            index.store.apply(projection_ops(event))

        row = index.store.query(
            "SELECT hit_count, last_hit_at FROM cache_entries WHERE cache_key = 'key-1'"
        )[0]
        assert row["hit_count"] == 1
        assert row["last_hit_at"] == "2026-08-28T11:00:00+00:00"
