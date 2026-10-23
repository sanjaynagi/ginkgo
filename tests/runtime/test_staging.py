"""Unit tests for the remote staging cache."""

from __future__ import annotations

from pathlib import Path

from ginkgo.core.hashing import hash_bytes
from ginkgo.core.remote import remote_file, remote_folder
from ginkgo.remote.staging import StagingCache, StagingEntry, StagingIndex
from ginkgo.store.sqlite import MEMORY
from tests.remote_fakes import FakeObjectStore


def _store(tmp_path: Path, objects: dict[str, bytes]) -> FakeObjectStore:
    """Return a fake store holding *objects* (key: bytes) in ``s3://bucket``."""
    store = FakeObjectStore(root=tmp_path / "object-store")
    for key, data in objects.items():
        store.put(bucket="bucket", key=key, data=data)
    return store


class TestStageFile:
    def test_downloads_and_returns_path(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        store = _store(tmp_path, {"data/file.txt": b"hello world"})
        ref = remote_file("s3://bucket/data/file.txt")

        path = cache.stage_file(ref=ref, backend=store)

        assert path.read_bytes() == b"hello world"
        assert store.downloads == 1

    def test_cached_file_not_redownloaded(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        store = _store(tmp_path, {"data/file.txt": b"hello world"})
        ref = remote_file("s3://bucket/data/file.txt")

        path1 = cache.stage_file(ref=ref, backend=store)
        path2 = cache.stage_file(ref=ref, backend=store)

        assert path1 == path2
        assert store.downloads == 1

    def test_changed_etag_triggers_redownload(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        ref = remote_file("s3://bucket/data/file.txt")

        store = _store(tmp_path, {"data/file.txt": b"v1"})
        cache.stage_file(ref=ref, backend=store)

        store.put(bucket="bucket", key="data/file.txt", data=b"v2")
        path = cache.stage_file(ref=ref, backend=store)

        assert path.read_bytes() == b"v2"
        assert store.downloads == 2

    def test_pinned_version_skips_head_check(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        store = _store(tmp_path, {"key.txt": b"hello world"})
        ref = remote_file("s3://bucket/key.txt", version_id="v42")

        cache.stage_file(ref=ref, backend=store)
        cache.stage_file(ref=ref, backend=store)

        # A pinned version needs no freshness check on reuse.
        assert store.heads == 0
        assert store.downloads == 1

    def test_content_addressed_dedup(self, tmp_path) -> None:
        """Two different URIs with identical content share one blob."""
        cache = StagingCache(root=tmp_path / "staging")
        store = _store(tmp_path, {"a.txt": b"shared", "b.txt": b"shared"})

        path_a = cache.stage_file(ref=remote_file("s3://bucket/a.txt"), backend=store)
        path_b = cache.stage_file(ref=remote_file("s3://bucket/b.txt"), backend=store)

        assert path_a == path_b  # Same blob.
        blobs = list((tmp_path / "staging" / "blobs").iterdir())
        assert len(blobs) == 1


class TestDigestOf:
    def test_a_staged_path_carries_the_digest_of_what_that_call_staged(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        store = _store(tmp_path, {"data/file.txt": b"first"})
        v1 = cache.stage_file(
            ref=remote_file("s3://bucket/data/file.txt", version_id="v1"), backend=store
        )
        # A second version of the same URI replaces the index row.
        store.put(bucket="bucket", key="data/file.txt", data=b"second")
        cache.stage_file(
            ref=remote_file("s3://bucket/data/file.txt", version_id="v2"), backend=store
        )

        assert StagingCache.digest_of(v1) == hash_bytes(b"first")
        assert cache.lookup(uri="s3://bucket/data/file.txt").digest == hash_bytes(b"second")


class TestStageFolder:
    def test_stages_prefix_contents(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        store = _store(tmp_path, {"prefix/a.txt": b"a", "prefix/sub/b.txt": b"b"})

        folder_path = cache.stage_folder(ref=remote_folder("s3://bucket/prefix/"), backend=store)

        assert (folder_path / "a.txt").read_bytes() == b"a"
        assert (folder_path / "sub" / "b.txt").read_bytes() == b"b"


class TestLookup:
    def test_returns_entry_after_staging(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        store = FakeObjectStore(root=tmp_path / "object-store")
        store.put(bucket="bucket", key="lookup.txt", data=b"hello world", etag="e1")
        ref = remote_file("s3://bucket/lookup.txt")

        cache.stage_file(ref=ref, backend=store)
        entry = cache.lookup(uri=ref.uri)

        assert entry is not None
        assert isinstance(entry, StagingEntry)
        assert entry.uri == "s3://bucket/lookup.txt"
        assert entry.etag == "e1"
        assert len(entry.digest) == 64  # BLAKE3 hex digest

    def test_returns_none_for_unstaged(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        assert cache.lookup(uri="s3://bucket/never-staged.txt") is None


class TestStagingIndex:
    def test_construction_creates_no_database(self, tmp_path) -> None:
        StagingCache(root=tmp_path / "staging")
        assert not (tmp_path / "ginkgo.db").exists()

    def test_row_round_trip(self, tmp_path) -> None:
        entry = StagingEntry(
            uri="s3://b/k",
            digest="abc123",
            etag="etag",
            version_id=None,
            size=100,
            staged_at="2026-01-01T00:00:00Z",
            blob_path="blobs/abc123",
        )
        with StagingIndex.open(path=tmp_path / "ginkgo.db") as index:
            index.record(entry)
            assert index.entry(uri="s3://b/k") == entry
            assert index.entries() == [entry]

    def test_record_replaces_a_previous_row(self, tmp_path) -> None:
        with StagingIndex.open(path=tmp_path / "ginkgo.db") as index:
            for digest in ("one", "two"):
                index.record(
                    StagingEntry(
                        uri="s3://b/k",
                        digest=digest,
                        etag=None,
                        version_id=None,
                        size=1,
                        staged_at="2026-01-01T00:00:00Z",
                        blob_path=f"blobs/{digest}",
                    )
                )
            entries = index.entries()
        assert [entry.digest for entry in entries] == ["two"]

    def test_folder_staging_is_recorded(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        store = _store(tmp_path, {"prefix/a.txt": b"abc"})
        folder_path = cache.stage_folder(ref=remote_folder("s3://bucket/prefix/"), backend=store)

        entry = cache.lookup(uri="s3://bucket/prefix/")
        assert entry is not None
        assert entry.blob_path == f"folders/{folder_path.name}"
        assert entry.size == 3


class TestWorkerStaging:
    """A worker has the bytes it staged but no workspace database to index them."""

    def test_an_in_memory_index_writes_no_database(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging", db_path=MEMORY)
        store = _store(tmp_path, {"worker.txt": b"hello world"})
        ref = remote_file("s3://bucket/worker.txt")

        path = cache.stage_file(ref=ref, backend=store)

        assert path.read_bytes() == b"hello world"
        assert cache.lookup(uri=ref.uri) is not None
        assert not list(tmp_path.glob("*.db"))


class TestPrune:
    """Staged bytes are the only store with an eviction path of its own."""

    def _stage(self, tmp_path: Path, cache: StagingCache, key: str) -> None:
        store = _store(tmp_path, {key: b"hello world"})
        cache.stage_file(ref=remote_file(f"s3://bucket/{key}"), backend=store)

    def test_an_unused_entry_and_its_bytes_go(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(tmp_path, cache, "old.txt")
        entry = cache.lookup(uri="s3://bucket/old.txt")
        assert entry is not None

        count, freed = cache.prune(before="2099-01-01T00:00:00+00:00")

        assert count == 1
        assert freed == len(b"hello world")
        assert cache.lookup(uri="s3://bucket/old.txt") is None
        assert not (tmp_path / "staging" / entry.blob_path).exists()

    def test_a_recently_used_entry_stays(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(tmp_path, cache, "fresh.txt")

        assert cache.prune(before="2000-01-01T00:00:00+00:00") == (0, 0)
        assert cache.lookup(uri="s3://bucket/fresh.txt") is not None

    def test_a_dry_run_measures_without_deleting(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(tmp_path, cache, "old.txt")

        count, freed = cache.prune(before="2099-01-01T00:00:00+00:00", dry_run=True)

        assert (count, freed) == (1, len(b"hello world"))
        assert cache.lookup(uri="s3://bucket/old.txt") is not None

    def test_bytes_shared_with_a_surviving_entry_are_kept(self, tmp_path) -> None:
        """Two URIs, identical content: one blob, and it outlives the first row."""
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(tmp_path, cache, "a.txt")
        entry = cache.lookup(uri="s3://bucket/a.txt")
        assert entry is not None
        # Backdate only the first entry, so the second is the surviving user.
        cache.index.record(
            StagingEntry(
                uri="s3://bucket/b.txt",
                digest=entry.digest,
                etag=None,
                version_id=None,
                size=entry.size,
                staged_at="2099-01-01T00:00:00+00:00",
                blob_path=entry.blob_path,
            )
        )

        count, freed = cache.prune(before="2098-01-01T00:00:00+00:00")

        assert count == 1
        assert freed == 0
        assert (tmp_path / "staging" / entry.blob_path).exists()

    def test_pruning_a_workspace_that_never_staged_anything(self, tmp_path) -> None:
        assert StagingCache(root=tmp_path / "staging").prune(
            before="2099-01-01T00:00:00+00:00"
        ) == (
            0,
            0,
        )
