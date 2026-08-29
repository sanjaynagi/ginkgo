"""Unit tests for the remote staging cache."""

from __future__ import annotations

from unittest.mock import MagicMock

from ginkgo.core.remote import remote_file, remote_folder
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.staging import StagingCache, StagingEntry, StagingIndex
from ginkgo.store.sqlite import MEMORY
from tests.conftest import make_download_backend as _make_mock_backend


class TestStageFile:
    def test_downloads_and_returns_path(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = _make_mock_backend()
        ref = remote_file("s3://bucket/data/file.txt")

        path = cache.stage_file(ref=ref, backend=backend)

        assert path.exists()
        assert path.read_bytes() == b"hello world"
        backend.download.assert_called_once()

    def test_cached_file_not_redownloaded(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = _make_mock_backend()
        ref = remote_file("s3://bucket/data/file.txt")

        path1 = cache.stage_file(ref=ref, backend=backend)
        path2 = cache.stage_file(ref=ref, backend=backend)

        assert path1 == path2
        # download called once, head called once for freshness check.
        assert backend.download.call_count == 1

    def test_changed_etag_triggers_redownload(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        ref = remote_file("s3://bucket/data/file.txt")

        # First download.
        backend1 = _make_mock_backend(content=b"v1", etag="etag-v1")
        cache.stage_file(ref=ref, backend=backend1)

        # Second download with changed etag.
        backend2 = _make_mock_backend(content=b"v2", etag="etag-v2")
        path = cache.stage_file(ref=ref, backend=backend2)

        assert path.read_bytes() == b"v2"
        assert backend2.download.call_count == 1

    def test_pinned_version_skips_head_check(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = _make_mock_backend()
        ref = remote_file("s3://bucket/key.txt", version_id="v42")

        cache.stage_file(ref=ref, backend=backend)
        cache.stage_file(ref=ref, backend=backend)

        # head should not be called on reuse since version_id is pinned.
        backend.head.assert_not_called()
        assert backend.download.call_count == 1

    def test_content_addressed_dedup(self, tmp_path) -> None:
        """Two different URIs with identical content share one blob."""
        cache = StagingCache(root=tmp_path / "staging")
        backend = _make_mock_backend(content=b"shared")

        ref_a = remote_file("s3://bucket/a.txt")
        ref_b = remote_file("s3://bucket/b.txt")

        path_a = cache.stage_file(ref=ref_a, backend=backend)
        path_b = cache.stage_file(ref=ref_b, backend=backend)

        assert path_a == path_b  # Same blob.
        blobs = list((tmp_path / "staging" / "blobs").iterdir())
        assert len(blobs) == 1


class TestStageFolder:
    def test_stages_prefix_contents(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = MagicMock()

        backend.list_prefix.return_value = [
            RemoteObjectMeta(uri="s3://bkt/prefix/a.txt", size=3),
            RemoteObjectMeta(uri="s3://bkt/prefix/sub/b.txt", size=3),
        ]

        def _download(*, bucket, key, dest_path):
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_text(key.split("/")[-1])
            return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=3)

        backend.download.side_effect = _download

        ref = remote_folder("s3://bkt/prefix/")
        folder_path = cache.stage_folder(ref=ref, backend=backend)

        assert folder_path.is_dir()
        assert (folder_path / "a.txt").read_text() == "a.txt"
        assert (folder_path / "sub" / "b.txt").read_text() == "b.txt"


class TestLookup:
    def test_returns_entry_after_staging(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = _make_mock_backend()
        ref = remote_file("s3://bucket/lookup.txt")

        cache.stage_file(ref=ref, backend=backend)
        entry = cache.lookup(uri=ref.uri)

        assert entry is not None
        assert isinstance(entry, StagingEntry)
        assert entry.uri == "s3://bucket/lookup.txt"
        assert entry.etag == "etag1"
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
        backend = MagicMock()
        backend.list_prefix.return_value = [
            RemoteObjectMeta(uri="s3://bkt/prefix/a.txt", size=3),
        ]

        def _download(*, bucket, key, dest_path):
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_text("abc")
            return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=3)

        backend.download.side_effect = _download
        folder_path = cache.stage_folder(ref=remote_folder("s3://bkt/prefix/"), backend=backend)

        entry = cache.lookup(uri="s3://bkt/prefix/")
        assert entry is not None
        assert entry.blob_path == f"folders/{folder_path.name}"
        assert entry.size == 3


class TestWorkerStaging:
    """A worker has the bytes it staged but no workspace database to index them."""

    def test_an_in_memory_index_writes_no_database(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging", db_path=MEMORY)
        backend = _make_mock_backend()
        ref = remote_file("s3://bucket/worker.txt")

        path = cache.stage_file(ref=ref, backend=backend)

        assert path.read_bytes() == b"hello world"
        assert cache.lookup(uri=ref.uri) is not None
        assert not list(tmp_path.glob("*.db"))


class TestPrune:
    """Staged bytes are the only store with an eviction path of its own."""

    def _stage(self, cache, uri: str, content: bytes = b"hello world") -> None:
        cache.stage_file(ref=remote_file(uri), backend=_make_mock_backend(content=content))

    def test_an_unused_entry_and_its_bytes_go(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(cache, "s3://bucket/old.txt")
        entry = cache.lookup(uri="s3://bucket/old.txt")
        assert entry is not None

        count, freed = cache.prune(before="2099-01-01T00:00:00+00:00")

        assert count == 1
        assert freed == len(b"hello world")
        assert cache.lookup(uri="s3://bucket/old.txt") is None
        assert not (tmp_path / "staging" / entry.blob_path).exists()

    def test_a_recently_used_entry_stays(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(cache, "s3://bucket/fresh.txt")

        assert cache.prune(before="2000-01-01T00:00:00+00:00") == (0, 0)
        assert cache.lookup(uri="s3://bucket/fresh.txt") is not None

    def test_a_dry_run_measures_without_deleting(self, tmp_path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(cache, "s3://bucket/old.txt")

        count, freed = cache.prune(before="2099-01-01T00:00:00+00:00", dry_run=True)

        assert (count, freed) == (1, len(b"hello world"))
        assert cache.lookup(uri="s3://bucket/old.txt") is not None

    def test_bytes_shared_with_a_surviving_entry_are_kept(self, tmp_path) -> None:
        """Two URIs, identical content: one blob, and it outlives the first row."""
        cache = StagingCache(root=tmp_path / "staging")
        self._stage(cache, "s3://bucket/a.txt")
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
