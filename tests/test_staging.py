"""Unit tests for the remote staging cache."""

from __future__ import annotations

from unittest.mock import MagicMock

from ginkgo.core.remote import remote_file, remote_folder
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.staging import StagingCache, StagingEntry


def _make_mock_backend(*, content: bytes = b"hello world", etag: str = "etag1"):
    """Create a mock backend that writes fixed content on download."""
    backend = MagicMock()

    def _download(*, bucket, key, dest_path):
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(content)
        return RemoteObjectMeta(
            uri=f"s3://{bucket}/{key}",
            size=len(content),
            etag=etag,
        )

    def _head(*, bucket, key):
        return RemoteObjectMeta(
            uri=f"s3://{bucket}/{key}",
            size=len(content),
            etag=etag,
        )

    backend.download.side_effect = _download
    backend.head.side_effect = _head
    return backend


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


class TestStagingEntry:
    def test_json_round_trip(self) -> None:
        entry = StagingEntry(
            uri="s3://b/k",
            digest="abc123",
            etag="etag",
            version_id=None,
            size=100,
            staged_at="2026-01-01T00:00:00Z",
            blob_path="blobs/abc123",
        )
        restored = StagingEntry.from_json(entry.to_json())
        assert restored == entry
