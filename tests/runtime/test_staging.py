"""The remote staging cache: what it downloads, reuses, records and prunes."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from ginkgo.core.hashing import hash_bytes
from ginkgo.core.remote import remote_file, remote_folder
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.staging import StagingCache, StagingEntry, StagingIndex
from ginkgo.store.sqlite import MEMORY
from tests.conftest import make_download_backend

OLD = "2099-01-01T00:00:00+00:00"  # a cutoff after everything staged in a test


def _folder_backend() -> MagicMock:
    """A backend listing ``prefix/a.txt`` and ``prefix/sub/b.txt``; each downloads as its name."""
    backend = MagicMock()
    backend.list_prefix.return_value = [
        RemoteObjectMeta(uri="s3://bkt/prefix/a.txt", size=5),
        RemoteObjectMeta(uri="s3://bkt/prefix/sub/b.txt", size=5),
    ]

    def _download(*, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text(key.split("/")[-1])
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=5)

    backend.download.side_effect = _download
    return backend


def _entry(uri: str, digest: str, **fields) -> StagingEntry:
    return StagingEntry(
        uri=uri,
        digest=digest,
        etag=fields.get("etag"),
        version_id=None,
        size=fields.get("size", 1),
        staged_at=fields.get("staged_at", "2026-01-01T00:00:00Z"),
        blob_path=fields.get("blob_path", f"blobs/{digest}"),
    )


class TestStageFile:
    def test_a_file_is_downloaded_once_and_recorded(self, tmp_path: Path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = make_download_backend()
        ref = remote_file("s3://bucket/data/file.txt")

        path = cache.stage_file(ref=ref, backend=backend)

        assert path.read_bytes() == b"hello world"
        assert cache.stage_file(ref=ref, backend=backend) == path
        assert backend.download.call_count == 1
        entry = cache.lookup(uri=ref.uri)
        assert (entry.etag, entry.digest) == ("etag1", hash_bytes(b"hello world"))
        assert cache.lookup(uri="s3://bucket/never-staged.txt") is None

    def test_a_changed_etag_is_downloaded_again(self, tmp_path: Path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        ref = remote_file("s3://bucket/data/file.txt")
        cache.stage_file(ref=ref, backend=make_download_backend(content=b"v1", etag="etag-v1"))

        path = cache.stage_file(
            ref=ref, backend=make_download_backend(content=b"v2", etag="etag-v2")
        )

        assert path.read_bytes() == b"v2"

    def test_a_pinned_version_is_reused_without_a_freshness_check(self, tmp_path: Path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = make_download_backend()
        ref = remote_file("s3://bucket/key.txt", version_id="v42")

        cache.stage_file(ref=ref, backend=backend)
        cache.stage_file(ref=ref, backend=backend)

        backend.head.assert_not_called()
        assert backend.download.call_count == 1

    def test_identical_content_under_two_uris_shares_one_blob(self, tmp_path: Path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        backend = make_download_backend(content=b"shared")

        path_a = cache.stage_file(ref=remote_file("s3://bucket/a.txt"), backend=backend)
        path_b = cache.stage_file(ref=remote_file("s3://bucket/b.txt"), backend=backend)

        assert path_a == path_b
        assert len(list((tmp_path / "staging" / "blobs").iterdir())) == 1

    def test_a_staged_path_carries_the_digest_of_what_that_call_staged(
        self, tmp_path: Path
    ) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        v1 = cache.stage_file(
            ref=remote_file("s3://bucket/data/file.txt", version_id="v1"),
            backend=make_download_backend(content=b"first", etag="e1"),
        )
        # A second version of the same URI replaces the index row.
        cache.stage_file(
            ref=remote_file("s3://bucket/data/file.txt", version_id="v2"),
            backend=make_download_backend(content=b"second", etag="e2"),
        )

        assert StagingCache.digest_of(v1) == hash_bytes(b"first")
        assert cache.lookup(uri="s3://bucket/data/file.txt").digest == hash_bytes(b"second")

    def test_a_worker_s_in_memory_index_writes_no_database(self, tmp_path: Path) -> None:
        cache = StagingCache(root=tmp_path / "staging", db_path=MEMORY)
        ref = remote_file("s3://bucket/worker.txt")

        assert cache.stage_file(ref=ref, backend=make_download_backend()).exists()
        assert cache.lookup(uri=ref.uri) is not None
        assert not list(tmp_path.rglob("*.db"))


def test_a_folder_is_staged_and_recorded(tmp_path: Path) -> None:
    cache = StagingCache(root=tmp_path / "staging")

    folder_path = cache.stage_folder(
        ref=remote_folder("s3://bkt/prefix/"), backend=_folder_backend()
    )

    assert (folder_path / "a.txt").read_text() == "a.txt"
    assert (folder_path / "sub" / "b.txt").read_text() == "b.txt"
    entry = cache.lookup(uri="s3://bkt/prefix/")
    assert (entry.blob_path, entry.size) == (f"folders/{folder_path.name}", 10)


def test_the_index_round_trips_and_replaces_rows(tmp_path: Path) -> None:
    StagingCache(root=tmp_path / "staging")
    assert not (tmp_path / "ginkgo.db").exists()  # constructing creates no database

    with StagingIndex.open(path=tmp_path / "ginkgo.db") as index:
        index.record(_entry("s3://b/k", "one", etag="etag", size=100))
        assert index.entry(uri="s3://b/k") == _entry("s3://b/k", "one", etag="etag", size=100)
        index.record(_entry("s3://b/k", "two"))
        assert [entry.digest for entry in index.entries()] == ["two"]


class TestPrune:
    """Staged bytes are the only store with an eviction path of its own."""

    def test_an_unused_entry_and_its_bytes_go_unless_dry_run(self, tmp_path: Path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        cache.stage_file(ref=remote_file("s3://bucket/old.txt"), backend=make_download_backend())
        entry = cache.lookup(uri="s3://bucket/old.txt")

        assert cache.prune(before="2000-01-01T00:00:00+00:00") == (0, 0)  # recently used
        assert cache.prune(before=OLD, dry_run=True) == (1, len(b"hello world"))
        assert cache.lookup(uri="s3://bucket/old.txt") is not None
        assert cache.prune(before=OLD) == (1, len(b"hello world"))
        assert cache.lookup(uri="s3://bucket/old.txt") is None
        assert not (tmp_path / "staging" / entry.blob_path).exists()

    def test_bytes_shared_with_a_surviving_entry_are_kept(self, tmp_path: Path) -> None:
        cache = StagingCache(root=tmp_path / "staging")
        cache.stage_file(ref=remote_file("s3://bucket/a.txt"), backend=make_download_backend())
        entry = cache.lookup(uri="s3://bucket/a.txt")
        # A second URI on the same blob, used later than the cutoff.
        cache.index.record(
            _entry("s3://bucket/b.txt", entry.digest, staged_at=OLD, blob_path=entry.blob_path)
        )

        assert cache.prune(before="2098-01-01T00:00:00+00:00") == (1, 0)
        assert (tmp_path / "staging" / entry.blob_path).exists()

    def test_pruning_a_workspace_that_never_staged_anything(self, tmp_path: Path) -> None:
        assert StagingCache(root=tmp_path / "staging").prune(before=OLD) == (0, 0)
