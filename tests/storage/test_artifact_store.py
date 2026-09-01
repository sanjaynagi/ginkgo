"""Unit tests for LocalArtifactStore."""

import stat
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest

from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.core.hashing import hash_directory


@pytest.fixture()
def store(tmp_path):
    """Return a LocalArtifactStore rooted in a temporary directory."""
    with CacheIndex.in_memory() as index:
        yield LocalArtifactStore(root=tmp_path / "artifacts", index=index)


class TestStoreFile:
    def test_round_trip(self, store, tmp_path):
        src = tmp_path / "hello.txt"
        src.write_text("hello world")

        record = store.store(src_path=src)
        assert record.kind == "blob"
        assert record.extension == ".txt"
        assert store.exists(artifact_id=record.artifact_id)

        dest = tmp_path / "restored.txt"
        store.retrieve(artifact_id=record.artifact_id, dest_path=dest)
        assert dest.is_symlink()
        assert dest.read_text() == "hello world"

        restored = tmp_path / "restored-copy.txt"
        store.restore(artifact_id=record.artifact_id, dest_path=restored)
        assert restored.is_file()
        assert not restored.is_symlink()
        assert restored.read_text() == "hello world"

    def test_idempotent(self, store, tmp_path):
        src = tmp_path / "data.csv"
        src.write_text("a,b,c")

        r1 = store.store(src_path=src)
        r2 = store.store(src_path=src)
        assert r1.artifact_id == r2.artifact_id

    def test_read_only(self, store, tmp_path):
        src = tmp_path / "readonly.txt"
        src.write_text("locked")

        record = store.store(src_path=src)
        artifact_path = store.artifact_path(artifact_id=record.artifact_id)
        mode = artifact_path.stat().st_mode
        assert not (mode & stat.S_IWUSR)
        assert not (mode & stat.S_IWGRP)
        assert not (mode & stat.S_IWOTH)

    def test_no_extension(self, store, tmp_path):
        src = tmp_path / "noext"
        src.write_text("content")

        record = store.store(src_path=src)
        assert record.extension == ""
        assert "." not in record.artifact_id

    def test_artifact_id_is_digest_only(self, store, tmp_path):
        src = tmp_path / "file.csv"
        src.write_text("data")

        record = store.store(src_path=src)
        # Artifact ID is now a bare digest, no extension.
        assert "." not in record.artifact_id
        assert record.artifact_id == record.digest_hex


class TestStoreDirectory:
    def test_round_trip(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa")
        (src_dir / "sub").mkdir()
        (src_dir / "sub" / "b.txt").write_text("bbb")

        record = store.store(src_path=src_dir)
        assert record.kind == "tree"
        assert store.exists(artifact_id=record.artifact_id)

        dest = tmp_path / "restored_dir"
        store.retrieve(artifact_id=record.artifact_id, dest_path=dest)
        assert dest.is_dir()
        assert (dest / "a.txt").read_text() == "aaa"
        assert (dest / "sub" / "b.txt").read_text() == "bbb"
        # Individual files are symlinks to blobs.
        assert (dest / "a.txt").is_symlink()
        assert (dest / "sub" / "b.txt").is_symlink()

        restored = tmp_path / "restored_copy"
        store.restore(artifact_id=record.artifact_id, dest_path=restored)
        assert restored.is_dir()
        assert (restored / "a.txt").read_text() == "aaa"
        assert (restored / "sub" / "b.txt").read_text() == "bbb"
        assert not (restored / "a.txt").is_symlink()
        assert not (restored / "sub" / "b.txt").is_symlink()

    def test_idempotent(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "x.txt").write_text("xxx")

        r1 = store.store(src_path=src_dir)
        r2 = store.store(src_path=src_dir)
        assert r1.artifact_id == r2.artifact_id

    def test_blob_dedup_across_directories(self, store, tmp_path):
        """Two directories sharing identical files reuse the same blobs."""
        dir_a = tmp_path / "dir_a"
        dir_a.mkdir()
        (dir_a / "shared.txt").write_text("same content")

        dir_b = tmp_path / "dir_b"
        dir_b.mkdir()
        (dir_b / "shared.txt").write_text("same content")

        store.store(src_path=dir_a)
        store.store(src_path=dir_b)

        # Only one blob for the shared content.
        blobs = list((store._blobs_dir).iterdir())
        assert len(blobs) == 1


class TestDirectoryHashing:
    def test_hash_is_stable_across_creation_order(self, tmp_path: Path):
        first = tmp_path / "first"
        second = tmp_path / "second"

        for root, ordered_names in (
            (first, ("b.txt", "sub/c.txt", "a.txt")),
            (second, ("a.txt", "b.txt", "sub/c.txt")),
        ):
            root.mkdir()
            for relative_name in ordered_names:
                path = root / relative_name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(relative_name)

        assert hash_directory(first) == hash_directory(second)

    def test_hash_changes_when_relative_paths_change(self, tmp_path: Path):
        original = tmp_path / "original"
        renamed = tmp_path / "renamed"

        for root in (original, renamed):
            root.mkdir()
            (root / "sub").mkdir()

        (original / "sub" / "data.txt").write_text("same")
        (renamed / "other.txt").write_text("same")

        assert hash_directory(original) != hash_directory(renamed)

    def test_hash_changes_when_empty_directory_is_added(self, tmp_path: Path):
        without_empty = tmp_path / "without_empty"
        with_empty = tmp_path / "with_empty"

        without_empty.mkdir()
        with_empty.mkdir()
        (without_empty / "data.txt").write_text("payload")
        (with_empty / "data.txt").write_text("payload")
        (with_empty / "empty").mkdir()

        assert hash_directory(without_empty) != hash_directory(with_empty)


class TestStoreBytes:
    def test_round_trip(self, store):
        data = b"binary payload"
        record = store.store_bytes(data=data, extension="bin")
        assert store.exists(artifact_id=record.artifact_id)
        assert store.read_bytes(artifact_id=record.artifact_id) == data

    def test_idempotent(self, store):
        data = b"same content"
        r1 = store.store_bytes(data=data, extension="pkl")
        r2 = store.store_bytes(data=data, extension="pkl")
        assert r1.artifact_id == r2.artifact_id


class TestRetrieve:
    def test_creates_symlink_for_blob(self, store, tmp_path):
        src = tmp_path / "file.dat"
        src.write_bytes(b"\x00\x01\x02")

        record = store.store(src_path=src)
        dest = tmp_path / "link.dat"
        store.retrieve(artifact_id=record.artifact_id, dest_path=dest)

        assert dest.is_symlink()
        target = dest.resolve()
        assert target == store.artifact_path(artifact_id=record.artifact_id).resolve()

    def test_missing_artifact_raises(self, store, tmp_path):
        with pytest.raises(FileNotFoundError):
            store.retrieve(artifact_id="nonexistent", dest_path=tmp_path / "out")

    def test_creates_parent_dirs(self, store, tmp_path):
        src = tmp_path / "file.txt"
        src.write_text("content")
        record = store.store(src_path=src)

        dest = tmp_path / "deep" / "nested" / "link.txt"
        store.retrieve(artifact_id=record.artifact_id, dest_path=dest)
        assert dest.is_symlink()


class TestRestore:
    def test_creates_regular_file_for_blob(self, store, tmp_path):
        src = tmp_path / "file.dat"
        src.write_bytes(b"\x00\x01\x02")

        record = store.store(src_path=src)
        dest = tmp_path / "copy.dat"
        store.restore(artifact_id=record.artifact_id, dest_path=dest)

        assert dest.is_file()
        assert not dest.is_symlink()
        assert dest.read_bytes() == b"\x00\x01\x02"

    def test_creates_regular_files_for_tree(self, store, tmp_path):
        src_dir = tmp_path / "tree"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa")
        (src_dir / "sub").mkdir()
        (src_dir / "sub" / "b.txt").write_text("bbb")

        record = store.store(src_path=src_dir)
        dest = tmp_path / "tree_copy"
        store.restore(artifact_id=record.artifact_id, dest_path=dest)

        assert dest.is_dir()
        assert not (dest / "a.txt").is_symlink()
        assert not (dest / "sub" / "b.txt").is_symlink()
        assert (dest / "a.txt").read_text() == "aaa"
        assert (dest / "sub" / "b.txt").read_text() == "bbb"


class TestMatches:
    def test_matches_blob_content(self, store, tmp_path):
        src = tmp_path / "data.txt"
        src.write_text("payload")
        record = store.store(src_path=src)

        same = tmp_path / "same.txt"
        same.write_text("payload")
        different = tmp_path / "different.txt"
        different.write_text("other")

        assert store.matches(artifact_id=record.artifact_id, path=same) is True
        assert store.matches(artifact_id=record.artifact_id, path=different) is False

    def test_matches_tree_content(self, store, tmp_path):
        src_dir = tmp_path / "tree"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa")
        (src_dir / "sub").mkdir()
        (src_dir / "sub" / "b.txt").write_text("bbb")
        record = store.store(src_path=src_dir)

        same = tmp_path / "same"
        same.mkdir()
        (same / "a.txt").write_text("aaa")
        (same / "sub").mkdir()
        (same / "sub" / "b.txt").write_text("bbb")

        different = tmp_path / "different"
        different.mkdir()
        (different / "a.txt").write_text("aaa")
        (different / "sub").mkdir()
        (different / "sub" / "b.txt").write_text("changed")

        assert store.matches(artifact_id=record.artifact_id, path=same) is True
        assert store.matches(artifact_id=record.artifact_id, path=different) is False


class TestDelete:
    def test_delete_file(self, store, tmp_path):
        src = tmp_path / "to_delete.txt"
        src.write_text("bye")

        record = store.store(src_path=src)
        assert store.exists(artifact_id=record.artifact_id)

        store.delete(artifact_id=record.artifact_id)
        assert not store.exists(artifact_id=record.artifact_id)

    def test_delete_directory(self, store, tmp_path):
        src_dir = tmp_path / "dir_del"
        src_dir.mkdir()
        (src_dir / "f.txt").write_text("data")

        record = store.store(src_path=src_dir)
        assert store.exists(artifact_id=record.artifact_id)

        store.delete(artifact_id=record.artifact_id)
        assert not store.exists(artifact_id=record.artifact_id)

    def test_delete_nonexistent_is_noop(self, store):
        store.delete(artifact_id="does_not_exist")


class TestExists:
    def test_true_for_stored(self, store, tmp_path):
        src = tmp_path / "f.txt"
        src.write_text("content")
        record = store.store(src_path=src)
        assert store.exists(artifact_id=record.artifact_id) is True

    def test_false_for_missing(self, store):
        assert store.exists(artifact_id="missing") is False


class TestStorageLayout:
    """Verify the blobs/trees/refs directory structure."""

    def test_blob_stored_under_blobs_dir(self, store, tmp_path):
        src = tmp_path / "file.csv"
        src.write_text("data")

        record = store.store(src_path=src)
        blob_path = store._blobs_dir / f"{record.digest_hex}{record.extension}"
        assert blob_path.exists()
        assert blob_path.read_text() == "data"

    def test_tree_manifest_stored_under_trees_dir(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa")

        record = store.store(src_path=src_dir)
        tree_path = store._trees_dir / f"{record.digest_hex}.json"
        assert tree_path.exists()

    def test_record_is_a_row_not_a_file(self, store, tmp_path):
        src = tmp_path / "file.txt"
        src.write_text("content")

        record = store.store(src_path=src)

        assert not (store._root / "refs").exists()
        assert store.load_record(artifact_id=record.artifact_id) == record

    def test_subdirs_created_on_init(self, tmp_path):
        root = tmp_path / "new_store"
        LocalArtifactStore(root=root, index=CacheIndex.in_memory())
        assert (root / "blobs").is_dir()
        assert (root / "trees").is_dir()


@contextmanager
def _spy_share_bytes():
    """Record every share_bytes call the artifact store makes."""
    import ginkgo.runtime.artifacts.artifact_store as module

    calls: list[dict] = []
    real = module.share_bytes

    def recorder(*, src, dst, allow_hardlink=False):
        calls.append({"src": src, "dst": dst, "allow_hardlink": allow_hardlink})
        return real(src=src, dst=dst, allow_hardlink=allow_hardlink)

    with mock.patch.object(module, "share_bytes", recorder):
        yield calls


class TestBlobExtensions:
    """A recorded blob's store filename carries its extension (#231)."""

    def test_stored_file_keeps_its_suffix(self, store, tmp_path):
        src = tmp_path / "figure.png"
        src.write_bytes(b"png bytes")

        record = store.store(src_path=src)

        path = store.artifact_path(artifact_id=record.artifact_id)
        assert path.suffix == ".png"
        assert path.name == f"{record.digest_hex}.png"
        assert path.exists()

    def test_store_bytes_keeps_its_suffix(self, store):
        record = store.store_bytes(data=b"payload", extension="parquet")

        path = store.artifact_path(artifact_id=record.artifact_id)
        assert path.suffix == ".parquet"
        assert path.read_bytes() == b"payload"

    def test_extensionless_source_stays_bare(self, store, tmp_path):
        src = tmp_path / "README"
        src.write_text("no suffix")

        record = store.store(src_path=src)

        path = store.artifact_path(artifact_id=record.artifact_id)
        assert path.name == record.digest_hex

    def test_retrieve_restore_read_delete_agree_on_the_path(self, store, tmp_path):
        src = tmp_path / "data.csv"
        src.write_text("a,b")
        record = store.store(src_path=src)

        linked = tmp_path / "linked.csv"
        store.retrieve(artifact_id=record.artifact_id, dest_path=linked)
        assert linked.resolve() == store.artifact_path(artifact_id=record.artifact_id)

        restored = tmp_path / "restored.csv"
        store.restore(artifact_id=record.artifact_id, dest_path=restored)
        assert restored.read_text() == "a,b"

        assert store.read_bytes(artifact_id=record.artifact_id) == b"a,b"

        store.delete(artifact_id=record.artifact_id)
        assert not store.artifact_path(artifact_id=record.artifact_id).exists()

    def test_same_bytes_under_two_names_keep_the_first_record(self, store, tmp_path):
        """Re-labelling the digest would strand the first file as an orphan."""
        first = tmp_path / "table.csv"
        first.write_text("same content")
        second = tmp_path / "table.txt"
        second.write_text("same content")

        r1 = store.store(src_path=first)
        r2 = store.store(src_path=second)

        assert r1.artifact_id == r2.artifact_id
        assert r2.extension == ".csv"
        assert [p.name for p in store._blobs_dir.iterdir()] == [f"{r1.digest_hex}.csv"]
        assert store.integrity_problems() == []

    def test_standalone_blob_adopts_a_tree_member_file(self, store, tmp_path):
        """The second store filename for the same bytes shares from the first.

        share_bytes reflinks or hardlinks, so the observable is its source:
        the second store must read from the sibling blob, not the user file.
        """
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "member.csv").write_text("shared bytes")
        solo = tmp_path / "solo.csv"
        solo.write_text("shared bytes")

        store.store(src_path=src_dir)
        with _spy_share_bytes() as calls:
            record = store.store(src_path=solo)

        bare = store._blobs_dir / record.digest_hex
        extended = store.artifact_path(artifact_id=record.artifact_id)
        assert bare.exists() and extended.exists()
        assert extended.read_text() == "shared bytes"
        assert [c for c in calls if c["dst"] == extended] == [
            {"src": bare, "dst": extended, "allow_hardlink": True}
        ]
        assert store.integrity_problems() == []

    def test_tree_member_adopts_an_already_recorded_blob(self, store, tmp_path):
        """Same dedup in the other order: standalone first, folder second."""
        solo = tmp_path / "solo.csv"
        solo.write_text("shared bytes")
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "member.csv").write_text("shared bytes")

        record = store.store(src_path=solo)
        with _spy_share_bytes() as calls:
            store.store(src_path=src_dir)

        bare = store._blobs_dir / record.digest_hex
        extended = store.artifact_path(artifact_id=record.artifact_id)
        assert bare.read_text() == "shared bytes"
        assert [c for c in calls if c["dst"] == bare] == [
            {"src": extended, "dst": bare, "allow_hardlink": True}
        ]
        assert store.integrity_problems() == []

    def test_an_absurd_suffix_is_not_an_extension(self, store, tmp_path):
        src = tmp_path / ("data." + "x" * 200)
        src.write_text("bytes")

        record = store.store(src_path=src)

        assert record.extension == ""
        assert store.artifact_path(artifact_id=record.artifact_id).exists()

    def test_tree_member_blobs_stay_bare_digests(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "a.csv").write_text("aaa")

        store.store(src_path=src_dir)

        blob_names = [p.name for p in store._blobs_dir.iterdir()]
        assert len(blob_names) == 1
        assert "." not in blob_names[0]


class TestRecordAccess:
    def test_load_record_returns_stored_metadata(self, store):
        record = store.store_bytes(data=b"payload", extension="txt")

        loaded = store.load_record(artifact_id=record.artifact_id)
        assert loaded is not None
        assert loaded.artifact_id == record.artifact_id
        assert loaded.digest_hex == record.digest_hex

    def test_load_record_missing_returns_none(self, store):
        assert store.load_record(artifact_id="missing") is None

    def test_list_artifact_ids(self, store):
        assert store.list_artifact_ids() == []

        first = store.store_bytes(data=b"one", extension="txt")
        second = store.store_bytes(data=b"two", extension="txt")
        assert store.list_artifact_ids() == sorted([first.artifact_id, second.artifact_id])
