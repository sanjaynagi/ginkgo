"""Unit tests for LocalArtifactStore."""

import stat
from pathlib import Path

import pytest

from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.caching.hashing import hash_directory


@pytest.fixture()
def store(tmp_path):
    """Return a LocalArtifactStore rooted in a temporary directory."""
    return LocalArtifactStore(root=tmp_path / "artifacts")


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
        blob_path = store._blobs_dir / record.digest_hex
        assert blob_path.exists()
        assert blob_path.read_text() == "data"

    def test_tree_manifest_stored_under_trees_dir(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa")

        record = store.store(src_path=src_dir)
        tree_path = store._trees_dir / f"{record.digest_hex}.json"
        assert tree_path.exists()

    def test_ref_stored_under_refs_dir(self, store, tmp_path):
        src = tmp_path / "file.txt"
        src.write_text("content")

        record = store.store(src_path=src)
        ref_path = store._refs_dir / f"{record.artifact_id}.json"
        assert ref_path.exists()

    def test_subdirs_created_on_init(self, tmp_path):
        root = tmp_path / "new_store"
        LocalArtifactStore(root=root)
        assert (root / "blobs").is_dir()
        assert (root / "trees").is_dir()
        assert (root / "refs").is_dir()
