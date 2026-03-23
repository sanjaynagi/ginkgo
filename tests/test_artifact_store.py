"""Unit tests for LocalArtifactStore."""

import stat

import pytest

from ginkgo.runtime.artifact_store import LocalArtifactStore


@pytest.fixture()
def store(tmp_path):
    """Return a LocalArtifactStore rooted in a temporary directory."""
    return LocalArtifactStore(root=tmp_path / "artifacts")


class TestStoreFile:
    def test_round_trip(self, store, tmp_path):
        src = tmp_path / "hello.txt"
        src.write_text("hello world")

        artifact_id = store.store(src_path=src)
        assert artifact_id.endswith(".txt")
        assert store.exists(artifact_id=artifact_id)

        dest = tmp_path / "restored.txt"
        store.retrieve(artifact_id=artifact_id, dest_path=dest)
        assert dest.is_symlink()
        assert dest.read_text() == "hello world"

    def test_idempotent(self, store, tmp_path):
        src = tmp_path / "data.csv"
        src.write_text("a,b,c")

        id1 = store.store(src_path=src)
        id2 = store.store(src_path=src)
        assert id1 == id2

    def test_read_only(self, store, tmp_path):
        src = tmp_path / "readonly.txt"
        src.write_text("locked")

        artifact_id = store.store(src_path=src)
        artifact_path = store.artifact_path(artifact_id=artifact_id)
        mode = artifact_path.stat().st_mode
        assert not (mode & stat.S_IWUSR)
        assert not (mode & stat.S_IWGRP)
        assert not (mode & stat.S_IWOTH)

    def test_no_extension(self, store, tmp_path):
        src = tmp_path / "noext"
        src.write_text("content")

        artifact_id = store.store(src_path=src)
        assert "." not in artifact_id


class TestStoreDirectory:
    def test_round_trip(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa")
        (src_dir / "sub").mkdir()
        (src_dir / "sub" / "b.txt").write_text("bbb")

        artifact_id = store.store(src_path=src_dir)
        assert store.exists(artifact_id=artifact_id)

        dest = tmp_path / "restored_dir"
        store.retrieve(artifact_id=artifact_id, dest_path=dest)
        assert dest.is_symlink()
        assert (dest / "a.txt").read_text() == "aaa"
        assert (dest / "sub" / "b.txt").read_text() == "bbb"

    def test_idempotent(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "x.txt").write_text("xxx")

        id1 = store.store(src_path=src_dir)
        id2 = store.store(src_path=src_dir)
        assert id1 == id2

    def test_contents_read_only(self, store, tmp_path):
        src_dir = tmp_path / "mydir"
        src_dir.mkdir()
        (src_dir / "f.txt").write_text("data")

        artifact_id = store.store(src_path=src_dir)
        stored_file = store.artifact_path(artifact_id=artifact_id) / "f.txt"
        mode = stored_file.stat().st_mode
        assert not (mode & stat.S_IWUSR)


class TestStoreBytes:
    def test_round_trip(self, store):
        data = b"binary payload"
        artifact_id = store.store_bytes(data=data, extension="bin")
        assert store.exists(artifact_id=artifact_id)
        assert store.read_bytes(artifact_id=artifact_id) == data

    def test_idempotent(self, store):
        data = b"same content"
        id1 = store.store_bytes(data=data, extension="pkl")
        id2 = store.store_bytes(data=data, extension="pkl")
        assert id1 == id2


class TestRetrieve:
    def test_creates_symlink(self, store, tmp_path):
        src = tmp_path / "file.dat"
        src.write_bytes(b"\x00\x01\x02")

        artifact_id = store.store(src_path=src)
        dest = tmp_path / "link.dat"
        store.retrieve(artifact_id=artifact_id, dest_path=dest)

        assert dest.is_symlink()
        target = dest.resolve()
        assert target == store.artifact_path(artifact_id=artifact_id).resolve()

    def test_missing_artifact_raises(self, store, tmp_path):
        with pytest.raises(FileNotFoundError):
            store.retrieve(artifact_id="nonexistent.txt", dest_path=tmp_path / "out")

    def test_creates_parent_dirs(self, store, tmp_path):
        src = tmp_path / "file.txt"
        src.write_text("content")
        artifact_id = store.store(src_path=src)

        dest = tmp_path / "deep" / "nested" / "link.txt"
        store.retrieve(artifact_id=artifact_id, dest_path=dest)
        assert dest.is_symlink()


class TestDelete:
    def test_delete_file(self, store, tmp_path):
        src = tmp_path / "to_delete.txt"
        src.write_text("bye")

        artifact_id = store.store(src_path=src)
        assert store.exists(artifact_id=artifact_id)

        store.delete(artifact_id=artifact_id)
        assert not store.exists(artifact_id=artifact_id)

    def test_delete_directory(self, store, tmp_path):
        src_dir = tmp_path / "dir_del"
        src_dir.mkdir()
        (src_dir / "f.txt").write_text("data")

        artifact_id = store.store(src_path=src_dir)
        assert store.exists(artifact_id=artifact_id)

        store.delete(artifact_id=artifact_id)
        assert not store.exists(artifact_id=artifact_id)

    def test_delete_nonexistent_is_noop(self, store):
        store.delete(artifact_id="does_not_exist.txt")


class TestExists:
    def test_true_for_stored(self, store, tmp_path):
        src = tmp_path / "f.txt"
        src.write_text("content")
        artifact_id = store.store(src_path=src)
        assert store.exists(artifact_id=artifact_id) is True

    def test_false_for_missing(self, store):
        assert store.exists(artifact_id="missing.txt") is False
