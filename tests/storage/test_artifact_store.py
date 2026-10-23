"""LocalArtifactStore: content-addressed blobs and trees under one root."""

import stat
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest

import ginkgo.runtime.artifacts.artifact_store as artifact_store_module
from ginkgo.core.hashing import hash_directory
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.caching.index import CacheIndex


@pytest.fixture()
def store(tmp_path):
    with CacheIndex.in_memory() as index:
        yield LocalArtifactStore(root=tmp_path / "artifacts", index=index)


def _tree(root: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    return root


def _corrupt(path: Path) -> None:
    """Overwrite a read-only stored file in place, as bit rot would."""
    path.chmod(0o644)
    path.write_bytes(b"BAD")


def test_a_file_round_trips_as_a_read_only_blob_named_by_its_digest(store, tmp_path):
    src = tmp_path / "data.csv"
    src.write_text("a,b")

    record = store.store(src_path=src)

    assert (record.kind, record.extension) == ("blob", ".csv")
    assert record.artifact_id == record.digest_hex
    assert store.store(src_path=src).artifact_id == record.artifact_id
    assert store.load_record(artifact_id=record.artifact_id) == record
    assert not (store._root / "refs").exists()  # the record is a row, not a file

    blob = store.artifact_path(artifact_id=record.artifact_id)
    assert blob == store._blobs_dir / f"{record.digest_hex}.csv"
    assert not blob.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

    linked = tmp_path / "deep" / "nested" / "linked.csv"
    store.retrieve(artifact_id=record.artifact_id, dest_path=linked)
    assert linked.is_symlink() and linked.resolve() == blob

    restored = tmp_path / "restored.csv"
    assert store.restore(artifact_id=record.artifact_id, dest_path=restored) is True
    assert restored.is_file() and not restored.is_symlink()
    assert restored.read_text() == "a,b"
    assert store.read_bytes(artifact_id=record.artifact_id) == b"a,b"

    store.delete(artifact_id=record.artifact_id)
    assert not store.exists(artifact_id=record.artifact_id)
    assert not blob.exists()


def test_a_directory_round_trips_as_a_tree_of_shared_blobs(store, tmp_path):
    src = _tree(tmp_path / "mydir", {"a.txt": "aaa", "sub/b.txt": "bbb"})

    record = store.store(src_path=src)

    assert record.kind == "tree"
    assert store.store(src_path=src).artifact_id == record.artifact_id
    assert (store._trees_dir / f"{record.digest_hex}.json").exists()
    # Member blobs are bare digests: a tree member has no name of its own.
    assert all("." not in blob.name for blob in store._blobs_dir.iterdir())

    linked = tmp_path / "linked"
    store.retrieve(artifact_id=record.artifact_id, dest_path=linked)
    assert (linked / "sub" / "b.txt").is_symlink()
    assert (linked / "sub" / "b.txt").read_text() == "bbb"

    restored = tmp_path / "restored"
    store.restore(artifact_id=record.artifact_id, dest_path=restored)
    assert not (restored / "a.txt").is_symlink()
    assert (restored / "a.txt").read_text() == "aaa"
    assert (restored / "sub" / "b.txt").read_text() == "bbb"

    store.delete(artifact_id=record.artifact_id)
    assert not store.exists(artifact_id=record.artifact_id)


def test_identical_files_in_two_directories_share_one_blob(store, tmp_path):
    store.store(src_path=_tree(tmp_path / "a", {"shared.txt": "same"}))
    store.store(src_path=_tree(tmp_path / "b", {"shared.txt": "same"}))

    assert len(list(store._blobs_dir.iterdir())) == 1


def test_bytes_round_trip_under_their_extension(store):
    record = store.store_bytes(data=b"payload", extension="parquet")

    assert (
        store.store_bytes(data=b"payload", extension="parquet").artifact_id == record.artifact_id
    )
    path = store.artifact_path(artifact_id=record.artifact_id)
    assert path.name == f"{record.digest_hex}.parquet"
    assert store.read_bytes(artifact_id=record.artifact_id) == b"payload"


@pytest.mark.parametrize("name", ["README", "data." + "x" * 200], ids=["none", "absurd"])
def test_a_name_without_a_real_extension_stores_a_bare_digest(store, tmp_path, name):
    src = tmp_path / name
    src.write_text("bytes")

    record = store.store(src_path=src)

    assert record.extension == ""
    assert store.artifact_path(artifact_id=record.artifact_id).name == record.digest_hex


def test_missing_artifacts(store, tmp_path):
    assert store.exists(artifact_id="missing") is False
    assert store.load_record(artifact_id="missing") is None
    store.delete(artifact_id="missing")  # a no-op
    with pytest.raises(FileNotFoundError):
        store.retrieve(artifact_id="missing", dest_path=tmp_path / "out")


def test_list_artifact_ids(store):
    assert store.list_artifact_ids() == []
    first = store.store_bytes(data=b"one", extension="txt")
    second = store.store_bytes(data=b"two", extension="txt")

    assert store.list_artifact_ids() == sorted([first.artifact_id, second.artifact_id])


@pytest.mark.parametrize(
    ("changed", "matches"),
    [({}, True), ({"sub/b.txt": "changed"}, False)],
    ids=["same", "different"],
)
@pytest.mark.parametrize("kind", ["blob", "tree"])
def test_matches_compares_content(store, tmp_path, kind, changed, matches):
    files = {"a.txt": "aaa", "sub/b.txt": "bbb"}
    if kind == "blob":
        src, candidate = tmp_path / "src.txt", tmp_path / "candidate.txt"
        src.write_text("bbb")
        candidate.write_text(changed.get("sub/b.txt", "bbb"))
    else:
        src = _tree(tmp_path / "src", files)
        candidate = _tree(tmp_path / "candidate", {**files, **changed})
    record = store.store(src_path=src)

    assert store.matches(artifact_id=record.artifact_id, path=candidate) is matches


class TestRestoreVerification:
    def test_a_corrupt_blob_is_not_restored_and_its_bytes_are_discarded(self, store, tmp_path):
        src = tmp_path / "good.txt"
        src.write_text("GOOD")
        record = store.store(src_path=src)
        blob = store.artifact_path(artifact_id=record.artifact_id)
        _corrupt(blob)

        dest = tmp_path / "out.txt"
        assert store.restore(artifact_id=record.artifact_id, dest_path=dest) is False
        assert not dest.exists() and not blob.exists()

        # The re-run stores the good bytes again and they restore.
        store.store(src_path=src)
        assert store.restore(artifact_id=record.artifact_id, dest_path=dest) is True
        assert dest.read_text() == "GOOD"

    def test_a_corrupt_tree_member_fails_the_whole_restore(self, store, tmp_path):
        record = store.store(src_path=_tree(tmp_path / "tree", {"a.txt": "aaa"}))
        _corrupt(next(store._blobs_dir.iterdir()))

        dest = tmp_path / "tree_copy"
        assert store.restore(artifact_id=record.artifact_id, dest_path=dest) is False
        assert not dest.exists()

    def test_verify_reports_corrupt_bytes(self, store, tmp_path):
        src = tmp_path / "good.txt"
        src.write_text("GOOD")
        record = store.store(src_path=src)
        assert store.integrity_problems(verify=True) == []

        _corrupt(store.artifact_path(artifact_id=record.artifact_id))

        assert store.integrity_problems() == []
        assert store.integrity_problems(verify=True) == [
            f"artifact {record.artifact_id}: bytes do not match its digest"
        ]


@pytest.mark.parametrize(
    ("layouts", "same"),
    [
        (
            (
                {"b.txt": "b", "sub/c.txt": "c", "a.txt": "a"},
                {"a.txt": "a", "b.txt": "b", "sub/c.txt": "c"},
            ),
            True,
        ),
        (({"sub/data.txt": "same"}, {"other.txt": "same"}), False),
    ],
    ids=["creation-order-is-irrelevant", "relative-paths-matter"],
)
def test_directory_hashing(tmp_path, layouts, same):
    first, second = (_tree(tmp_path / f"d{i}", files) for i, files in enumerate(layouts))
    assert (hash_directory(first) == hash_directory(second)) is same


def test_an_empty_directory_changes_the_directory_hash(tmp_path):
    without = _tree(tmp_path / "without", {"data.txt": "payload"})
    with_empty = _tree(tmp_path / "with", {"data.txt": "payload"})
    (with_empty / "empty").mkdir()

    assert hash_directory(without) != hash_directory(with_empty)


class TestBlobExtensions:
    """A recorded blob's store filename carries its extension (#231)."""

    @contextmanager
    def _shares(self):
        """Record every share_bytes call the store makes, still sharing for real."""
        calls: list[tuple[Path, Path, bool]] = []
        real = artifact_store_module.share_bytes

        def recorder(*, src, dst, allow_hardlink=False):
            calls.append((src, dst, allow_hardlink))
            return real(src=src, dst=dst, allow_hardlink=allow_hardlink)

        with mock.patch.object(artifact_store_module, "share_bytes", recorder):
            yield calls

    def test_the_same_bytes_under_two_names_keep_the_first_record(self, store, tmp_path):
        """Re-labelling the digest would strand the first file as an orphan."""
        first, second = tmp_path / "table.csv", tmp_path / "table.txt"
        first.write_text("same content")
        second.write_text("same content")

        r1 = store.store(src_path=first)
        r2 = store.store(src_path=second)

        assert r1.artifact_id == r2.artifact_id
        assert r2.extension == ".csv"
        assert [p.name for p in store._blobs_dir.iterdir()] == [f"{r1.digest_hex}.csv"]
        assert store.integrity_problems() == []

    @pytest.mark.parametrize("tree_first", [True, False], ids=["tree-first", "blob-first"])
    def test_a_second_name_for_the_same_bytes_shares_from_the_first(
        self, store, tmp_path, tree_first
    ):
        """share_bytes reflinks or hardlinks, so the observable is its source."""
        tree = _tree(tmp_path / "mydir", {"member.csv": "shared bytes"})
        solo = tmp_path / "solo.csv"
        solo.write_text("shared bytes")
        first, second = (tree, solo) if tree_first else (solo, tree)

        store.store(src_path=first)
        with self._shares() as calls:
            store.store(src_path=second)

        record = store.store(src_path=solo)
        bare = store._blobs_dir / record.digest_hex
        extended = store.artifact_path(artifact_id=record.artifact_id)
        assert bare.read_text() == extended.read_text() == "shared bytes"
        src, dst = (bare, extended) if tree_first else (extended, bare)
        assert [call for call in calls if call[1] == dst] == [(src, dst, True)]
        assert store.integrity_problems() == []
