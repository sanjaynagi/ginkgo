"""Tests for ``CacheStore.label_inputs`` — metadata alongside the cache key.

The central guarantee under test: computing or storing these labels never
changes the cache key itself (issue #307 phase 1). Each category is exercised
through ``build_cache_key``'s own parameter loop, not by re-deriving the
labelling rules, so a change to one cannot silently drift from the other.
"""

from __future__ import annotations

from pathlib import Path

from ginkgo import Out, file, folder, task, tmp_dir
from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex


@task()
def analyze(coords: str, threads: int, scratch: tmp_dir) -> str:
    return coords


@task()
def read_file(data: file) -> str:
    return str(data)


@task()
def consume_asset(dataset: str) -> str:
    return dataset


@task()
def write_output(payload: str, out_path: Out[file]) -> file:
    return file(out_path)


def _store(tmp_path: Path) -> CacheStore:
    index = CacheIndex.open(path=tmp_path / "ginkgo.db")
    return CacheStore(index=index, root=tmp_path / "cache")


class TestLabelsDoNotAffectTheKey:
    def test_key_is_identical_whether_or_not_labels_are_computed(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        resolved_args = {"coords": "north", "threads": "4", "scratch": "unused"}

        key_before, _ = store.build_cache_key(task_def=analyze, resolved_args=resolved_args)
        store.label_inputs(task_def=analyze, resolved_args=resolved_args)
        key_after, _ = store.build_cache_key(task_def=analyze, resolved_args=resolved_args)

        assert key_before == key_after

    def test_a_directory_labelled_input_keys_the_same_as_before_labelling_existed(
        self, tmp_path: Path
    ) -> None:
        """The remaining silent-staleness trap (a directory, never auto-hashed)
        must stay exactly as silent. Labelling a ``str``-annotated existing
        directory ``"path"`` is metadata only: the key must still be the path
        string, not its contents, so writing a new file into the directory
        at the same path must not move the key.
        """
        store = _store(tmp_path)
        target = tmp_path / "coords_dir"
        target.mkdir()
        (target / "a.txt").write_text("v1", encoding="utf-8")
        resolved_args = {"coords": str(target), "threads": "4", "scratch": "unused"}

        key_v1, _ = store.build_cache_key(task_def=analyze, resolved_args=resolved_args)
        assert store.label_inputs(task_def=analyze, resolved_args=resolved_args)["coords"] == (
            "path"
        )

        (target / "b.txt").write_text("v2", encoding="utf-8")
        key_v2, _ = store.build_cache_key(task_def=analyze, resolved_args=resolved_args)
        assert key_v1 == key_v2

    def test_a_root_input_file_is_content_hashed_and_moves_the_key(self, tmp_path: Path) -> None:
        """#307 phase 2 / #121 / #281: unlike a directory, a plain ``str``
        naming an existing *file* is content-hashed by default, so editing it
        does move the key — this is the fix, not a trap."""
        store = _store(tmp_path)
        target = tmp_path / "coords.txt"
        target.write_text("v1", encoding="utf-8")
        resolved_args = {"coords": str(target), "threads": "4", "scratch": "unused"}

        key_v1, _ = store.build_cache_key(task_def=analyze, resolved_args=resolved_args)
        assert store.label_inputs(task_def=analyze, resolved_args=resolved_args)["coords"] == (
            "content"
        )

        target.write_text("v2", encoding="utf-8")
        key_v2, _ = store.build_cache_key(task_def=analyze, resolved_args=resolved_args)
        assert key_v1 != key_v2


class TestLabelCategories:
    def test_tmp_dir_is_untracked(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        labels = store.label_inputs(
            task_def=analyze,
            resolved_args={"coords": "north", "threads": "4", "scratch": "unused"},
        )
        assert labels == {"coords": "value", "threads": "value", "scratch": "untracked"}

    def test_str_naming_an_existing_directory_is_path(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        target = tmp_path / "coords_dir"
        target.mkdir()
        labels = store.label_inputs(
            task_def=analyze,
            resolved_args={"coords": str(target), "threads": "4", "scratch": "unused"},
        )
        assert labels["coords"] == "path"

    def test_str_naming_an_existing_file_is_content(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        target = tmp_path / "coords.txt"
        target.write_text("v1", encoding="utf-8")
        labels = store.label_inputs(
            task_def=analyze,
            resolved_args={"coords": str(target), "threads": "4", "scratch": "unused"},
        )
        assert labels["coords"] == "content"

    def test_file_annotation_is_content(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        target = tmp_path / "data.bin"
        target.write_bytes(b"payload")
        labels = store.label_inputs(task_def=read_file, resolved_args={"data": str(target)})
        assert labels == {"data": "content"}

    def test_asset_ref_is_asset(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        ref = AssetRef(
            key=AssetKey(namespace="table", name="producer.dataset"),
            version_id="v1",
            kind="table",
            artifact_id="abc123",
            content_hash="def456",
            artifact_path=str(tmp_path / "abc123"),
        )
        labels = store.label_inputs(task_def=consume_asset, resolved_args={"dataset": ref})
        assert labels == {"dataset": "asset"}

    def test_out_parameter_is_output(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        out_path = str(tmp_path / "result.txt")
        labels = store.label_inputs(
            task_def=write_output,
            resolved_args={"payload": "hi", "out_path": out_path},
        )
        assert labels == {"payload": "value", "out_path": "output"}


@task()
def analyze_folder(readings: folder) -> str:
    return str(readings)


class TestFolderCategory:
    def test_folder_annotation_is_content(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        readings = tmp_path / "readings"
        readings.mkdir()
        (readings / "a.txt").write_text("x", encoding="utf-8")
        labels = store.label_inputs(
            task_def=analyze_folder, resolved_args={"readings": str(readings)}
        )
        assert labels == {"readings": "content"}
