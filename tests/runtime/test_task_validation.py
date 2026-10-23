"""Input tracking labels and scalar annotation checks (issue #307).

``label_input_value`` is the label behind ``cache explain`` and the run
summary's "tracked by path only" line. Each case must match the branch
``CacheStore._hash_value`` takes for the same ``(annotation, value)`` pair.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest

from ginkgo import Out, file, folder, task, tmp_dir, untracked
from ginkgo.core.remote import RemoteFileRef
from ginkgo.core.secret import SecretRef
from ginkgo.remote.access.protocol import encode_remote_input
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.task_validation import (
    TaskValidator,
    is_content_trackable_path_value,
    is_untracked_directory_value,
    label_input_value,
)

REMOTE = RemoteFileRef(
    uri="s3://bucket/key", scheme="s3", bucket="bucket", key="key", version_id="v1"
)


@pytest.fixture
def tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A working directory holding files ``a.txt``, ``b.txt``, ``alpha`` and a ``sub`` dir."""
    monkeypatch.chdir(tmp_path)
    for name in ("a.txt", "b.txt", "alpha"):
        (tmp_path / name).write_text("x", encoding="utf-8")
    (tmp_path / "sub").mkdir()
    return tmp_path


# Values naming paths are written relative to the ``tree`` fixture; ``{t}`` is its root.
@pytest.mark.parametrize(
    ("annotation", "value", "label"),
    [
        (file, "{t}/a.txt", "content"),
        (folder, "{t}", "content"),
        (str, file("{t}/a.txt"), "content"),
        (str, "{t}/a.txt", "content"),  # #307 phase 2: a root-input path is content-hashed
        (str, "a.txt", "content"),  # an extension alone reads as a path
        (Any, "{t}/a.txt", "content"),
        (list[file], ["{t}/a.txt", "{t}/b.txt"], "content"),
        (str, REMOTE, "version"),
        (str, encode_remote_input(ref=REMOTE, policy="fuse"), "version"),
        (str, "{t}", "path"),  # a directory is never auto-hashed
        (str, "alpha", "path"),  # a bare word naming a file does not read as a path
        (list[str], ["{t}/sub", "nope"], "path"),
        (dict[str, str], {"a": "{t}/sub", "b": "plain"}, "path"),
        (list[str], ["{t}/a.txt", "{t}/sub"], "path"),  # the least tracked element wins
        (str, "not-a-real-path-xyz", "value"),
        (int, 5, "value"),
        (str, SecretRef(name="api_key"), "value"),
        (object, [1, 2], "value"),
        (str | None, None, "value"),
        (list, [REMOTE, 5], "value"),  # a repr digest is weaker than a version id
        (list[str], [], "value"),
        (tmp_dir, tmp_dir("/tmp/x"), "untracked"),
        (str, tmp_dir("/tmp/x"), "untracked"),
        (untracked, "{t}/a.txt", "untracked"),  # the explicit opt-out
        (str, untracked("{t}/a.txt"), "untracked"),
        (list[untracked], ["{t}/a.txt"], "untracked"),
        (untracked | None, "{t}/a.txt", "untracked"),
    ],
)
def test_label_input_value(tree: Path, annotation, value, label: str) -> None:
    assert label_input_value(annotation=annotation, value=_rooted(value, tree)) == label


@pytest.mark.parametrize("value", [file("a.txt"), REMOTE])
def test_an_output_parameter_is_labelled_output(tree: Path, value) -> None:
    assert label_input_value(annotation=file, value=value, is_output=True) == "output"


@pytest.mark.parametrize(
    ("annotation", "value", "trackable"),
    [
        (str, "{t}/a.txt", True),
        (str, "alpha", False),  # bare word
        (str, "{t}/sub", False),  # directory
        (str, "{t}/gone.txt", False),  # missing
        (file, "{t}/a.txt", False),  # already tracked by its annotation
        (untracked, "{t}/a.txt", False),
        (str, "s3://bucket/key.csv", False),
    ],
)
def test_is_content_trackable_path_value(tree: Path, annotation, value, trackable) -> None:
    assert is_content_trackable_path_value(annotation=annotation, value=_rooted(value, tree)) is (
        trackable
    )


@pytest.mark.parametrize(
    ("annotation", "value", "warned"),
    [(str, "{t}", True), (str, "{t}/a.txt", False), (folder, "{t}", False)],
)
def test_is_untracked_directory_value(tree: Path, annotation, value, warned) -> None:
    """The narrowed #121 predicate: a file is content-hashed now, so only a directory warns."""
    assert (
        is_untracked_directory_value(annotation=annotation, value=_rooted(value, tree)) is warned
    )


def _rooted(value: Any, root: Path) -> Any:
    if isinstance(value, str):
        return type(value)(value.replace("{t}", str(root)))
    if isinstance(value, list):
        return [_rooted(item, root) for item in value]
    if isinstance(value, dict):
        return {key: _rooted(item, root) for key, item in value.items()}
    return value


@task()
def analyze(coords: str, threads: int, scratch: tmp_dir) -> str:
    return coords


@task()
def write_output(payload: str, out_path: Out[file]) -> file:
    return file(out_path)


class TestCacheStoreLabels:
    """``CacheStore.label_inputs`` labels a task's inputs without moving its key."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> CacheStore:
        return CacheStore(index=CacheIndex.open(path=tmp_path / "ginkgo.db"), root=tmp_path / "c")

    def test_labels_cover_every_parameter(self, store: CacheStore, tmp_path: Path) -> None:
        args = {"coords": "north", "threads": "4", "scratch": "unused"}
        assert store.label_inputs(task_def=analyze, resolved_args=args) == {
            "coords": "value",
            "threads": "value",
            "scratch": "untracked",
        }
        assert store.label_inputs(
            task_def=write_output, resolved_args={"payload": "hi", "out_path": str(tmp_path / "r")}
        ) == {"payload": "value", "out_path": "output"}

    @pytest.mark.parametrize(
        ("is_dir", "label", "moves"), [(True, "path", False), (False, "content", True)]
    )
    def test_an_edit_moves_the_key_only_for_a_content_label(
        self, store: CacheStore, tmp_path: Path, is_dir: bool, label: str, moves: bool
    ) -> None:
        """A directory stays keyed by its path (still silent); a file by its bytes."""
        target = tmp_path / ("coords_dir" if is_dir else "coords.txt")
        if is_dir:
            target.mkdir()
            (target / "a.txt").write_text("v1", encoding="utf-8")
        else:
            target.write_text("v1", encoding="utf-8")
        args = {"coords": str(target), "threads": "4", "scratch": "unused"}

        before, _ = store.build_cache_key(task_def=analyze, resolved_args=args)
        assert store.label_inputs(task_def=analyze, resolved_args=args)["coords"] == label
        assert store.build_cache_key(task_def=analyze, resolved_args=args)[0] == before
        if is_dir:
            (target / "b.txt").write_text("v2", encoding="utf-8")
        else:
            target.write_text("v2", encoding="utf-8")
        after, _ = store.build_cache_key(task_def=analyze, resolved_args=args)

        assert (after != before) is moves


class TestScalarAnnotation:
    @staticmethod
    def _validate(annotation, value) -> None:
        TaskValidator().validate_annotated_value(annotation=annotation, value=value, label="t.n")

    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            (int, 3),
            (int, np.int64(3)),
            (float, 3),
            (float, np.float32(3.5)),
            (bool, np.bool_(True)),
            (str, "a"),
            (str, file("a.txt")),
            (str, Path("a.txt")),
            (int, None),
            (int, SecretRef(name="N")),
        ],
    )
    def test_accepts(self, annotation, value) -> None:
        self._validate(annotation, value)

    @pytest.mark.parametrize(
        ("annotation", "value"),
        [(int, "thirty"), (int, 3.7), (int, True), (float, "1.5"), (bool, 1), (str, 3)],
    )
    def test_rejects(self, annotation, value) -> None:
        with pytest.raises(TypeError, match=f"t.n is annotated '{annotation.__name__}'"):
            self._validate(annotation, value)

    @pytest.mark.parametrize("value", [[1, 2], (1, 2)])
    def test_rejects_a_list_for_one_value(self, value) -> None:
        with pytest.raises(TypeError, match="annotated 'int' but was given a list"):
            self._validate(int, value)

    @pytest.mark.parametrize("bad", ["3", True, 2.5])
    def test_a_list_int_names_the_wrong_element(self, bad: object) -> None:
        with pytest.raises(TypeError, match=r"xs\[1\]"):
            TaskValidator().validate_annotated_value(
                annotation=list[int], value=[1, bad, 3], label="xs"
            )
