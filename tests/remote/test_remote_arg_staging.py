"""Staging of container-typed ``file``/``folder`` arguments for remote tasks."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ginkgo import file, folder
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from collections.abc import Sequence

from ginkgo.runtime.artifacts.remote_arg_transfer import (
    hydrate_args_from_remote,
    stage_args_for_remote,
)
from ginkgo.runtime.artifacts.remote_artifact_store import RemoteArtifactStore
from ginkgo.runtime.artifacts.value_codec import encode_value
from ginkgo.runtime.caching.index import CacheIndex

_REMOTE_FILE = "__ginkgo_remote_file__"


@pytest.fixture
def remote_store(tmp_path: Path) -> RemoteArtifactStore:
    root = tmp_path / ".ginkgo" / "artifacts"
    root.mkdir(parents=True)
    backend = MagicMock()
    backend.upload.return_value = RemoteObjectMeta(uri="gs://bucket/key", size=1)
    backend.head.side_effect = FileNotFoundError("not found")
    return RemoteArtifactStore(
        local=LocalArtifactStore(root=root, index=CacheIndex.in_memory()),
        backend=backend,
        bucket="bucket",
        prefix="artifacts/",
        scheme="gs",
    )


def _inputs(tmp_path: Path, count: int) -> list[file]:
    paths = []
    for index in range(count):
        path = tmp_path / f"in{index}.txt"
        path.write_text(f"input {index}", encoding="utf-8")
        paths.append(file(str(path)))
    return paths


def _stage(value, annotation, *, tmp_path: Path, remote_store: RemoteArtifactStore):
    encoded = encode_value(value, base_dir=tmp_path)
    staged = stage_args_for_remote(
        args={"x": encoded}, type_hints={"x": annotation}, remote_store=remote_store
    )
    return staged["x"]


def test_an_encoded_list_of_files_is_staged(tmp_path, remote_store) -> None:
    """Before the fix the encoded list was passed through, so the worker got
    driver-local paths it could not read."""
    staged = _stage(_inputs(tmp_path, 2), list[file], tmp_path=tmp_path, remote_store=remote_store)

    assert staged["__ginkgo_type__"] == "list"
    assert [item["__ginkgo_type__"] for item in staged["items"]] == [_REMOTE_FILE] * 2


def test_an_optional_list_of_files_is_staged(tmp_path, remote_store) -> None:
    staged = _stage(
        _inputs(tmp_path, 1), list[file] | None, tmp_path=tmp_path, remote_store=remote_store
    )

    assert staged["items"][0]["__ginkgo_type__"] == _REMOTE_FILE


def test_a_heterogeneous_tuple_stages_only_its_path_elements(tmp_path, remote_store) -> None:
    (only,) = _inputs(tmp_path, 1)
    staged = _stage(
        (only, "label"), tuple[file, str], tmp_path=tmp_path, remote_store=remote_store
    )

    assert staged["__ginkgo_type__"] == "tuple"
    assert staged["items"][0]["__ginkgo_type__"] == _REMOTE_FILE
    assert staged["items"][1] == encode_value("label", base_dir=tmp_path)


def test_an_encoded_dict_of_files_stages_its_values(tmp_path, remote_store) -> None:
    first, second = _inputs(tmp_path, 2)
    staged = _stage(
        {"a": first, "b": second}, dict[str, file], tmp_path=tmp_path, remote_store=remote_store
    )

    assert staged["__ginkgo_type__"] == "dict"
    assert [entry["value"]["__ginkgo_type__"] for entry in staged["items"]] == [_REMOTE_FILE] * 2


def test_a_list_of_folders_is_staged(tmp_path, remote_store) -> None:
    directory = tmp_path / "d"
    directory.mkdir()
    (directory / "x.txt").write_text("x", encoding="utf-8")

    staged = _stage(
        [folder(str(directory))], list[folder], tmp_path=tmp_path, remote_store=remote_store
    )

    assert staged["items"][0]["__ginkgo_type__"] == "__ginkgo_remote_folder__"


def test_a_list_of_plain_strings_is_untouched(tmp_path, remote_store) -> None:
    encoded = encode_value(["a", "b"], base_dir=tmp_path)

    staged = stage_args_for_remote(
        args={"x": encoded}, type_hints={"x": list[str]}, remote_store=remote_store
    )

    assert staged["x"] == encoded


def test_a_union_of_container_types_stages_the_matching_member(tmp_path, remote_store) -> None:
    staged = _stage(
        _inputs(tmp_path, 1), list[file] | list[str], tmp_path=tmp_path, remote_store=remote_store
    )

    assert staged["items"][0]["__ginkgo_type__"] == _REMOTE_FILE


def test_an_abstract_sequence_of_files_is_staged(tmp_path, remote_store) -> None:
    staged = _stage(
        _inputs(tmp_path, 2), Sequence[file], tmp_path=tmp_path, remote_store=remote_store
    )

    assert [item["__ginkgo_type__"] for item in staged["items"]] == [_REMOTE_FILE] * 2


def test_file_keys_of_a_dict_are_staged(tmp_path, remote_store) -> None:
    (only,) = _inputs(tmp_path, 1)
    staged = _stage({only: "label"}, dict[file, str], tmp_path=tmp_path, remote_store=remote_store)

    assert staged["items"][0]["key"]["__ginkgo_type__"] == _REMOTE_FILE


def test_none_elements_and_nested_lists_are_handled(tmp_path, remote_store) -> None:
    first, second = _inputs(tmp_path, 2)
    optional = _stage(
        [first, None], list[file | None], tmp_path=tmp_path, remote_store=remote_store
    )
    nested = _stage(
        [[first], [second]], list[list[file]], tmp_path=tmp_path, remote_store=remote_store
    )

    assert optional["items"][0]["__ginkgo_type__"] == _REMOTE_FILE
    assert optional["items"][1] == encode_value(None, base_dir=tmp_path)
    assert [inner["items"][0]["__ginkgo_type__"] for inner in nested["items"]] == [
        _REMOTE_FILE
    ] * 2


def test_a_raw_list_of_files_is_staged(tmp_path, remote_store) -> None:
    staged = stage_args_for_remote(
        args={"x": [str(path) for path in _inputs(tmp_path, 2)]},
        type_hints={"x": list[file]},
        remote_store=remote_store,
    )

    assert [item["__ginkgo_type__"] for item in staged["x"]] == [_REMOTE_FILE] * 2


def test_staged_containers_hydrate_back_to_readable_files(tmp_path, remote_store) -> None:
    """The worker side turns every staged reference back into a local file."""
    inputs = _inputs(tmp_path, 2)
    staged = stage_args_for_remote(
        args={"x": encode_value(inputs, base_dir=tmp_path)},
        type_hints={"x": list[file]},
        remote_store=remote_store,
    )

    hydrated = hydrate_args_from_remote(
        args=staged, remote_store=remote_store, scratch_dir=tmp_path / "worker"
    )

    items = hydrated["x"]["items"]
    assert [Path(item).read_text(encoding="utf-8") for item in items] == ["input 0", "input 1"]
