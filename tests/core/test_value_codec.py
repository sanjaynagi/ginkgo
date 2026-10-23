"""Unit tests for value codec hashing behavior."""

from __future__ import annotations

import shutil
import sys
from collections import OrderedDict, defaultdict, namedtuple
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ginkgo.core.types import file, folder
from ginkgo.remote.access.protocol import REMOTE_FILE_TYPE
from ginkgo.runtime.artifacts.value_codec import (
    CodecError,
    check_encoded,
    decode_value,
    encode_value,
    hash_value_bytes,
)
from ginkgo.runtime.module_loader import load_module_from_path

Point = namedtuple("Point", "x y")


class Numbers(list):
    """A list subclass, as a workflow might define one."""


class TestPathMarkers:
    def test_a_file_marker_round_trips(self, tmp_path) -> None:
        decoded = decode_value(
            encode_value(file("/some/path"), base_dir=tmp_path), base_dir=tmp_path
        )

        assert isinstance(decoded, file)
        assert str(decoded) == "/some/path"

    def test_a_folder_marker_round_trips(self, tmp_path) -> None:
        decoded = decode_value(
            encode_value(folder("/some/dir"), base_dir=tmp_path), base_dir=tmp_path
        )

        assert isinstance(decoded, folder)
        assert str(decoded) == "/some/dir"


class TestScalarSubclasses:
    @pytest.mark.parametrize("value", [np.float64(2.5), np.int64(3), np.bool_(True)])
    def test_a_numpy_scalar_round_trips_as_itself(self, tmp_path, value) -> None:
        decoded = decode_value(encode_value(value, base_dir=tmp_path), base_dir=tmp_path)

        assert type(decoded) is type(value)
        assert decoded == value


class TestContainerSubclasses:
    @pytest.mark.parametrize(
        "value",
        [
            Point(1, 2),
            OrderedDict(b=1, a=2),
            defaultdict(list, {"a": [1]}),
            Numbers([1, 2]),
        ],
        ids=["namedtuple", "ordereddict", "defaultdict", "list-subclass"],
    )
    def test_a_subclass_round_trips_as_itself(self, tmp_path, value) -> None:
        decoded = decode_value(encode_value(value, base_dir=tmp_path), base_dir=tmp_path)

        assert type(decoded) is type(value)
        assert decoded == value

    def test_a_defaultdict_keeps_its_default_factory(self, tmp_path) -> None:
        value = defaultdict(list, {"a": [1]})

        decoded = decode_value(encode_value(value, base_dir=tmp_path), base_dir=tmp_path)

        assert decoded["missing"] == []

    def test_exact_containers_are_still_encoded_item_by_item(self, tmp_path) -> None:
        encoded = encode_value([(1,), {"a": 2}], base_dir=tmp_path)

        assert encoded["__ginkgo_type__"] == "list"
        assert [item["__ginkgo_type__"] for item in encoded["items"]] == ["tuple", "dict"]

    def test_a_class_that_cannot_be_imported_is_a_codec_error(self, tmp_path) -> None:
        Local = namedtuple("Local", "x")

        with pytest.raises(CodecError, match="module level"):
            encode_value(Local(1), base_dir=tmp_path)


WORKFLOW_SOURCE = """
import dataclasses
import enum


@dataclasses.dataclass
class Point:
    x: int


class Colour(enum.Enum):
    RED = 1
"""


@pytest.fixture
def workflow_project(tmp_path, monkeypatch) -> Path:
    """A project whose single-file workflow defines a dataclass and an enum."""
    project = tmp_path / "project"
    (project / "workflow").mkdir(parents=True)
    (project / "ginkgo.toml").write_text("", encoding="utf-8")
    (project / "workflow" / "flow.py").write_text(WORKFLOW_SOURCE, encoding="utf-8")
    monkeypatch.chdir(project)
    # Loading the workflow puts its directory on sys.path; undo that after.
    monkeypatch.setattr(sys, "path", list(sys.path))
    return project


def _forget_workflow_modules() -> None:
    """Drop every loaded single-file workflow, as a fresh process has none."""
    for name in [name for name in sys.modules if name.startswith("ginkgo_user_")]:
        del sys.modules[name]


class TestWorkflowClasses:
    def test_a_value_decodes_in_a_process_that_never_loaded_the_workflow(
        self, workflow_project
    ) -> None:
        module = load_module_from_path(workflow_project / "workflow" / "flow.py")
        encoded = encode_value([module.Point(3), module.Colour.RED], base_dir=workflow_project)
        _forget_workflow_modules()

        point, colour = decode_value(encoded, base_dir=workflow_project)

        assert type(point).__qualname__ == "Point"
        assert point.x == 3
        assert colour.name == "RED"

    def test_a_value_decodes_as_the_loaded_workflow_class(self, workflow_project) -> None:
        module = load_module_from_path(workflow_project / "workflow" / "flow.py")

        decoded = decode_value(
            encode_value(module.Colour.RED, base_dir=workflow_project),
            base_dir=workflow_project,
        )

        assert decoded is module.Colour.RED

    def test_a_moved_project_decodes_its_values(self, workflow_project, monkeypatch) -> None:
        module = load_module_from_path(workflow_project / "workflow" / "flow.py")
        encoded = encode_value(module.Point(3), base_dir=workflow_project)
        moved = workflow_project.with_name("moved")
        shutil.move(workflow_project, moved)
        monkeypatch.chdir(moved)
        _forget_workflow_modules()

        assert decode_value(encoded, base_dir=moved).x == 3

    def test_a_copied_workflow_hashes_its_values_the_same(self, workflow_project) -> None:
        original = load_module_from_path(workflow_project / "workflow" / "flow.py")
        copy_path = workflow_project / "workflow" / "flow_copy.py"
        copy_path.write_text(WORKFLOW_SOURCE, encoding="utf-8")
        copy = load_module_from_path(copy_path)

        assert hash_value_bytes(original.Point(3)) == hash_value_bytes(copy.Point(3))


class TestArrayCodecs:
    def test_an_array_round_trips(self, tmp_path) -> None:
        value = np.arange(6, dtype=np.int32).reshape(2, 3)

        decoded = decode_value(
            encode_value(value, base_dir=tmp_path, inline_limit=0), base_dir=tmp_path
        )

        assert decoded.dtype == value.dtype
        assert (decoded == value).all()


class TestDecodeTags:
    def test_an_unknown_tag_raises(self, tmp_path) -> None:
        with pytest.raises(CodecError, match="Unknown encoded value type: 'retired'"):
            decode_value({"__ginkgo_type__": "retired", "value": 1}, base_dir=tmp_path)

    def test_an_unknown_tag_nested_in_a_list_raises(self, tmp_path) -> None:
        payload = {"__ginkgo_type__": "list", "items": [{"__ginkgo_type__": "retired"}]}

        with pytest.raises(CodecError):
            decode_value(payload, base_dir=tmp_path)

    def test_check_encoded_finds_an_unknown_tag_without_decoding(self) -> None:
        payload = {
            "__ginkgo_type__": "dict",
            "items": [{"key": "a", "value": {"__ginkgo_type__": "asset_ref"}}],
        }

        with pytest.raises(CodecError, match="'asset_ref'"):
            check_encoded(payload)

    def test_check_encoded_accepts_what_encode_value_writes(self, tmp_path) -> None:
        payload = encode_value({"x": [file("/a"), (1, 2)]}, base_dir=tmp_path)

        check_encoded(payload)

    def test_a_fuse_marker_passes_through_for_worker_hydration(self, tmp_path) -> None:
        marker = {"__ginkgo_type__": REMOTE_FILE_TYPE, "uri": "gs://bucket/key"}

        assert decode_value(marker, base_dir=tmp_path) is marker


class TestHashValueBytes:
    def test_numpy_hash_is_stable_for_equal_values_with_different_layouts(self) -> None:
        base = np.arange(12, dtype=np.int64).reshape(3, 4)
        contiguous = np.array(base, order="C")
        fortran = np.array(base, order="F")

        codec_contiguous, digest_contiguous = hash_value_bytes(contiguous)
        codec_fortran, digest_fortran = hash_value_bytes(fortran)

        assert codec_contiguous == "numpy.ndarray"
        assert codec_fortran == "numpy.ndarray"
        assert digest_contiguous == digest_fortran

    def test_numpy_hash_changes_when_shape_changes(self) -> None:
        left = np.arange(6, dtype=np.int64).reshape(2, 3)
        right = np.arange(6, dtype=np.int64).reshape(3, 2)

        _codec_left, digest_left = hash_value_bytes(left)
        _codec_right, digest_right = hash_value_bytes(right)

        assert digest_left != digest_right

    def test_object_dtype_uses_existing_codec_path(self) -> None:
        value = np.array([{"a": 1}, {"b": 2}], dtype=object)

        codec_name, _digest = hash_value_bytes(value)

        assert codec_name == "numpy.npy"

    def test_dataframe_hash_is_stable_for_equal_frames(self) -> None:
        left = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        right = left.copy(deep=True)

        codec_left, digest_left = hash_value_bytes(left)
        codec_right, digest_right = hash_value_bytes(right)

        assert codec_left == "pandas.DataFrame"
        assert codec_right == "pandas.DataFrame"
        assert digest_left == digest_right

    def test_dataframe_hash_changes_when_index_changes(self) -> None:
        left = pd.DataFrame({"a": [1, 2]})
        right = left.set_index(pd.Index([10, 11]))

        _codec_left, digest_left = hash_value_bytes(left)
        _codec_right, digest_right = hash_value_bytes(right)

        assert digest_left != digest_right

    @pytest.mark.parametrize(
        "frame",
        [
            pd.DataFrame({"a": [1, 2, 3]}),
            pd.DataFrame({"a": [1, 2, 3]}).iloc[1:],
            pd.DataFrame({"a": [1, 2, 3]}, index=pd.RangeIndex(0, 6, 2, name="r")),
            pd.DataFrame({"a": pd.Series([], dtype="int64")}),
        ],
        ids=["default", "sliced", "stepped-named", "empty"],
    )
    def test_dataframe_hash_survives_the_codec_round_trip(self, tmp_path, frame) -> None:
        encoded = encode_value(frame, base_dir=tmp_path, inline_limit=0)
        decoded = decode_value(encoded, base_dir=tmp_path)

        assert encoded["codec"] == "pandas.parquet"
        assert type(decoded.index) is type(frame.index)
        assert hash_value_bytes(decoded) == hash_value_bytes(frame)

    def test_dataframe_hash_changes_when_dtype_changes(self) -> None:
        object_frame = pd.DataFrame({"a": ["x", "y"]})
        categorical_frame = pd.DataFrame({"a": pd.Categorical(["x", "y"])})

        codec_object, digest_object = hash_value_bytes(object_frame)
        codec_categorical, digest_categorical = hash_value_bytes(categorical_frame)

        assert codec_object == "pandas.DataFrame"
        assert codec_categorical == "pandas.DataFrame"
        assert digest_object != digest_categorical
