"""Unit tests for the value codec: round trips, refusals and hashing."""

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


def _round_trip(value, base_dir: Path, **kwargs):
    return decode_value(encode_value(value, base_dir=base_dir, **kwargs), base_dir=base_dir)


@pytest.mark.parametrize(
    "value",
    [
        file("/some/path"),
        folder("/some/dir"),
        np.float64(2.5),
        np.int64(3),
        np.bool_(True),
        Point(1, 2),
        OrderedDict(b=1, a=2),
        defaultdict(list, {"a": [1]}),
        Numbers([1, 2]),
    ],
    ids=repr,
)
def test_a_value_round_trips_as_its_own_type(tmp_path: Path, value) -> None:
    decoded = _round_trip(value, tmp_path)

    assert type(decoded) is type(value)
    assert decoded == value


def test_a_defaultdict_keeps_its_default_factory(tmp_path: Path) -> None:
    assert _round_trip(defaultdict(list, {"a": [1]}), tmp_path)["missing"] == []


def test_exact_containers_are_still_encoded_item_by_item(tmp_path: Path) -> None:
    encoded = encode_value([(1,), {"a": 2}], base_dir=tmp_path)

    assert encoded["__ginkgo_type__"] == "list"
    assert [item["__ginkgo_type__"] for item in encoded["items"]] == ["tuple", "dict"]


def test_a_class_that_cannot_be_imported_is_a_codec_error(tmp_path: Path) -> None:
    Local = namedtuple("Local", "x")

    with pytest.raises(CodecError, match="module level"):
        encode_value(Local(1), base_dir=tmp_path)


def test_an_array_round_trips(tmp_path: Path) -> None:
    value = np.arange(6, dtype=np.int32).reshape(2, 3)

    decoded = _round_trip(value, tmp_path, inline_limit=0)

    assert decoded.dtype == value.dtype
    assert (decoded == value).all()


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
def workflow_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
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

        assert _round_trip(module.Colour.RED, workflow_project) is module.Colour.RED

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


class TestDecodeTags:
    @pytest.mark.parametrize(
        "payload",
        [
            {"__ginkgo_type__": "retired", "value": 1},
            {"__ginkgo_type__": "list", "items": [{"__ginkgo_type__": "retired"}]},
        ],
    )
    def test_an_unknown_tag_raises(self, tmp_path: Path, payload) -> None:
        with pytest.raises(CodecError, match="'retired'"):
            decode_value(payload, base_dir=tmp_path)

    def test_check_encoded_finds_an_unknown_tag_without_decoding(self, tmp_path: Path) -> None:
        payload = {
            "__ginkgo_type__": "dict",
            "items": [{"key": "a", "value": {"__ginkgo_type__": "asset_ref"}}],
        }

        with pytest.raises(CodecError, match="'asset_ref'"):
            check_encoded(payload)
        check_encoded(encode_value({"x": [file("/a"), (1, 2)]}, base_dir=tmp_path))

    def test_a_fuse_marker_passes_through_for_worker_hydration(self, tmp_path: Path) -> None:
        marker = {"__ginkgo_type__": REMOTE_FILE_TYPE, "uri": "gs://bucket/key"}

        assert decode_value(marker, base_dir=tmp_path) is marker


class TestHashValueBytes:
    def test_numpy_hash_depends_on_values_and_shape_not_layout(self) -> None:
        base = np.arange(12, dtype=np.int64).reshape(3, 4)
        contiguous = hash_value_bytes(np.array(base, order="C"))
        fortran = hash_value_bytes(np.array(base, order="F"))

        assert contiguous == fortran
        assert contiguous[0] == "numpy.ndarray"
        assert hash_value_bytes(base.reshape(4, 3)) != contiguous
        assert hash_value_bytes(np.array([{"a": 1}], dtype=object))[0] == "numpy.npy"

    def test_dataframe_hash_depends_on_values_index_and_dtype(self) -> None:
        frame = pd.DataFrame({"a": [1, 2]})

        assert hash_value_bytes(frame) == hash_value_bytes(frame.copy(deep=True))
        assert hash_value_bytes(frame)[0] == "pandas.DataFrame"
        assert hash_value_bytes(frame) != hash_value_bytes(frame.set_index(pd.Index([10, 11])))
        assert hash_value_bytes(pd.DataFrame({"a": ["x", "y"]})) != hash_value_bytes(
            pd.DataFrame({"a": pd.Categorical(["x", "y"])})
        )

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
    def test_dataframe_hash_survives_the_codec_round_trip(self, tmp_path: Path, frame) -> None:
        encoded = encode_value(frame, base_dir=tmp_path, inline_limit=0)
        decoded = decode_value(encoded, base_dir=tmp_path)

        assert encoded["codec"] == "pandas.parquet"
        assert type(decoded.index) is type(frame.index)
        assert hash_value_bytes(decoded) == hash_value_bytes(frame)
