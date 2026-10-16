"""Unit tests for value codec hashing behavior."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ginkgo.core.asset import AssetKey, AssetRef, table
from ginkgo.core.types import file, folder
from ginkgo.remote.access.protocol import FUSE_FILE_TYPE
from ginkgo.runtime.artifacts.value_codec import (
    CodecError,
    decode_value,
    encode_value,
    hash_value_bytes,
)


def _make_ref(*, kind: str = "file") -> AssetRef:
    return AssetRef(
        key=AssetKey(namespace=kind, name="prepared"),
        version_id="version-123",
        kind=kind,
        artifact_id="artifact-123",
        content_hash="content-123",
        artifact_path="/artifacts/prepared",
        metadata={},
    )


class TestDecodeTags:
    def test_an_unknown_tag_raises(self, tmp_path) -> None:
        with pytest.raises(CodecError, match="Unknown encoded value type: 'retired'"):
            decode_value({"__ginkgo_type__": "retired", "value": 1}, base_dir=tmp_path)

    def test_an_unknown_tag_nested_in_a_list_raises(self, tmp_path) -> None:
        payload = {"__ginkgo_type__": "list", "items": [{"__ginkgo_type__": "retired"}]}

        with pytest.raises(CodecError):
            decode_value(payload, base_dir=tmp_path)

    def test_a_fuse_marker_passes_through_for_worker_hydration(self, tmp_path) -> None:
        marker = {"__ginkgo_type__": FUSE_FILE_TYPE, "uri": "gs://bucket/key"}

        assert decode_value(marker, base_dir=tmp_path) is marker


class TestPathMarkerAssetRoundtrip:
    """A ``file``/``folder`` value's ``.asset`` survives the transport codec.

    Rule #307: a ``file``/``folder`` execution-arg value carries the
    producing ``AssetRef`` on ``.asset`` (a plain instance attribute, since
    neither marker declares ``__slots__``). That attribute crosses a
    process-pool worker transport only if the codec carries it across
    explicitly — this is what ``value_codec`` does for both markers.
    """

    def test_file_asset_attribute_survives_the_codec(self, tmp_path) -> None:
        ref = _make_ref(kind="file")
        value = file(ref.artifact_path)
        value.asset = ref

        encoded = encode_value(value, base_dir=tmp_path)
        decoded = decode_value(encoded, base_dir=tmp_path)

        assert isinstance(decoded, file)
        assert str(decoded) == ref.artifact_path
        assert decoded.asset == ref

    def test_folder_asset_attribute_survives_the_codec(self, tmp_path) -> None:
        ref = _make_ref(kind="folder")
        value = folder(ref.artifact_path)
        value.asset = ref

        encoded = encode_value(value, base_dir=tmp_path)
        decoded = decode_value(encoded, base_dir=tmp_path)

        assert isinstance(decoded, folder)
        assert str(decoded) == ref.artifact_path
        assert decoded.asset == ref

    def test_file_without_an_asset_decodes_with_asset_none(self, tmp_path) -> None:
        """A plain ``file(...)`` with no upstream asset round-trips as before."""
        value = file("/some/path")

        encoded = encode_value(value, base_dir=tmp_path)
        decoded = decode_value(encoded, base_dir=tmp_path)

        assert isinstance(decoded, file)
        assert str(decoded) == "/some/path"
        assert decoded.asset is None


class TestHashValueBytes:
    def test_asset_ref_hash_uses_version_identity(self, tmp_path) -> None:
        value = AssetRef(
            key=AssetKey(namespace="file", name="prepared"),
            version_id="version-123",
            kind="file",
            artifact_id="artifact-123",
            content_hash="content-123",
            artifact_path="/artifacts/prepared",
            metadata={},
        )

        codec_name, digest = hash_value_bytes(value)
        encoded = encode_value(value, base_dir=tmp_path)
        decoded = decode_value(encoded, base_dir=tmp_path)

        assert codec_name == "ginkgo.asset_ref"
        assert digest
        assert decoded == value

    def test_asset_result_roundtrip_preserves_group(self, tmp_path) -> None:
        value = table(
            pd.DataFrame({"a": [1]}),
            name="features",
            group="QC metrics",
            caption="Variant counts after QC filtering",
        )

        encoded = encode_value(value, base_dir=tmp_path)
        decoded = decode_value(encoded, base_dir=tmp_path)

        assert decoded.kind == "table"
        assert decoded.name == "features"
        assert decoded.group == "QC metrics"
        assert decoded.caption == "Variant counts after QC filtering"

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
