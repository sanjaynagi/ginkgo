"""Unit tests for value codec hashing behavior."""

from __future__ import annotations

import numpy as np
import pandas as pd

from ginkgo.runtime.value_codec import hash_value_bytes


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

    def test_dataframe_hash_changes_when_dtype_changes(self) -> None:
        object_frame = pd.DataFrame({"a": ["x", "y"]})
        categorical_frame = pd.DataFrame({"a": pd.Categorical(["x", "y"])})

        codec_object, digest_object = hash_value_bytes(object_frame)
        codec_categorical, digest_categorical = hash_value_bytes(categorical_frame)

        assert codec_object == "pandas.DataFrame"
        assert codec_categorical == "pandas.DataFrame"
        assert digest_object != digest_categorical
