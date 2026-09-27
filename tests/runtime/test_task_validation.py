"""Tests for ``label_input_value`` — the tracking label behind ``cache explain``
and the run-summary "tracked by path only" line (issue #307 phase 1).

Each case checks that the label matches the branch
``CacheStore._hash_value`` actually takes for the same ``(annotation, value)``
pair, without duplicating its hashing logic.
"""

from __future__ import annotations

from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.core.remote import RemoteFileRef
from ginkgo.core.secret import SecretRef
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.remote.access.protocol import encode_fuse_ref
from ginkgo.runtime.task_validation import label_input_value


def _asset_ref() -> AssetRef:
    return AssetRef(
        key=AssetKey(namespace="table", name="producer.summary"),
        version_id="v1",
        kind="table",
        artifact_id="abc123",
        content_hash="def456",
        artifact_path="/tmp/blobs/abc123",
    )


class TestContentLabel:
    def test_file_annotation_is_content(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=file, value=str(path)) == "content"

    def test_folder_annotation_is_content(self, tmp_path) -> None:
        assert label_input_value(annotation=folder, value=str(tmp_path)) == "content"

    def test_file_instance_is_content_even_under_a_plain_annotation(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=str, value=file(str(path))) == "content"


class TestAssetLabel:
    def test_asset_ref_is_asset(self) -> None:
        assert label_input_value(annotation=str, value=_asset_ref()) == "asset"

    def test_remote_ref_is_asset(self) -> None:
        ref = RemoteFileRef(
            uri="s3://bucket/key", scheme="s3", bucket="bucket", key="key", version_id="v1"
        )
        assert label_input_value(annotation=str, value=ref) == "asset"

    def test_fuse_marker_is_asset(self) -> None:
        ref = RemoteFileRef(
            uri="s3://bucket/key", scheme="s3", bucket="bucket", key="key", version_id="v1"
        )
        marker = encode_fuse_ref(ref=ref, policy="fuse")
        assert label_input_value(annotation=str, value=marker) == "asset"


class TestPathLabel:
    def test_plain_str_naming_an_existing_path_is_path(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=str, value=str(path)) == "path"

    def test_str_that_is_not_a_path_is_value(self) -> None:
        assert label_input_value(annotation=str, value="not-a-real-path-xyz") == "value"

    def test_a_path_inside_a_list_labels_the_whole_list_path(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert (
            label_input_value(annotation=list[str], value=[str(path), "not-a-real-path"]) == "path"
        )

    def test_a_path_inside_a_dict_value_labels_the_dict_path(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert (
            label_input_value(annotation=dict[str, str], value={"a": str(path), "b": "plain"})
            == "path"
        )


class TestValueLabel:
    def test_plain_scalar_is_value(self) -> None:
        assert label_input_value(annotation=int, value=5) == "value"

    def test_secret_ref_is_value(self) -> None:
        assert label_input_value(annotation=str, value=SecretRef(name="api_key")) == "value"

    def test_arbitrary_object_is_value(self) -> None:
        assert label_input_value(annotation=object, value={"nested": [1, 2]}["nested"]) == "value"

    def test_absent_optional_is_value(self) -> None:
        assert label_input_value(annotation=str | None, value=None) == "value"


class TestOutputLabel:
    def test_output_parameter_is_output_regardless_of_annotation(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        assert label_input_value(annotation=file, value=str(path), is_output=True) == "output"

    def test_output_wins_over_asset_ref_shape(self) -> None:
        # is_output is checked before the AssetRef branch, mirroring _hash_value.
        assert label_input_value(annotation=str, value=_asset_ref(), is_output=True) == "output"


class TestUntrackedLabel:
    def test_tmp_dir_annotation_is_untracked(self) -> None:
        assert label_input_value(annotation=tmp_dir, value=tmp_dir("/tmp/x")) == "untracked"

    def test_tmp_dir_instance_is_untracked_even_under_a_plain_annotation(self) -> None:
        assert label_input_value(annotation=str, value=tmp_dir("/tmp/x")) == "untracked"


class TestContainerCombination:
    def test_content_wins_only_when_every_element_is_content(self, tmp_path) -> None:
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_text("x", encoding="utf-8")
        b.write_text("y", encoding="utf-8")
        assert label_input_value(annotation=list[file], value=[str(a), str(b)]) == "content"

    def test_value_beats_asset_in_a_mixed_list(self) -> None:
        # Precedence surfaces the weaker-tracked element: "value" (a repr
        # digest) is treated as less specific than "asset" (a version id).
        assert label_input_value(annotation=list, value=[_asset_ref(), 5]) == "value"

    def test_empty_container_is_value(self) -> None:
        assert label_input_value(annotation=list[str], value=[]) == "value"
