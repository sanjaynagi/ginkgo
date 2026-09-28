"""Tests for ``label_input_value`` — the tracking label behind ``cache explain``
and the run-summary "tracked by path only" line (issue #307).

Each case checks that the label matches the branch
``CacheStore._hash_value`` actually takes for the same ``(annotation, value)``
pair, without duplicating its hashing logic.
"""

from __future__ import annotations

from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.core.remote import RemoteFileRef
from ginkgo.core.secret import SecretRef
from ginkgo.core.types import file, folder, tmp_dir, untracked
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

    def test_plain_str_naming_an_existing_file_with_a_separator_is_content(self, tmp_path) -> None:
        """#307 phase 2 / #121 / #281: a root-input path is content-hashed by
        default now, whatever its annotation, as long as it reads as a path."""
        path = tmp_path / "coords.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=str, value=str(path)) == "content"

    def test_plain_str_naming_an_existing_file_with_an_extension_only_is_content(
        self, tmp_path, monkeypatch
    ) -> None:
        path = tmp_path / "coords.txt"
        path.write_text("x", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        assert label_input_value(annotation=str, value="coords.txt") == "content"

    def test_any_annotation_naming_an_existing_file_is_content(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        from typing import Any

        assert label_input_value(annotation=Any, value=str(path)) == "content"


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
    """The remaining ``"path"`` cases, after #307 phase 2's content-hash rule:

    a directory (never auto-hashed) and a bare word that happens to collide
    with a file name (no separator, no extension — see
    ``looks_like_path_string``).
    """

    def test_str_naming_an_existing_directory_is_path(self, tmp_path) -> None:
        assert label_input_value(annotation=str, value=str(tmp_path)) == "path"

    def test_str_that_is_not_a_path_is_value(self) -> None:
        assert label_input_value(annotation=str, value="not-a-real-path-xyz") == "value"

    def test_bare_word_naming_an_existing_file_is_path_not_content(
        self, tmp_path, monkeypatch
    ) -> None:
        """The eligibility rule's documented exclusion: no separator, no
        extension — even though ``alpha`` exists on disk, it does not read
        as a path, so it stays keyed by its own repr, not its bytes."""
        (tmp_path / "alpha").write_text("x", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        assert label_input_value(annotation=str, value="alpha") == "path"

    def test_a_directory_inside_a_list_labels_the_whole_list_path(self, tmp_path) -> None:
        (tmp_path / "sub").mkdir()
        assert (
            label_input_value(annotation=list[str], value=[str(tmp_path / "sub"), "nope"])
            == "path"
        )

    def test_a_directory_inside_a_dict_value_labels_the_dict_path(self, tmp_path) -> None:
        (tmp_path / "sub").mkdir()
        assert (
            label_input_value(
                annotation=dict[str, str], value={"a": str(tmp_path / "sub"), "b": "plain"}
            )
            == "path"
        )

    def test_content_wins_over_path_in_a_mixed_list(self, tmp_path) -> None:
        """A container's label surfaces the *least* tracked element — but a
        directory (``"path"``) still beats a fully content-tracked file
        (``"content"``), since ``"path"`` is the silent-staleness risk."""
        target = tmp_path / "a.txt"
        target.write_text("x", encoding="utf-8")
        (tmp_path / "sub").mkdir()
        assert (
            label_input_value(annotation=list[str], value=[str(target), str(tmp_path / "sub")])
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

    def test_untracked_annotation_is_untracked_even_for_an_existing_file(self, tmp_path) -> None:
        """The explicit opt-out: an ``untracked`` path is never content-hashed,
        whatever exists on disk at that path — including an eligible file."""
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=untracked, value=str(path)) == "untracked"

    def test_untracked_instance_is_untracked_even_under_a_plain_annotation(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=str, value=untracked(str(path))) == "untracked"

    def test_list_of_untracked_is_untracked(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=list[untracked], value=[str(path)]) == "untracked"

    def test_optional_untracked_is_untracked(self, tmp_path) -> None:
        path = tmp_path / "a.txt"
        path.write_text("x", encoding="utf-8")
        assert label_input_value(annotation=untracked | None, value=str(path)) == "untracked"


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


class TestExcludedPaths:
    """The ledger exclusion for a legacy task's own previous output."""

    def test_excluded_path_is_labelled_path_not_content(self, tmp_path) -> None:
        target = tmp_path / "output.txt"
        target.write_text("x", encoding="utf-8")
        excluded = frozenset({str(target.resolve())})
        assert (
            label_input_value(annotation=str, value=str(target), excluded_paths=excluded) == "path"
        )

    def test_a_different_path_is_unaffected_by_the_exclusion_set(self, tmp_path) -> None:
        target = tmp_path / "output.txt"
        target.write_text("x", encoding="utf-8")
        other = tmp_path / "other.txt"
        excluded = frozenset({str(other.resolve())})
        assert (
            label_input_value(annotation=str, value=str(target), excluded_paths=excluded)
            == "content"
        )

    def test_excluded_path_nested_in_a_list_is_labelled_path(self, tmp_path) -> None:
        target = tmp_path / "output.txt"
        target.write_text("x", encoding="utf-8")
        excluded = frozenset({str(target.resolve())})
        assert (
            label_input_value(annotation=list[str], value=[str(target)], excluded_paths=excluded)
            == "path"
        )


class TestIsContentTrackablePathValue:
    """The exact #307 phase 2 eligibility rule, over the annotation/value table."""

    def test_separator_and_extension_qualify(self, tmp_path) -> None:
        from ginkgo.runtime.task_validation import is_content_trackable_path_value

        target = tmp_path / "counts.tsv"
        target.write_text("x", encoding="utf-8")
        assert is_content_trackable_path_value(annotation=str, value=str(target)) is True

    def test_bare_word_does_not_qualify(self, tmp_path, monkeypatch) -> None:
        from ginkgo.runtime.task_validation import is_content_trackable_path_value

        (tmp_path / "alpha").write_text("x", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        assert is_content_trackable_path_value(annotation=str, value="alpha") is False

    def test_directory_does_not_qualify(self, tmp_path) -> None:
        from ginkgo.runtime.task_validation import is_content_trackable_path_value

        sub = tmp_path / "sub.dir"
        sub.mkdir()
        assert is_content_trackable_path_value(annotation=str, value=str(sub)) is False

    def test_missing_path_does_not_qualify(self, tmp_path) -> None:
        from ginkgo.runtime.task_validation import is_content_trackable_path_value

        assert (
            is_content_trackable_path_value(annotation=str, value=str(tmp_path / "gone.txt"))
            is False
        )

    def test_out_annotation_does_not_qualify(self, tmp_path) -> None:
        from ginkgo.runtime.task_validation import is_content_trackable_path_value

        target = tmp_path / "a.txt"
        target.write_text("x", encoding="utf-8")
        assert is_content_trackable_path_value(annotation=file, value=str(target)) is False

    def test_untracked_annotation_does_not_qualify(self, tmp_path) -> None:
        from ginkgo.runtime.task_validation import is_content_trackable_path_value

        target = tmp_path / "a.txt"
        target.write_text("x", encoding="utf-8")
        assert is_content_trackable_path_value(annotation=untracked, value=str(target)) is False

    def test_remote_uri_does_not_qualify(self) -> None:
        from ginkgo.runtime.task_validation import is_content_trackable_path_value

        assert (
            is_content_trackable_path_value(annotation=str, value="s3://bucket/key.csv") is False
        )


class TestIsUntrackedDirectoryValue:
    """The narrowed #121 predicate behind the evaluator's cross-task warning."""

    def test_existing_directory_under_plain_annotation_is_untracked(self, tmp_path) -> None:
        from ginkgo.runtime.task_validation import is_untracked_directory_value

        assert is_untracked_directory_value(annotation=str, value=str(tmp_path)) is True

    def test_existing_file_no_longer_warns(self, tmp_path) -> None:
        """The retired half of #121: a file is content-hashed now, so it must
        not still be flagged as an untracked-by-content directory."""
        from ginkgo.runtime.task_validation import is_untracked_directory_value

        target = tmp_path / "a.txt"
        target.write_text("x", encoding="utf-8")
        assert is_untracked_directory_value(annotation=str, value=str(target)) is False

    def test_folder_annotation_is_not_flagged(self, tmp_path) -> None:
        from ginkgo.runtime.task_validation import is_untracked_directory_value

        assert is_untracked_directory_value(annotation=folder, value=str(tmp_path)) is False
