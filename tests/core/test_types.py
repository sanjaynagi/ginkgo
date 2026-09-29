"""Unit tests for ``ginkgo.core.types`` annotation/name predicates."""

from __future__ import annotations

from typing import Optional

import pytest

from ginkgo.core.types import (
    file,
    folder,
    is_str_path_annotation,
    is_untracked_annotation,
    looks_like_path_string,
    looks_path_like_param_name,
    untracked,
)


class TestLooksPathLikeParamName:
    """Cover for issue #307's name heuristic: which parameter names count as paths."""

    @pytest.mark.parametrize(
        "name",
        [
            "path",
            "file",
            "dir",
            "PATH",
            "File",
            "Dir",
            "output_path",
            "output_paths",
            "input_path",
            "report_file",
            "report_files",
            "scratch_dir",
            "scratch_dirs",
            "assets_folder",
            "assets_folders",
            "OUTPUT_PATH",
        ],
    )
    def test_path_like_names_match(self, name: str) -> None:
        assert looks_path_like_param_name(name) is True

    @pytest.mark.parametrize(
        "name",
        [
            "name",
            "threshold",
            "token",
            "label",
            "profile",
            "directive",  # contains "dir" but does not end with the "_dir" suffix
            "filename",  # contains "file" but neither exact nor suffixed
            "pathway",
        ],
    )
    def test_non_path_like_names_do_not_match(self, name: str) -> None:
        assert looks_path_like_param_name(name) is False


class TestIsStrPathAnnotation:
    """Cover for issue #307: which ``str``-family shapes count as untracked paths."""

    @pytest.mark.parametrize(
        "annotation",
        [str, Optional[str], str | None, list[str], tuple[str, ...]],
    )
    def test_str_shapes_match(self, annotation: object) -> None:
        assert is_str_path_annotation(annotation) is True

    @pytest.mark.parametrize(
        "annotation",
        [
            int,
            file,
            folder,
            list[int],
            tuple[str, int],
            tuple[int, ...],
            str | int,
        ],
    )
    def test_other_shapes_do_not_match(self, annotation: object) -> None:
        assert is_str_path_annotation(annotation) is False

    def test_untracked_does_not_match(self) -> None:
        """``untracked`` is a ``str`` subclass but not ``str`` itself, so the
        doctor's ``path_like_str_param`` check — built on this predicate —
        must never fire for a deliberately opted-out parameter (#307 phase 2)."""
        assert is_str_path_annotation(untracked) is False

    def test_untracked_container_shapes_do_not_match(self) -> None:
        assert is_str_path_annotation(list[untracked]) is False
        assert is_str_path_annotation(untracked | None) is False


class TestIsUntrackedAnnotation:
    def test_bare_untracked_matches(self) -> None:
        assert is_untracked_annotation(untracked) is True

    def test_composed_shapes_match(self) -> None:
        assert is_untracked_annotation(list[untracked]) is True
        assert is_untracked_annotation(tuple[untracked, ...]) is True
        assert is_untracked_annotation(untracked | None) is True

    def test_unrelated_annotations_do_not_match(self) -> None:
        assert is_untracked_annotation(str) is False
        assert is_untracked_annotation(file) is False
        assert is_untracked_annotation(list[str]) is False


class TestLooksLikePathString:
    """The #307 phase 2 eligibility rule's textual half: a separator or a
    file extension, so a bare word never qualifies."""

    @pytest.mark.parametrize(
        "text",
        ["data/counts.tsv", "./x", "../y", "counts.tsv", "a.b.c", "/abs/path"],
    )
    def test_separator_or_extension_matches(self, text: str) -> None:
        assert looks_like_path_string(text) is True

    @pytest.mark.parametrize("text", ["alpha", "results", "north", ""])
    def test_bare_word_does_not_match(self, text: str) -> None:
        assert looks_like_path_string(text) is False
