"""Unit tests for ``ginkgo.core.types`` annotation/name predicates."""

from __future__ import annotations

from typing import Optional

import pytest

from ginkgo.core.types import file, folder, is_str_path_annotation, looks_path_like_param_name


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
