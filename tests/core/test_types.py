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


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        *[
            (name, True)
            for name in ("path", "file", "dir", "PATH", "File", "output_path", "output_paths")
        ],
        *[(name, True) for name in ("report_files", "scratch_dir", "assets_folders")],
        *[(name, False) for name in ("name", "threshold", "label", "pathway")],
        # Contains "dir"/"file" but neither exact nor suffixed.
        ("directive", False),
        ("filename", False),
    ],
)
def test_looks_path_like_param_name(name: str, expected: bool) -> None:
    """Issue #307's name heuristic: which parameter names count as paths."""
    assert looks_path_like_param_name(name) is expected


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        *[(shape, True) for shape in (str, Optional[str], str | None, list[str], tuple[str, ...])],
        *[(shape, False) for shape in (int, file, folder, list[int], tuple[str, int], str | int)],
        # ``untracked`` is a ``str`` subclass but not ``str``, so the doctor's
        # ``path_like_str_param`` check must never fire for an opted-out
        # parameter (#307 phase 2).
        *[(shape, False) for shape in (untracked, list[untracked], untracked | None)],
    ],
)
def test_is_str_path_annotation(annotation: object, expected: bool) -> None:
    assert is_str_path_annotation(annotation) is expected


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        *[
            (shape, True)
            for shape in (untracked, list[untracked], tuple[untracked, ...], untracked | None)
        ],
        *[(shape, False) for shape in (str, file, list[str])],
    ],
)
def test_is_untracked_annotation(annotation: object, expected: bool) -> None:
    assert is_untracked_annotation(annotation) is expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        *[(text, True) for text in ("data/counts.tsv", "./x", "../y", "counts.tsv", "/abs/path")],
        *[(text, False) for text in ("alpha", "results", "")],
    ],
)
def test_looks_like_path_string(text: str, expected: bool) -> None:
    """The #307 phase 2 rule's textual half: a separator or an extension."""
    assert looks_like_path_string(text) is expected
