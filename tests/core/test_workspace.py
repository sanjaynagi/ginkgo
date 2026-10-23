"""Tests for canonical workspace discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.workspace import (
    canonical_workflow_candidates,
    discover_default_workflow,
    resolve_envs_workflow_root,
)


def _entries(root: Path, *paths: str) -> list[Path]:
    """Write empty files at *paths* under *root* and return them."""
    written = []
    for relative in paths:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
        written.append(path)
    return written


@pytest.mark.parametrize(
    ("layout", "found"),
    [
        (["workflow/__init__.py", "workflow/flow.py"], ["workflow/flow.py"]),
        (["analysis/flow.py"], ["analysis/flow.py"]),
        # __init__.py is only needed for relative imports, not to be found.
        (["workflow/flow.py"], ["workflow/flow.py"]),
        (["flow.py"], ["flow.py"]),
        (["workflow/workflow.py", "workflow.py"], []),
        (["src/workflow/flow.py"], []),
        ([".ginkgo/flow.py", "__pycache__/flow.py", "node_modules/flow.py"], []),
    ],
)
def test_canonical_workflow_candidates(tmp_path: Path, layout, found) -> None:
    _entries(tmp_path, *layout)

    assert canonical_workflow_candidates(project_root=tmp_path) == [tmp_path / p for p in found]


class TestDiscoverDefaultWorkflow:
    @pytest.mark.parametrize(
        "layout", [["pipeline/flow.py", "other/flow.py"], ["flow.py", "workflow/flow.py"]]
    )
    def test_rejects_multiple_candidates(self, tmp_path: Path, layout) -> None:
        _entries(tmp_path, *layout)

        with pytest.raises(RuntimeError, match="multiple workflow entrypoints"):
            discover_default_workflow(project_root=tmp_path)

    def test_workflow_entry_wins_over_a_child_workflow(self, tmp_path: Path) -> None:
        (entry, _) = _entries(tmp_path, "workflow/flow.py", "child/flow.py")

        assert discover_default_workflow(project_root=tmp_path) == entry.resolve()

    def test_reports_expected_paths_when_nothing_is_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match=r"no flow\.py was found"):
            discover_default_workflow(project_root=tmp_path)


class TestResolveEnvsWorkflowRoot:
    """Regression tests for issue #309.

    A project laid out as the docs describe -- ``workflow/envs/`` beside
    ``workflow/flow.py`` -- must resolve its envs root even when the
    canonical-package heuristic finds no candidate, or more than one.
    """

    def test_uses_the_canonical_package_when_it_is_unambiguous(self, tmp_path: Path) -> None:
        (entry,) = _entries(tmp_path, "workflow/flow.py")

        assert resolve_envs_workflow_root(project_root=tmp_path, fallback=tmp_path / "x") == (
            entry.parent
        )

    @pytest.mark.parametrize(
        "layout", [["src/workflow/flow.py"], ["pipeline/flow.py", "src/workflow/flow.py"]]
    )
    def test_falls_back_when_no_single_canonical_entry_is_found(
        self, tmp_path: Path, layout
    ) -> None:
        fallback = _entries(tmp_path, *layout)[-1].parent
        if len(layout) > 1:
            _entries(tmp_path, "other/flow.py")

        assert resolve_envs_workflow_root(project_root=tmp_path, fallback=fallback) == fallback

    def test_returns_none_when_nothing_can_be_resolved(self, tmp_path: Path) -> None:
        assert resolve_envs_workflow_root(project_root=tmp_path) is None
