"""Tests for canonical workspace discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.workspace import (
    canonical_workflow_candidates,
    discover_default_workflow,
    resolve_envs_workflow_root,
)


def _package(root: Path, *, name: str, entry: str, marker: bool = True) -> Path:
    """Create a directory holding one workflow entry file.

    ``marker`` writes an ``__init__.py`` alongside, making the directory a
    Python package.
    """
    package_dir = root / name
    package_dir.mkdir(parents=True)
    if marker:
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
    entry_path = package_dir / entry
    entry_path.write_text("", encoding="utf-8")
    return entry_path


class TestCanonicalWorkflowCandidates:
    def test_finds_canonical_flow_entry(self, tmp_path: Path) -> None:
        entry_path = _package(tmp_path, name="workflow", entry="flow.py")

        assert canonical_workflow_candidates(project_root=tmp_path) == [entry_path]

    def test_finds_entry_in_any_directory_name(self, tmp_path: Path) -> None:
        entry_path = _package(tmp_path, name="analysis", entry="flow.py")

        assert canonical_workflow_candidates(project_root=tmp_path) == [entry_path]

    def test_finds_entry_at_the_project_root(self, tmp_path: Path) -> None:
        entry_path = tmp_path / "flow.py"
        entry_path.write_text("", encoding="utf-8")

        assert canonical_workflow_candidates(project_root=tmp_path) == [entry_path]

    def test_ignores_other_file_names(self, tmp_path: Path) -> None:
        _package(tmp_path, name="workflow", entry="workflow.py")
        (tmp_path / "workflow.py").write_text("", encoding="utf-8")

        assert canonical_workflow_candidates(project_root=tmp_path) == []

    def test_finds_entry_in_a_directory_that_is_not_a_package(self, tmp_path: Path) -> None:
        """__init__.py is only needed for relative imports, not to be found."""
        entry_path = _package(tmp_path, name="workflow", entry="flow.py", marker=False)

        assert canonical_workflow_candidates(project_root=tmp_path) == [entry_path]

    def test_ignores_entries_more_than_one_level_deep(self, tmp_path: Path) -> None:
        nested = tmp_path / "src" / "workflow"
        nested.mkdir(parents=True)
        (nested / "flow.py").write_text("", encoding="utf-8")

        assert canonical_workflow_candidates(project_root=tmp_path) == []

    def test_ignores_runtime_and_tooling_directories(self, tmp_path: Path) -> None:
        for name in (".ginkgo", "__pycache__", "node_modules"):
            (tmp_path / name).mkdir()
            (tmp_path / name / "flow.py").write_text("", encoding="utf-8")

        assert canonical_workflow_candidates(project_root=tmp_path) == []


class TestDiscoverDefaultWorkflow:
    def test_rejects_multiple_candidate_packages(self, tmp_path: Path) -> None:
        _package(tmp_path, name="workflow", entry="flow.py")
        _package(tmp_path, name="other", entry="flow.py")

        with pytest.raises(RuntimeError, match="multiple workflow entrypoints"):
            discover_default_workflow(project_root=tmp_path)

    def test_reports_expected_paths_when_nothing_is_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match=r"no flow\.py was found"):
            discover_default_workflow(project_root=tmp_path)

    def test_a_root_entry_and_a_child_entry_are_ambiguous(self, tmp_path: Path) -> None:
        (tmp_path / "flow.py").write_text("", encoding="utf-8")
        _package(tmp_path, name="workflow", entry="flow.py")

        with pytest.raises(RuntimeError, match="multiple workflow entrypoints"):
            discover_default_workflow(project_root=tmp_path)


class TestResolveEnvsWorkflowRoot:
    """Regression tests for issue #309.

    A project laid out exactly as the docs describe -- ``workflow/envs/``
    beside ``workflow/flow.py`` -- must resolve its envs root even when the
    canonical-package heuristic cannot uniquely identify a single entry file
    (no candidate, or more than one).
    """

    def test_uses_the_canonical_package_when_it_is_unambiguous(self, tmp_path: Path) -> None:
        entry_path = _package(tmp_path, name="workflow", entry="flow.py")

        root = resolve_envs_workflow_root(project_root=tmp_path, fallback=tmp_path / "elsewhere")

        assert root == entry_path.parent

    def test_falls_back_when_no_canonical_entry_is_found(self, tmp_path: Path) -> None:
        # No flow.py anywhere -- e.g. a src-layout project, where the entry
        # sits more than one directory below the project root.
        nested = tmp_path / "src" / "workflow"
        nested.mkdir(parents=True)
        (nested / "flow.py").write_text("", encoding="utf-8")

        root = resolve_envs_workflow_root(project_root=tmp_path, fallback=nested)

        assert root == nested

    def test_falls_back_when_the_canonical_entry_is_ambiguous(self, tmp_path: Path) -> None:
        _package(tmp_path, name="workflow", entry="flow.py")
        other_entry = _package(tmp_path, name="other", entry="flow.py")

        root = resolve_envs_workflow_root(project_root=tmp_path, fallback=other_entry.parent)

        assert root == other_entry.parent

    def test_returns_none_when_nothing_can_be_resolved(self, tmp_path: Path) -> None:
        assert resolve_envs_workflow_root(project_root=tmp_path) is None
