"""Tests for canonical workspace discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.workspace import canonical_workflow_candidates, discover_default_workflow


def _package(root: Path, *, name: str, entry: str) -> Path:
    """Create a package directory holding one workflow entry file."""
    package_dir = root / name
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    entry_path = package_dir / entry
    entry_path.write_text("", encoding="utf-8")
    return entry_path


class TestCanonicalWorkflowCandidates:
    def test_finds_canonical_flow_entry(self, tmp_path: Path) -> None:
        entry_path = _package(tmp_path, name="workflow", entry="flow.py")

        assert canonical_workflow_candidates(project_root=tmp_path) == [entry_path]

    def test_finds_pre_rename_workflow_entry(self, tmp_path: Path) -> None:
        entry_path = _package(tmp_path, name="demo_project", entry="workflow.py")

        assert canonical_workflow_candidates(project_root=tmp_path) == [entry_path]

    def test_prefers_flow_over_workflow_within_one_package(self, tmp_path: Path) -> None:
        entry_path = _package(tmp_path, name="workflow", entry="flow.py")
        (tmp_path / "workflow" / "workflow.py").write_text("", encoding="utf-8")

        assert canonical_workflow_candidates(project_root=tmp_path) == [entry_path]

    def test_ignores_directories_without_init(self, tmp_path: Path) -> None:
        (tmp_path / "notapackage").mkdir()
        (tmp_path / "notapackage" / "flow.py").write_text("", encoding="utf-8")

        assert canonical_workflow_candidates(project_root=tmp_path) == []


class TestDiscoverDefaultWorkflow:
    def test_falls_back_to_legacy_root_level_workflow(self, tmp_path: Path) -> None:
        legacy = tmp_path / "workflow.py"
        legacy.write_text("", encoding="utf-8")

        assert discover_default_workflow(project_root=tmp_path) == legacy

    def test_rejects_multiple_candidate_packages(self, tmp_path: Path) -> None:
        _package(tmp_path, name="workflow", entry="flow.py")
        _package(tmp_path, name="other", entry="flow.py")

        with pytest.raises(RuntimeError, match="multiple canonical workflow entrypoints"):
            discover_default_workflow(project_root=tmp_path)

    def test_reports_expected_paths_when_nothing_is_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match=r"workflow/flow\.py"):
            discover_default_workflow(project_root=tmp_path)
