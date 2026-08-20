"""Tests for project root discovery.

The point of :mod:`ginkgo.project` is that "where is the project root" has one
answer, so the tests that matter are the ones pinning the walk upward, the
fallback when a project has no config file, and the delegation from the module
loader that previously carried its own copy of the walk.
"""

from __future__ import annotations

from pathlib import Path

import ginkgo
from ginkgo.project import PROJECT_CONFIG_NAMES, find_project_root, project_root
from ginkgo.runtime.module_loader import import_roots_for_path


class TestFindProjectRoot:
    """The walk upward from an explicit starting directory."""

    def test_returns_the_start_directory_when_it_holds_the_config(self, tmp_path: Path):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")

        assert find_project_root(tmp_path) == tmp_path

    def test_walks_up_from_a_subdirectory(self, tmp_path: Path):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        nested = tmp_path / "workflow" / "steps"
        nested.mkdir(parents=True)

        assert find_project_root(nested) == tmp_path

    def test_returns_none_when_no_directory_holds_a_config(self, tmp_path: Path):
        nested = tmp_path / "workflow"
        nested.mkdir()

        # tmp_path itself has no marker, and neither does any real ancestor.
        assert find_project_root(nested) is None

    def test_stops_at_the_nearest_root(self, tmp_path: Path):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        inner = tmp_path / "inner"
        inner.mkdir()
        (inner / "ginkgo.toml").write_text("", encoding="utf-8")

        assert find_project_root(inner / "deeper") == inner

    def test_accepts_every_recognised_config_name(self, tmp_path: Path):
        for index, config_name in enumerate(PROJECT_CONFIG_NAMES):
            root = tmp_path / f"p{index}"
            root.mkdir()
            (root / config_name).write_text("", encoding="utf-8")

            assert find_project_root(root) == root

    def test_ignores_a_directory_named_like_the_config(self, tmp_path: Path):
        (tmp_path / "ginkgo.toml").mkdir()

        assert find_project_root(tmp_path) is None


class TestProjectRoot:
    """The public helper, anchored on the working directory."""

    def test_resolves_the_same_root_from_a_subdirectory(self, tmp_path, monkeypatch):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        nested = tmp_path / "workflow"
        nested.mkdir()

        monkeypatch.chdir(tmp_path)
        from_root = project_root()
        monkeypatch.chdir(nested)
        from_subdirectory = project_root()

        # Compared via resolve() because macOS reports /private/var for /var.
        assert from_root == from_subdirectory == tmp_path.resolve()

    def test_falls_back_to_the_working_directory_without_a_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        assert project_root() == tmp_path.resolve()

    def test_is_exported_from_the_package(self, tmp_path, monkeypatch):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        assert ginkgo.project_root() == tmp_path.resolve()
        assert "project_root" in ginkgo.__all__


class TestModuleLoaderSharesTheDefinition:
    """The module loader's import roots come from the same walk."""

    def test_import_roots_include_the_discovered_project_root(self, tmp_path: Path):
        root = tmp_path / "proj"
        nested = root / "tests" / "workflows"
        nested.mkdir(parents=True)
        (root / "ginkgo.toml").write_text("", encoding="utf-8")
        workflow_path = nested / "smoke.py"
        workflow_path.write_text("", encoding="utf-8")

        roots = import_roots_for_path(workflow_path)

        assert str(find_project_root(nested.resolve())) in roots
