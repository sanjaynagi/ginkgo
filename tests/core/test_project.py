"""Tests for project root discovery.

"Where is the project root" has one answer, so these pin the walk upward, the
fallback when a project has no config file, and the module loader's use of it.
"""

from __future__ import annotations

from pathlib import Path

import ginkgo
from ginkgo.project import PROJECT_CONFIG_NAMES, find_project_root, project_root
from ginkgo.runtime.module_loader import import_roots_for_path


def test_find_project_root_walks_up_to_the_nearest_config(tmp_path: Path):
    (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
    nested = tmp_path / "workflow" / "steps"
    nested.mkdir(parents=True)
    inner = tmp_path / "inner"
    inner.mkdir()
    (inner / "ginkgo.toml").write_text("", encoding="utf-8")

    assert find_project_root(tmp_path) == tmp_path
    assert find_project_root(nested) == tmp_path
    assert find_project_root(inner / "deeper") == inner


def test_find_project_root_accepts_every_recognised_config_name(tmp_path: Path):
    for index, config_name in enumerate(PROJECT_CONFIG_NAMES):
        root = tmp_path / f"p{index}"
        root.mkdir()
        (root / config_name).write_text("", encoding="utf-8")

        assert find_project_root(root) == root


def test_find_project_root_needs_a_config_file(tmp_path: Path):
    (tmp_path / "workflow").mkdir()
    assert find_project_root(tmp_path / "workflow") is None

    (tmp_path / "ginkgo.toml").mkdir()
    assert find_project_root(tmp_path) is None


def test_project_root_is_anchored_on_the_working_directory(tmp_path, monkeypatch):
    # Without a config, the working directory is the root.
    assert project_root() == tmp_path.resolve()

    (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
    (tmp_path / "workflow").mkdir()
    monkeypatch.chdir(tmp_path / "workflow")

    # Compared via resolve() because macOS reports /private/var for /var.
    assert project_root() == ginkgo.project_root() == tmp_path.resolve()
    assert "project_root" in ginkgo.__all__


def test_module_loader_import_roots_include_the_project_root(tmp_path: Path):
    nested = tmp_path / "proj" / "tests" / "workflows"
    nested.mkdir(parents=True)
    (tmp_path / "proj" / "ginkgo.toml").write_text("", encoding="utf-8")
    workflow_path = nested / "smoke.py"
    workflow_path.write_text("", encoding="utf-8")

    assert str(find_project_root(nested.resolve())) in import_roots_for_path(workflow_path)
