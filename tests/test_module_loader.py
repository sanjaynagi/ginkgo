"""Tests for workflow module loading by source path."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

import ginkgo
from ginkgo.runtime.module_loader import import_roots_for_path, load_module_from_path


class TestLoadModuleFromPath:
    def test_importing_ginkgo_does_not_eagerly_import_config_module(self) -> None:
        sys.modules.pop("ginkgo.config", None)

        importlib.reload(ginkgo)

        assert "ginkgo.config" not in sys.modules

    def test_importing_config_submodule_preserves_ginkgo_config_callable(self) -> None:
        sys.modules.pop("ginkgo.config", None)

        importlib.reload(ginkgo)

        from ginkgo.config import config_session

        assert config_session is not None
        assert callable(ginkgo.config)
        assert not isinstance(ginkgo.config, ModuleType)

    def test_import_roots_for_path_include_package_root_parent(self, tmp_path: Path) -> None:
        repo_root = tmp_path / "AmpSeeker"
        package_dir = repo_root / "ampseeker_ginkgo"
        package_dir.mkdir(parents=True)

        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        workflow_path = package_dir / "workflow.py"
        workflow_path.write_text("VALUE = 1\n", encoding="utf-8")

        roots = import_roots_for_path(workflow_path)

        assert roots == [str(package_dir.resolve()), str(repo_root.resolve())]

    def test_load_module_supports_importing_own_package(self, tmp_path: Path) -> None:
        repo_root = tmp_path / "AmpSeeker"
        package_dir = repo_root / "ampseeker_ginkgo"
        package_dir.mkdir(parents=True)

        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "helpers.py").write_text(
            "def build_message() -> str:\n    return 'ok'\n",
            encoding="utf-8",
        )
        workflow_path = package_dir / "workflow.py"
        workflow_path.write_text(
            "from ampseeker_ginkgo.helpers import build_message\n\nMESSAGE = build_message()\n",
            encoding="utf-8",
        )

        original_sys_path = list(sys.path)
        try:
            module = load_module_from_path(workflow_path)
            assert str(repo_root) in sys.path
        finally:
            sys.path[:] = original_sys_path

        assert module.MESSAGE == "ok"

    def test_import_roots_for_path_include_project_root_for_unpackaged_test_workflow(
        self, tmp_path: Path
    ) -> None:
        # Mirrors a fresh `ginkgo init` scaffold: a package with __init__.py
        # and workflow.py, a ginkgo.toml at the project root, and a test
        # workflow under tests/workflows/ (no __init__.py there) that
        # imports from the project's own package.
        project_root = tmp_path / "w1"
        package_dir = project_root / "w1"
        package_dir.mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "workflow.py").write_text(
            "def main() -> str:\n    return 'ok'\n",
            encoding="utf-8",
        )
        (project_root / "ginkgo.toml").write_text("", encoding="utf-8")

        test_workflows_dir = project_root / "tests" / "workflows"
        test_workflows_dir.mkdir(parents=True)
        smoke_path = test_workflows_dir / "smoke.py"
        smoke_path.write_text(
            "from w1.workflow import main\n\nRESULT = main()\n",
            encoding="utf-8",
        )

        roots = import_roots_for_path(smoke_path)

        assert str(project_root.resolve()) in roots

    def test_load_module_supports_test_workflow_importing_project_package(
        self, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "w1"
        package_dir = project_root / "w1"
        package_dir.mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "workflow.py").write_text(
            "def main() -> str:\n    return 'ok'\n",
            encoding="utf-8",
        )
        (project_root / "ginkgo.toml").write_text("", encoding="utf-8")

        test_workflows_dir = project_root / "tests" / "workflows"
        test_workflows_dir.mkdir(parents=True)
        smoke_path = test_workflows_dir / "smoke.py"
        smoke_path.write_text(
            "from w1.workflow import main\n\nRESULT = main()\n",
            encoding="utf-8",
        )

        original_sys_path = list(sys.path)
        try:
            module = load_module_from_path(smoke_path)
        finally:
            sys.path[:] = original_sys_path

        assert module.RESULT == "ok"
