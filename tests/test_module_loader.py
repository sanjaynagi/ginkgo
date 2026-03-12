"""Tests for workflow module loading by source path."""

from __future__ import annotations

import sys
from pathlib import Path

from ginkgo.runtime.module_loader import load_module_from_path


class TestLoadModuleFromPath:
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
