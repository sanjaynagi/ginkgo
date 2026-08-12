"""Tests for workflow module loading by source path."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

import pytest

import ginkgo
from ginkgo.runtime.module_loader import (
    import_roots_for_path,
    load_module_from_path,
    module_name_for_path,
    package_qualified_name,
)


@pytest.fixture
def reimportable_ginkgo_config():
    """Restore ``ginkgo.config`` after a test drops it from ``sys.modules``.

    Re-importing the module builds a second module object with its own session
    stack. A module that already imported names from the original keeps pushing
    onto the old stack while lazy importers read the new one, so every later test
    in the session sees an empty stack. Restoring the original object keeps the
    two views in agreement.
    """
    original = sys.modules.get("ginkgo.config")
    try:
        yield
    finally:
        if original is None:
            sys.modules.pop("ginkgo.config", None)
        else:
            sys.modules["ginkgo.config"] = original
        importlib.reload(ginkgo)


class TestLoadModuleFromPath:
    def test_importing_ginkgo_does_not_eagerly_import_config_module(
        self, reimportable_ginkgo_config
    ) -> None:
        sys.modules.pop("ginkgo.config", None)

        importlib.reload(ginkgo)

        assert "ginkgo.config" not in sys.modules

    def test_importing_config_submodule_preserves_ginkgo_config_callable(
        self, reimportable_ginkgo_config
    ) -> None:
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


def _scaffold_package(tmp_path: Path, *, entry_name: str) -> Path:
    """Create a canonical ``workflow/`` package whose entry imports relatively."""
    package_dir = tmp_path / "w1" / "workflow"
    (package_dir / "modules").mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "modules" / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "modules" / "analysis.py").write_text(
        "def build() -> str:\n    return 'ok'\n",
        encoding="utf-8",
    )
    entry_path = package_dir / entry_name
    entry_path.write_text(
        "from .modules.analysis import build\n\nRESULT = build()\n",
        encoding="utf-8",
    )
    return entry_path


def _forget_workflow_package() -> None:
    """Drop the scaffolded package from ``sys.modules`` between tests."""
    for name in [
        name for name in sys.modules if name == "workflow" or name.startswith("workflow.")
    ]:
        del sys.modules[name]


class TestPackageQualifiedLoading:
    def test_entry_in_package_supports_relative_imports(self, tmp_path: Path) -> None:
        workflow_path = _scaffold_package(tmp_path, entry_name="flow.py")

        original_sys_path = list(sys.path)
        try:
            module = load_module_from_path(workflow_path)
        finally:
            sys.path[:] = original_sys_path
            _forget_workflow_package()

        assert package_qualified_name(workflow_path) == "workflow.flow"
        assert module.__name__ == "workflow.flow"
        assert module.__package__ == "workflow"
        assert module.RESULT == "ok"

    def test_entry_named_after_its_own_package_does_not_shadow_it(self, tmp_path: Path) -> None:
        """A pkg/pkg.py entry must not shadow pkg/ during the parent import.

        The entry file's own directory is on sys.path, so ``import pkg`` would
        resolve to that sibling module rather than the package unless the
        package root's parent is ordered ahead of it. Autodiscovery cannot
        produce this layout — it only accepts flow.py — but an explicit path
        accepts any file name, including pkg/pkg.py.
        """
        package_dir = tmp_path / "proj" / "analysis"
        (package_dir / "modules").mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "modules" / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "modules" / "helpers.py").write_text(
            "def build() -> str:\n    return 'ok'\n", encoding="utf-8"
        )
        entry_path = package_dir / "analysis.py"
        entry_path.write_text(
            "from .modules.helpers import build\n\nRESULT = build()\n", encoding="utf-8"
        )

        original_sys_path = list(sys.path)
        try:
            module = load_module_from_path(entry_path)
        finally:
            sys.path[:] = original_sys_path
            for name in [
                name for name in sys.modules if name == "analysis" or name.startswith("analysis.")
            ]:
                del sys.modules[name]

        assert module.__name__ == "analysis.analysis"
        assert module.RESULT == "ok"

    def test_failed_entry_module_is_not_left_cached(self, tmp_path: Path) -> None:
        package_dir = tmp_path / "w1" / "workflow"
        package_dir.mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        entry_path = package_dir / "flow.py"
        entry_path.write_text("raise RuntimeError('boom')\n", encoding="utf-8")

        original_sys_path = list(sys.path)
        try:
            with pytest.raises(RuntimeError, match="boom"):
                load_module_from_path(entry_path)
            assert "workflow.flow" not in sys.modules

            # A later load must re-execute rather than short-circuit onto the
            # half-initialised module.
            entry_path.write_text("RESULT = 'recovered'\n", encoding="utf-8")
            assert load_module_from_path(entry_path).RESULT == "recovered"
        finally:
            sys.path[:] = original_sys_path
            _forget_workflow_package()

    def test_failed_bare_module_is_not_left_cached(self, tmp_path: Path) -> None:
        entry_path = tmp_path / "flow.py"
        entry_path.write_text("raise RuntimeError('boom')\n", encoding="utf-8")

        original_sys_path = list(sys.path)
        try:
            with pytest.raises(RuntimeError, match="boom"):
                load_module_from_path(entry_path)
        finally:
            sys.path[:] = original_sys_path

        assert module_name_for_path(entry_path) not in sys.modules

    def test_submodules_of_the_same_package_survive_a_reload(self, tmp_path: Path) -> None:
        """One ginkgo run loads the entry twice; task identity must not churn."""
        entry_path = _scaffold_package(tmp_path, entry_name="flow.py")

        original_sys_path = list(sys.path)
        try:
            load_module_from_path(entry_path)
            first = sys.modules["workflow.modules.analysis"]
            load_module_from_path(entry_path)
            second = sys.modules["workflow.modules.analysis"]
        finally:
            sys.path[:] = original_sys_path
            _forget_workflow_package()

        assert first is second

    def test_a_same_named_package_from_elsewhere_is_evicted(self, tmp_path: Path) -> None:
        """Two trees cannot share a package name; the stale one must not win."""
        first_entry = _scaffold_package(tmp_path / "one", entry_name="flow.py")
        second_entry = _scaffold_package(tmp_path / "two", entry_name="flow.py")

        original_sys_path = list(sys.path)
        try:
            load_module_from_path(first_entry)
            second = load_module_from_path(second_entry)
            loaded_package = sys.modules["workflow"]
        finally:
            sys.path[:] = original_sys_path
            _forget_workflow_package()

        assert second.__file__ == str(second_entry)
        assert Path(loaded_package.__file__).parent == second_entry.parent

    def test_explicit_dotted_module_name_gets_its_parent_package(self, tmp_path: Path) -> None:
        """The worker binding path names modules explicitly; same invariant."""
        entry_path = _scaffold_package(tmp_path, entry_name="flow.py")
        analysis_path = entry_path.parent / "modules" / "analysis.py"
        analysis_path.write_text(
            "from . import __name__ as package_name\n\nPACKAGE = package_name\n",
            encoding="utf-8",
        )

        original_sys_path = list(sys.path)
        try:
            module = load_module_from_path(analysis_path, module_name="workflow.modules.analysis")
        finally:
            sys.path[:] = original_sys_path
            _forget_workflow_package()

        assert module.__name__ == "workflow.modules.analysis"
        assert module.PACKAGE == "workflow.modules"

    def test_bare_entry_file_keeps_synthetic_top_level_name(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("RESULT = 'ok'\n", encoding="utf-8")

        original_sys_path = list(sys.path)
        try:
            module = load_module_from_path(workflow_path)
        finally:
            sys.path[:] = original_sys_path

        assert package_qualified_name(workflow_path) is None
        assert module.__name__.startswith("ginkgo_user_")
        assert module.RESULT == "ok"

    def test_repeated_loads_re_execute_the_entry_module(self, tmp_path: Path) -> None:
        project_root = tmp_path / "w1"
        package_dir = project_root / "workflow"
        package_dir.mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        workflow_path = package_dir / "flow.py"
        workflow_path.write_text("VALUE = 1\n", encoding="utf-8")

        original_sys_path = list(sys.path)
        try:
            first = load_module_from_path(workflow_path)
            # Differ in size so the stale bytecode cache cannot mask a reload.
            workflow_path.write_text("VALUE = 22222\n", encoding="utf-8")
            second = load_module_from_path(workflow_path)
        finally:
            sys.path[:] = original_sys_path
            _forget_workflow_package()

        assert first.VALUE == 1
        assert second.VALUE == 22222


class TestRelativeImportGuidance:
    def test_relative_import_without_a_package_names_the_missing_init(
        self, tmp_path: Path
    ) -> None:
        package_dir = tmp_path / "w1" / "workflow"
        (package_dir / "modules").mkdir(parents=True)
        (package_dir / "modules" / "analysis.py").write_text("VALUE = 1\n", encoding="utf-8")
        entry_path = package_dir / "flow.py"
        entry_path.write_text("from .modules.analysis import VALUE\n", encoding="utf-8")

        original_sys_path = list(sys.path)
        try:
            with pytest.raises(ImportError) as excinfo:
                load_module_from_path(entry_path)
        finally:
            sys.path[:] = original_sys_path

        message = str(excinfo.value)
        assert "flow.py uses a relative import" in message
        assert "workflow/ is not a Python package" in message
        assert "workflow/__init__.py" in message
        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_unrelated_import_errors_are_left_alone(self, tmp_path: Path) -> None:
        entry_path = tmp_path / "flow.py"
        entry_path.write_text("import definitely_not_a_real_module\n", encoding="utf-8")

        original_sys_path = list(sys.path)
        try:
            with pytest.raises(ModuleNotFoundError, match="definitely_not_a_real_module"):
                load_module_from_path(entry_path)
        finally:
            sys.path[:] = original_sys_path
