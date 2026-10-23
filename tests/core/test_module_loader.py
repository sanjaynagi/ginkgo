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

pytestmark = pytest.mark.usefixtures("restore_imports")

_GREET = "from ginkgo import task\n\n\n@task()\ndef greet(name: str) -> str:\n    return name\n"


def _write(path: Path, text: str = "") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _scaffold_package(root: Path, *, package: str = "workflow", entry: str = "flow.py") -> Path:
    """Create a ``<package>/`` whose entry imports a sibling module relatively."""
    _write(root / package / "__init__.py")
    _write(root / package / "modules" / "__init__.py")
    _write(root / package / "modules" / "analysis.py", "def build() -> str:\n    return 'ok'\n")
    return _write(
        root / package / entry, "from .modules.analysis import build\n\nRESULT = build()\n"
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


def test_importing_ginkgo_defers_the_config_module(reimportable_ginkgo_config) -> None:
    sys.modules.pop("ginkgo.config", None)
    importlib.reload(ginkgo)

    assert "ginkgo.config" not in sys.modules

    from ginkgo.config import config_session

    assert config_session is not None
    assert callable(ginkgo.config)
    assert not isinstance(ginkgo.config, ModuleType)


class TestImportRoots:
    def test_a_package_entry_can_import_its_own_package(self, tmp_path: Path, monkeypatch) -> None:
        repo_root = tmp_path / "AmpSeeker"
        repo_root.mkdir()
        monkeypatch.chdir(repo_root)
        _write(repo_root / "ampseeker_ginkgo" / "__init__.py")
        _write(
            repo_root / "ampseeker_ginkgo" / "helpers.py", "def msg() -> str:\n    return 'ok'\n"
        )
        workflow_path = _write(
            repo_root / "ampseeker_ginkgo" / "workflow.py",
            "from ampseeker_ginkgo.helpers import msg\n\nMESSAGE = msg()\n",
        )

        assert import_roots_for_path(workflow_path) == [
            str(workflow_path.parent.resolve()),
            str(repo_root.resolve()),
        ]
        assert load_module_from_path(workflow_path).MESSAGE == "ok"

    def test_roots_fall_back_to_the_working_directory_without_ginkgo_toml(
        self, tmp_path: Path
    ) -> None:
        """N7: ``exp/flow.py`` can import ``exp.tasks`` from an unconfigured project."""
        flow_path = _write(tmp_path / "exp" / "flow.py")

        assert import_roots_for_path(flow_path) == [
            str(flow_path.parent.resolve()),
            str(tmp_path.resolve()),
        ]

    def test_an_unpackaged_test_workflow_can_import_the_project_package(
        self, tmp_path: Path
    ) -> None:
        # Mirrors a fresh `ginkgo init` scaffold: a test workflow under
        # tests/workflows/ (no __init__.py) importing the project's package.
        project_root = tmp_path / "w1"
        _write(project_root / "ginkgo.toml")
        _write(project_root / "w1" / "__init__.py")
        _write(project_root / "w1" / "workflow.py", "def main() -> str:\n    return 'ok'\n")
        smoke_path = _write(
            project_root / "tests" / "workflows" / "smoke.py",
            "from w1.workflow import main\n\nRESULT = main()\n",
        )

        assert str(project_root.resolve()) in import_roots_for_path(smoke_path)
        assert load_module_from_path(smoke_path).RESULT == "ok"


class TestPackageQualifiedLoading:
    def test_entry_in_package_supports_relative_imports(self, tmp_path: Path) -> None:
        workflow_path = _scaffold_package(tmp_path / "w1")

        module = load_module_from_path(workflow_path)

        assert package_qualified_name(workflow_path) == "workflow.flow"
        assert (module.__name__, module.__package__, module.RESULT) == (
            "workflow.flow",
            "workflow",
            "ok",
        )

    def test_entry_named_after_its_own_package_does_not_shadow_it(self, tmp_path: Path) -> None:
        """A pkg/pkg.py entry must not shadow pkg/ during the parent import.

        The entry file's own directory is on sys.path, so ``import pkg`` would
        resolve to that sibling module unless the package root's parent is
        ordered ahead of it. An explicit path accepts any file name.
        """
        entry_path = _scaffold_package(tmp_path / "proj", package="analysis", entry="analysis.py")

        module = load_module_from_path(entry_path)

        assert (module.__name__, module.RESULT) == ("analysis.analysis", "ok")

    @pytest.mark.parametrize("packaged", [True, False])
    def test_a_failed_entry_module_is_not_left_cached(self, tmp_path: Path, packaged) -> None:
        if packaged:
            _write(tmp_path / "w1" / "workflow" / "__init__.py")
        entry_path = _write(
            tmp_path / "w1" / "workflow" / "flow.py", "raise RuntimeError('boom')\n"
        )
        name = "workflow.flow" if packaged else module_name_for_path(entry_path)

        with pytest.raises(RuntimeError, match="boom"):
            load_module_from_path(entry_path)
        assert name not in sys.modules

        # A later load must re-execute rather than short-circuit onto the
        # half-initialised module.
        entry_path.write_text("RESULT = 'recovered'\n", encoding="utf-8")
        assert load_module_from_path(entry_path).RESULT == "recovered"

    def test_reloading_re_executes_the_entry_but_keeps_its_submodules(
        self, tmp_path: Path
    ) -> None:
        """One ginkgo run loads the entry twice; task identity must not churn."""
        entry_path = _scaffold_package(tmp_path / "w1")

        load_module_from_path(entry_path)
        first = sys.modules["workflow.modules.analysis"]
        # Differ in size so the stale bytecode cache cannot mask a reload.
        entry_path.write_text(
            "from .modules.analysis import build\n\nRESULT = build() * 2\n", encoding="utf-8"
        )
        second = load_module_from_path(entry_path)

        assert second.RESULT == "okok"
        assert sys.modules["workflow.modules.analysis"] is first

    def test_a_same_named_package_from_elsewhere_is_evicted(self, tmp_path: Path) -> None:
        """Two trees cannot share a package name; the stale one must not win."""
        load_module_from_path(_scaffold_package(tmp_path / "one"))
        second_entry = _scaffold_package(tmp_path / "two")
        second = load_module_from_path(second_entry)

        assert second.__file__ == str(second_entry)
        assert Path(sys.modules["workflow"].__file__).parent == second_entry.parent

    def test_explicit_dotted_module_name_gets_its_parent_package(self, tmp_path: Path) -> None:
        """The worker binding path names modules explicitly; same invariant."""
        entry_path = _scaffold_package(tmp_path / "w1")
        analysis_path = entry_path.parent / "modules" / "analysis.py"
        analysis_path.write_text(
            "from . import __name__ as package_name\n\nPACKAGE = package_name\n",
            encoding="utf-8",
        )

        module = load_module_from_path(analysis_path, module_name="workflow.modules.analysis")

        assert (module.__name__, module.PACKAGE) == (
            "workflow.modules.analysis",
            "workflow.modules",
        )

    def test_bare_entry_file_keeps_synthetic_top_level_name(self, tmp_path: Path) -> None:
        workflow_path = _write(tmp_path / "workflow.py", "RESULT = 'ok'\n")

        module = load_module_from_path(workflow_path)

        assert package_qualified_name(workflow_path) is None
        assert module.__name__.startswith("ginkgo_user_")
        assert module.RESULT == "ok"


class TestCacheNameIsPathIndependent:
    """A single-file workflow's cache identity must not depend on its path.

    Regression coverage for issue #291: renaming, moving, or copying a
    byte-identical single-file workflow used to cold-start its entire cache,
    because the synthetic per-path module name leaked into the task's identity.
    """

    def test_byte_identical_workflows_share_a_name_across_paths(self, tmp_path: Path) -> None:
        first = load_module_from_path(_write(tmp_path / "a" / "workflow.py", _GREET))
        second = load_module_from_path(_write(tmp_path / "b" / "renamed.py", _GREET))

        assert first.__name__ != second.__name__
        assert first.greet.name == second.greet.name == "<workflow>.greet"
        assert first.greet.base_name == "greet"
        # The source hash folds in the entry module's own name, so that must be
        # path-independent too, or an identical name lands on a different key.
        assert first.greet.cache_source_hash == second.greet.cache_source_hash

    def test_dotted_package_tasks_keep_their_real_name(self, tmp_path: Path) -> None:
        _write(tmp_path / "workflow" / "__init__.py")
        module = load_module_from_path(_write(tmp_path / "workflow" / "flow.py", _GREET))

        assert (module.greet.name, module.greet.base_name) == ("workflow.flow.greet", "greet")


class TestRelativeImportGuidance:
    def test_relative_import_without_a_package_names_the_missing_init(
        self, tmp_path: Path
    ) -> None:
        _write(tmp_path / "w1" / "workflow" / "modules" / "analysis.py", "VALUE = 1\n")
        entry_path = _write(
            tmp_path / "w1" / "workflow" / "flow.py", "from .modules.analysis import VALUE\n"
        )

        with pytest.raises(ImportError) as excinfo:
            load_module_from_path(entry_path)

        message = str(excinfo.value)
        assert "flow.py uses a relative import" in message
        assert "workflow/__init__.py" in message
        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_unrelated_import_errors_are_left_alone(self, tmp_path: Path) -> None:
        entry_path = _write(tmp_path / "flow.py", "import definitely_not_a_real_module\n")

        with pytest.raises(ModuleNotFoundError, match="definitely_not_a_real_module"):
            load_module_from_path(entry_path)
