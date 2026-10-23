"""Pixi environment backend tests.

Integration tests need pixi on PATH and use the test environment at
``plugins/ginkgo-pixi/tests/envs/test_env/``; they are skipped without pixi.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from ginkgo import Out, evaluate, file, shell, task
from ginkgo.envs.manifest import find_pixi_manifest, is_pixi_pyproject
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo_pixi.environment import (
    PixiEnvImportError,
    PixiEnvironment,
    PixiEnvNotFoundError,
    PixiEnvPrepareError,
    PixiRegistry,
    list_envs,
)
from tests.conftest import EventCollector

_TESTS_DIR = Path(__file__).resolve().parent
_TEST_ENV_NAME = "test_env"
_PIXI_TOML = "[workspace]\nname = 'x'\nchannels = []\nplatforms = []\n"
_PYPROJECT_WITH_PIXI = (
    "[project]\nname = 'demo'\nversion = '0.1.0'\n\n"
    "[tool.pixi.workspace]\nchannels = []\nplatforms = []\n"
)
_PYPROJECT_WITHOUT_PIXI = "[project]\nname = 'demo'\nversion = '0.1.0'\n"

pixi_required = pytest.mark.skipif(shutil.which("pixi") is None, reason="pixi not found on PATH")

PixiRun = Callable[[list[str]], subprocess.CompletedProcess[str]]


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The evaluator's cache lives under the working directory.
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def fake_pixi(monkeypatch: pytest.MonkeyPatch) -> Callable[[PixiRun], list[list[str]]]:
    """Stand in for the ``pixi`` binary: route its subprocess calls to *handler*.

    Returns a function that installs a handler and returns the list of argv the
    backend ran, so a test can assert on what was invoked.
    """

    def install(handler: PixiRun) -> list[list[str]]:
        calls: list[list[str]] = []

        def run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            calls.append(argv)
            return handler(argv)

        monkeypatch.setattr("ginkgo_pixi.environment._require_pixi", lambda: None)
        monkeypatch.setattr("ginkgo_pixi.environment.subprocess.run", run)
        return calls

    return install


def _completed(argv: list[str], code: int = 0, stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(argv, code, stdout="", stderr=stderr)


def _write_env(envs_root: Path, name: str, *, filename: str, content: str) -> Path:
    """Create ``envs_root/<name>/<filename>`` with *content* and return its path."""
    manifest = envs_root / name / filename
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(content, encoding="utf-8")
    return manifest


class TestResolve:
    def test_named_and_explicit_paths(self, tmp_path: Path) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        assert registry.resolve(env=_TEST_ENV_NAME) == _TESTS_DIR / "envs" / "test_env" / (
            "pixi.toml"
        )
        manifest = tmp_path / "pixi.toml"
        manifest.write_text(_PIXI_TOML)
        assert registry.resolve(env=str(manifest)) == manifest.resolve()

    def test_unknown_env_raises(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        with pytest.raises(PixiEnvNotFoundError, match="nonexistent_env"):
            registry.resolve(env="nonexistent_env")
        # Path resolution fires before the pixi availability check.
        with pytest.raises(PixiEnvNotFoundError, match="missing_env"):
            registry.validate_envs(env_names={"missing_env"})

    def test_workflow_local_envs_directory(self, tmp_path: Path) -> None:
        workflow_root = tmp_path / "demo_project"
        manifest = _write_env(
            workflow_root / "envs", "demo", filename="pixi.toml", content=_PIXI_TOML
        )

        registry = PixiRegistry(project_root=tmp_path, workflow_root=workflow_root)

        assert registry.resolve(env="demo") == manifest.resolve()

    def test_pyproject_manifests(self, tmp_path: Path) -> None:
        envs = tmp_path / "envs"
        ml = _write_env(envs, "ml_env", filename="pyproject.toml", content=_PYPROJECT_WITH_PIXI)
        both = _write_env(envs, "both", filename="pixi.toml", content=_PIXI_TOML)
        _write_env(envs, "both", filename="pyproject.toml", content=_PYPROJECT_WITH_PIXI)
        _write_env(envs, "plain", filename="pyproject.toml", content=_PYPROJECT_WITHOUT_PIXI)
        (envs / "empty").mkdir()
        (ml.parent / "pixi.lock").write_text("locked\n", encoding="utf-8")
        registry = PixiRegistry(project_root=tmp_path)

        assert registry.resolve(env="ml_env") == ml.resolve()
        assert registry.lock_hash(env="ml_env") is not None
        # pixi.toml wins over a pyproject beside it.
        assert registry.resolve(env="both") == both.resolve()
        with pytest.raises(PixiEnvNotFoundError, match="plain"):
            registry.resolve(env="plain")
        assert list_envs(envs) == ["both", "ml_env"]
        assert find_pixi_manifest(envs / "empty") is None

    @pytest.mark.parametrize(
        ("filename", "content", "expected"),
        [
            ("pyproject.toml", _PYPROJECT_WITH_PIXI, True),
            ("pyproject.toml", _PYPROJECT_WITHOUT_PIXI, False),
            ("pixi.toml", "[tool.pixi.workspace]\n", False),
            ("pyproject.toml", "[tool.pixi\nbroken = ", False),
        ],
    )
    def test_is_pixi_pyproject(
        self, tmp_path: Path, filename: str, content: str, expected: bool
    ) -> None:
        manifest = tmp_path / filename
        manifest.write_text(content, encoding="utf-8")
        assert is_pixi_pyproject(manifest) is expected


class TestCondaEnvFileImport:
    def test_imports_to_a_generated_pixi_workspace(self, tmp_path: Path, fake_pixi) -> None:
        env_file = tmp_path / "environment.yml"
        env_file.write_text("name: demo\ndependencies:\n  - python\n", encoding="utf-8")
        generated = tmp_path / ".ginkgo-pixi" / "pixi.toml"

        def pixi_init(argv: list[str]) -> subprocess.CompletedProcess[str]:
            generated.parent.mkdir(parents=True, exist_ok=True)
            generated.write_text("[workspace]\nname = 'demo'\n", encoding="utf-8")
            return _completed(argv)

        calls = fake_pixi(pixi_init)

        assert PixiRegistry(project_root=_TESTS_DIR).resolve(env=str(env_file)) == (
            generated.resolve()
        )
        assert calls == [["pixi", "init", str(generated.parent), "--import", str(env_file)]]

    def test_a_fresh_import_is_reused_and_an_edited_file_is_imported_again(
        self, tmp_path: Path, fake_pixi
    ) -> None:
        env_file = tmp_path / "environment.yaml"
        env_file.write_text("name: demo\n", encoding="utf-8")
        generated = tmp_path / ".ginkgo-pixi" / "pixi.toml"
        generated.parent.mkdir(parents=True)
        generated.write_text("[workspace]\nname = 'old'\n", encoding="utf-8")

        def pixi_init(argv: list[str]) -> subprocess.CompletedProcess[str]:
            # Like `pixi init`, refuse a directory that already has a manifest.
            if generated.exists():
                return _completed(argv, 1, "pixi.toml already exists")
            generated.write_text("[workspace]\nname = 'new'\n", encoding="utf-8")
            return _completed(argv)

        calls = fake_pixi(pixi_init)
        registry = PixiRegistry(project_root=_TESTS_DIR)

        assert registry.resolve(env=str(env_file)) == generated.resolve()
        assert calls == []

        os.utime(generated, (0, 0))
        PixiRegistry(project_root=_TESTS_DIR).resolve(env=str(env_file))
        assert "new" in generated.read_text(encoding="utf-8")

    def test_an_import_failure_is_explained(self, tmp_path: Path, fake_pixi) -> None:
        env_file = tmp_path / "environment.yml"
        env_file.write_text("name: broken\n", encoding="utf-8")
        fake_pixi(lambda argv: _completed(argv, 1, "import failed"))

        with pytest.raises(PixiEnvImportError, match="import failed"):
            PixiRegistry(project_root=_TESTS_DIR).resolve(env=str(env_file))


class TestIdentity:
    def test_env_identity_is_stable_when_the_lock_file_appears(self, tmp_path: Path) -> None:
        """Regression for #194: installing the environment must not move identity.

        ``pixi install`` writes the neighbouring ``pixi.lock`` after the cache key
        has been built, so an identity taken from the lock file re-ran every
        Pixi-backed task once.
        """
        manifest = tmp_path / "pixi.toml"
        manifest.write_text(_PIXI_TOML, encoding="utf-8")

        before = PixiRegistry(project_root=tmp_path).env_identity(env=str(manifest))
        (tmp_path / "pixi.lock").write_text("version: 6\n", encoding="utf-8")
        assert PixiRegistry(project_root=tmp_path).env_identity(env=str(manifest)) == before

        manifest.write_text(_PIXI_TOML + "\n[dependencies]\npython = '>=3.11'\n")
        assert PixiRegistry(project_root=tmp_path).env_identity(env=str(manifest)) != before

    def test_lock_hash(self, tmp_path: Path) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        digest = registry.lock_hash(env=_TEST_ENV_NAME)
        assert digest is not None and digest == registry.lock_hash(env=_TEST_ENV_NAME)

        manifest = tmp_path / "pixi.toml"
        manifest.write_text(_PIXI_TOML)
        assert registry.lock_hash(env=str(manifest)) is None


class TestPrepare:
    def test_installs_a_manifest_once(self, fake_pixi) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        manifest = registry.resolve(env=_TEST_ENV_NAME)
        calls = fake_pixi(_completed)

        assert registry.prepare(env=_TEST_ENV_NAME) == manifest
        assert registry.prepare(env=_TEST_ENV_NAME) == manifest
        assert calls == [["pixi", "install", "--manifest-path", str(manifest)]]

    def test_an_install_failure_is_explained(self, fake_pixi) -> None:
        fake_pixi(lambda argv: _completed(argv, 1, "install failed"))

        with pytest.raises(PixiEnvPrepareError, match="install failed"):
            PixiRegistry(project_root=_TESTS_DIR).prepare(env=_TEST_ENV_NAME)

    def test_exec_argv_runs_strict_bash_under_pixi(self) -> None:
        argv = PixiRegistry(project_root=_TESTS_DIR).exec_argv(env=_TEST_ENV_NAME, cmd="echo hi")
        assert argv[:2] == ["pixi", "run"]
        assert "--manifest-path" in argv
        assert argv[-5:] == ["bash", "-eo", "pipefail", "-c", "echo hi"]


# ---------------------------------------------------------------------------
# Integration tests: real pixi
# ---------------------------------------------------------------------------


def _backend() -> PixiEnvironment:
    return PixiEnvironment(pixi_registry=PixiRegistry(project_root=_TESTS_DIR))


@task(env=_TEST_ENV_NAME, kind="shell")
def shell_touch(output_path: Out[file]) -> file:
    return shell(cmd=f"echo pixi_ran > {output_path}", output=output_path)


@pixi_required
def test_a_shell_task_runs_in_the_pixi_env_and_is_cached_on_rerun(tmp_path: Path) -> None:
    PixiRegistry(project_root=_TESTS_DIR).validate_envs(env_names={_TEST_ENV_NAME})
    output = str(tmp_path / "sentinel.txt")

    assert evaluate(shell_touch(output_path=output), backend=_backend()) == output
    assert Path(output).read_text().strip() == "pixi_ran"

    collector = EventCollector()
    ConcurrentEvaluator(backend=_backend(), event_bus=collector.bus).evaluate(
        shell_touch(output_path=output)
    )
    assert len(collector.cached()) >= 1
    assert collector.started() == []


@pixi_required
def test_shell_tasks_do_not_import_the_workflow_module_inside_the_pixi_env(
    tmp_path: Path,
) -> None:
    # pandas is not installed in test_env: importing the workflow there would fail.
    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text(
        "import pandas as pd\n\nfrom ginkgo import shell, task\n\n\n"
        '@task(env="test_env", kind="shell")\n'
        "def shell_only(output_path: str) -> str:\n"
        "    return shell(cmd=f\"printf 'ok' > {output_path}\", output=output_path)\n",
        encoding="utf-8",
    )
    module = load_module_from_path(workflow_path)
    output = tmp_path / "shell-only.txt"

    assert evaluate(module.shell_only(output_path=str(output)), backend=_backend()) == str(output)
    assert output.read_text(encoding="utf-8") == "ok"


def test_python_tasks_with_a_pixi_env_are_rejected_before_env_resolution() -> None:
    @task(env="definitely_does_not_exist")
    def needs_foreign_env(x: int) -> int:
        return x + 1

    with pytest.raises(TypeError, match="cannot run in a foreign environment"):
        evaluate(needs_foreign_env(x=1), backend=_backend())
