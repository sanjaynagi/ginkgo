"""Phase 5 — Pixi environment backend tests.

These tests cover:
- PixiRegistry resolution, lock hash, and argv helpers (unit).
- Shell tasks executed inside a Pixi environment (integration).
- Validation that foreign execution environments are shell-only.
- Startup validation for undeclared environments (unit + integration).
- Tasks with ``env=None`` still run correctly alongside env-isolated tasks.

Integration tests require pixi on PATH and the test environment installed at
``tests/envs/test_env/``.  They are skipped automatically when pixi is
unavailable.
"""

from __future__ import annotations

import subprocess
import shutil
from pathlib import Path

import pytest

from ginkgo import flow, shell, task
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.pixi import (
    PixiEnvImportError,
    PixiEnvNotFoundError,
    PixiEnvPrepareError,
    PixiRegistry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TESTS_DIR = Path(__file__).parent
_TEST_ENV_NAME = "test_env"


def _make_registry(tmp_path: Path) -> PixiRegistry:
    """Return a PixiRegistry pointing at the real test envs directory."""
    return PixiRegistry(project_root=_TESTS_DIR)


def _evaluate(expr, *, registry: PixiRegistry):
    """Evaluate an expression without importing the evaluator at module import time."""
    from ginkgo import evaluate

    return evaluate(expr, pixi_registry=registry)


def _pixi_available() -> bool:
    return shutil.which("pixi") is not None


pixi_required = pytest.mark.skipif(
    not _pixi_available(),
    reason="pixi not found on PATH",
)


# ---------------------------------------------------------------------------
# Unit tests — PixiRegistry (no subprocess)
# ---------------------------------------------------------------------------


class TestPixiRegistry:
    def test_resolve_named_env(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        manifest = registry.resolve(env=_TEST_ENV_NAME)
        assert manifest.name == "pixi.toml"
        assert manifest.parent.name == _TEST_ENV_NAME

    def test_resolve_explicit_path(self, tmp_path: Path) -> None:
        manifest = tmp_path / "pixi.toml"
        manifest.write_text("[workspace]\nname = 'x'\nchannels = []\nplatforms = []\n")
        registry = PixiRegistry(project_root=_TESTS_DIR)
        resolved = registry.resolve(env=str(manifest))
        assert resolved == manifest.resolve()

    def test_resolve_unknown_env_raises(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        with pytest.raises(PixiEnvNotFoundError, match="nonexistent_env"):
            registry.resolve(env="nonexistent_env")

    def test_resolve_named_env_from_workflow_local_envs_directory(self, tmp_path: Path) -> None:
        workflow_root = tmp_path / "demo_project"
        env_manifest = workflow_root / "envs" / "demo" / "pixi.toml"
        env_manifest.parent.mkdir(parents=True)
        env_manifest.write_text(
            "[workspace]\nname = 'demo'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )

        registry = PixiRegistry(project_root=tmp_path, workflow_root=workflow_root)

        assert registry.resolve(env="demo") == env_manifest.resolve()

    def test_resolve_conda_env_file_imports_to_generated_pixi_workspace(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        env_file = tmp_path / "environment.yml"
        env_file.write_text("name: demo\ndependencies:\n  - python\n", encoding="utf-8")

        def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            assert argv[:4] == ["pixi", "init", str(tmp_path / ".ginkgo-pixi"), "--import"]
            assert argv[4] == str(env_file)
            generated_manifest = tmp_path / ".ginkgo-pixi" / "pixi.toml"
            generated_manifest.parent.mkdir(parents=True, exist_ok=True)
            generated_manifest.write_text("[workspace]\nname = 'demo'\n", encoding="utf-8")
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        monkeypatch.setattr("ginkgo.envs.pixi._require_pixi", lambda: None)
        monkeypatch.setattr("ginkgo.envs.pixi.subprocess.run", fake_run)

        registry = PixiRegistry(project_root=_TESTS_DIR)
        manifest = registry.resolve(env=str(env_file))

        assert manifest == (tmp_path / ".ginkgo-pixi" / "pixi.toml").resolve()

    def test_resolve_conda_env_file_reuses_generated_manifest_when_fresh(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        env_file = tmp_path / "environment.yaml"
        env_file.write_text("name: demo\n", encoding="utf-8")
        generated_manifest = tmp_path / ".ginkgo-pixi" / "pixi.toml"
        generated_manifest.parent.mkdir(parents=True, exist_ok=True)
        generated_manifest.write_text("[workspace]\nname = 'demo'\n", encoding="utf-8")
        generated_manifest.touch()

        monkeypatch.setattr("ginkgo.envs.pixi._require_pixi", lambda: None)

        def fail_run(*_: object, **__: object) -> subprocess.CompletedProcess[str]:
            raise AssertionError("pixi import should not run for a fresh generated manifest")

        monkeypatch.setattr("ginkgo.envs.pixi.subprocess.run", fail_run)
        registry = PixiRegistry(project_root=_TESTS_DIR)

        manifest = registry.resolve(env=str(env_file))
        assert manifest == generated_manifest.resolve()

    def test_resolve_conda_env_file_raises_clear_error_on_import_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        env_file = tmp_path / "environment.yml"
        env_file.write_text("name: broken\n", encoding="utf-8")

        def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="import failed")

        monkeypatch.setattr("ginkgo.envs.pixi._require_pixi", lambda: None)
        monkeypatch.setattr("ginkgo.envs.pixi.subprocess.run", fake_run)

        registry = PixiRegistry(project_root=_TESTS_DIR)
        with pytest.raises(PixiEnvImportError, match="import failed"):
            registry.resolve(env=str(env_file))

    def test_lock_hash_returns_string(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        digest = registry.lock_hash(env=_TEST_ENV_NAME)
        assert isinstance(digest, str)
        assert len(digest) == 64  # SHA-256 hex digest

    def test_lock_hash_is_stable(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        assert registry.lock_hash(env=_TEST_ENV_NAME) == registry.lock_hash(env=_TEST_ENV_NAME)

    def test_lock_hash_absent_lockfile(self, tmp_path: Path) -> None:
        manifest = tmp_path / "pixi.toml"
        manifest.write_text("[workspace]\nname = 'x'\nchannels = []\nplatforms = []\n")
        registry = PixiRegistry(project_root=_TESTS_DIR)
        # No pixi.lock alongside the manifest.
        digest = registry.lock_hash(env=str(manifest))
        assert digest is None

    @pixi_required
    def test_validate_envs_passes_for_known(self) -> None:
        # Requires pixi on PATH — validate_envs checks availability after path resolution.
        registry = PixiRegistry(project_root=_TESTS_DIR)
        registry.validate_envs(env_names={_TEST_ENV_NAME})

    def test_validate_envs_raises_for_unknown(self) -> None:
        # Env path resolution fires before the pixi availability check, so this
        # raises PixiEnvNotFoundError regardless of whether pixi is installed.
        registry = PixiRegistry(project_root=_TESTS_DIR)
        with pytest.raises(PixiEnvNotFoundError, match="missing_env"):
            registry.validate_envs(env_names={"missing_env"})

    def test_prepare_installs_manifest_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        manifest = registry.resolve(env=_TEST_ENV_NAME)
        install_calls: list[list[str]] = []

        def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            install_calls.append(argv)
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        monkeypatch.setattr("ginkgo.envs.pixi._require_pixi", lambda: None)
        monkeypatch.setattr("ginkgo.envs.pixi.subprocess.run", fake_run)

        assert registry.prepare(env=_TEST_ENV_NAME) == manifest
        assert registry.prepare(env=_TEST_ENV_NAME) == manifest
        assert install_calls == [["pixi", "install", "--manifest-path", str(manifest)]]

    def test_prepare_raises_clear_error_on_install_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)

        def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="install failed")

        monkeypatch.setattr("ginkgo.envs.pixi._require_pixi", lambda: None)
        monkeypatch.setattr("ginkgo.envs.pixi.subprocess.run", fake_run)

        with pytest.raises(PixiEnvPrepareError, match="install failed"):
            registry.prepare(env=_TEST_ENV_NAME)

    def test_shell_argv_structure(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        argv = registry.shell_argv(env=_TEST_ENV_NAME, cmd="echo hello")
        assert argv[0] == "pixi"
        assert argv[1] == "run"
        assert "--manifest-path" in argv
        assert "bash" in argv
        assert "-c" in argv
        assert "echo hello" in argv

    def test_python_task_with_env_is_rejected_before_env_resolution(self, tmp_path: Path) -> None:
        """evaluate() rejects Python tasks with env= before any env lookup occurs."""

        @task(env="definitely_does_not_exist")
        def my_task(x: int) -> int:
            return x + 1

        @flow
        def my_flow():
            return my_task(x=1)

        registry = PixiRegistry(project_root=_TESTS_DIR)
        with pytest.raises(TypeError, match="Foreign environments only support shell tasks"):
            _evaluate(my_flow(), registry=registry)


# ---------------------------------------------------------------------------
# Integration tests — require pixi on PATH
# ---------------------------------------------------------------------------


@task(env=_TEST_ENV_NAME, kind="shell")
def shell_touch(output_path: str) -> str:
    """Shell task: uses the pixi env to create a sentinel file."""
    return shell(
        cmd=f"echo pixi_ran > {output_path}",
        output=output_path,
    )


@task()
def plain_add(x: int, y: int) -> int:
    """Pure Python task without an env — runs in the current Python."""
    return x + y


class TestPixiShellTask:
    @pixi_required
    def test_shell_task_runs_in_pixi_env(self, tmp_path: Path) -> None:
        output = str(tmp_path / "sentinel.txt")
        registry = _make_registry(tmp_path)
        result = _evaluate(shell_touch(output_path=output), registry=registry)
        assert result == output
        assert Path(output).read_text().strip() == "pixi_ran"

    @pixi_required
    def test_shell_task_cached_on_rerun(self, tmp_path: Path) -> None:
        """Second evaluate() with unchanged inputs returns from cache (no pixi invocation)."""
        import io

        output = str(tmp_path / "sentinel.txt")
        registry = _make_registry(tmp_path)

        # Run 1 — shell command executes, result cached.
        _evaluate(shell_touch(output_path=output), registry=registry)
        assert Path(output).exists()

        # Capture log output on run 2 to confirm the task was served from cache.
        log = io.StringIO()
        from ginkgo.evaluator import _ConcurrentEvaluator
        from ginkgo.runtime.backend import LocalBackend

        evaluator = _ConcurrentEvaluator(backend=LocalBackend(pixi_registry=registry), _stderr=log)
        evaluator.evaluate(shell_touch(output_path=output))
        assert '"cached"' in log.getvalue()

    @pixi_required
    def test_shell_tasks_do_not_import_workflow_module_inside_pixi_env(
        self,
        tmp_path: Path,
    ) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text(
            """
import pandas as pd

from ginkgo import shell, task


@task(env="test_env", kind="shell")
def shell_only(output_path: str) -> str:
    return shell(cmd=f"printf 'ok' > {output_path}", output=output_path)
""".strip()
            + "\n",
            encoding="utf-8",
        )

        module = load_module_from_path(workflow_path)
        output = tmp_path / "shell-only.txt"
        registry = _make_registry(tmp_path)

        result = _evaluate(module.shell_only(output_path=str(output)), registry=registry)

        assert result == str(output)
        assert output.read_text(encoding="utf-8") == "ok"


class TestPixiPythonTask:
    def test_python_tasks_with_pixi_env_are_rejected_at_validation(self, tmp_path: Path) -> None:
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text(
            """
from ginkgo import task


@task(env="test_env")
def needs_foreign_env(x: int) -> int:
    return x + 1
""".strip()
            + "\n",
            encoding="utf-8",
        )

        module = load_module_from_path(workflow_path)
        registry = _make_registry(tmp_path)

        with pytest.raises(TypeError, match="Foreign environments only support shell tasks"):
            _evaluate(module.needs_foreign_env(x=1), registry=registry)
