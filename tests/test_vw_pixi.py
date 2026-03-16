"""Phase 5 — Pixi environment backend tests.

These tests cover:
- PixiRegistry resolution, lock hash, and argv helpers (unit).
- Shell tasks executed inside a Pixi environment (integration).
- Python tasks executed inside a Pixi environment (integration).
- Env lock hash invalidation of the task cache (integration).
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

from ginkgo import flow, shell_task, task
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

    def test_python_argv_c_structure(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        argv = registry.python_argv_c(env=_TEST_ENV_NAME, code="print('hi')", args=("a", "b"))
        assert argv[0] == "pixi"
        assert argv[1] == "run"
        assert "--manifest-path" in argv
        assert "python" in argv
        assert "-c" in argv
        assert "print('hi')" in argv
        assert "a" in argv
        assert "b" in argv

    def test_startup_validation_raises_for_missing_env(self, tmp_path: Path) -> None:
        """evaluate() raises PixiEnvNotFoundError before any task runs."""

        @task(env="definitely_does_not_exist")
        def my_task(x: int) -> int:
            return x + 1

        @flow
        def my_flow():
            return my_task(x=1)

        registry = PixiRegistry(project_root=_TESTS_DIR)
        with pytest.raises(PixiEnvNotFoundError):
            _evaluate(my_flow(), registry=registry)


# ---------------------------------------------------------------------------
# Integration tests — require pixi on PATH
# ---------------------------------------------------------------------------


@task(env=_TEST_ENV_NAME)
def shell_touch(output_path: str) -> str:
    """Shell task: uses the pixi env to create a sentinel file."""
    return shell_task(
        cmd=f"echo pixi_ran > {output_path}",
        output=output_path,
    )


@task(env=_TEST_ENV_NAME)
def pixi_double(x: int) -> int:
    """Pure Python task for fan-out test."""
    return x * 2


@task(env=_TEST_ENV_NAME)
def pixi_python_add(x: int, y: int) -> int:
    """Pure Python task running inside the Pixi environment."""
    return x + y


@task()
def plain_add(x: int, y: int) -> int:
    """Pure Python task without an env — runs in the current Python."""
    return x + y


@flow
def mixed_flow(x: int, y: int):
    """Fan-out combining pixi and non-pixi tasks."""
    pixi_result = pixi_python_add(x=x, y=y)
    plain_result = plain_add(x=x, y=y)
    return pixi_result, plain_result


@task(env=_TEST_ENV_NAME)
def logged_pixi_add(x: int, y: int, log_path: str) -> int:
    """Pixi Python task that appends to a log file (for cache invalidation tests)."""
    with open(log_path, "a") as f:
        f.write(f"run:{x}+{y}\n")
    return x + y


@flow
def logged_flow(x: int, y: int, log_path: str):
    return logged_pixi_add(x=x, y=y, log_path=log_path)


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

        evaluator = _ConcurrentEvaluator(pixi_registry=registry, _stderr=log)
        evaluator.evaluate(shell_touch(output_path=output))
        assert '"cached"' in log.getvalue()


class TestPixiPythonTask:
    @pixi_required
    def test_python_task_runs_in_pixi_env(self, tmp_path: Path) -> None:
        registry = _make_registry(tmp_path)
        result = _evaluate(pixi_python_add(x=3, y=4), registry=registry)
        assert result == 7

    @pixi_required
    def test_python_task_fan_out(self, tmp_path: Path) -> None:
        """Fan-out of pixi Python tasks produces correct results in order."""

        @flow
        def fan_flow(items: list[int]):
            return pixi_double().map(x=items)

        registry = _make_registry(tmp_path)
        result = _evaluate(fan_flow(items=[1, 2, 3, 4, 5]), registry=registry)
        assert result == [2, 4, 6, 8, 10]

    @pixi_required
    def test_mixed_pixi_and_plain_tasks(self, tmp_path: Path) -> None:
        """Pixi and non-pixi tasks coexist correctly in the same run."""
        registry = _make_registry(tmp_path)
        pixi_result, plain_result = _evaluate(mixed_flow(x=10, y=5), registry=registry)
        assert pixi_result == 15
        assert plain_result == 15


class TestPixiCacheInvalidation:
    @pixi_required
    def test_unchanged_inputs_served_from_cache(self, tmp_path: Path) -> None:
        log_path = str(tmp_path / "log.txt")
        registry = _make_registry(tmp_path)

        _evaluate(logged_flow(x=2, y=3, log_path=log_path), registry=registry)
        _evaluate(logged_flow(x=2, y=3, log_path=log_path), registry=registry)

        # Task should have run exactly once despite two evaluate() calls.
        lines = Path(log_path).read_text().splitlines()
        assert lines.count("run:2+3") == 1

    @pixi_required
    def test_lock_hash_change_invalidates_cache(self, tmp_path: Path) -> None:
        """Touching pixi.lock forces the task to re-run on the next evaluate()."""
        log_path = str(tmp_path / "log.txt")
        registry = _make_registry(tmp_path)
        lock_path = _TESTS_DIR / "envs" / _TEST_ENV_NAME / "pixi.lock"

        _evaluate(logged_flow(x=2, y=3, log_path=log_path), registry=registry)

        # Invalidate the lock hash by appending a harmless comment.
        original = lock_path.read_text(encoding="utf-8")
        try:
            lock_path.write_text(original + "\n# test-invalidation\n", encoding="utf-8")
            # Flush the registry's in-memory hash cache.
            registry2 = _make_registry(tmp_path)
            _evaluate(logged_flow(x=2, y=3, log_path=log_path), registry=registry2)
        finally:
            lock_path.write_text(original, encoding="utf-8")

        lines = Path(log_path).read_text().splitlines()
        assert lines.count("run:2+3") == 2
