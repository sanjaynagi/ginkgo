"""Pixi environment backend tests.

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
from ginkgo.envs.pixi import (
    PixiEnvImportError,
    PixiEnvNotFoundError,
    PixiEnvPrepareError,
    PixiRegistry,
    resolve_shared_env_root,
    _env_manifest,
    _is_pixi_pyproject,
    _list_envs,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TESTS_DIR = Path(__file__).resolve().parents[1]
_TEST_ENV_NAME = "test_env"


def _make_registry() -> PixiRegistry:
    """Return a PixiRegistry pointing at the real test envs directory."""
    return PixiRegistry(project_root=_TESTS_DIR)


def _evaluate(expr, *, registry: PixiRegistry):
    """Evaluate an expression without importing the evaluator at module import time."""
    from ginkgo import evaluate
    from ginkgo.runtime.backend import LocalEnvironment

    return evaluate(expr, backend=LocalEnvironment(pixi_registry=registry))


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
        assert len(digest) == 64  # BLAKE3 hex digest

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

    def test_exec_argv_structure(self) -> None:
        registry = PixiRegistry(project_root=_TESTS_DIR)
        argv = registry.exec_argv(env=_TEST_ENV_NAME, cmd="echo hello")
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
        with pytest.raises(TypeError, match="Foreign environments only support driver tasks"):
            _evaluate(my_flow(), registry=registry)


# ---------------------------------------------------------------------------
# Unit tests — pyproject.toml as a Pixi manifest (no subprocess)
# ---------------------------------------------------------------------------


_PYPROJECT_WITH_PIXI = (
    "[project]\n"
    "name = 'demo'\n"
    "version = '0.1.0'\n"
    "\n"
    "[tool.pixi.workspace]\n"
    "channels = []\n"
    "platforms = []\n"
)

_PYPROJECT_WITHOUT_PIXI = "[project]\nname = 'demo'\nversion = '0.1.0'\n"


def _write_env(envs_root: Path, name: str, *, filename: str, content: str) -> Path:
    """Create ``envs_root/<name>/<filename>`` with *content* and return its path."""
    manifest = envs_root / name / filename
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(content, encoding="utf-8")
    return manifest


class TestPixiPyprojectManifest:
    def test_is_pixi_pyproject_detects_tool_pixi_section(self, tmp_path: Path) -> None:
        manifest = tmp_path / "pyproject.toml"
        manifest.write_text(_PYPROJECT_WITH_PIXI, encoding="utf-8")
        assert _is_pixi_pyproject(manifest) is True

    def test_is_pixi_pyproject_rejects_pyproject_without_tool_pixi(self, tmp_path: Path) -> None:
        manifest = tmp_path / "pyproject.toml"
        manifest.write_text(_PYPROJECT_WITHOUT_PIXI, encoding="utf-8")
        assert _is_pixi_pyproject(manifest) is False

    def test_is_pixi_pyproject_rejects_non_pyproject_filename(self, tmp_path: Path) -> None:
        manifest = tmp_path / "pixi.toml"
        manifest.write_text("[tool.pixi.workspace]\n", encoding="utf-8")
        assert _is_pixi_pyproject(manifest) is False

    def test_is_pixi_pyproject_rejects_malformed_toml(self, tmp_path: Path) -> None:
        manifest = tmp_path / "pyproject.toml"
        manifest.write_text("[tool.pixi\nbroken = ", encoding="utf-8")
        assert _is_pixi_pyproject(manifest) is False

    def test_resolve_named_env_with_pyproject_manifest(self, tmp_path: Path) -> None:
        manifest = _write_env(
            tmp_path / "envs", "ml_env", filename="pyproject.toml", content=_PYPROJECT_WITH_PIXI
        )
        registry = PixiRegistry(project_root=tmp_path)
        assert registry.resolve(env="ml_env") == manifest.resolve()

    def test_resolve_prefers_pixi_toml_over_pyproject(self, tmp_path: Path) -> None:
        pixi_toml = _write_env(
            tmp_path / "envs",
            "both",
            filename="pixi.toml",
            content="[workspace]\nname = 'both'\nchannels = []\nplatforms = []\n",
        )
        _write_env(
            tmp_path / "envs", "both", filename="pyproject.toml", content=_PYPROJECT_WITH_PIXI
        )
        registry = PixiRegistry(project_root=tmp_path)
        assert registry.resolve(env="both") == pixi_toml.resolve()

    def test_resolve_named_env_ignores_pyproject_without_tool_pixi(self, tmp_path: Path) -> None:
        _write_env(
            tmp_path / "envs", "plain", filename="pyproject.toml", content=_PYPROJECT_WITHOUT_PIXI
        )
        registry = PixiRegistry(project_root=tmp_path)
        with pytest.raises(PixiEnvNotFoundError, match="plain"):
            registry.resolve(env="plain")

    def test_lock_hash_reads_lockfile_beside_pyproject(self, tmp_path: Path) -> None:
        _write_env(
            tmp_path / "envs", "ml_env", filename="pyproject.toml", content=_PYPROJECT_WITH_PIXI
        )
        (tmp_path / "envs" / "ml_env" / "pixi.lock").write_text("locked\n", encoding="utf-8")
        registry = PixiRegistry(project_root=tmp_path)
        digest = registry.lock_hash(env="ml_env")
        assert isinstance(digest, str)
        assert len(digest) == 64  # BLAKE3 hex digest

    def test_list_envs_discovers_pyproject_and_pixi_toml(self, tmp_path: Path) -> None:
        envs_root = tmp_path / "envs"
        _write_env(
            envs_root,
            "alpha",
            filename="pixi.toml",
            content="[workspace]\nname = 'alpha'\nchannels = []\nplatforms = []\n",
        )
        _write_env(envs_root, "beta", filename="pyproject.toml", content=_PYPROJECT_WITH_PIXI)
        _write_env(envs_root, "gamma", filename="pyproject.toml", content=_PYPROJECT_WITHOUT_PIXI)
        assert _list_envs(envs_root) == ["alpha", "beta"]

    def test_env_manifest_returns_none_for_empty_directory(self, tmp_path: Path) -> None:
        env_dir = tmp_path / "envs" / "empty"
        env_dir.mkdir(parents=True)
        assert _env_manifest(env_dir) is None


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
        registry = _make_registry()
        result = _evaluate(shell_touch(output_path=output), registry=registry)
        assert result == output
        assert Path(output).read_text().strip() == "pixi_ran"

    @pixi_required
    def test_shell_task_cached_on_rerun(self, tmp_path: Path) -> None:
        """Second evaluate() with unchanged inputs returns from cache (no pixi invocation)."""
        from ginkgo.runtime.evaluator import ConcurrentEvaluator
        from ginkgo.runtime.backend import LocalEnvironment
        from ginkgo.runtime.events import EventBus, TaskCacheHit

        output = str(tmp_path / "sentinel.txt")
        registry = _make_registry()

        # Run 1 — shell command executes, result cached.
        _evaluate(shell_touch(output_path=output), registry=registry)
        assert Path(output).exists()

        # Subscribe to runtime events on run 2 to confirm the task was served from cache.
        cache_events: list[TaskCacheHit] = []
        bus = EventBus()
        bus.subscribe(
            lambda event: cache_events.append(event) if isinstance(event, TaskCacheHit) else None
        )

        evaluator = ConcurrentEvaluator(
            backend=LocalEnvironment(pixi_registry=registry),
            event_bus=bus,
        )
        evaluator.evaluate(shell_touch(output_path=output))
        assert len(cache_events) == 1

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
        registry = _make_registry()

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
        registry = _make_registry()

        with pytest.raises(TypeError, match="Foreign environments only support driver tasks"):
            _evaluate(module.needs_foreign_env(x=1), registry=registry)


# ---------------------------------------------------------------------------
# Shared environment prefix
# ---------------------------------------------------------------------------


def _write_shared_env(dir_path: Path, *, deps: str = "", lock: str | None = None) -> Path:
    """Write a minimal pixi env under ``dir_path/envs/demo`` and return its root."""
    env_dir = dir_path / "envs" / "demo"
    env_dir.mkdir(parents=True)
    (env_dir / "pixi.toml").write_text(
        '[workspace]\nname = "demo"\nchannels = ["conda-forge"]\n'
        'platforms = ["linux-64"]\n\n[dependencies]\n' + deps,
        encoding="utf-8",
    )
    if lock is not None:
        (env_dir / "pixi.lock").write_text(lock, encoding="utf-8")
    return dir_path


class TestSharedEnvPrefix:
    """Content-keyed sharing of installed environments across workflows."""

    def test_without_prefix_installs_beside_the_manifest(self, tmp_path):
        project = _write_shared_env(tmp_path / "a")
        registry = PixiRegistry(project_root=project)

        assert registry.install_manifest(env="demo") == project / "envs" / "demo" / "pixi.toml"

    def test_identical_envs_in_two_projects_share_one_directory(self, tmp_path):
        shared = tmp_path / "shared"
        first = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "a", deps='python = "3.13.*"\n'),
            shared_env_root=shared,
        )
        second = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "b", deps='python = "3.13.*"\n'),
            shared_env_root=shared,
        )

        first_manifest = first.install_manifest(env="demo")
        second_manifest = second.install_manifest(env="demo")

        assert first_manifest == second_manifest
        assert first_manifest.is_relative_to(shared)
        assert [path.name for path in shared.iterdir()] == [first_manifest.parent.name]

    def test_differing_envs_do_not_share(self, tmp_path):
        shared = tmp_path / "shared"
        first = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "a", deps='python = "3.13.*"\n'),
            shared_env_root=shared,
        )
        second = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "b", deps='python = "3.12.*"\n'),
            shared_env_root=shared,
        )

        assert first.install_manifest(env="demo") != second.install_manifest(env="demo")
        assert len(list(shared.iterdir())) == 2

    def test_lock_file_joins_the_key_and_is_copied(self, tmp_path):
        shared = tmp_path / "shared"
        locked = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "a", lock="version: 6\n"),
            shared_env_root=shared,
        )
        unlocked = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "b"),
            shared_env_root=shared,
        )

        locked_manifest = locked.install_manifest(env="demo")

        # Same manifest, different lock state, so they must not collide.
        assert locked_manifest != unlocked.install_manifest(env="demo")
        assert (locked_manifest.parent / "pixi.lock").read_text() == "version: 6\n"

    def test_path_dependency_stays_local(self, tmp_path):
        project = _write_shared_env(tmp_path / "a", deps='local = { path = "../pkg" }\n')
        registry = PixiRegistry(project_root=project, shared_env_root=tmp_path / "shared")

        # Relocating would break the relative path, so it must not be shared.
        assert registry.install_manifest(env="demo") == project / "envs" / "demo" / "pixi.toml"

    def test_editable_dependency_stays_local(self, tmp_path):
        project = _write_shared_env(tmp_path / "a")
        (project / "envs" / "demo" / "pixi.toml").write_text(
            '[workspace]\nname = "demo"\nchannels = ["conda-forge"]\n'
            'platforms = ["linux-64"]\n\n[pypi-dependencies]\n'
            'mine = { path = "..", editable = true }\n',
            encoding="utf-8",
        )
        registry = PixiRegistry(project_root=project, shared_env_root=tmp_path / "shared")

        assert registry.install_manifest(env="demo") == project / "envs" / "demo" / "pixi.toml"

    def test_feature_table_path_dependency_stays_local(self, tmp_path):
        project = _write_shared_env(tmp_path / "a")
        (project / "envs" / "demo" / "pixi.toml").write_text(
            '[workspace]\nname = "demo"\nchannels = ["conda-forge"]\n'
            'platforms = ["linux-64"]\n\n[feature.dev.pypi-dependencies]\n'
            'helper = { path = "../helper" }\n',
            encoding="utf-8",
        )
        registry = PixiRegistry(project_root=project, shared_env_root=tmp_path / "shared")

        assert registry.install_manifest(env="demo") == project / "envs" / "demo" / "pixi.toml"

    def test_exec_argv_targets_the_shared_manifest(self, tmp_path):
        shared = tmp_path / "shared"
        registry = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "a"),
            shared_env_root=shared,
        )

        argv = registry.exec_argv(env="demo", cmd="echo hi")

        assert str(registry.install_manifest(env="demo")) in argv
        assert not any(str(tmp_path / "a") in part for part in argv)

    def test_existing_shared_copy_is_reused_not_rewritten(self, tmp_path):
        shared = tmp_path / "shared"
        registry = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "a"),
            shared_env_root=shared,
        )

        first = registry.install_manifest(env="demo")
        marker = first.parent / "installed-marker"
        marker.write_text("x", encoding="utf-8")

        # A second registry must adopt the existing directory untouched.
        again = PixiRegistry(
            project_root=_write_shared_env(tmp_path / "b"),
            shared_env_root=shared,
        ).install_manifest(env="demo")

        assert again == first
        assert marker.is_file()
        assert not any(child.name.startswith(".staging-") for child in shared.iterdir())


class TestResolveSharedEnvRoot:
    """Precedence between the CLI flag and ``[envs] shared_prefix``."""

    def test_returns_none_when_unset(self):
        assert resolve_shared_env_root(cli_value=None, config={}) is None

    def test_reads_the_config_table(self, tmp_path):
        config = {"envs": {"shared_prefix": str(tmp_path / "envs")}}

        assert resolve_shared_env_root(cli_value=None, config=config) == tmp_path / "envs"

    def test_cli_value_wins_over_config(self, tmp_path):
        config = {"envs": {"shared_prefix": str(tmp_path / "from-config")}}

        resolved = resolve_shared_env_root(cli_value=str(tmp_path / "from-cli"), config=config)

        assert resolved == tmp_path / "from-cli"

    def test_expands_user_home(self):
        resolved = resolve_shared_env_root(cli_value="~/ginkgo-envs", config={})

        assert resolved == Path.home() / "ginkgo-envs"

    def test_ignores_a_non_string_prefix(self):
        assert (
            resolve_shared_env_root(cli_value=None, config={"envs": {"shared_prefix": 7}}) is None
        )
