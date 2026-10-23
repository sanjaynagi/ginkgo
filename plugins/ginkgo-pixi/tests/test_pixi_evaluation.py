"""Pixi environments under the evaluator: caching across runs and one install per env."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from ginkgo import Out, evaluate, file, shell, task
from ginkgo.core.shell import shell_argv
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo_pixi.environment import PixiEnvironment, PixiRegistry
from tests.conftest import EventCollector


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The evaluator's cache lives under the working directory.
    monkeypatch.chdir(tmp_path)


@task(kind="shell", env="test_env")
def write_payload(output_path: Out[file]) -> file:
    return shell(cmd=f"printf 'payload' > {output_path}", output=output_path)


def _declare_env(root: Path, name: str = "test_env") -> Path:
    """Write ``envs/<name>/pixi.toml`` with no lock file beside it."""
    manifest = root / "envs" / name / "pixi.toml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        f"[workspace]\nname = '{name}'\nchannels = []\nplatforms = []\n", encoding="utf-8"
    )
    return manifest


def _bash(registry: PixiRegistry, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run commands under plain bash instead of ``pixi run``."""
    monkeypatch.setattr(
        registry, "exec_argv", lambda *, env, cmd: shell_argv(shell="bash", cmd=cmd)
    )


class TestEnvBackedCaching:
    """Declaring ``env=`` must not cost a task its cache hit on the next run."""

    def _run(self, *, tmp_path: Path, manifest: Path, monkeypatch) -> EventCollector:
        """Evaluate the task the way a fresh process would, and report its events.

        Each call builds its own registry, so no in-memory environment state
        carries over between runs, while all of them share the cwd-scoped
        on-disk cache. ``prepare`` stands in for ``pixi install``: the side
        effect that matters here is writing the lock file next to the manifest.
        """
        registry = PixiRegistry(project_root=tmp_path)

        def fake_prepare(*, env: str) -> Path:
            lock_path = manifest.parent / "pixi.lock"
            if not lock_path.is_file():
                lock_path.write_text("version: 6\nsolved: first\n", encoding="utf-8")
            return manifest

        monkeypatch.setattr(registry, "prepare", fake_prepare)
        _bash(registry, monkeypatch)
        collector = EventCollector()
        output = tmp_path / "out.txt"
        result = ConcurrentEvaluator(
            jobs=1,
            cores=1,
            backend=PixiEnvironment(pixi_registry=registry),
            event_bus=collector.bus,
        ).evaluate(write_payload(output_path=str(output)))
        assert result == file(str(output))
        return collector

    def test_pixi_env_task_is_cached_on_the_second_run(self, tmp_path: Path, monkeypatch) -> None:
        """Regression test for issue #194.

        The cache key is built before the environment is materialised, so an
        identity only knowable once the environment is installed made every
        env-backed task re-run once, on the second run of an unchanged workflow.
        """
        manifest = _declare_env(tmp_path)

        first = self._run(tmp_path=tmp_path, manifest=manifest, monkeypatch=monkeypatch)
        assert first.cached() == []
        assert (manifest.parent / "pixi.lock").is_file()

        second = self._run(tmp_path=tmp_path, manifest=manifest, monkeypatch=monkeypatch)
        assert second.cached() != []
        assert second.started() == []

    def test_pixi_env_task_reruns_when_the_installed_environment_drifts(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A re-solved lock file (``pixi update``) invalidates the entry, though the key is the same."""
        manifest = _declare_env(tmp_path)
        self._run(tmp_path=tmp_path, manifest=manifest, monkeypatch=monkeypatch)

        (manifest.parent / "pixi.lock").write_text("version: 6\nsolved: again\n", encoding="utf-8")

        rerun = self._run(tmp_path=tmp_path, manifest=manifest, monkeypatch=monkeypatch)
        assert rerun.cached() == []
        assert rerun.started() != []

    def test_pixi_env_task_is_cached_when_the_environment_is_not_installed_here(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """With no local environment there is no evidence of drift, so entries stand.

        This is a shared cache on a machine that never installed the environment;
        serving the hit must not install it.
        """
        manifest = _declare_env(tmp_path)
        self._run(tmp_path=tmp_path, manifest=manifest, monkeypatch=monkeypatch)
        (manifest.parent / "pixi.lock").unlink()

        second = self._run(tmp_path=tmp_path, manifest=manifest, monkeypatch=monkeypatch)
        assert second.cached() != []
        assert second.started() == []
        assert not (manifest.parent / "pixi.lock").exists()


@task(env="race_env", kind="shell")
def pixi_shell_output_task(output_path: str) -> file:
    return shell(cmd=f"printf 'payload' > {output_path}", output=output_path)


def test_pixi_environment_is_prepared_once_before_shell_fan_out(tmp_path: Path, monkeypatch):
    manifest = _declare_env(tmp_path, "race_env")
    install_calls: list[list[str]] = []
    real_run = subprocess.run

    def fake_pixi_install(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[:2] != ["pixi", "install"]:
            return real_run(argv, check=False, text=True, capture_output=True)
        install_calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    registry = PixiRegistry(project_root=tmp_path)
    monkeypatch.setattr("ginkgo_pixi.environment._require_pixi", lambda: None)
    monkeypatch.setattr("ginkgo_pixi.environment.subprocess.run", fake_pixi_install)
    _bash(registry, monkeypatch)

    outputs = [tmp_path / f"pixi-shell-{index}.txt" for index in range(3)]
    result = evaluate(
        [pixi_shell_output_task(output_path=str(path)) for path in outputs],
        backend=PixiEnvironment(pixi_registry=registry),
    )

    assert result == [file(str(path)) for path in outputs]
    assert install_calls == [["pixi", "install", "--manifest-path", str(manifest.resolve())]]
    assert all(path.read_text(encoding="utf-8") == "payload" for path in outputs)
