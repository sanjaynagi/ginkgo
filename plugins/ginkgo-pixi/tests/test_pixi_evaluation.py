"""Pixi environments under the evaluator: caching across runs and one install per env."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from ginkgo import Out, evaluate, file, shell, task
from ginkgo.core.shell import shell_argv
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.events import EventBus, GinkgoEvent, TaskCacheHit, TaskCompleted, TaskStarted
from ginkgo_pixi.environment import PixiEnvironment, PixiRegistry


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The evaluator's cache lives under the working directory.
    monkeypatch.chdir(tmp_path)


@dataclass
class _Events:
    """The events one evaluation published."""

    bus: EventBus = field(default_factory=EventBus)
    events: list[GinkgoEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.bus.subscribe(self.events.append)

    def started(self) -> list[GinkgoEvent]:
        return [event for event in self.events if isinstance(event, TaskStarted)]

    def cached(self) -> list[GinkgoEvent]:
        return [
            event
            for event in self.events
            if isinstance(event, TaskCacheHit)
            or (isinstance(event, TaskCompleted) and event.status == "cached")
        ]


@task(kind="shell", env="test_env")
def shell_env_write_output_task(output_path: Out[file]) -> file:
    return shell(cmd=f"printf 'payload' > {output_path}", output=output_path)


_PIXI_TEST_ENV = "race_env"


@task(env=_PIXI_TEST_ENV, kind="shell")
def pixi_shell_output_task(output_path: str) -> file:
    return shell(
        cmd=f"printf 'payload' > {output_path}",
        output=output_path,
    )


class TestEnvBackedCaching:
    """Declaring ``env=`` must not cost a task its cache hit on the next run."""

    def _declare_env(self, *, project_root: Path) -> Path:
        """Write an ``envs/test_env/pixi.toml`` with no lock file beside it."""
        env_dir = project_root / "envs" / "test_env"
        env_dir.mkdir(parents=True)
        manifest = env_dir / "pixi.toml"
        manifest.write_text(
            "[workspace]\nname = 'test-env'\nchannels = []\nplatforms = []\n", encoding="utf-8"
        )
        return manifest

    def _run(
        self,
        *,
        expr: Any,
        project_root: Path,
        manifest: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> tuple[Any, _Events]:
        """Evaluate *expr* the way a fresh process would, and report its events.

        Each call builds its own registry, so no in-memory environment state
        carries over between runs, while all of them share the cwd-scoped
        on-disk cache. ``prepare`` stands in for ``pixi install``: the side
        effect that matters here is writing the lock file next to the manifest.
        """
        registry = PixiRegistry(project_root=project_root)

        def fake_prepare(*, env: str) -> Path:
            lock_path = manifest.parent / "pixi.lock"
            if not lock_path.is_file():
                lock_path.write_text("version: 6\nsolved: first\n", encoding="utf-8")
            return manifest

        monkeypatch.setattr(registry, "prepare", fake_prepare)
        monkeypatch.setattr(
            registry, "exec_argv", lambda *, env, cmd: shell_argv(shell="bash", cmd=cmd)
        )

        collector = _Events()
        evaluator = ConcurrentEvaluator(
            jobs=1,
            cores=1,
            backend=PixiEnvironment(pixi_registry=registry),
            event_bus=collector.bus,
        )
        return evaluator.evaluate(expr), collector

    def test_pixi_env_task_is_cached_on_the_second_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test for issue #194.

        The cache key is built before the environment is materialised, so an
        environment identity that only becomes knowable once the environment is
        installed differs between the run that installed it and every run after
        it — and every env-backed task re-ran once, on the second run of an
        otherwise unchanged workflow.
        """
        manifest = self._declare_env(project_root=tmp_path)
        output_path = tmp_path / "out.txt"
        expr = shell_env_write_output_task(output_path=str(output_path))

        result, first = self._run(
            expr=expr, project_root=tmp_path, manifest=manifest, monkeypatch=monkeypatch
        )

        assert result == file(str(output_path))
        assert first.cached() == []
        # The first run materialised the environment, which is what wrote the
        # lock file the identity used to be taken from.
        assert (manifest.parent / "pixi.lock").is_file()

        result, second = self._run(
            expr=expr, project_root=tmp_path, manifest=manifest, monkeypatch=monkeypatch
        )

        assert result == file(str(output_path))
        assert second.cached() != []
        assert second.started() == []

    def test_pixi_env_task_reruns_when_the_installed_environment_drifts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A re-solved lock file invalidates the entry, though the key is the same.

        Cache keys name the declared environment, so ``pixi update`` — which
        re-solves every dependency while leaving ``pixi.toml`` untouched — does
        not move the key. The entry records the digest of the environment as
        installed when it was written, and that is what catches the drift.
        """
        manifest = self._declare_env(project_root=tmp_path)
        output_path = tmp_path / "out.txt"
        expr = shell_env_write_output_task(output_path=str(output_path))

        self._run(expr=expr, project_root=tmp_path, manifest=manifest, monkeypatch=monkeypatch)

        # `pixi update`: dependencies re-solved, declaration unchanged.
        (manifest.parent / "pixi.lock").write_text("version: 6\nsolved: again\n", encoding="utf-8")
        before = manifest.read_text(encoding="utf-8")

        result, rerun = self._run(
            expr=expr, project_root=tmp_path, manifest=manifest, monkeypatch=monkeypatch
        )

        assert manifest.read_text(encoding="utf-8") == before
        assert result == file(str(output_path))
        assert rerun.cached() == []
        assert rerun.started() != []

    def test_pixi_env_task_is_cached_when_the_environment_is_not_installed_here(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With no local environment there is no evidence of drift, so entries stand.

        This is the case a shared cache lands in on a machine that has never
        installed the environment. Establishing evidence would mean installing
        it to serve a cache hit, which is the cost the deferred materialisation
        exists to avoid.
        """
        manifest = self._declare_env(project_root=tmp_path)
        output_path = tmp_path / "out.txt"
        expr = shell_env_write_output_task(output_path=str(output_path))

        self._run(expr=expr, project_root=tmp_path, manifest=manifest, monkeypatch=monkeypatch)

        (manifest.parent / "pixi.lock").unlink()

        result, second = self._run(
            expr=expr, project_root=tmp_path, manifest=manifest, monkeypatch=monkeypatch
        )

        assert result == file(str(output_path))
        assert second.cached() != []
        assert second.started() == []
        # Serving the hit did not install anything.
        assert not (manifest.parent / "pixi.lock").exists()


class TestPixiFanOut:
    def test_pixi_environment_is_prepared_once_before_shell_fan_out(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        env_dir = tmp_path / "envs" / _PIXI_TEST_ENV
        env_dir.mkdir(parents=True)
        manifest = env_dir / "pixi.toml"
        manifest.write_text(
            "[workspace]\nname = 'race-env'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )

        install_calls: list[list[str]] = []
        real_subprocess_run = subprocess.run

        def fake_pixi_install(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            if argv[:2] != ["pixi", "install"]:
                return real_subprocess_run(
                    argv, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
            install_calls.append(argv)
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        registry = PixiRegistry(project_root=tmp_path)
        monkeypatch.setattr("ginkgo_pixi.environment._require_pixi", lambda: None)
        monkeypatch.setattr("ginkgo_pixi.environment.subprocess.run", fake_pixi_install)
        monkeypatch.setattr(
            registry,
            "exec_argv",
            lambda *, env, cmd: shell_argv(shell="bash", cmd=cmd),
        )

        outputs = [tmp_path / f"pixi-shell-{index}.txt" for index in range(3)]
        result = evaluate(
            [pixi_shell_output_task(output_path=str(path)) for path in outputs],
            backend=PixiEnvironment(pixi_registry=registry),
        )

        assert result == [file(str(path)) for path in outputs]
        assert install_calls == [["pixi", "install", "--manifest-path", str(manifest.resolve())]]
        for path in outputs:
            assert path.read_text(encoding="utf-8") == "payload"
