"""A remote shell task in a Pixi env, which the worker installs from the code bundle.

The worker is the in-process round trip of ``tests/remote_fakes.py``, and pixi
is real: the env has no packages, so it installs without the network.
"""

from __future__ import annotations

import shutil
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from ginkgo import Out, file, shell, task
from ginkgo.plugins.api import EnvironmentContext
from ginkgo.runtime.backend import EnvironmentRouter
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.rundir import RunDir
from tests.remote_fakes import RemoteRoundTrip, RoundTripExecutor, set_up_round_trip

pytestmark = pytest.mark.skipif(shutil.which("pixi") is None, reason="pixi not found on PATH")

TOOLS_ENV = """\
[workspace]
name = "tools"
channels = []
platforms = ["linux-64", "linux-aarch64", "osx-64", "osx-arm64"]
"""


@task(kind="shell", env="tools")
def in_a_pixi_env(dest: Out[file]):
    return shell(cmd=f'echo "$CONDA_PREFIX" > {dest}')


@pytest.fixture
def remote_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RemoteRoundTrip:
    monkeypatch.chdir(tmp_path)
    return set_up_round_trip(root=tmp_path, monkeypatch=monkeypatch)


def _declare_tools_env(envs_dir: Path, *, extra: str = "") -> None:
    """Write the ``tools`` env's manifest under *envs_dir*."""
    (envs_dir / "tools").mkdir(parents=True)
    (envs_dir / "tools" / "pixi.toml").write_text(TOOLS_ENV + extra, encoding="utf-8")


def _evaluate(
    expr: object, *, context: EnvironmentContext, code: dict | None, image: Path | None = None
) -> object:
    """Evaluate *expr* remotely, with the driver's envs found from *context*.

    With *code*, the executor ships the workflow package in a code bundle, as
    ``[remote.executors.<name>.code]`` does. *image* is the worker image's
    working directory.
    """
    registry = ExecutorRegistry.for_executor(RoundTripExecutor(workdir=image), name="remote")
    registry.specs["remote"] = replace(registry.specs["remote"], code=code)
    return ConcurrentEvaluator(
        jobs=1,
        executor_registry=registry,
        backend=EnvironmentRouter(context=context),
        run_dir=RunDir.create(run_id=uuid.uuid4().hex, root=Path.cwd() / "runs"),
    ).evaluate(expr)


def test_the_command_runs_in_the_env_the_code_bundle_carries(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    package = tmp_path / "wf"
    _declare_tools_env(package / "envs")
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "flow.py").write_text("", encoding="utf-8")
    dest = tmp_path / "prefix.txt"

    _evaluate(
        in_a_pixi_env(dest=str(dest)),
        context=EnvironmentContext(project_root=tmp_path, workflow_root=package, config={}),
        code={"mode": "sync", "package": "wf"},
    )

    # The worker installed the env from the bundle's copy of the manifest and
    # lock, not the driver's install, which the bundle leaves out.
    pod_env = remote_round_trip.pod_scratch / "ginkgo-code-bundle/wf/envs/tools/.pixi/envs/default"
    prefix = Path(dest.read_text(encoding="utf-8").strip())
    assert prefix.resolve() == pod_env.resolve()


def test_a_worker_env_unlike_the_drivers_fails_the_task(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    # The driver finds ``tools`` under driver/; the worker, with no code
    # bundle, under its image's working directory, where the manifest differs.
    _declare_tools_env(tmp_path / "driver" / "envs")
    _declare_tools_env(tmp_path / "image" / "envs", extra="description = 'an older copy'\n")

    with pytest.raises(RuntimeError, match="worker's env 'tools' is not the one the driver"):
        _evaluate(
            in_a_pixi_env(dest=str(tmp_path / "prefix.txt")),
            context=EnvironmentContext(
                project_root=tmp_path / "driver", workflow_root=None, config={}
            ),
            code=None,
            image=tmp_path / "image",
        )
