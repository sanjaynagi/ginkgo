"""A remote shell task in a Pixi env, which the worker installs from the code bundle.

The worker is the in-process round trip of ``tests/remote_fakes.py``, and pixi
is real: the env has no packages, so it installs without the network.
"""

from __future__ import annotations

import shutil
import sys
import tarfile
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from ginkgo import Out, file, shell, task
from ginkgo_pixi.environment import PixiRegistry
from ginkgo.plugins.api import EnvironmentContext
from ginkgo.runtime.backend import EnvironmentRouter
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.rundir import RunDir
from tests.remote_fakes import (
    FakeObjectStore,
    RemoteRoundTrip,
    RoundTripExecutor,
    set_up_round_trip,
)

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


@task()
def upstream() -> str:
    return "token"


@task(kind="shell", env="tools")
def downstream(token: str, dest: Out[file]):
    return shell(cmd=f'echo "$CONDA_PREFIX" > {dest}')


@pytest.fixture
def remote_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RemoteRoundTrip:
    monkeypatch.chdir(tmp_path)
    return set_up_round_trip(root=tmp_path, monkeypatch=monkeypatch)


def _declare_tools_env(envs_dir: Path, *, extra: str = "") -> None:
    """Write the ``tools`` env's manifest under *envs_dir*."""
    (envs_dir / "tools").mkdir(parents=True)
    (envs_dir / "tools" / "pixi.toml").write_text(TOOLS_ENV + extra, encoding="utf-8")


def _declare_package(root: Path) -> Path:
    """Write a ``wf`` package under *root* with its ``tools`` env, not yet locked."""
    package = root / "wf"
    _declare_tools_env(package / "envs")
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "flow.py").write_text("", encoding="utf-8")
    return package


def _bundled(store: FakeObjectStore, member: str) -> list[bytes]:
    """Return *member* from each code bundle published to *store* that has it."""
    found = []
    for bundle in sorted(store.root.rglob("code-bundles/*.tar.gz")):
        with tarfile.open(bundle) as tar:
            if member in tar.getnames():
                found.append(tar.extractfile(member).read())
    return found


def _assert_ran_in_the_bundles_lock(
    *, package: Path, dest: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    """Assert the command ran in the pod's install of the lock the driver wrote.

    The bundle carries the driver's ``pixi.lock``, the pod's copy is still that
    lock, and the command's ``CONDA_PREFIX`` is the pod's install, not the
    driver's, which the bundle leaves out.
    """
    driver_lock = (package / "envs/tools/pixi.lock").read_bytes()
    assert driver_lock in _bundled(remote_round_trip.store, "wf/envs/tools/pixi.lock")
    pod_env = remote_round_trip.pod_scratch / "ginkgo-code-bundle/wf/envs/tools"
    assert (pod_env / "pixi.lock").read_bytes() == driver_lock
    prefix = Path(dest.read_text(encoding="utf-8").strip())
    assert prefix.resolve() == (pod_env / ".pixi/envs/default").resolve()


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
    package = _declare_package(tmp_path)
    dest = tmp_path / "prefix.txt"

    _evaluate(
        in_a_pixi_env(dest=str(dest)),
        context=EnvironmentContext(project_root=tmp_path, workflow_root=package, config={}),
        code={"mode": "sync", "package": "wf"},
    )

    _assert_ran_in_the_bundles_lock(
        package=package, dest=dest, remote_round_trip=remote_round_trip
    )


def test_the_worker_finds_the_env_of_a_package_nested_in_the_project(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    # The bundle holds src/, the package's parent, rather than the project.
    package = _declare_package(tmp_path / "src")
    dest = tmp_path / "prefix.txt"

    _evaluate(
        in_a_pixi_env(dest=str(dest)),
        context=EnvironmentContext(project_root=tmp_path, workflow_root=package, config={}),
        code={"mode": "sync", "package": "src/wf"},
    )

    _assert_ran_in_the_bundles_lock(
        package=package, dest=dest, remote_round_trip=remote_round_trip
    )


def test_a_task_locked_after_the_bundle_was_first_built_gets_its_lock(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    # upstream, with no env, is dispatched first, before the driver prepares
    # downstream's env and so writes its lock.
    package = _declare_package(tmp_path)
    dest = tmp_path / "prefix.txt"

    _evaluate(
        downstream(token=upstream(), dest=str(dest)),
        context=EnvironmentContext(project_root=tmp_path, workflow_root=package, config={}),
        code={"mode": "sync", "package": "wf"},
    )

    _assert_ran_in_the_bundles_lock(
        package=package, dest=dest, remote_round_trip=remote_round_trip
    )


def test_the_worker_finds_an_env_where_the_driver_did(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip, monkeypatch: pytest.MonkeyPatch
) -> None:
    # With no flow.py, the driver searches the entry file's directory, wfx/,
    # while the task lives a level down, in wfx/tasks/.
    package = tmp_path / "wfx"
    _declare_tools_env(package / "envs")
    (package / "tasks").mkdir()
    for init in (package / "__init__.py", package / "tasks" / "__init__.py"):
        init.write_text("", encoding="utf-8")
    (package / "main.py").write_text("", encoding="utf-8")
    (package / "tasks" / "t.py").write_text(
        "from ginkgo import Out, file, shell, task\n\n\n"
        '@task(kind="shell", env="tools")\n'
        "def in_a_pixi_env(dest: Out[file]):\n"
        "    return shell(cmd=f'echo \"$CONDA_PREFIX\" > {dest}')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "path", list(sys.path))
    dest = tmp_path / "prefix.txt"

    try:
        tasks = load_module_from_path(package / "tasks" / "t.py")
        _evaluate(
            tasks.in_a_pixi_env(dest=str(dest)),
            context=EnvironmentContext(project_root=tmp_path, workflow_root=package, config={}),
            code={"mode": "sync", "package": "wfx"},
        )
    finally:
        for name in [name for name in sys.modules if name.split(".")[0] == "wfx"]:
            del sys.modules[name]

    pod_env = remote_round_trip.pod_scratch / "ginkgo-code-bundle/wfx/envs/tools"
    prefix = Path(dest.read_text(encoding="utf-8").strip())
    assert prefix.resolve() == (pod_env / ".pixi/envs/default").resolve()


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


def test_a_worker_lock_unlike_the_drivers_fails_the_task(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    # The image installed the same manifest at build time, but its lock has
    # since diverged from the driver's, as a local ``pixi update`` would make
    # it. A comment stands in for a different solve: pixi still accepts it.
    _declare_tools_env(tmp_path / "driver" / "envs")
    _declare_tools_env(tmp_path / "image" / "envs")
    PixiRegistry(project_root=tmp_path / "image").prepare(env="tools")
    with (tmp_path / "image/envs/tools/pixi.lock").open("a", encoding="utf-8") as fh:
        fh.write("# solved before the driver's\n")

    with pytest.raises(RuntimeError, match="worker's env 'tools' is not locked as the driver"):
        _evaluate(
            in_a_pixi_env(dest=str(tmp_path / "prefix.txt")),
            context=EnvironmentContext(
                project_root=tmp_path / "driver", workflow_root=None, config={}
            ),
            code=None,
            image=tmp_path / "image",
        )
