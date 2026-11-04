"""Tests for remote shell and script tasks whose command runs in their own image.

:class:`ContainerRoundTripExecutor` runs such a task's steps in turn: the
worker's stage-in, the rendered ``command.sh`` under this host's ``sh`` in
place of the image, and the worker's stage-out.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from ginkgo import Out, file, script, secret, shell, task
from ginkgo.envs.container import ContainerBackend
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.rundir import RunDir
from ginkgo.workspace_layout import WorkspaceLayout
from ginkgo.runtime.task_runners.shell import ShellTaskError
from tests.conftest import EventCollector, ShellEnvironment
from tests.remote_fakes import ContainerRoundTripExecutor, RemoteRoundTrip, evaluate_remotely

pytestmark = pytest.mark.usefixtures("remote_round_trip")

IMAGE = "docker://busybox:1.36"

UPPER_SCRIPT = """\
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--src")
parser.add_argument("--dest")
args = parser.parse_args()
Path(args.dest).write_text(Path(args.src).read_text().upper())
"""


@task(kind="shell", env=IMAGE, threads=2)
def shout(src: file, dest: Out[file]):
    return shell(
        cmd=f"echo wrote {dest} with $GINKGO_THREADS threads; tr a-z A-Z < {src} > {dest}"
    )


@task(kind="script", env=IMAGE)
def shout_script(src: file, dest: Out[file]):
    return script("wf/upper.py")


@task(kind="shell", env=IMAGE)
def writes(dest: Out[file]):
    return shell(cmd=f"echo ok > {dest}")


@task(kind="shell", env=IMAGE, retries=1, retry_on_exit_codes=(7,))
def fails_with_seven(dest: Out[file]):
    return shell(cmd=f"echo partial > {dest}; exit 7")


@task(kind="shell", env=IMAGE)
def writes_here() -> file:
    return shell(cmd="echo here > here.txt", output="here.txt")


@task(kind="shell", env=IMAGE)
def writes_nothing(dest: Out[file]):
    return shell(cmd="true")


@task(kind="shell", env=IMAGE)
def touches(marker: str, dest: Out[file]):
    return shell(cmd=f"touch {marker}; echo ok > {dest}")


@task(kind="shell", env=IMAGE, retries=1, retry_on_exit_codes=(7,))
def fails_with_three(dest: Out[file]):
    return shell(cmd=f"echo partial > {dest}; exit 3")


@task(kind="shell", env=IMAGE)
def echoes(token: str, dest: Out[file]):
    return shell(cmd=f"echo from-the-command {token}; echo err-line >&2; echo ok > {dest}")


def test_the_command_reads_an_input_and_writes_an_output_in_its_image(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    src = tmp_path / "in.txt"
    src.write_text("hello\n", encoding="utf-8")
    dest = tmp_path / "out" / "shout.txt"
    executor = ContainerRoundTripExecutor()
    collector = EventCollector()

    result = evaluate_remotely(
        shout(src=file(src), dest=str(dest)), event_bus=collector.bus, executor=executor
    )

    assert Path(result) == dest
    assert dest.read_text(encoding="utf-8") == "HELLO\n"
    assert executor.images == ["busybox:1.36"]
    # The command ran against the pod's shared scratch, with the variables
    # the local runner sets.
    [log] = Path.cwd().rglob("*.stdout.log")
    assert f"wrote {remote_round_trip.pod_scratch}" in log.read_text(encoding="utf-8")
    assert "with 2 threads" in log.read_text(encoding="utf-8")
    [started] = collector.started()
    assert started.execution_backend == "remote"
    # The command ran in another container, which the worker cannot sample.
    [completed] = collector.succeeded()
    assert "measured" not in completed.resource_usage


def test_a_script_runs_in_its_image_from_the_code_bundle(tmp_path: Path) -> None:
    (tmp_path / "wf").mkdir()
    (tmp_path / "wf" / "upper.py").write_text(UPPER_SCRIPT, encoding="utf-8")
    src = tmp_path / "in.txt"
    src.write_text("quiet", encoding="utf-8")
    dest = tmp_path / "out" / "loud.txt"
    registry = ExecutorRegistry.for_executor(ContainerRoundTripExecutor(), name="remote")
    registry.specs["remote"] = replace(
        registry.specs["remote"], code={"mode": "sync", "package": "wf"}
    )

    # The script's path is relative to the project, as it would be locally.
    result = ConcurrentEvaluator(jobs=1, executor_registry=registry).evaluate(
        shout_script(src=file(src), dest=str(dest))
    )

    assert Path(result) == dest
    assert dest.read_text(encoding="utf-8") == "QUIET"


def test_a_relative_output_is_written_where_stage_out_checks_it() -> None:
    # Without a code bundle the command works in an empty directory on the
    # scratch the steps share.
    result = evaluate_remotely(writes_here(), executor=ContainerRoundTripExecutor())

    assert Path(result).read_text(encoding="utf-8") == "here\n"


def test_a_matching_exit_code_retries_the_task() -> None:
    executor = ContainerRoundTripExecutor()

    # Each attempt is a job of its own, so the command fails both.
    with pytest.raises(ShellTaskError, match="exit code 7"):
        evaluate_remotely(fails_with_seven(dest="out.txt"), executor=executor)

    assert executor.images == ["busybox:1.36", "busybox:1.36"]


def test_another_exit_code_fails_the_task_without_a_retry(tmp_path: Path) -> None:
    executor = ContainerRoundTripExecutor()
    collector = EventCollector()
    dest = tmp_path / "x"
    dest.write_text("from an earlier run", encoding="utf-8")

    with pytest.raises(ShellTaskError, match="exit code 3") as raised:
        evaluate_remotely(
            fails_with_three(dest=str(dest)), event_bus=collector.bus, executor=executor
        )

    assert raised.value.exit_code == 3
    assert len(executor.images) == 1
    [failed] = collector.failed()
    assert failed.exit_code == 3
    assert failed.failure["kind"] == "command_error"
    # The declared output holds neither an earlier run's file nor the failed
    # command's partial one, as it would locally.
    assert not dest.exists()


def test_an_unwritten_output_is_named_by_its_driver_path(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    dest = tmp_path / "out" / "never.txt"

    # Stage-out finds the output missing, against the pod's scratch path.
    with pytest.raises(FileNotFoundError, match="did not create") as raised:
        evaluate_remotely(writes_nothing(dest=str(dest)), executor=ContainerRoundTripExecutor())

    assert repr(str(dest)) in str(raised.value)
    assert str(remote_round_trip.pod_scratch) not in str(raised.value)


def test_the_commands_output_reaches_the_task_log_redacted(tmp_path: Path) -> None:
    evaluator = ConcurrentEvaluator(
        jobs=1,
        executor_registry=ExecutorRegistry.for_executor(
            ContainerRoundTripExecutor(), name="remote"
        ),
        run_dir=RunDir.create(run_id=uuid.uuid4().hex, root=tmp_path / "runs"),
        secret_resolver=build_secret_resolver(
            project_root=tmp_path, config={}, environ={"API_TOKEN": "super-secret"}
        ),
    )

    evaluator.evaluate(echoes(token=secret("API_TOKEN"), dest=str(tmp_path / "ok.txt")))

    [log] = (tmp_path / "runs").rglob("*.stdout.log")
    text = log.read_text(encoding="utf-8")
    assert "from-the-command [REDACTED]" in text
    assert "err-line" in text
    assert "super-secret" not in text


def test_the_driver_needs_no_container_runtime(tmp_path: Path) -> None:
    dest = tmp_path / "ok.txt"
    evaluator = ConcurrentEvaluator(
        jobs=1,
        executor_registry=ExecutorRegistry.for_executor(
            ContainerRoundTripExecutor(), name="remote"
        ),
        backend=ContainerBackend(runtime="no-such-runtime", project_root=tmp_path),
    )

    evaluator.evaluate(writes(dest=str(dest)))

    assert dest.read_text(encoding="utf-8") == "ok\n"


def test_a_command_naming_a_driver_path_fails_the_fake(tmp_path: Path) -> None:
    # The task's image shares only the job's scratch volume with the worker.
    with pytest.raises(AssertionError, match="not on the scratch the task's image shares"):
        evaluate_remotely(
            touches(marker=str(tmp_path / "marker"), dest=str(tmp_path / "ok.txt")),
            executor=ContainerRoundTripExecutor(),
        )
    assert not (tmp_path / "marker").exists()


@dataclass(kw_only=True)
class _DriverHoldsTheImage(ShellEnvironment):
    """A driver whose own container runtime holds the task's image, as *image_id*."""

    image_id: str

    def materialized_digest(self, *, env: str) -> str | None:
        return self.image_id


def _cached_image_id(cache_key: str) -> str | None:
    """Return the image ID the cache entry for *cache_key* was recorded with."""
    index = CacheIndex.open(path=WorkspaceLayout.for_cwd().db)
    try:
        entry = index.entry(cache_key)
        assert entry is not None
        return entry.env_materialized_digest
    finally:
        index.close()


def _run_writes(dest: Path, *, image_id: str, executor: ContainerRoundTripExecutor | None):
    """Run ``writes`` once, remotely on *executor* or else locally, and return its events."""
    collector = EventCollector()
    registry = ExecutorRegistry() if executor is None else ExecutorRegistry.for_executor(executor)
    ConcurrentEvaluator(
        jobs=1,
        executor_registry=registry,
        backend=_DriverHoldsTheImage(image_id=image_id),
        event_bus=collector.bus,
    ).evaluate(writes(dest=str(dest)))
    return collector


def test_the_drivers_copy_of_a_remote_image_is_not_in_the_cache(tmp_path: Path) -> None:
    executor = ContainerRoundTripExecutor()

    [completed] = _run_writes(
        tmp_path / "remote.txt", image_id="sha256:one", executor=executor
    ).succeeded()
    again = _run_writes(tmp_path / "remote.txt", image_id="sha256:two", executor=executor)

    # The executor pulled the image, so the driver's copy says nothing of it.
    assert _cached_image_id(completed.cache_key) is None
    assert again.cached()
    assert executor.images == ["busybox:1.36"]


def test_a_local_run_still_records_and_checks_its_image(tmp_path: Path) -> None:
    [completed] = _run_writes(
        tmp_path / "local.txt", image_id="sha256:one", executor=None
    ).succeeded()
    again = _run_writes(tmp_path / "local.txt", image_id="sha256:two", executor=None)

    assert _cached_image_id(completed.cache_key) == "sha256:one"
    assert not again.cached()
