"""Tests for remote shell and script tasks, whose command runs in the pod."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from ginkgo import Out, file, script, secret, shell, task
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.task_runners.shell import ShellTaskError
from tests.conftest import EventCollector
from tests.remote_fakes import (
    RemoteRoundTrip,
    RoundTripExecutor,
    evaluate_remotely,
    task_stdout_log,
)


pytestmark = pytest.mark.usefixtures("remote_round_trip")

# Anchored to this file, as a workflow's scripts are: a pod has its own
# working directory, so a relative path would not resolve there.
UPPER_SCRIPT = Path(__file__).parent / "fixtures" / "upper.py"


@task(kind="shell")
def shout(src: file, dest: Out[file]):
    # Long enough for the command's usage to be sampled.
    return shell(cmd=f"echo wrote {dest}; sleep 0.6; tr a-z A-Z < {src} > {dest}")


@task(kind="script")
def shout_script(src: file, dest: Out[file]):
    return script(UPPER_SCRIPT)


@task(kind="shell", retries=1, retry_on_exit_codes=(7,))
def fails_once(marker: str, dest: Out[file]):
    return shell(cmd=f"[ -f {marker} ] || {{ touch {marker}; exit 7; }}; echo ok > {dest}")


@task(kind="shell", retries=1, retry_on_exit_codes=(7,))
def fails_with_three(dest: Out[file]):
    return shell(cmd="exit 3")


@task(kind="shell")
def echoes(token: str, dest: Out[file]):
    return shell(cmd=f"echo from-the-command {token}; echo err-line >&2; echo ok > {dest}")


@task(kind="shell", env="docker://alpine:3.20")
def in_a_container(dest: Out[file]):
    return shell(cmd=f"echo hi > {dest}")


@task(kind="shell", env="tools")
def in_a_pixi_env(dest: Out[file]):
    return shell(cmd=f"echo hi > {dest}")


def test_a_shell_command_reads_and_writes_pod_paths(
    tmp_path: Path, remote_round_trip: RemoteRoundTrip
) -> None:
    src = tmp_path / "in.txt"
    src.write_text("hello\n", encoding="utf-8")
    dest = tmp_path / "out" / "shout.txt"
    collector = EventCollector()

    result = evaluate_remotely(shout(src=file(src), dest=str(dest)), event_bus=collector.bus)

    assert Path(result) == dest
    assert dest.read_text(encoding="utf-8") == "HELLO\n"
    # The command ran in the pod, against the pod's scratch path.
    assert f"wrote {remote_round_trip.pod_scratch}" in task_stdout_log(tmp_path / "runs")
    [started] = collector.started()
    assert started.execution_backend == "remote"
    # The command's own usage, sampled in the pod, reaches the driver.
    [completed] = collector.succeeded()
    assert completed.resource_usage["measured"]["source"] == "sampled"


def test_a_script_runs_in_the_pod(tmp_path: Path) -> None:
    src = tmp_path / "in.txt"
    src.write_text("quiet", encoding="utf-8")
    dest = tmp_path / "out" / "loud.txt"
    collector = EventCollector()

    result = evaluate_remotely(
        shout_script(src=file(src), dest=str(dest)), event_bus=collector.bus
    )

    assert Path(result) == dest
    assert dest.read_text(encoding="utf-8") == "QUIET"
    [started] = collector.started()
    assert started.execution_backend == "remote"


def test_a_matching_exit_code_retries_the_remote_task(tmp_path: Path) -> None:
    dest = tmp_path / "ok.txt"
    collector = EventCollector()

    evaluate_remotely(
        fails_once(marker=str(tmp_path / "marker"), dest=str(dest)), event_bus=collector.bus
    )

    assert dest.read_text(encoding="utf-8") == "ok\n"
    assert [event.execution_backend for event in collector.started()] == ["remote", "remote"]


def test_another_exit_code_fails_the_remote_task_without_a_retry(tmp_path: Path) -> None:
    collector = EventCollector()

    with pytest.raises(ShellTaskError, match="exit code 3") as raised:
        evaluate_remotely(fails_with_three(dest=str(tmp_path / "x")), event_bus=collector.bus)

    assert raised.value.exit_code == 3
    assert [event.execution_backend for event in collector.started()] == ["remote"]
    [failed] = collector.failed()
    assert failed.exit_code == 3
    assert failed.failure["kind"] == "command_error"


def test_the_commands_output_reaches_the_task_log_redacted(tmp_path: Path) -> None:
    evaluator = ConcurrentEvaluator(
        jobs=1,
        executor_registry=ExecutorRegistry.for_executor(RoundTripExecutor(), name="remote"),
        run_dir=RunDir.create(run_id=uuid.uuid4().hex, root=tmp_path / "runs"),
        secret_resolver=build_secret_resolver(
            project_root=tmp_path, config={}, environ={"API_TOKEN": "super-secret"}
        ),
    )

    evaluator.evaluate(echoes(token=secret("API_TOKEN"), dest=str(tmp_path / "ok.txt")))

    text = task_stdout_log(tmp_path / "runs")
    assert "from-the-command [REDACTED]" in text
    assert "err-line" in text
    assert "super-secret" not in text


@pytest.mark.parametrize(
    "make", [in_a_container, in_a_pixi_env], ids=["container-env", "pixi-env"]
)
def test_an_env_on_a_remote_executor_fails_before_the_run(tmp_path: Path, make) -> None:
    collector = EventCollector()

    with pytest.raises(ValueError, match="a remote worker cannot run a task in its own env"):
        evaluate_remotely(make(dest=str(tmp_path / "x")), event_bus=collector.bus)

    assert collector.started() == []
