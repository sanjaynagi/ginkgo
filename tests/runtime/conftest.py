"""Shared doubles for runtime tests: a remote executor and a container runtime."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.remote_executor import RemoteJobResult, RemoteJobState
from tests.remote_fakes import use_stand_in_artifact_store


@dataclass
class _Handle:
    job_id: str
    payload: dict[str, Any]
    executor: "RecordingExecutor"

    def state(self) -> RemoteJobState:
        return RemoteJobState.SUCCEEDED

    def result(self) -> RemoteJobResult:
        return RemoteJobResult(state=RemoteJobState.SUCCEEDED, payload=self.payload, exit_code=0)

    def cancel(self) -> None:
        self.executor.cancelled.append(self.job_id)

    def logs_tail(self, *, lines: int = 100) -> str:
        return ""


@dataclass
class RecordingExecutor:
    """A remote executor that records each submit and finishes it at once.

    A job succeeds with the task's name as its result. Jobs are named after
    their task, so ``cancelled`` lists the tasks whose jobs were cancelled.
    """

    name: str = "remote"
    submitted: list[dict[str, Any]] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)

    def submit(self, *, attempt: dict[str, Any]) -> _Handle:
        self.submitted.append(attempt)
        payload = {"ok": True, "result": attempt["task_name"], "result_encoding": "raw"}
        return _Handle(job_id=attempt["task_name"], payload=payload, executor=self)


@pytest.fixture
def recording_executors(monkeypatch: pytest.MonkeyPatch) -> dict[str, RecordingExecutor]:
    """Build every configured executor as a :class:`RecordingExecutor`.

    Returns the executors built so far, by name. The remote artifact store is
    a stand-in: these payloads carry no file arguments.
    """
    import ginkgo.runtime.executor_registry as registry_module

    built: dict[str, RecordingExecutor] = {}
    monkeypatch.setattr(
        registry_module,
        "_build_executor",
        lambda spec: built.setdefault(spec.name, RecordingExecutor(spec.name)),
    )
    use_stand_in_artifact_store(monkeypatch)
    return built


def prebuilt_registry(config: dict | None = None, *, default: str | None = None, name="remote"):
    """Return a registry whose executors are all prebuilt :class:`RecordingExecutor` s.

    Without *config*, the registry holds one executor called *name*, as the run default.
    """
    if config is None:
        return ExecutorRegistry.for_executor(RecordingExecutor(name), name=name)
    registry = ExecutorRegistry.from_config(config, default=default)
    for executor_name in registry.specs:
        registry._built[executor_name] = RecordingExecutor(executor_name)
    return registry


_RUNTIME_SCRIPT = """#!/bin/bash
# Fake container runtime: log argv, then answer from <dir>/<subcommand>.{rc,out,err,sleep}.
dir="$(dirname "$0")/responses"
printf '%s\\n' "$(basename "$0") $*" >> "$(dirname "$0")/calls.log"
key="$1"
[ -f "$dir/$key.sleep" ] && exec sleep "$(cat "$dir/$key.sleep")"
[ -f "$dir/$key.out" ] && cat "$dir/$key.out"
[ -f "$dir/$key.err" ] && cat "$dir/$key.err" >&2
if [ "$key" = run ] && [ ! -f "$dir/run.rc" ]; then
    # Run the container's command on the host, as the image would.
    exec bash -c "${@: -1}"
fi
exit "$(cat "$dir/$key.rc" 2>/dev/null || echo 0)"
"""


@dataclass
class FakeRuntime:
    """A ``docker``/``podman`` executable on ``PATH`` that answers as told.

    Every subcommand succeeds silently unless :meth:`respond` says otherwise;
    ``run`` executes the container's command on the host. A response that
    sleeps answers nothing, so a caller's timeout is what ends it.
    """

    bin_dir: Path

    def respond(
        self, subcommand: str, *, rc: int = 0, out: str = "", err: str = "", sleep: float = 0
    ) -> None:
        responses = self.bin_dir / "responses"
        (responses / f"{subcommand}.rc").write_text(str(rc))
        (responses / f"{subcommand}.out").write_text(out)
        (responses / f"{subcommand}.err").write_text(err)
        if sleep:
            (responses / f"{subcommand}.sleep").write_text(str(sleep))

    @property
    def calls(self) -> list[str]:
        """Each invocation as ``"<runtime> <args>"``, in order."""
        log = self.bin_dir / "calls.log"
        return log.read_text().splitlines() if log.exists() else []


@pytest.fixture
def fake_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeRuntime:
    """Put a fake ``docker``, ``podman`` and ``custom-runtime`` first on ``PATH``."""
    bin_dir = tmp_path / "fake-runtime-bin"
    (bin_dir / "responses").mkdir(parents=True)
    for runtime in ("docker", "podman", "custom-runtime"):
        script = bin_dir / runtime
        script.write_text(_RUNTIME_SCRIPT)
        script.chmod(script.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return FakeRuntime(bin_dir=bin_dir)
