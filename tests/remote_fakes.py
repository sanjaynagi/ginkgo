"""Shared stand-ins for remote execution: an object store and executors.

Every remote test uses these rather than its own copy, so they all exercise
the same behaviour: a store that really moves bytes and reports S3-style
ETags, and an executor that runs the real worker entry point in-process with
the payload passed in its environment and the result read from its logs, as
between a driver and a pod.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.remote_executor import (
    RemoteExecutor,
    RemoteJobHandle,
    RemoteJobResult,
    RemoteJobState,
)
from ginkgo.runtime.rundir import RunDir

_MD5 = object()
"""Default ``etag`` for :meth:`FakeObjectStore.put`: the MD5 of the bytes."""


@dataclass
class FakeObjectStore:
    """An object store on a local directory, addressed like S3.

    Objects live at ``root/<bucket>/<key>``. Each one's ETag is the MD5 of its
    bytes unless :meth:`put` gave another (or ``None``, for a store that
    reports none). The store counts the requests made of it, and the most it
    was ever serving at once.

    Parameters
    ----------
    root : Path
        Directory holding the buckets.
    latency : float
        Seconds each request takes, so that concurrent requests overlap.
    """

    root: Path
    latency: float = 0.0
    heads: int = 0
    lists: int = 0
    downloads: int = 0
    uploads: int = 0
    peak_requests: int = 0
    _in_flight: int = field(default=0, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _etags: dict[tuple[str, str], str | None] = field(default_factory=dict, repr=False)

    def put(self, *, bucket: str, key: str, data: bytes, etag: Any = _MD5) -> str:
        """Write an object, as its uploader would, and return its ``s3://`` URI."""
        path = self._path(bucket=bucket, key=key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        if etag is _MD5:
            self._etags.pop((bucket, key), None)
        else:
            self._etags[(bucket, key)] = etag
        return f"s3://{bucket}/{key}"

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        with self._request("heads"):
            if not self._path(bucket=bucket, key=key).exists():
                raise FileNotFoundError(key)
            return self._meta(bucket=bucket, key=key)

    def list_prefix(self, *, bucket: str, prefix: str) -> list[RemoteObjectMeta]:
        with self._request("lists"):
            bucket_root = self.root / bucket
            keys = sorted(
                path.relative_to(bucket_root).as_posix()
                for path in bucket_root.rglob("*")
                if path.is_file()
            )
            return [self._meta(bucket=bucket, key=key) for key in keys if key.startswith(prefix)]

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        with self._request("downloads"):
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self._path(bucket=bucket, key=key), dest_path)
            return self._meta(bucket=bucket, key=key)

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        with self._request("uploads"):
            dest = self._path(bucket=bucket, key=key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dest)
            self._etags.pop((bucket, key), None)
            return self._meta(bucket=bucket, key=key)

    @contextlib.contextmanager
    def _request(self, counter: str) -> Iterator[None]:
        """Count one request of kind *counter* while it is served."""
        with self._lock:
            setattr(self, counter, getattr(self, counter) + 1)
            self._in_flight += 1
            self.peak_requests = max(self.peak_requests, self._in_flight)
        try:
            time.sleep(self.latency)
            yield
        finally:
            with self._lock:
                self._in_flight -= 1

    def _path(self, *, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def _meta(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        data = self._path(bucket=bucket, key=key).read_bytes()
        etag = self._etags.get((bucket, key), hashlib.md5(data).hexdigest())
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=len(data), etag=etag)


class NeverSubmits(RemoteExecutor):
    """An executor for placement and routing tests, which never submit."""

    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        raise AssertionError("this test never submits a remote job")


@dataclass
class _ImmediateHandle:
    job_id: str
    _result: RemoteJobResult
    logs: str = ""

    def state(self) -> RemoteJobState:
        return self._result.state

    def result(self) -> RemoteJobResult:
        return self._result

    def cancel(self) -> None:
        pass

    def logs_tail(self, *, lines: int = 100) -> str:
        return self.logs


@dataclass
class RoundTripExecutor(RemoteExecutor):
    """Runs the worker's entry point in-process, as if it were a pod.

    It keeps to the boundary between a driver and a pod as closely as one
    process allows. ``main`` reads the payload from ``GINKGO_WORKER_PAYLOAD``,
    in a working directory of its own and with an empty temporary directory
    in place of the driver's. What it prints, stdout and stderr together, is
    the job's logs, as a cluster's log API returns them, and the result is
    parsed from those logs with ``parse_worker_output``, as executors do.

    The pod still shares the driver's process: its imported modules, the
    rest of its environment, and every absolute path on its filesystem. Its
    output is captured as Python writes it, so anything written straight to
    file descriptor 1 or 2 escapes the logs.

    Parameters
    ----------
    workdir : Path | None
        The image's working directory, holding what a baked image would.
        ``None`` gives each pod an empty one.
    """

    workdir: Path | None = None

    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        from ginkgo.remote.executor_support import encode_payload

        logs, exit_code = _run_worker(
            env={"GINKGO_WORKER_PAYLOAD": encode_payload(attempt)}, workdir=self.workdir
        )
        assert exit_code is not None, "the worker returned without exiting"
        return _finished_job(logs=logs, exit_code=exit_code)


class ContainerRoundTripExecutor(RoundTripExecutor):
    """A :class:`RoundTripExecutor` that runs a task's own image as a step of its own.

    A task with an ``image`` runs as the steps
    :func:`~ginkgo.remote.executor_support.job_steps` lays out, in turn, and
    a step that fails ends the job, as a failed init container fails a
    Kubernetes pod. Each worker step goes through ``main`` as
    :class:`RoundTripExecutor`'s pod does, with its own variables and
    working directory. The steps share the scratch root, which stands in for
    the volume at :data:`~ginkgo.remote.executor_support.SHARED_SCRATCH`.

    This host's ``sh`` stands in for the task's image. It runs ``command.sh``
    in an empty working directory with only ``PATH``, as a container of its
    own would, once the fake has checked that every absolute path the script
    names is on the shared scratch, since the image sees nothing else of the
    job's filesystem. Records each attempt's image.

    What this cannot catch: the worker steps share one process, so its
    imports, ``sys.path`` and the rest of its environment; the command runs
    with this host's tools, user and shell rather than the image's, and
    without its ``ENTRYPOINT``; and the path check reads ``command.sh`` as
    text, so it misses a path the command builds at run time, a relative path
    that climbs out of its working directory, and any path the script a
    ``script`` task runs opens itself.
    """

    def __init__(self) -> None:
        self.images: list[str | None] = []

    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        from ginkgo.remote.executor_support import encode_payload, job_steps

        self.images.append(attempt.get("image"))
        if attempt.get("image") is None:
            return super().submit(attempt=attempt)
        scratch = os.environ["GINKGO_SCRATCH_ROOT"]
        logs = ""
        for step in job_steps(attempt):
            if step.image is None:
                env = {**step.env, "GINKGO_SCRATCH_ROOT": scratch}
                output, exit_code = _run_worker(
                    env={**env, "GINKGO_WORKER_PAYLOAD": encode_payload(attempt)},
                    workdir=self.workdir,
                )
            else:
                output, exit_code = _run_in_image(command=step.command, scratch=scratch)
            logs += output
            if exit_code:
                break
        return _finished_job(logs=logs, exit_code=exit_code or 0)


def _run_worker(*, env: dict[str, str], workdir: Path | None) -> tuple[str, int | None]:
    """Run the worker's entry point as a pod's container, with *env* set.

    It works in *workdir*, the image's working directory, or an empty one.

    Returns what it printed, stdout and stderr together, and its exit code,
    or ``None`` if it returned without exiting.
    """
    from ginkgo.remote.worker import main

    output = io.StringIO()
    exit_code = None
    with (
        _in_a_pod(env=env, workdir=workdir),
        contextlib.redirect_stdout(output),
        contextlib.redirect_stderr(output),
    ):
        try:
            main()
        except SystemExit as exc:
            exit_code = exc.code
    return output.getvalue(), exit_code


_ABSOLUTE_PATH = re.compile(r"(?<![\w.$])/[^\s'\"\\;|&<>()]+")


def _run_in_image(*, command: tuple[str, ...], scratch: str) -> tuple[str, int]:
    """Run a task step's *command* with this host's ``sh`` in place of its image.

    Fails the test if ``command.sh`` names a path off the shared *scratch*,
    other than a device, since the task's image would not have it.
    """
    from ginkgo.remote.executor_support import SHARED_SCRATCH

    argv = [arg.replace(SHARED_SCRATCH, scratch, 1) for arg in command]
    [script] = [arg for arg in argv if arg.endswith("command.sh")]
    for path in _ABSOLUTE_PATH.findall(Path(script).read_text(encoding="utf-8")):
        assert path.startswith((scratch, "/dev/")), (
            f"command.sh names {path}, which is not on the scratch the task's image shares"
        )
    with _driver_temp_hidden():
        completed = subprocess.run(
            argv,
            cwd=tempfile.mkdtemp(prefix="task-cwd-"),
            env={"PATH": os.environ["PATH"]},
            capture_output=True,
            text=True,
        )
    return completed.stdout + completed.stderr, completed.returncode


def _finished_job(*, logs: str, exit_code: int) -> _ImmediateHandle:
    """Return the handle of a job that has exited, its result parsed from its *logs*."""
    from ginkgo.remote.executor_support import parse_worker_output

    result = RemoteJobResult(
        state=RemoteJobState.SUCCEEDED if exit_code == 0 else RemoteJobState.FAILED,
        payload=parse_worker_output(logs, source_label="pod logs"),
        exit_code=exit_code,
        logs=logs,
    )
    return _ImmediateHandle(job_id="test-job", _result=result, logs=logs)


@contextlib.contextmanager
def _in_a_pod(*, env: dict[str, str], workdir: Path | None) -> Iterator[None]:
    """Give the worker a pod's surroundings: its variables, and its own directories.

    The driver's environment is restored afterwards, so it never holds the
    payload.
    """
    driver_cwd = Path.cwd()
    with _driver_temp_hidden(), mock.patch.dict(os.environ, env):
        os.chdir(workdir or tempfile.mkdtemp(prefix="pod-cwd-"))
        try:
            yield
        finally:
            os.chdir(driver_cwd)


@contextlib.contextmanager
def _driver_temp_hidden() -> Iterator[None]:
    """Swap the driver's temporary directory for an empty one, as a pod sees it."""
    driver_tmp = Path(tempfile.gettempdir())
    hidden = driver_tmp.with_name(f"{driver_tmp.name}-hidden")
    driver_tmp.rename(hidden)
    driver_tmp.mkdir()
    try:
        yield
    finally:
        shutil.rmtree(driver_tmp)
        hidden.rename(driver_tmp)


@dataclass(frozen=True)
class RemoteRoundTrip:
    """A driver and an in-process "pod" sharing only a fake object store.

    Parameters
    ----------
    store : FakeObjectStore
        The store both sides reach: the artifact store lives in its
        ``test-bucket``, and tests put remote inputs in other buckets.
    pod_scratch : Path
        The worker's scratch root, separate from the driver's directories.
    """

    store: FakeObjectStore
    pod_scratch: Path


def evaluate_remotely(
    expr: Any, *, event_bus: Any = None, executor: RemoteExecutor | None = None
) -> Any:
    """Evaluate *expr* with every remote task run by *executor*.

    The executor defaults to a :class:`RoundTripExecutor`. The run directory
    gives tasks log files, as ``ginkgo run`` does; read them with
    :func:`task_stdout_log`.
    """
    kwargs: dict[str, Any] = {
        "executor_registry": ExecutorRegistry.for_executor(
            executor or RoundTripExecutor(), name="remote"
        ),
        "run_dir": RunDir.create(run_id=uuid.uuid4().hex, root=Path.cwd() / "runs"),
    }
    if event_bus is not None:
        kwargs["event_bus"] = event_bus
    return ConcurrentEvaluator(jobs=1, **kwargs).evaluate(expr)


def task_stdout_log(runs: Path) -> str:
    """Return the stdout log of the one task run under *runs*.

    A remote task's log holds what its pod printed, which the driver copies
    from the job's logs.
    """
    [log] = runs.rglob("*.stdout.log")
    return log.read_text(encoding="utf-8")


def set_up_round_trip(*, root: Path, monkeypatch: pytest.MonkeyPatch) -> RemoteRoundTrip:
    """Configure an artifact store on a :class:`FakeObjectStore`, with a separate pod.

    Writes a ``ginkgo.toml`` naming ``s3://test-bucket/artifacts/`` under
    *root* (the working directory), points every object-store lookup at the
    fake, and gives the worker its own scratch root and the driver its own
    temporary directory, so a passing test proves bytes travelled through the
    store rather than through a shared filesystem.
    """
    (root / "ginkgo.toml").write_text(
        'schema_version = 1\n\n[remote.artifacts]\nstore = "s3://test-bucket/artifacts/"\n',
        encoding="utf-8",
    )
    store = FakeObjectStore(root=root / "object-store")
    pod_scratch = root / "pod-scratch"
    pod_scratch.mkdir()
    driver_tmp = root / "driver-tmp"
    driver_tmp.mkdir()

    monkeypatch.setattr(tempfile, "tempdir", str(driver_tmp))
    monkeypatch.setattr("ginkgo.remote.resolve.resolve_backend", lambda scheme, **kw: store)
    monkeypatch.setattr("ginkgo.remote.staging.resolve_backend", lambda scheme, **kw: store)
    monkeypatch.setenv("GINKGO_SCRATCH_ROOT", str(pod_scratch))
    return RemoteRoundTrip(store=store, pod_scratch=pod_scratch)


def use_stand_in_artifact_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give remote dispatch a stand-in artifact store, for tests whose fakes never use one.

    A task placed on a remote executor needs ``[remote.artifacts] store``;
    executors that never touch it need no real one.
    """
    from unittest.mock import MagicMock

    from ginkgo.runtime.remote_dispatch import RemoteDispatchManager

    monkeypatch.setattr(
        RemoteDispatchManager,
        "_ensure_artifact_store",
        lambda self: setattr(self, "_artifact_store", MagicMock()),
    )
