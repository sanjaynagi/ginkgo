"""Shared stand-ins for remote execution: an object store and executors.

Every remote test uses these rather than its own copy, so they all exercise
the same behaviour: a store that really moves bytes and reports S3-style
ETags, and an executor that runs the real worker entry point in-process with
the payload and result passed through JSON, as between a driver and a pod.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import shutil
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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

    def state(self) -> RemoteJobState:
        return self._result.state

    def result(self) -> RemoteJobResult:
        return self._result

    def cancel(self) -> None:
        pass

    def logs_tail(self, *, lines: int = 100) -> str:
        return ""


class RoundTripExecutor(RemoteExecutor):
    """Runs ``run_worker_payload`` synchronously, as if it were a pod.

    The payload and result go through JSON exactly as they do between a
    driver and a pod, and the driver's temporary directory is hidden while
    the worker runs, so anything that only survives in-process fails here.
    """

    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        from ginkgo.remote.executor_support import encode_payload
        from ginkgo.remote.worker import run_worker_payload

        payload = json.loads(base64.b64decode(encode_payload(attempt)))
        with _driver_temp_hidden():
            result = json.loads(json.dumps(run_worker_payload(payload), default=str))
        state = RemoteJobState.SUCCEEDED if result.get("ok") else RemoteJobState.FAILED
        return _ImmediateHandle(
            job_id="test-job", _result=RemoteJobResult(state=state, payload=result)
        )


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


def evaluate_remotely(expr: Any, *, event_bus: Any = None) -> Any:
    """Evaluate *expr* with every remote task run by a :class:`RoundTripExecutor`.

    The run directory gives tasks log files, as ``ginkgo run`` does.
    """
    kwargs: dict[str, Any] = {
        "executor_registry": ExecutorRegistry.for_executor(RoundTripExecutor(), name="remote"),
        "run_dir": RunDir.create(run_id=uuid.uuid4().hex, root=Path.cwd() / "runs"),
    }
    if event_bus is not None:
        kwargs["event_bus"] = event_bus
    return ConcurrentEvaluator(jobs=1, **kwargs).evaluate(expr)


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
    monkeypatch.setattr("ginkgo.remote.worker._scratch_root", lambda: pod_scratch)
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
