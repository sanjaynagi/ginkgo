"""Functional tests for remote round trips: payload transport and ``Out[...]`` parameters.

Exercises the full round trip through a fake ``RemoteExecutor`` that runs
``ginkgo.remote.worker.run_worker_payload`` in-process against a distinct
"pod" scratch directory and a local-directory-backed fake ``ObjectStore`` —
close enough to a real backend to prove worker-local path rewriting, staging
the written output back, and restoring it at the driver's declared path all
actually work, not just that they typecheck.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import shutil
import sys
import tempfile
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ginkgo import Out, file, folder, task
from ginkgo.core.remote import remote_file
from ginkgo.remote.staging import RemoteIdentityError
from ginkgo.core.hashing import hash_bytes
from ginkgo.runtime.events import TaskAnnotated, TaskStaging
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.remote_executor import (
    RemoteExecutor,
    RemoteJobHandle,
    RemoteJobResult,
    RemoteJobState,
)
from tests.conftest import EventCollector


# ---------------------------------------------------------------------------
# Fake object store: a real local directory, addressed like S3.
# ---------------------------------------------------------------------------


@dataclass
class _LocalDirObjectStore:
    """``ObjectStore`` backed by a plain local directory.

    Real enough to prove uploads and downloads actually move bytes, without
    needing network access or mocking every call site individually.
    """

    root: Path

    def _path(self, *, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def _meta(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        # An S3-style ETag: the MD5 of the bytes.
        data = self._path(bucket=bucket, key=key).read_bytes()
        return RemoteObjectMeta(
            uri=f"s3://{bucket}/{key}", size=len(data), etag=hashlib.md5(data).hexdigest()
        )

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        if not self._path(bucket=bucket, key=key).exists():
            raise FileNotFoundError(key)
        return self._meta(bucket=bucket, key=key)

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        src = self._path(bucket=bucket, key=key)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest_path)
        return self._meta(bucket=bucket, key=key)

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        dest = self._path(bucket=bucket, key=key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dest)
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=dest.stat().st_size)


# ---------------------------------------------------------------------------
# Fake executor: runs the real worker entry point in-process, against a
# scratch directory standing in for a separate pod's filesystem.
# ---------------------------------------------------------------------------


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


class _RoundTripExecutor(RemoteExecutor):
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


@pytest.fixture(autouse=True)
def _configure_remote_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``[remote.artifacts]`` at a fake local-directory object store.

    Also gives the (simulated) worker a scratch root separate from the
    driver's own working directory, so a passing test proves the output
    actually travelled through the artifact store rather than just being
    visible because driver and worker share a filesystem.
    """
    (tmp_path / "ginkgo.toml").write_text(
        'schema_version = 1\n\n[remote.artifacts]\nstore = "s3://test-bucket/artifacts/"\n',
        encoding="utf-8",
    )
    object_root = tmp_path / "object-store"
    object_root.mkdir()
    pod_scratch = tmp_path / "pod-scratch"
    pod_scratch.mkdir()
    driver_tmp = tmp_path / "driver-tmp"
    driver_tmp.mkdir()

    monkeypatch.setattr(tempfile, "tempdir", str(driver_tmp))
    fake_backend = _LocalDirObjectStore(root=object_root)
    monkeypatch.setattr("ginkgo.remote.resolve.resolve_backend", lambda scheme, **kw: fake_backend)
    monkeypatch.setattr("ginkgo.remote.staging.resolve_backend", lambda scheme, **kw: fake_backend)
    monkeypatch.setattr("ginkgo.remote.worker._scratch_root", lambda: pod_scratch)
    return pod_scratch


def _registry() -> ExecutorRegistry:
    return ExecutorRegistry.for_executor(_RoundTripExecutor(), name="remote")


def _evaluate(expr: Any, *, event_bus: Any = None) -> Any:
    """Evaluate through a fresh evaluator wired to the round-trip executor.

    The run directory gives tasks log files, as ``ginkgo run`` does.
    """
    kwargs: dict[str, Any] = {
        "executor_registry": _registry(),
        "run_dir": RunDir.create(run_id=uuid.uuid4().hex, root=Path.cwd() / "runs"),
    }
    if event_bus is not None:
        kwargs["event_bus"] = event_bus
    return ConcurrentEvaluator(jobs=1, **kwargs).evaluate(expr)


# ---------------------------------------------------------------------------
# Module-level tasks
# ---------------------------------------------------------------------------


@task(remote=True)
def remote_write_output(payload: str, out_path: Out[file]) -> file:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return file(out_path)


@task(remote=True)
def remote_write_output_untracked_return(payload: str, out_path: Out[file]) -> int:
    """Writes its ``Out[file]`` but returns something else entirely.

    Isolates the ``Out[...]`` path from the return-value artifact-restoration
    machinery — a ``-> file`` return of the same path would let a cache hit
    heal the deleted file from CAS, masking the ``Out[...]`` presence check
    this is meant to exercise. Mirrors ``tests/runtime/test_out_annotation.py``.
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return len(payload)


@task(remote=True)
def remote_write_output_dir(payload: str, out_dir: Out[folder]) -> folder:
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    (path / "a.txt").write_text(payload, encoding="utf-8")
    return folder(out_dir)


@task(remote=True)
def remote_write_output_list(payloads: list[str], out_paths: Out[list[file]]) -> list[file]:
    for text, out_path in zip(payloads, out_paths):
        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return [file(p) for p in out_paths]


@task(remote=True)
def remote_maybe_write(payload: str | None, out_path: Out[file | None]) -> file | None:
    if out_path is None:
        return None
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload or "", encoding="utf-8")
    return file(out_path)


@task(remote=True)
def remote_write_output_inferred(payload: str, out_path: Out[file]):
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


@task(remote=True)
def remote_forgetful(marker: file, out_path: Out[file]) -> file:
    """Declares an output but never writes it."""
    return marker


@task()
def read_file_content(*, path: file) -> str:
    return Path(str(path)).read_text(encoding="utf-8").strip()


@task(remote=True)
def remote_chatty(value: int) -> int:
    print("hello from the pod")
    print("a warning", file=sys.stderr)
    return value + 1


@task(remote=True)
def remote_double(frame: pd.DataFrame) -> pd.DataFrame:
    return frame * 2


@task(remote=True)
def remote_read(path: file) -> str:
    return Path(str(path)).read_text(encoding="utf-8")


@task(remote=True)
def remote_read_both(direct: file, staged: file) -> str:
    return Path(str(direct)).read_text(encoding="utf-8") + Path(str(staged)).read_text(
        encoding="utf-8"
    )


@task(remote=True, retries=2)
def remote_read_with_retries(path: file) -> str:
    return Path(str(path)).read_text(encoding="utf-8")


@task(remote=True)
def remote_fails() -> int:
    raise ValueError("boom in the pod")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRemotePayloadTransport:
    def test_task_output_goes_to_the_pods_own_streams(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert _evaluate(remote_chatty(value=1)) == 2

        # Not to log files at the driver's paths, which only the pod would see.
        captured = capsys.readouterr()
        assert "hello from the pod" in captured.out
        assert "a warning" in captured.err

    def test_failing_task_traceback_goes_to_the_pods_stderr(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(ValueError, match="boom in the pod"):
            _evaluate(remote_fails())

        # Writing the traceback must leave the process's stderr open.
        assert "Traceback" in capsys.readouterr().err

    def test_large_dataframe_crosses_both_ways(self) -> None:
        # Random floats keep the encoded frame well over the inline limit.
        frame = pd.DataFrame({"x": np.random.default_rng(0).random(100_000)})

        result = _evaluate(remote_double(frame=frame))

        pd.testing.assert_frame_equal(result, frame * 2)


class TestRemoteOutFile:
    def test_out_file_lands_at_driver_path(self, tmp_path: Path) -> None:
        out_path = tmp_path / "results" / "x.txt"
        result = _evaluate(
            remote_write_output(payload="hello", out_path=str(out_path)),
        )
        assert Path(str(result)) == out_path
        assert out_path.read_text(encoding="utf-8") == "hello"

    def test_downstream_local_consumer_reads_it(self, tmp_path: Path) -> None:
        out_path = tmp_path / "produced.txt"
        upstream = remote_write_output(payload="downstream content", out_path=str(out_path))
        result = _evaluate(read_file_content(path=upstream))
        assert result == "downstream content"

    def test_second_run_is_a_cache_hit(self, tmp_path: Path) -> None:
        out_path = tmp_path / "cached.txt"
        _evaluate(
            remote_write_output(payload="v1", out_path=str(out_path)),
        )

        collector = EventCollector()
        _evaluate(
            remote_write_output(payload="v1", out_path=str(out_path)),
            event_bus=collector.bus,
        )
        assert collector.cached()

    def test_a_deleted_output_is_restored_on_a_hit(self, tmp_path: Path) -> None:
        out_path = tmp_path / "deleted.txt"
        _evaluate(
            remote_write_output_untracked_return(payload="v1", out_path=str(out_path)),
        )
        out_path.unlink()

        collector = EventCollector()
        _evaluate(
            remote_write_output_untracked_return(payload="v1", out_path=str(out_path)),
            event_bus=collector.bus,
        )
        assert collector.cached()
        assert out_path.read_text(encoding="utf-8") == "v1"

    def test_task_that_fails_to_write_its_output_fails_clearly(self, tmp_path: Path) -> None:
        marker = tmp_path / "marker.txt"
        marker.write_text("ok", encoding="utf-8")
        missing = tmp_path / "never_written.txt"

        with pytest.raises(FileNotFoundError, match="was not written"):
            _evaluate(
                remote_forgetful(marker=file(str(marker)), out_path=str(missing)),
            )

    def test_an_earlier_jobs_output_on_the_worker_does_not_count(self, tmp_path: Path) -> None:
        """Each dispatch writes Out paths into its own worker scratch directory.

        Both outputs share a basename, so with one shared scratch directory the
        first job's file would sit exactly where the second job's unwritten
        output is looked for, and the second job would wrongly succeed.
        """
        first = tmp_path / "a" / "out.txt"
        _evaluate(remote_write_output(payload="first", out_path=str(first)))
        marker = tmp_path / "marker.txt"
        marker.write_text("ok", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match="was not written"):
            _evaluate(
                remote_forgetful(
                    marker=file(str(marker)), out_path=str(tmp_path / "b" / "out.txt")
                ),
            )

    def test_the_worker_removes_its_scratch_outputs_after_the_dispatch(
        self, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.txt"
        _evaluate(remote_write_output(payload="x", out_path=str(out)))

        assert out.read_text(encoding="utf-8") == "x"
        assert list((tmp_path / "pod-scratch" / "ginkgo-outputs").iterdir()) == []


class TestRemoteOutFolder:
    def test_out_folder_lands_at_driver_path(self, tmp_path: Path) -> None:
        out_dir = tmp_path / "qc"
        result = _evaluate(
            remote_write_output_dir(payload="folder content", out_dir=str(out_dir)),
        )
        assert Path(str(result)) == out_dir
        assert (out_dir / "a.txt").read_text(encoding="utf-8") == "folder content"


class TestRemoteOutList:
    def test_out_list_of_files_all_land_at_their_driver_paths(self, tmp_path: Path) -> None:
        paths = [str(tmp_path / f"item-{i}.txt") for i in range(3)]
        result = _evaluate(
            remote_write_output_list(payloads=["a", "b", "c"], out_paths=paths),
        )
        assert [str(f) for f in result] == paths
        for path, expected in zip(paths, ["a", "b", "c"]):
            assert Path(path).read_text(encoding="utf-8") == expected


class TestRemoteOutOptional:
    def test_absent_optional_output_produces_nothing(self, tmp_path: Path) -> None:
        result = _evaluate(
            remote_maybe_write(payload=None, out_path=None),
        )
        assert result is None

    def test_present_optional_output_lands_at_driver_path(self, tmp_path: Path) -> None:
        out_path = tmp_path / "present.txt"
        result = _evaluate(
            remote_maybe_write(payload="hi", out_path=str(out_path)),
        )
        assert Path(str(result)) == out_path
        assert out_path.read_text(encoding="utf-8") == "hi"


class TestRemoteOutInferredReturn:
    def test_inferred_return_from_out_param(self, tmp_path: Path) -> None:
        out_path = tmp_path / "inferred.txt"
        result = _evaluate(
            remote_write_output_inferred(payload="inferred", out_path=str(out_path)),
        )
        assert Path(str(result)) == out_path
        assert out_path.read_text(encoding="utf-8") == "inferred"

    def test_named_output_access(self, tmp_path: Path) -> None:
        out_path = tmp_path / "named.txt"
        call = remote_write_output(payload="named", out_path=str(out_path))
        result = _evaluate(call.output["out_path"])
        assert Path(str(result)) == out_path


class TestDirectRemoteInputs:
    """A remote task reads its remote inputs itself, straight from the source."""

    def _put_source(self, tmp_path: Path, content: bytes) -> str:
        source = tmp_path / "object-store" / "data-bucket" / "in.txt"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(content)
        return "s3://data-bucket/in.txt"

    def _artifact_blobs(self, tmp_path: Path) -> list[bytes]:
        artifacts = tmp_path / "object-store" / "test-bucket"
        return [path.read_bytes() for path in artifacts.rglob("*") if path.is_file()]

    def test_the_driver_neither_downloads_nor_uploads_a_direct_input(self, tmp_path: Path) -> None:
        uri = self._put_source(tmp_path, b"from the source")

        assert _evaluate(remote_read(path=remote_file(uri))) == "from the source"

        assert not (tmp_path / ".ginkgo" / "staging").exists()
        assert b"from the source" not in self._artifact_blobs(tmp_path)

    def test_staging_reports_direct_access(self, tmp_path: Path) -> None:
        uri = self._put_source(tmp_path, b"from the source")
        collector = EventCollector()

        _evaluate(remote_read(path=remote_file(uri)), event_bus=collector.bus)

        staging = [event for event in collector.events if isinstance(event, TaskStaging)]
        assert [event.access_method for event in staging] == ["direct"]

    def test_an_input_marked_stage_goes_through_the_driver(self, tmp_path: Path) -> None:
        uri = self._put_source(tmp_path, b"staged by the driver")

        result = _evaluate(remote_read(path=remote_file(uri, access="stage")))

        assert result == "staged by the driver"
        assert (tmp_path / ".ginkgo" / "staging").exists()
        assert b"staged by the driver" in self._artifact_blobs(tmp_path)

    def test_a_change_before_the_worker_reads_fails_without_retries(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uri = self._put_source(tmp_path, b"before")
        submits = 0
        real_submit = _RoundTripExecutor.submit

        def overwrite_then_submit(self, *, attempt: dict) -> RemoteJobHandle:
            nonlocal submits
            submits += 1
            (tmp_path / "object-store" / "data-bucket" / "in.txt").write_bytes(b"after")
            return real_submit(self, attempt=attempt)

        monkeypatch.setattr(_RoundTripExecutor, "submit", overwrite_then_submit)

        with pytest.raises(RemoteIdentityError, match="changed after its cache key"):
            _evaluate(remote_read_with_retries(path=remote_file(uri)))
        assert submits == 1

    def test_the_worker_records_the_digest_of_a_direct_input(self, tmp_path: Path) -> None:
        uri = self._put_source(tmp_path, b"from the source")
        collector = EventCollector()

        _evaluate(remote_read(path=remote_file(uri)), event_bus=collector.bus)

        assert _recorded_input_digests(collector) == {uri: hash_bytes(b"from the source")}

    def test_driver_and_worker_digests_are_recorded_together(self, tmp_path: Path) -> None:
        direct_uri = self._put_source(tmp_path, b"direct")
        staged_source = tmp_path / "object-store" / "data-bucket" / "staged.txt"
        staged_source.write_bytes(b"staged")
        staged_uri = "s3://data-bucket/staged.txt"
        collector = EventCollector()

        result = _evaluate(
            remote_read_both(
                direct=remote_file(direct_uri), staged=remote_file(staged_uri, access="stage")
            ),
            event_bus=collector.bus,
        )

        assert result == "directstaged"
        assert _recorded_input_digests(collector) == {
            direct_uri: hash_bytes(b"direct"),
            staged_uri: hash_bytes(b"staged"),
        }


def _recorded_input_digests(collector: EventCollector) -> dict[str, str]:
    """Return the last ``remote_input_digests`` annotation the run recorded."""
    recorded = [
        event.fields["remote_input_digests"]
        for event in collector.events
        if isinstance(event, TaskAnnotated) and "remote_input_digests" in event.fields
    ]
    return recorded[-1]
