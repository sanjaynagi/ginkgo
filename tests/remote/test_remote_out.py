"""Functional tests for ``Out[...]`` parameters on remote tasks (issue #307 phase 2C).

Exercises the full round trip through a fake ``RemoteExecutor`` that runs
``ginkgo.remote.worker.run_worker_payload`` in-process against a distinct
"pod" scratch directory and a local-directory-backed fake ``ObjectStore`` —
close enough to a real backend to prove worker-local path rewriting, staging
the written output back, and restoring it at the driver's declared path all
actually work, not just that they typecheck.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ginkgo import Out, file, folder, task
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
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

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        path = self._path(bucket=bucket, key=key)
        if not path.exists():
            raise FileNotFoundError(key)
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=path.stat().st_size)

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        src = self._path(bucket=bucket, key=key)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest_path)
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=src.stat().st_size)

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
    """Runs ``run_worker_payload`` synchronously, as if it were a pod."""

    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        from ginkgo.remote.worker import run_worker_payload

        result = run_worker_payload(dict(attempt))
        state = RemoteJobState.SUCCEEDED if result.get("ok") else RemoteJobState.FAILED
        return _ImmediateHandle(
            job_id="test-job", _result=RemoteJobResult(state=state, payload=result)
        )


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

    fake_backend = _LocalDirObjectStore(root=object_root)
    monkeypatch.setattr("ginkgo.remote.resolve.resolve_backend", lambda scheme, **kw: fake_backend)
    monkeypatch.setattr("ginkgo.remote.worker._scratch_root", lambda: pod_scratch)
    return pod_scratch


def _registry() -> ExecutorRegistry:
    return ExecutorRegistry.for_executor(_RoundTripExecutor(), name="remote")


def _evaluate(expr: Any, *, event_bus: Any = None) -> Any:
    """Evaluate through a fresh evaluator wired to the round-trip executor."""
    kwargs: dict[str, Any] = {"executor_registry": _registry()}
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


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

    def test_deleting_the_output_forces_a_rerun(self, tmp_path: Path) -> None:
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
        assert not collector.cached()
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
