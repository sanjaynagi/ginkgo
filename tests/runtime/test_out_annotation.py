"""Tests for ``Out[...]`` — a direction wrapper for task path parameters.

Covers the runtime side of phase 1 (issue #307): pre-execution validation,
post-execution existence checks, cache-key contribution, and cache-hit
revalidation. Definition-time acceptance/rejection lives in
``tests/core/test_task.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, evaluate, file, folder, shell, task
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.remote_executor import RemoteExecutor, RemoteJobHandle
from tests.conftest import EventCollector


# ---------------------------------------------------------------------------
# Module-level tasks (importable, as ``@task`` requires for process execution)
# ---------------------------------------------------------------------------


@task()
def write_output(payload: str, out_path: Out[file]) -> file:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return file(out_path)


@task()
def write_output_untracked_return(payload: str, out_path: Out[file]) -> int:
    """Writes its ``Out[file]`` but returns something else entirely.

    Isolates the ``Out[...]`` path from the return-value artifact-restoration
    machinery (``-> file`` would restore/heal the same path from the
    artifact store on a hit, which is a different, pre-existing mechanism).
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return len(payload)


@task()
def write_output_dir(payload: str, out_dir: Out[folder]) -> folder:
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    (path / "a.txt").write_text(payload, encoding="utf-8")
    return folder(out_dir)


@task()
def forgetful(marker: file, out_path: Out[file]) -> file:
    """Declares an output but never writes it — returns an unrelated file."""
    return marker


@task()
def maybe_write(payload: str | None, out_path: Out[file | None]) -> file | None:
    if out_path is None:
        return None
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload or "", encoding="utf-8")
    return file(out_path)


@task(kind="shell")
def shell_write_output(payload: str, out_path: Out[file]) -> file:
    return shell(cmd=f"echo '{payload}' > {out_path}", output=out_path)


@task(remote=True)
def remote_with_output(bam: Out[file]) -> file:
    raise AssertionError("never runs")


@task(executor="gpu-k8s")
def executor_with_output(bam: Out[file]) -> file:
    raise AssertionError("never runs")


class _FakeRemoteExecutor(RemoteExecutor):
    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        raise AssertionError("routing tests never submit")


# ---------------------------------------------------------------------------
# Remote dispatch places Out[...] tasks like any other python task
# ---------------------------------------------------------------------------


class TestOutRemoteDispatchAllowed:
    """``Out[...]`` no longer blocks remote placement (issue #307 phase 2C).

    Functional remote round-trips (worker-local scratch paths, staging the
    written output back, restoring it at the driver path) live in
    ``tests/remote/test_remote_out.py``. These just confirm placement itself
    is no longer rejected.
    """

    def _evaluator(self) -> ConcurrentEvaluator:
        config = {"remote": {"executors": {"gpu-k8s": {"type": "k8s", "namespace": "ml"}}}}
        registry = ExecutorRegistry.from_config(config, default="gpu-k8s")
        for name in registry.specs:
            registry._built[name] = _FakeRemoteExecutor()
        return ConcurrentEvaluator(jobs=1, executor_registry=registry)

    def test_remote_true_with_output_param_is_placed(self):
        evaluator = self._evaluator()
        assert evaluator._resolve_placement(task_def=remote_with_output) == "gpu-k8s"

    def test_named_executor_with_output_param_is_placed(self):
        evaluator = self._evaluator()
        assert evaluator._resolve_placement(task_def=executor_with_output) == "gpu-k8s"


# ---------------------------------------------------------------------------
# Cache-key contribution: path string only, never content
# ---------------------------------------------------------------------------


class TestOutCacheKey:
    def test_output_param_key_does_not_touch_disk(self, tmp_path: Path):
        """Building the key must not require the declared path to exist."""
        with CacheIndex.in_memory() as index:
            store = CacheStore(index=index, root=tmp_path / "cache")
            path = str(tmp_path / "does-not-exist" / "out.bam")

            key, hashes = store.build_cache_key(
                task_def=write_output,
                resolved_args={"payload": "x", "out_path": path},
            )
            assert key
            assert hashes["out_path"]["type"] == "str"
            assert "sha256" in hashes["out_path"]

    def test_output_param_key_is_stable_across_calls(self, tmp_path: Path):
        with CacheIndex.in_memory() as index:
            store = CacheStore(index=index, root=tmp_path / "cache")
            path = str(tmp_path / "out.bam")

            key1, _ = store.build_cache_key(
                task_def=write_output, resolved_args={"payload": "x", "out_path": path}
            )
            key2, _ = store.build_cache_key(
                task_def=write_output, resolved_args={"payload": "x", "out_path": path}
            )
            assert key1 == key2

    def test_output_param_key_changes_with_path_not_content(self, tmp_path: Path):
        with CacheIndex.in_memory() as index:
            store = CacheStore(index=index, root=tmp_path / "cache")
            path_a = str(tmp_path / "a.bam")
            path_b = str(tmp_path / "b.bam")

            key_a, _ = store.build_cache_key(
                task_def=write_output, resolved_args={"payload": "x", "out_path": path_a}
            )
            key_b, _ = store.build_cache_key(
                task_def=write_output, resolved_args={"payload": "x", "out_path": path_b}
            )
            assert key_a != key_b


# ---------------------------------------------------------------------------
# Pre-execution validation
# ---------------------------------------------------------------------------


class TestOutPreExecutionValidation:
    def test_missing_output_path_is_not_an_error(self, tmp_path: Path):
        """Unlike ``file``, an ``Out[file]`` need not already exist."""
        out_path = tmp_path / "fresh.txt"
        result = evaluate(write_output(payload="hi", out_path=str(out_path)))
        assert Path(str(result)).read_text() == "hi"

    def test_out_file_pointing_at_existing_directory_fails(self, tmp_path: Path):
        existing_dir = tmp_path / "already_a_dir"
        existing_dir.mkdir()

        with pytest.raises(TypeError, match="already exists and is a directory"):
            evaluate(write_output(payload="hi", out_path=str(existing_dir)))

    def test_out_folder_pointing_at_existing_file_fails(self, tmp_path: Path):
        existing_file = tmp_path / "already_a_file"
        existing_file.write_text("x")

        with pytest.raises(TypeError, match="already exists and is a file"):
            evaluate(write_output_dir(payload="hi", out_dir=str(existing_file)))


# ---------------------------------------------------------------------------
# Post-execution checks
# ---------------------------------------------------------------------------


class TestOutPostExecutionValidation:
    def test_task_that_fails_to_write_its_output_fails_clearly(self, tmp_path: Path):
        marker = tmp_path / "marker.txt"
        marker.write_text("ok", encoding="utf-8")
        missing = tmp_path / "never_written.txt"

        with pytest.raises(FileNotFoundError, match="was not written"):
            evaluate(forgetful(marker=file(str(marker)), out_path=str(missing)))

    def test_shell_task_output_is_checked_after_the_command_runs(self, tmp_path: Path):
        out_path = tmp_path / "shell_out.txt"
        result = evaluate(shell_write_output(payload="hello", out_path=str(out_path)))
        assert Path(str(result)).read_text().strip() == "hello"


# ---------------------------------------------------------------------------
# End-to-end: caching behaviour
# ---------------------------------------------------------------------------


class TestOutEndToEndCaching:
    def test_second_run_is_a_cache_hit(self, tmp_path: Path):
        out_path = tmp_path / "cached.txt"
        evaluate(write_output(payload="v1", out_path=str(out_path)))

        collector = EventCollector()
        evaluate(write_output(payload="v1", out_path=str(out_path)), event_bus=collector.bus)
        assert collector.cached()

    def test_changing_output_content_does_not_invalidate(self, tmp_path: Path):
        out_path = tmp_path / "content_changes.txt"
        evaluate(write_output_untracked_return(payload="v1", out_path=str(out_path)))

        out_path.write_text("tampered", encoding="utf-8")

        collector = EventCollector()
        evaluate(
            write_output_untracked_return(payload="v1", out_path=str(out_path)),
            event_bus=collector.bus,
        )
        assert collector.cached()
        # A cache hit contributes only the Out[...] path to the key, and
        # nothing restores its content on a hit — the tampered bytes are left
        # exactly as they were, proving content never gated this hit.
        assert out_path.read_text(encoding="utf-8") == "tampered"

    def test_deleting_the_output_forces_a_rerun(self, tmp_path: Path):
        out_path = tmp_path / "deleted.txt"
        evaluate(write_output_untracked_return(payload="v1", out_path=str(out_path)))

        out_path.unlink()
        assert not out_path.exists()

        collector = EventCollector()
        evaluate(
            write_output_untracked_return(payload="v1", out_path=str(out_path)),
            event_bus=collector.bus,
        )
        assert not collector.cached()
        assert out_path.read_text(encoding="utf-8") == "v1"

    def test_optional_output_absent_is_a_cache_hit_with_no_path_to_check(self, tmp_path: Path):
        result = evaluate(maybe_write(payload=None, out_path=None))
        assert result is None

        collector = EventCollector()
        result2 = evaluate(maybe_write(payload=None, out_path=None), event_bus=collector.bus)
        assert result2 is None
        assert collector.cached()

    def test_present_optional_output_is_checked_and_cached_normally(self, tmp_path: Path):
        out_path = tmp_path / "present.txt"
        result = evaluate(maybe_write(payload="hi", out_path=str(out_path)))
        assert Path(str(result)).read_text() == "hi"

        collector = EventCollector()
        evaluate(maybe_write(payload="hi", out_path=str(out_path)), event_bus=collector.bus)
        assert collector.cached()
