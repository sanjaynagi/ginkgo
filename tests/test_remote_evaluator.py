"""Integration tests for remote file staging in the evaluator."""

from __future__ import annotations

from pathlib import Path
from threading import Barrier, Lock
from unittest.mock import MagicMock, patch

from ginkgo import evaluate, file, task
from ginkgo.core.remote import remote_file
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.staging import StagingCache


@task()
def read_file_content(*, path: file) -> str:
    """Read and return the content of a file."""
    return Path(str(path)).read_text(encoding="utf-8").strip()


@task()
def count_files_in_list(*, paths: list[file]) -> int:
    """Count the number of file paths received."""
    return len(paths)


@task()
def count_file_bytes(*, path: file) -> int:
    """Return the file size in bytes."""
    return len(Path(str(path)).read_bytes())


def _make_mock_staging_cache(tmp_path: Path, content: bytes = b"staged content"):
    """Create a real staging cache with a mocked backend."""
    cache = StagingCache(root=tmp_path / "staging")
    backend = MagicMock()

    def _download(*, bucket, key, dest_path):
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(content)
        return RemoteObjectMeta(
            uri=f"s3://{bucket}/{key}",
            size=len(content),
            etag="test-etag",
        )

    def _head(*, bucket, key):
        return RemoteObjectMeta(
            uri=f"s3://{bucket}/{key}",
            size=len(content),
            etag="test-etag",
        )

    backend.download.side_effect = _download
    backend.head.side_effect = _head
    return cache, backend


class TestRemoteFileEvaluator:
    def test_remote_file_ref_is_staged_before_task(
        self,
        tmp_path,
        monkeypatch,
        capsys,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"hello from s3")

        ref = remote_file("s3://test-bucket/data/greeting.txt")

        with (
            patch(
                "ginkgo.runtime.evaluator._ConcurrentEvaluator._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result = evaluate(read_file_content(path=ref))
            captured = capsys.readouterr()

        assert result == "hello from s3"
        assert '"status": "staging"' in captured.err

    def test_raw_s3_uri_string_coerced_for_file_param(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"auto coerced")

        # Pass a raw s3:// string to a file-annotated parameter.
        with (
            patch(
                "ginkgo.runtime.evaluator._ConcurrentEvaluator._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result = evaluate(read_file_content(path="s3://bucket/auto.txt"))

        assert result == "auto coerced"

    def test_remote_file_in_list_is_staged(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"list item")

        refs = [
            remote_file("s3://bucket/a.txt"),
            remote_file("s3://bucket/b.txt"),
        ]

        with (
            patch(
                "ginkgo.runtime.evaluator._ConcurrentEvaluator._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result = evaluate(count_files_in_list(paths=refs))

        assert result == 2

    def test_local_file_unchanged(self, tmp_path, monkeypatch) -> None:
        """Local file paths are not affected by remote staging."""
        monkeypatch.chdir(tmp_path)
        local = tmp_path / "local.txt"
        local.write_text("local content")

        result = evaluate(read_file_content(path=file(str(local))))
        assert result == "local content"

    def test_cache_hit_with_remote_input(self, tmp_path, monkeypatch, capsys) -> None:
        """Remote input should produce a cache hit on rerun with same content."""
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"cacheable")

        ref = remote_file("s3://bucket/cacheable.txt")

        with (
            patch(
                "ginkgo.runtime.evaluator._ConcurrentEvaluator._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result1 = evaluate(read_file_content(path=ref))
            capsys.readouterr()
            result2 = evaluate(read_file_content(path=ref))
            captured = capsys.readouterr()

        assert result1 == result2 == "cacheable"
        assert '"status": "cached"' in captured.err

    def test_task_started_is_emitted_after_staging(self, tmp_path, monkeypatch, capsys) -> None:
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"ordered")

        with (
            patch(
                "ginkgo.runtime.evaluator._ConcurrentEvaluator._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            evaluate(read_file_content(path=remote_file("s3://bucket/ordered.txt")))
            captured = capsys.readouterr()

        assert captured.err.index('"status": "staging"') < captured.err.index(
            '"status": "running"'
        )

    def test_independent_remote_inputs_stage_concurrently(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GINKGO_STAGING_JOBS", "2")
        cache = StagingCache(root=tmp_path / "staging")
        backend = MagicMock()
        barrier = Barrier(2, timeout=1.0)

        contents = {
            "a.txt": b"alpha",
            "b.txt": b"beta",
        }

        def _download(*, bucket, key, dest_path):
            barrier.wait()
            payload = contents[key]
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(payload)
            return RemoteObjectMeta(
                uri=f"s3://{bucket}/{key}",
                size=len(payload),
                etag=f"etag-{key}",
            )

        def _head(*, bucket, key):
            payload = contents[key]
            return RemoteObjectMeta(
                uri=f"s3://{bucket}/{key}",
                size=len(payload),
                etag=f"etag-{key}",
            )

        backend.download.side_effect = _download
        backend.head.side_effect = _head

        with (
            patch(
                "ginkgo.runtime.evaluator._ConcurrentEvaluator._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result = evaluate(
                [
                    read_file_content(path=remote_file("s3://bucket/a.txt")),
                    read_file_content(path=remote_file("s3://bucket/b.txt")),
                ],
                jobs=2,
            )

        assert result == ["alpha", "beta"]
        assert backend.download.call_count == 2

    def test_shared_remote_ref_is_staged_once_across_concurrent_tasks(
        self,
        tmp_path,
        monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GINKGO_STAGING_JOBS", "2")
        cache = StagingCache(root=tmp_path / "staging")
        backend = MagicMock()
        download_lock = Lock()
        download_calls = 0

        def _download(*, bucket, key, dest_path):
            nonlocal download_calls
            with download_lock:
                download_calls += 1
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(b"shared payload")
            return RemoteObjectMeta(
                uri=f"s3://{bucket}/{key}",
                size=len(b"shared payload"),
                etag="shared-etag",
            )

        def _head(*, bucket, key):
            return RemoteObjectMeta(
                uri=f"s3://{bucket}/{key}",
                size=len(b"shared payload"),
                etag="shared-etag",
            )

        backend.download.side_effect = _download
        backend.head.side_effect = _head
        ref = remote_file("s3://bucket/shared.txt")

        with (
            patch(
                "ginkgo.runtime.evaluator._ConcurrentEvaluator._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result = evaluate(
                [
                    read_file_content(path=ref),
                    count_file_bytes(path=ref),
                ],
                jobs=2,
            )

        assert result == ["shared payload", len(b"shared payload")]
        assert download_calls == 1
