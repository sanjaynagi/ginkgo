"""Integration tests for remote file staging in the evaluator."""

from __future__ import annotations

from pathlib import Path
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
