"""Integration tests for remote file staging in the evaluator."""

from __future__ import annotations

from pathlib import Path
from threading import Barrier, Lock
from unittest.mock import MagicMock, patch

import pytest

from ginkgo import evaluate, file, folder, task
from ginkgo.core.remote import remote_file, remote_folder
from ginkgo.core.hashing import hash_bytes
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.events import TaskAnnotated
from ginkgo.remote.staging import RemoteIdentityError, StagingCache
from tests.conftest import EventCollector, make_download_backend


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
    """Create a real staging cache paired with a mocked download backend."""
    cache = StagingCache(root=tmp_path / "staging")
    backend = make_download_backend(content=content, etag="test-etag")
    return cache, backend


class TestRemoteFileEvaluator:
    def test_remote_file_ref_is_staged_before_task(
        self,
        tmp_path,
        monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"hello from s3")

        ref = remote_file("s3://test-bucket/data/greeting.txt")
        collector = EventCollector()

        with (
            patch(
                "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result = evaluate(read_file_content(path=ref), event_bus=collector.bus)

        assert result == "hello from s3"
        assert collector.staging()

    def test_raw_s3_uri_string_coerced_for_file_param(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"auto coerced")

        # Pass a raw s3:// string to a file-annotated parameter.
        with (
            patch(
                "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
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
                "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
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

    def test_cache_hit_with_remote_input(self, tmp_path, monkeypatch) -> None:
        """Remote input should produce a cache hit on rerun with same content."""
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"cacheable")

        ref = remote_file("s3://bucket/cacheable.txt")
        collector = EventCollector()

        with (
            patch(
                "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            result1 = evaluate(read_file_content(path=ref))
            result2 = evaluate(read_file_content(path=ref), event_bus=collector.bus)

        assert result1 == result2 == "cacheable"
        assert collector.cached()

    def test_task_started_is_emitted_after_staging(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        cache, backend = _make_mock_staging_cache(tmp_path, b"ordered")
        collector = EventCollector()

        with (
            patch(
                "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
                return_value=cache,
            ),
            patch(
                "ginkgo.remote.staging.resolve_backend",
                return_value=backend,
            ),
        ):
            evaluate(
                read_file_content(path=remote_file("s3://bucket/ordered.txt")),
                event_bus=collector.bus,
            )

        # Each task should report staging before running.
        from ginkgo.runtime.events import TaskStaging, TaskStarted

        first_staging = next(
            (i for i, e in enumerate(collector.events) if isinstance(e, TaskStaging)),
            None,
        )
        first_started = next(
            (i for i, e in enumerate(collector.events) if isinstance(e, TaskStarted)),
            None,
        )
        assert first_staging is not None and first_started is not None
        assert first_staging < first_started

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
                "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
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
                "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
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


# ---------------------------------------------------------------------------
# Remote input identity: the cache is checked before anything is downloaded
# ---------------------------------------------------------------------------


@task(retries=2)
def read_file_content_with_retries(*, path: file) -> str:
    """Read a file, retrying a failed attempt twice."""
    return Path(str(path)).read_text(encoding="utf-8").strip()


@task()
def read_optional_file(*, path: file | None) -> str:
    """Read a file the caller may leave out."""
    return "" if path is None else Path(str(path)).read_text(encoding="utf-8").strip()


@task()
def count_folder_files(*, path: folder) -> int:
    """Count the files a folder holds."""
    return sum(1 for item in Path(str(path)).rglob("*") if item.is_file())


class _CountingStore:
    """In-memory object store that counts the requests made of it."""

    def __init__(self, objects: dict[str, tuple[bytes, str | None]]) -> None:
        self.objects = objects
        self.heads = 0
        self.lists = 0
        self.downloads = 0

    def _meta(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        content, etag = self.objects[key]
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=len(content), etag=etag)

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        self.heads += 1
        return self._meta(bucket=bucket, key=key)

    def list_prefix(self, *, bucket: str, prefix: str) -> list[RemoteObjectMeta]:
        self.lists += 1
        return [
            self._meta(bucket=bucket, key=key)
            for key in sorted(self.objects)
            if key.startswith(prefix)
        ]

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        self.downloads += 1
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(self.objects[key][0])
        return self._meta(bucket=bucket, key=key)


def _evaluate_against(
    store: _CountingStore, *, staging_root: Path, expr: object, collector: EventCollector
) -> object:
    """Evaluate with a given staging cache, as a driver with that disk would."""
    with (
        patch(
            "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
            return_value=StagingCache(root=staging_root),
        ),
        patch("ginkgo.remote.staging.resolve_backend", return_value=store),
    ):
        return evaluate(expr, event_bus=collector.bus)


class TestRemoteInputIdentity:
    def test_a_cache_hit_on_a_fresh_driver_downloads_nothing(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", "e1")})
        ref = remote_file("s3://bucket/data/a.txt")

        _evaluate_against(
            store,
            staging_root=tmp_path / "first",
            expr=read_file_content(path=ref),
            collector=EventCollector(),
        )
        # A second driver with an empty staging cache, as a fresh pod has.
        collector = EventCollector()
        result = _evaluate_against(
            store,
            staging_root=tmp_path / "second",
            expr=read_file_content(path=ref),
            collector=collector,
        )

        assert result == "alpha"
        assert collector.cached()
        assert store.downloads == 1
        assert store.heads == 2

    def test_an_overwritten_object_misses(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", "e1")})
        ref = remote_file("s3://bucket/data/a.txt")
        _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_file_content(path=ref),
            collector=EventCollector(),
        )

        store.objects["data/a.txt"] = (b"beta", "e2")
        collector = EventCollector()
        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_file_content(path=ref),
            collector=collector,
        )

        assert result == "beta"
        assert not collector.cached()

    def test_a_folder_cache_hit_lists_without_downloading(self, tmp_path) -> None:
        store = _CountingStore({"run/a.txt": (b"a", "ea"), "run/b.txt": (b"b", "eb")})
        expr = count_folder_files(path=remote_folder("s3://bucket/run/"))
        _evaluate_against(
            store, staging_root=tmp_path / "first", expr=expr, collector=EventCollector()
        )

        collector = EventCollector()
        result = _evaluate_against(
            store, staging_root=tmp_path / "second", expr=expr, collector=collector
        )

        assert result == 2
        assert collector.cached()
        assert store.downloads == 2

    def test_an_object_changed_after_the_cache_check_fails_the_task(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", "e1")})
        real_download = store.download

        def download_after_an_overwrite(*, bucket, key, dest_path):
            store.objects[key] = (b"beta", "e2")
            return real_download(bucket=bucket, key=key, dest_path=dest_path)

        store.download = download_after_an_overwrite

        with pytest.raises(RemoteIdentityError, match="changed after its cache key"):
            _evaluate_against(
                store,
                staging_root=tmp_path / "staging",
                expr=read_file_content(path=remote_file("s3://bucket/data/a.txt")),
                collector=EventCollector(),
            )

    def test_an_object_without_an_etag_is_refused(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", None)})

        with pytest.raises(RemoteIdentityError, match="version_id"):
            _evaluate_against(
                store,
                staging_root=tmp_path / "staging",
                expr=read_file_content(path=remote_file("s3://bucket/data/a.txt")),
                collector=EventCollector(),
            )

    def test_tasks_reading_one_uri_ask_the_store_once(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", "e1")})
        ref = remote_file("s3://bucket/data/a.txt")

        _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=[read_file_content(path=ref), count_file_bytes(path=ref)],
            collector=EventCollector(),
        )

        assert store.heads == 1

    def test_a_changed_input_is_not_retried(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", "e1")})
        real_download = store.download

        def download_after_an_overwrite(*, bucket, key, dest_path):
            store.objects[key] = (b"beta", "e2")
            return real_download(bucket=bucket, key=key, dest_path=dest_path)

        store.download = download_after_an_overwrite

        with pytest.raises(RemoteIdentityError):
            _evaluate_against(
                store,
                staging_root=tmp_path / "staging",
                expr=read_file_content_with_retries(path=remote_file("s3://bucket/data/a.txt")),
                collector=EventCollector(),
            )

        assert store.downloads == 1

    def test_a_uri_under_an_optional_file_annotation_is_keyed_by_its_store(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", "e1")})
        expr = read_optional_file(path="s3://bucket/data/a.txt")
        _evaluate_against(
            store, staging_root=tmp_path / "staging", expr=expr, collector=EventCollector()
        )

        store.objects["data/a.txt"] = (b"beta", "e2")
        result = _evaluate_against(
            store, staging_root=tmp_path / "staging", expr=expr, collector=EventCollector()
        )

        assert result == "beta"

    def test_a_staged_input_records_its_content_digest(self, tmp_path) -> None:
        store = _CountingStore({"data/a.txt": (b"alpha", "e1")})
        collector = EventCollector()

        _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_file_content(path=remote_file("s3://bucket/data/a.txt")),
            collector=collector,
        )

        assert _recorded_input_digests(collector) == {
            "s3://bucket/data/a.txt": hash_bytes(b"alpha")
        }


def _recorded_input_digests(collector: EventCollector) -> dict[str, str]:
    """Return the last ``remote_input_digests`` annotation the run recorded."""
    recorded = [
        event.fields["remote_input_digests"]
        for event in collector.events
        if isinstance(event, TaskAnnotated) and "remote_input_digests" in event.fields
    ]
    return recorded[-1]
