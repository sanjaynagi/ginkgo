"""Integration tests for remote file staging in the evaluator."""

from __future__ import annotations

from pathlib import Path
from threading import Barrier
from unittest.mock import patch

import pytest

from ginkgo import evaluate, file, folder, task
from ginkgo.core.hashing import hash_bytes
from ginkgo.core.remote import remote_file, remote_folder
from ginkgo.remote.staging import RemoteIdentityError, StagingCache
from ginkgo.runtime.events import TaskAnnotated, TaskStaging, TaskStarted
from tests.conftest import EventCollector
from tests.remote_fakes import FakeObjectStore


@task()
def read_file_content(*, path: file) -> str:
    """Read and return the content of a file."""
    return Path(str(path)).read_text(encoding="utf-8").strip()


@task()
def read_files(*, paths: list[file]) -> list[str]:
    """Read each file in a list."""
    return [Path(str(path)).read_text(encoding="utf-8") for path in paths]


@task()
def read_unannotated(*, paths):  # noqa: ANN001, ANN202 - the point is no annotation
    """Read each file in a list nothing says holds files."""
    return [Path(str(path)).read_text(encoding="utf-8") for path in paths]


@task()
def read_file_dict(*, paths: dict[str, file]) -> dict[str, str]:
    """Read each file in a dict."""
    return {name: Path(str(path)).read_text(encoding="utf-8") for name, path in paths.items()}


@task()
def pass_on(*, refs: list) -> list:
    """Return remote refs as they came, for a downstream task to read."""
    return refs


@task()
def count_file_bytes(*, path: file) -> int:
    """Return the file size in bytes."""
    return len(Path(str(path)).read_bytes())


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


def _store(root: Path, objects: dict[str, tuple[bytes, str | None]]) -> FakeObjectStore:
    """Return a fake store holding *objects* (key: bytes and ETag) in ``s3://bucket``."""
    store = FakeObjectStore(root=root / "object-store")
    for key, (data, etag) in objects.items():
        store.put(bucket="bucket", key=key, data=data, etag=etag)
    return store


def _evaluate_against(
    store: FakeObjectStore,
    *,
    staging_root: Path,
    expr: object,
    collector: EventCollector | None = None,
    jobs: int | None = None,
) -> object:
    """Evaluate with a given staging cache, as a driver with that disk would."""
    kwargs: dict = {} if jobs is None else {"jobs": jobs}
    if collector is not None:
        kwargs["event_bus"] = collector.bus
    with (
        patch(
            "ginkgo.runtime.remote_input_resolver.RemoteStager._get_staging_cache",
            return_value=StagingCache(root=staging_root),
        ),
        patch("ginkgo.remote.staging.resolve_backend", return_value=store),
    ):
        return evaluate(expr, **kwargs)


def _overwrite_during_download(store: FakeObjectStore) -> None:
    """Make the store's object change between the cache check and the download."""
    real_download = store.download

    def download_after_an_overwrite(*, bucket: str, key: str, dest_path: Path) -> object:
        store.put(bucket=bucket, key=key, data=b"beta", etag="e2")
        return real_download(bucket=bucket, key=key, dest_path=dest_path)

    store.download = download_after_an_overwrite  # type: ignore[method-assign]


def _recorded_input_digests(collector: EventCollector) -> dict[str, str]:
    """Return the last ``remote_input_digests`` annotation the run recorded."""
    recorded = [
        event.fields["remote_input_digests"]
        for event in collector.events
        if isinstance(event, TaskAnnotated) and "remote_input_digests" in event.fields
    ]
    return recorded[-1]


class TestRemoteFileEvaluator:
    def test_remote_file_ref_is_staged_before_task(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/greeting.txt": (b"hello from s3", "e1")})
        collector = EventCollector()

        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_file_content(path=remote_file("s3://bucket/data/greeting.txt")),
            collector=collector,
        )

        assert result == "hello from s3"
        assert collector.staging()

    def test_raw_s3_uri_string_coerced_for_file_param(self, tmp_path) -> None:
        store = _store(tmp_path, {"auto.txt": (b"auto coerced", "e1")})

        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_file_content(path="s3://bucket/auto.txt"),
        )

        assert result == "auto coerced"

    def test_a_list_of_files_is_staged_concurrently_in_order(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("GINKGO_STAGING_JOBS", "8")
        store = _store(
            tmp_path, {f"run/{i:02}.txt": (str(i).encode(), f"e{i}") for i in range(50)}
        )
        store.latency = 0.05
        order = list(reversed(range(50)))

        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_files(paths=[remote_file(f"s3://bucket/run/{i:02}.txt") for i in order]),
        )
        assert result == [str(i) for i in order]
        # One LIST of run/ identifies all fifty; the downloads overlap, at most 8 at once.
        assert (store.lists, store.heads, store.downloads) == (1, 0, 50)
        assert 1 < store.peak_requests <= 8

    def test_a_dict_of_files_is_staged(self, tmp_path) -> None:
        store = _store(tmp_path, {"a.txt": (b"alpha", "ea"), "b.txt": (b"beta", "eb")})

        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_file_dict(
                paths={"a": remote_file("s3://bucket/a.txt"), "b": "s3://bucket/b.txt"}
            ),
        )

        assert result == {"a": "alpha", "b": "beta"}

    def test_refs_a_task_returns_are_staged_for_the_next(self, tmp_path) -> None:
        store = _store(tmp_path, {"run/a.txt": (b"alpha", "ea"), "run/b.txt": (b"beta", "eb")})
        refs = pass_on(
            refs=[remote_file("s3://bucket/run/a.txt"), remote_file("s3://bucket/run/b.txt")]
        )

        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=[read_files(paths=refs), read_unannotated(paths=refs)],
        )

        assert result == [["alpha", "beta"], ["alpha", "beta"]]
        assert store.downloads == 2

    def test_a_missing_file_fails_the_task(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("GINKGO_STAGING_JOBS", "2")
        store = _store(tmp_path, {f"run/{i}.txt": (b"x", f"e{i}") for i in range(5)})

        with pytest.raises(FileNotFoundError, match="run/5.txt"):
            _evaluate_against(
                store,
                staging_root=tmp_path / "staging",
                expr=read_files(paths=[remote_file(f"s3://bucket/run/{i}.txt") for i in range(6)]),
            )

    def test_local_file_unchanged(self, tmp_path) -> None:
        """Local file paths are not affected by remote staging."""
        local = tmp_path / "local.txt"
        local.write_text("local content")

        result = evaluate(read_file_content(path=file(str(local))))
        assert result == "local content"

    def test_cache_hit_with_remote_input(self, tmp_path) -> None:
        """Remote input should produce a cache hit on rerun with same content."""
        store = _store(tmp_path, {"cacheable.txt": (b"cacheable", "e1")})
        expr = read_file_content(path=remote_file("s3://bucket/cacheable.txt"))
        collector = EventCollector()

        first = _evaluate_against(store, staging_root=tmp_path / "staging", expr=expr)
        second = _evaluate_against(
            store, staging_root=tmp_path / "staging", expr=expr, collector=collector
        )

        assert first == second == "cacheable"
        assert collector.cached()

    def test_task_started_is_emitted_after_staging(self, tmp_path) -> None:
        store = _store(tmp_path, {"ordered.txt": (b"ordered", "e1")})
        collector = EventCollector()

        _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=read_file_content(path=remote_file("s3://bucket/ordered.txt")),
            collector=collector,
        )

        kinds = [type(event) for event in collector.events]
        assert kinds.index(TaskStaging) < kinds.index(TaskStarted)

    def test_independent_remote_inputs_stage_concurrently(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("GINKGO_STAGING_JOBS", "2")
        store = _store(tmp_path, {"a.txt": (b"alpha", "ea"), "b.txt": (b"beta", "eb")})
        # Each download waits for the other: staged one after the other, it times out.
        barrier = Barrier(2, timeout=1.0)
        real_download = store.download

        def download_together(*, bucket: str, key: str, dest_path: Path) -> object:
            barrier.wait()
            return real_download(bucket=bucket, key=key, dest_path=dest_path)

        store.download = download_together  # type: ignore[method-assign]

        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=[
                read_file_content(path=remote_file("s3://bucket/a.txt")),
                read_file_content(path=remote_file("s3://bucket/b.txt")),
            ],
            jobs=2,
        )

        assert result == ["alpha", "beta"]
        assert store.downloads == 2

    def test_shared_remote_ref_is_staged_once_across_concurrent_tasks(
        self, tmp_path, monkeypatch
    ) -> None:
        monkeypatch.setenv("GINKGO_STAGING_JOBS", "2")
        store = _store(tmp_path, {"shared.txt": (b"shared payload", "e1")})
        ref = remote_file("s3://bucket/shared.txt")

        result = _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=[read_file_content(path=ref), count_file_bytes(path=ref)],
            jobs=2,
        )

        assert result == ["shared payload", len(b"shared payload")]
        assert store.downloads == 1


class TestRemoteInputIdentity:
    """The cache is checked before anything is downloaded."""

    def test_a_cache_hit_on_a_fresh_driver_downloads_nothing(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", "e1")})
        expr = read_file_content(path=remote_file("s3://bucket/data/a.txt"))

        _evaluate_against(store, staging_root=tmp_path / "first", expr=expr)
        # A second driver with an empty staging cache, as a fresh pod has.
        collector = EventCollector()
        result = _evaluate_against(
            store, staging_root=tmp_path / "second", expr=expr, collector=collector
        )

        assert result == "alpha"
        assert collector.cached()
        assert store.downloads == 1
        assert store.heads == 2

    def test_an_overwritten_object_misses(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", "e1")})
        expr = read_file_content(path=remote_file("s3://bucket/data/a.txt"))
        _evaluate_against(store, staging_root=tmp_path / "staging", expr=expr)

        store.put(bucket="bucket", key="data/a.txt", data=b"beta", etag="e2")
        collector = EventCollector()
        result = _evaluate_against(
            store, staging_root=tmp_path / "staging", expr=expr, collector=collector
        )

        assert result == "beta"
        assert not collector.cached()

    def test_a_folder_cache_hit_lists_without_downloading(self, tmp_path) -> None:
        store = _store(tmp_path, {"run/a.txt": (b"a", "ea"), "run/b.txt": (b"b", "eb")})
        expr = count_folder_files(path=remote_folder("s3://bucket/run/"))
        _evaluate_against(store, staging_root=tmp_path / "first", expr=expr)

        collector = EventCollector()
        result = _evaluate_against(
            store, staging_root=tmp_path / "second", expr=expr, collector=collector
        )

        assert result == 2
        assert collector.cached()
        assert store.downloads == 2

    def test_an_object_changed_after_the_cache_check_fails_the_task(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", "e1")})
        _overwrite_during_download(store)

        with pytest.raises(RemoteIdentityError, match="changed after its cache key"):
            _evaluate_against(
                store,
                staging_root=tmp_path / "staging",
                expr=read_file_content(path=remote_file("s3://bucket/data/a.txt")),
            )

    def test_an_object_without_an_etag_is_refused(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", None)})

        with pytest.raises(RemoteIdentityError, match="version_id"):
            _evaluate_against(
                store,
                staging_root=tmp_path / "staging",
                expr=read_file_content(path=remote_file("s3://bucket/data/a.txt")),
            )

    def test_tasks_reading_one_uri_ask_the_store_once(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", "e1")})
        ref = remote_file("s3://bucket/data/a.txt")

        _evaluate_against(
            store,
            staging_root=tmp_path / "staging",
            expr=[read_file_content(path=ref), count_file_bytes(path=ref)],
        )

        assert store.heads == 1

    def test_a_changed_input_is_not_retried(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", "e1")})
        _overwrite_during_download(store)

        with pytest.raises(RemoteIdentityError):
            _evaluate_against(
                store,
                staging_root=tmp_path / "staging",
                expr=read_file_content_with_retries(path=remote_file("s3://bucket/data/a.txt")),
            )

        assert store.downloads == 1

    def test_a_uri_under_an_optional_file_annotation_is_keyed_by_its_store(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", "e1")})
        expr = read_optional_file(path="s3://bucket/data/a.txt")
        _evaluate_against(store, staging_root=tmp_path / "staging", expr=expr)

        store.put(bucket="bucket", key="data/a.txt", data=b"beta", etag="e2")
        result = _evaluate_against(store, staging_root=tmp_path / "staging", expr=expr)

        assert result == "beta"

    def test_a_staged_input_records_its_content_digest(self, tmp_path) -> None:
        store = _store(tmp_path, {"data/a.txt": (b"alpha", "e1")})
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
