"""An environment variable a task reads by a literal name is part of its cache key."""

from __future__ import annotations

import os
from os import getenv
from pathlib import Path

import pytest

from ginkgo import task
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.environ_reads import environ_digests, environ_reads
from ginkgo.runtime.caching.index import CacheIndex

SCALE = float(os.environ.get("R5_SCALE", "1"))
DERIVED = SCALE * 2


def _threshold() -> str | None:
    return os.getenv("R5_THRESHOLD")


@task()
def literal_reads() -> str:
    a = os.environ["R5_A"]
    b = os.environ.get("R5_B", "")
    c = getenv("R5_C") or ""
    d = "R5_D" in os.environ
    return f"{a}{b}{c}{d}"


@task()
def unnamed_read(key: str) -> str:
    return os.environ[key]


@task()
def module_level_read() -> float:
    return DERIVED


@task()
def helper_read() -> str:
    return _threshold() or ""


@task()
def write_only() -> None:
    os.environ["R5_WRITTEN"] = "1"


@task()
def reads_nothing(value: int) -> int:
    return value


class TestEnvironReads:
    def test_literal_reads_in_the_body_are_named(self) -> None:
        reads = environ_reads(literal_reads.fn)

        assert reads.names == {"R5_A", "R5_B", "R5_C", "R5_D"}
        assert not reads.unresolved

    def test_a_read_without_a_literal_name_is_unresolved(self) -> None:
        reads = environ_reads(unnamed_read.fn)

        assert not reads.names
        assert reads.unresolved == {"unnamed_read"}

    def test_a_module_level_read_the_task_uses_is_named(self) -> None:
        assert environ_reads(module_level_read.fn).names == {"R5_SCALE"}

    def test_a_read_in_a_helper_the_task_calls_is_named(self) -> None:
        assert environ_reads(helper_read.fn).names == {"R5_THRESHOLD"}

    def test_a_write_is_not_a_read(self) -> None:
        reads = environ_reads(write_only.fn)

        assert not reads.names
        assert not reads.unresolved

    def test_a_task_that_reads_nothing_has_no_digests(self) -> None:
        assert environ_digests(fn=reads_nothing.fn) == {}


class TestEnvironDigests:
    def test_digest_tracks_the_value_without_storing_it(self, monkeypatch) -> None:
        monkeypatch.delenv("R5_THRESHOLD", raising=False)
        unset = environ_digests(fn=helper_read.fn)
        monkeypatch.setenv("R5_THRESHOLD", "secret-value")
        first = environ_digests(fn=helper_read.fn)
        monkeypatch.setenv("R5_THRESHOLD", "other")
        second = environ_digests(fn=helper_read.fn)

        assert unset == {"R5_THRESHOLD": None}
        assert first["R5_THRESHOLD"] not in (None, "secret-value")
        assert first != second


class TestCacheKey:
    @pytest.fixture
    def store(self, tmp_path: Path):
        index = CacheIndex.open(path=tmp_path / "ginkgo.db")
        yield CacheStore(index=index, root=tmp_path / "cache")
        index.close()

    def test_changing_a_read_variable_changes_the_key(self, store, monkeypatch) -> None:
        monkeypatch.setenv("R5_THRESHOLD", "a")
        first, _ = store.build_cache_key(task_def=helper_read, resolved_args={})
        monkeypatch.setenv("R5_THRESHOLD", "b")
        second, _ = store.build_cache_key(task_def=helper_read, resolved_args={})

        assert first != second

    def test_a_task_that_reads_nothing_keeps_its_key(self, store, monkeypatch) -> None:
        first, _ = store.build_cache_key(task_def=reads_nothing, resolved_args={"value": 1})
        monkeypatch.setenv("R5_THRESHOLD", "changed")
        second, _ = store.build_cache_key(task_def=reads_nothing, resolved_args={"value": 1})

        assert first == second

    def test_a_miss_is_explained_as_an_environment_change(self, store, monkeypatch) -> None:
        monkeypatch.setenv("R5_THRESHOLD", "a")
        cache_key, input_hashes = store.build_cache_key(task_def=helper_read, resolved_args={})
        store.save(
            cache_key=cache_key,
            result="a",
            task_def=helper_read,
            resolved_args={},
            input_hashes=input_hashes,
        )
        monkeypatch.setenv("R5_THRESHOLD", "b")
        cache_key, input_hashes = store.build_cache_key(task_def=helper_read, resolved_args={})

        reasons = store.miss_reasons(
            cache_key=cache_key, task_def=helper_read, input_hashes=input_hashes
        )

        assert reasons == ["environ_changed"]
