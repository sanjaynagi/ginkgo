"""Tests for warm-run optimization layers.

Layer 1: Per-run hash memoization (HashMemo)
Layer 2: Artifact identity propagation
Layer 3: Stat-gated output validation (the ``materializations`` table)
Layer 4: --trust-mtimes stat-index fast path
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from ginkgo import Out, evaluate, file, task
from ginkgo.core.remote import remote_file
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.core.hashing import hash_file
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.events import TaskCacheHit
from tests.conftest import EventCollector


def _index(tmp_path: Path) -> CacheIndex:
    """Return a cache index over a database in a temporary workspace."""
    return CacheIndex.open(path=tmp_path / "ginkgo.db")


# ---------------------------------------------------------------------------
# Layer 1: HashMemo
# ---------------------------------------------------------------------------


def test_hash_memo_remembers_and_notices_change(tmp_path: Path) -> None:
    f = tmp_path / "data.txt"
    f.write_text("v1")
    d = tmp_path / "dir"
    d.mkdir()
    (d / "a.txt").write_text("a")
    memo = HashMemo()

    file_digest, dir_digest = memo.hash_file(f), memo.hash_directory(d)
    assert file_digest == memo.hash_file(f) == hash_file(f)
    assert dir_digest == memo.hash_directory(d)

    # A size change moves the stat fingerprint without waiting on mtime resolution.
    f.write_text("version2")
    (d / "a.txt").write_text("modified")
    assert memo.hash_file(f) != file_digest
    assert memo.hash_directory(d) != dir_digest

    memo.put_file(f, "injected_digest")
    assert memo.hash_file(f) == "injected_digest"


class TestPersistedDigests:
    """A second run starts warm: digests survive the process (issue #245)."""

    def _folder(self, tmp_path: Path) -> Path:
        folder = tmp_path / "reads"
        folder.mkdir()
        for index in range(64):
            (folder / f"part-{index:03d}.bin").write_bytes(b"x" * 4096)
        return folder

    def test_a_second_run_reads_no_file_under_the_folder(self, tmp_path: Path) -> None:
        folder = self._folder(tmp_path)

        with _index(tmp_path) as index:
            cold = HashMemo(index=index)
            digest = cold.hash_directory(folder)
            assert cold.reads == 1

        with _index(tmp_path) as index:
            warm = HashMemo(index=index)
            assert warm.hash_directory(folder) == digest
            assert warm.reads == 0, "a warm run re-read content it had already hashed"

    def test_a_second_run_re_reads_content_that_moved(self, tmp_path: Path) -> None:
        folder = self._folder(tmp_path)

        with _index(tmp_path) as index:
            cold = HashMemo(index=index)
            digest = cold.hash_directory(folder)

        (folder / "part-000.bin").write_bytes(b"y" * 4097)

        with _index(tmp_path) as index:
            warm = HashMemo(index=index)
            assert warm.hash_directory(folder) != digest
            assert warm.reads == 1

    def test_files_are_remembered_too(self, tmp_path: Path) -> None:
        target = tmp_path / "big.bin"
        target.write_bytes(b"z" * 65536)

        with _index(tmp_path) as index:
            digest = HashMemo(index=index).hash_file(target)

        with _index(tmp_path) as index:
            warm = HashMemo(index=index)
            assert warm.hash_file(target) == digest
            assert warm.reads == 0


# ---------------------------------------------------------------------------
# Layer 3: the materializations table
# ---------------------------------------------------------------------------


def test_materializations_match_only_the_recorded_untouched_path(tmp_path: Path) -> None:
    """Stat-gated output validation, persisted across index instances."""
    f = tmp_path / "output.txt"
    f.write_text("data")

    with _index(tmp_path) as first:
        first.record_materialization(path=f, artifact_id="abc123")
        assert first.materialization_matches(path=f, artifact_id="wrong_id") is False
    with _index(tmp_path) as second:
        assert second.materialization_matches(path=f, artifact_id="abc123") is True
        f.write_text("tampered!")
        assert second.materialization_matches(path=f, artifact_id="abc123") is False
        f.unlink()
        assert second.materialization_matches(path=f, artifact_id="abc123") is False


# ---------------------------------------------------------------------------
# Integration: warm run with memoization and propagation
# ---------------------------------------------------------------------------


@task()
def produce_file(*, output_path: Out[file]) -> file:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("content")
    return output_path


@task()
def consume_file(*, input_file: file) -> int:
    return len(Path(str(input_file)).read_text())


@task()
def make_shared(*, output_path: Out[file]) -> file:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("shared data")
    return output_path


@task()
def reader_a(*, input_file: file) -> int:
    return len(Path(str(input_file)).read_text())


@task()
def reader_b(*, input_file: file) -> int:
    return len(Path(str(input_file)).read_text()) * 2


@task()
def read_versioned_remote_file(*, path: file) -> str:
    return Path(str(path)).read_text()


class TestWarmRunIntegration:
    def test_a_warm_run_hits_every_task_without_dispatch_or_env_preparation(
        self, tmp_path: Path
    ) -> None:
        output = str(tmp_path / "output.txt")
        assert evaluate(consume_file(input_file=produce_file(output_path=output))) == 7

        collector = EventCollector()
        warm = ConcurrentEvaluator(event_bus=collector.bus)
        refuse = MagicMock(side_effect=AssertionError("a warm cached DAG must not run work"))
        warm._start_task_execution = refuse  # type: ignore[method-assign]
        warm._prepare_task_environment = refuse  # type: ignore[method-assign]

        assert warm.evaluate(consume_file(input_file=produce_file(output_path=output))) == 7
        assert len([e for e in collector.events if isinstance(e, TaskCacheHit)]) == 2
        refuse.assert_not_called()

    def test_a_file_read_by_two_tasks_is_hashed_once(self, tmp_path: Path) -> None:
        import ginkgo.runtime.caching.hash_memo as hash_memo_module

        shared = make_shared(output_path=str(tmp_path / "shared.txt"))
        real_hash_file = hash_memo_module.hash_file
        hashed_paths: list[str] = []

        def recording_hash_file(path: Path) -> str:
            hashed_paths.append(str(Path(path).resolve()))
            return real_hash_file(path)

        with patch.object(hash_memo_module, "hash_file", side_effect=recording_hash_file):
            result = evaluate((reader_a(input_file=shared), reader_b(input_file=shared)))

        assert result == (11, 22)
        assert sum(1 for p in hashed_paths if p.endswith("shared.txt")) == 1


# ---------------------------------------------------------------------------
# Layer 4: --trust-mtimes
# ---------------------------------------------------------------------------


class TestTrustMtimes:
    """Stat-index fast path for --trust-mtimes mode."""

    def test_trust_mtimes_warm_run(self, tmp_path: Path) -> None:
        output = str(tmp_path / "output.txt")
        assert (
            ConcurrentEvaluator().evaluate(
                consume_file(input_file=produce_file(output_path=output))
            )
            == 7
        )

        collector = EventCollector()
        warm = ConcurrentEvaluator(trust_mtimes=True, event_bus=collector.bus)
        assert warm.evaluate(consume_file(input_file=produce_file(output_path=output))) == 7
        assert collector.cached()

    def test_versioned_remote_input_warm_run_skips_staging(self, tmp_path: Path) -> None:
        """Pinned remote inputs can hit cache without staging on the warm run."""

        class _FakeStagingCache:
            def __init__(self, *, root: Path, fail: bool = False) -> None:
                self._root = root
                self._fail = fail

            def stage_file(self, *, ref) -> Path:
                if self._fail:
                    raise AssertionError("warm cached remote input should not stage")
                staged = self._root / "staged.txt"
                staged.parent.mkdir(parents=True, exist_ok=True)
                staged.write_text("cacheable")
                return staged

            def stage_folder(self, *, ref) -> Path:
                raise AssertionError("folder staging is not used in this test")

        ref = remote_file("s3://bucket/cacheable.txt", version_id="v1")

        evaluator1 = ConcurrentEvaluator()
        evaluator1._stager._staging_cache = _FakeStagingCache(root=tmp_path / "stage-cold")
        assert evaluator1.evaluate(read_versioned_remote_file(path=ref)) == "cacheable"

        evaluator2 = ConcurrentEvaluator()
        evaluator2._stager._staging_cache = _FakeStagingCache(
            root=tmp_path / "stage-warm", fail=True
        )
        assert evaluator2.evaluate(read_versioned_remote_file(path=ref)) == "cacheable"
