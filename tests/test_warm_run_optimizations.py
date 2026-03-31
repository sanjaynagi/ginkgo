"""Tests for warm-run optimization layers.

Layer 1: Per-run hash memoization (HashMemo)
Layer 2: Artifact identity propagation
Layer 3: Stat-gated output validation (MaterializationLog)
Layer 4: --trust-workspace stat-index fast path
"""

from __future__ import annotations

import time
from pathlib import Path

from ginkgo import evaluate, file, task
from ginkgo.runtime.hash_memo import HashMemo
from ginkgo.runtime.hashing import hash_file
from ginkgo.runtime.materialization_log import MaterializationLog


# ---------------------------------------------------------------------------
# Layer 1: HashMemo
# ---------------------------------------------------------------------------


class TestHashMemo:
    """Per-run hash memoization for files and directories."""

    def test_file_hash_is_memoized(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("hello")
        memo = HashMemo()

        digest1 = memo.hash_file(f)
        digest2 = memo.hash_file(f)
        assert digest1 == digest2
        assert digest1 == hash_file(f)

    def test_file_hash_updates_on_content_change(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("version1")
        memo = HashMemo()

        digest1 = memo.hash_file(f)
        # Sleep briefly to ensure mtime changes.
        time.sleep(0.05)
        f.write_text("version2")
        digest2 = memo.hash_file(f)
        assert digest1 != digest2

    def test_put_file_injects_known_digest(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("content")
        memo = HashMemo()

        memo.put_file(f, "injected_digest")
        assert memo.hash_file(f) == "injected_digest"

    def test_directory_hash_is_memoized(self, tmp_path: Path) -> None:
        d = tmp_path / "dir"
        d.mkdir()
        (d / "a.txt").write_text("a")
        (d / "b.txt").write_text("b")
        memo = HashMemo()

        digest1 = memo.hash_directory(d)
        digest2 = memo.hash_directory(d)
        assert digest1 == digest2

    def test_directory_hash_changes_on_file_modification(self, tmp_path: Path) -> None:
        d = tmp_path / "dir"
        d.mkdir()
        (d / "a.txt").write_text("original")
        memo = HashMemo()

        digest1 = memo.hash_directory(d)
        time.sleep(0.05)
        (d / "a.txt").write_text("modified")
        digest2 = memo.hash_directory(d)
        assert digest1 != digest2


# ---------------------------------------------------------------------------
# Layer 3: MaterializationLog
# ---------------------------------------------------------------------------


class TestMaterializationLog:
    """Stat-gated output validation."""

    def test_record_and_check(self, tmp_path: Path) -> None:
        log_path = tmp_path / "mat.json"
        f = tmp_path / "output.txt"
        f.write_text("data")

        log = MaterializationLog(path=log_path)
        log.record(path=f, artifact_id="abc123")
        assert log.check(path=f, artifact_id="abc123") is True

    def test_check_fails_on_wrong_artifact_id(self, tmp_path: Path) -> None:
        log_path = tmp_path / "mat.json"
        f = tmp_path / "output.txt"
        f.write_text("data")

        log = MaterializationLog(path=log_path)
        log.record(path=f, artifact_id="abc123")
        assert log.check(path=f, artifact_id="wrong_id") is False

    def test_check_fails_after_file_modification(self, tmp_path: Path) -> None:
        log_path = tmp_path / "mat.json"
        f = tmp_path / "output.txt"
        f.write_text("original")

        log = MaterializationLog(path=log_path)
        log.record(path=f, artifact_id="abc123")

        time.sleep(0.05)
        f.write_text("tampered")
        assert log.check(path=f, artifact_id="abc123") is False

    def test_persistence_across_instances(self, tmp_path: Path) -> None:
        log_path = tmp_path / "mat.json"
        f = tmp_path / "output.txt"
        f.write_text("data")

        log1 = MaterializationLog(path=log_path)
        log1.record(path=f, artifact_id="abc123")
        log1.save()

        log2 = MaterializationLog(path=log_path)
        assert log2.check(path=f, artifact_id="abc123") is True

    def test_stale_entries_pruned_on_load(self, tmp_path: Path) -> None:
        log_path = tmp_path / "mat.json"
        f = tmp_path / "output.txt"
        f.write_text("data")

        log1 = MaterializationLog(path=log_path)
        log1.record(path=f, artifact_id="abc123")
        log1.save()

        # Remove the file; the entry should be pruned on next load.
        f.unlink()
        log2 = MaterializationLog(path=log_path)
        assert log2.check(path=f, artifact_id="abc123") is False


# ---------------------------------------------------------------------------
# Integration: warm run with memoization and propagation
# ---------------------------------------------------------------------------


@task()
def produce_file(*, output_path: str) -> file:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("content")
    return output_path


@task()
def consume_file(*, input_file: file) -> int:
    return len(Path(str(input_file)).read_text())


@task()
def make_shared(*, output_path: str) -> file:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("shared data")
    return output_path


@task()
def reader_a(*, input_file: file) -> int:
    return len(Path(str(input_file)).read_text())


@task()
def reader_b(*, input_file: file) -> int:
    return len(Path(str(input_file)).read_text()) * 2


class TestWarmRunIntegration:
    """End-to-end test that layers 1-3 work together."""

    def test_warm_run_caches_all_tasks(self, tmp_path: Path, capsys) -> None:
        """A second run should hit cache for all tasks."""
        output = tmp_path / "output.txt"

        # Cold run.
        result1 = evaluate(consume_file(input_file=produce_file(output_path=str(output))))
        assert result1 == 7  # len("content")

        # Warm run.
        result2 = evaluate(consume_file(input_file=produce_file(output_path=str(output))))
        assert result2 == 7

        captured = capsys.readouterr()
        # On warm run, both tasks should be cached.
        assert captured.err.count('"status": "cached"') >= 2

    def test_file_hash_memoization_reduces_reads(self, tmp_path: Path) -> None:
        """When a file is consumed by multiple tasks, it should be hashed once."""
        output = tmp_path / "shared.txt"

        shared = make_shared(output_path=str(output))
        result = evaluate((reader_a(input_file=shared), reader_b(input_file=shared)))

        assert result[0] == 11  # len("shared data")
        assert result[1] == 22


# ---------------------------------------------------------------------------
# Layer 4: --trust-workspace
# ---------------------------------------------------------------------------


class TestTrustWorkspace:
    """Stat-index fast path for --trust-workspace mode."""

    def test_stat_index_round_trip(self, tmp_path: Path) -> None:
        """Stat index can be saved and loaded."""
        from ginkgo.runtime.cache import CacheStore

        store = CacheStore(root=tmp_path / "cache")
        store.record_stat_index(stat_key="stat_abc", cache_key="content_xyz")
        store.save_stat_index()

        # Reload.
        store2 = CacheStore(root=tmp_path / "cache")
        assert store2._stat_index.get("stat_abc") == "content_xyz"

    def test_trust_workspace_warm_run(self, tmp_path: Path, capsys) -> None:
        """A --trust-workspace warm run should hit cache via stat index."""
        from ginkgo.runtime.evaluator import _ConcurrentEvaluator

        output = tmp_path / "output.txt"

        # Cold run (builds stat index).
        expr1 = consume_file(input_file=produce_file(output_path=str(output)))
        evaluator1 = _ConcurrentEvaluator()
        result1 = evaluator1.evaluate(expr1)
        assert result1 == 7

        # Warm run with trust_workspace.
        expr2 = consume_file(input_file=produce_file(output_path=str(output)))
        evaluator2 = _ConcurrentEvaluator(trust_workspace=True)
        result2 = evaluator2.evaluate(expr2)
        assert result2 == 7

        captured = capsys.readouterr()
        assert '"status": "cached"' in captured.err
