"""``ginkgo db``: the maintenance surface for the ledger."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from ginkgo.cli.app import main
from ginkgo.formatting import format_bytes, now_iso
from ginkgo.remote.staging import StagingCache, StagingEntry
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store import sqlite as sqlite_module
from ginkgo.store.maintenance import large_ledger_warning, on_disk_bytes
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.schema import SCHEMA_VERSION
from ginkgo.store.sqlite import open_store
from tests.conftest import run_cli

WS = Path(".ginkgo")
DB = WS / "ginkgo.db"


@pytest.fixture(autouse=True)
def _in_tmp_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _db(capsys, *argv: str) -> tuple[int, str]:
    """Run ``ginkgo db *argv`` in-process; return its exit code and stdout."""
    code = main(["db", *argv])
    return code, capsys.readouterr().out


def _apply(*ops: ProjectionOp) -> None:
    with open_store(DB) as store, store.transaction():
        store.apply(list(ops))


def _run_row(run_id: str, *, finished_at: str | None = None, events: int = 0) -> None:
    """Record one run row, and optionally some events belonging to it."""
    _apply(
        ProjectionOp(
            sql="INSERT INTO runs (run_id, workflow, status, started_at, finished_at) "
            "VALUES (?, 'flow.py', 'succeeded', '2020-01-01T00:00:00+00:00', ?)",
            params=(run_id, finished_at),
        ),
        *(
            ProjectionOp(
                sql="INSERT INTO events (run_id, ts, type, v, payload) "
                "VALUES (?, '2020-01-01T00:00:00+00:00', 'task_started', 1, '{}')",
                params=(run_id,),
            )
            for _ in range(events)
        ),
    )


def _entry(cache_key: str, *, output: str | None = "{}") -> None:
    """Record one cache entry, with ``output`` as its bytes (``None``: no bytes)."""
    if output is not None:
        (WS / "cache" / cache_key).mkdir(parents=True)
        (WS / "cache" / cache_key / "output.json").write_text(output, encoding="utf-8")
    with CacheIndex.open(path=DB) as index:
        index.record_entry(
            cache_key=cache_key,
            meta={"function": "pkg.produce", "created_at": "2026-08-28T10:00:00+00:00"},
            artifact_ids={},
            size_bytes=2,
            run_id=None,
        )


def _artifact(artifact_id: str, size: int = 4) -> None:
    with CacheIndex.open(path=DB) as index:
        index.record_artifact(
            ArtifactRecord(
                artifact_id=artifact_id,
                kind="blob",
                digest_algorithm="blake3",
                digest_hex=artifact_id,
                extension="",
                size=size,
                created_at=now_iso(),
                storage_backend="local",
            )
        )


def _staged(root: Path | None = None, *, db_path: Path | None = None) -> None:
    cache = StagingCache(root=root) if root is not None else StagingCache(db_path=db_path)
    cache.index.record(
        StagingEntry(
            uri="s3://bucket/key",
            digest="deadbeef",
            etag=None,
            version_id=None,
            size=5,
            staged_at="2026-08-28T10:00:00+00:00",
            blob_path="blobs/deadbeef",
        )
    )
    cache.close()


def _mkdir(*parts: str) -> Path:
    path = WS.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _orphan_cache_dir() -> None:
    _entry("key-1")
    (_mkdir("cache", "orphan-1") / "output.json").write_text("{}", encoding="utf-8")


def _missing_cache_artifact() -> None:
    _entry("key-1")
    _artifact("artifact-1")
    _apply(
        ProjectionOp(
            sql="INSERT INTO cache_artifacts (cache_key, path, artifact_id) "
            "VALUES ('key-1', '/out/a.txt', 'artifact-1')"
        )
    )


def _truncated_output_then_a_later_problem() -> None:
    _entry("key-1", output='{"__ginkgo_type__": "asse')
    _run_row("run-1")  # a later check, which must still be reached


def _orphan_blob() -> None:
    open_store(DB).close()
    (_mkdir("artifacts", "blobs") / "abc123").write_bytes(b"orphaned")


def _env_drift(hosts: tuple[str, str]) -> Callable[[], None]:
    def record() -> None:
        with CacheIndex.open(path=DB) as index:
            for host, digest in zip(hosts, ("digest-a", "digest-b")):
                index.record_env_materialization(
                    env_hash="env-1", host=host, materialized_digest=digest
                )

    return record


def _run_with_its_directory() -> None:
    _run_row("run-1")
    _mkdir("runs", "run-1")


def _orphan_run_directory() -> None:
    open_store(DB).close()
    _mkdir("runs", "run-2")


def _save_in_flight() -> None:
    """A concurrent save's temporary directory is about to be renamed."""
    _entry("key-1")
    (_mkdir("cache", "key-2.tmp-abc123") / "output.json").write_text("{}", encoding="utf-8")


def _relocated_staging_root(monkeypatch) -> None:
    elsewhere = Path("scratch") / "staging"
    monkeypatch.setenv("GINKGO_STAGING_ROOT", str(elsewhere.resolve()))
    (elsewhere / "blobs").mkdir(parents=True)
    (elsewhere / "blobs" / "deadbeef").write_bytes(b"bytes")
    _staged(db_path=DB.resolve())


@pytest.mark.parametrize(
    ("setup", "code", "phrases"),
    [
        (
            lambda: _entry("key-1", output=None),
            1,
            ["cache entry key-1 has a row but no output.json"],
        ),
        (_orphan_cache_dir, 1, ["cache directory orphan-1 is not referenced by the ledger"]),
        (_missing_cache_artifact, 1, ["cache artifact artifact-1 is missing"]),
        (
            _truncated_output_then_a_later_problem,
            1,
            [
                "cache entry key-1 has an unreadable output.json",
                "run run-1 has a row but no run directory",
            ],
        ),
        (
            lambda: _entry("key-1", output=json.dumps({"__ginkgo_type__": "asset_ref"})),
            1,
            ["cache entry key-1 has an unreadable output.json"],
        ),
        (_orphan_run_directory, 1, ["run directory run-2 is not referenced", "db check --fix"]),
        (lambda: _staged(WS / "staging"), 1, ["staged s3://bucket/key has a row but no bytes"]),
        (
            _orphan_blob,
            1,
            [
                "artifact file blobs/abc123 is not referenced by the ledger",
                "(the ledger may have been reset)",
            ],
        ),
        (_env_drift(("alpha", "beta")), 1, ["environment env-1 materialized 2 different ways"]),
        # One row per host: the latest observation replaces the previous one.
        (_env_drift(("alpha", "alpha")), 0, ["integrity check passed"]),
        (lambda: _entry("key-1"), 0, ["integrity check passed"]),
        (_run_with_its_directory, 0, ["integrity check passed"]),
        (_save_in_flight, 0, ["integrity check passed", "!orphan"]),
        (
            lambda: open_store(DB).close(),
            0,
            [f"Schema version: {SCHEMA_VERSION}", "integrity check passed"],
        ),
        (
            lambda: _mkdir("runs", "20260101_000000_000000_deadbeef"),
            0,
            [
                "1 run directory under .ginkgo/runs but no database",
                "!nothing has run in this workspace",
            ],
        ),
        (
            lambda: (_mkdir("runs", "run-1"), _mkdir("runs", "run-2")),
            0,
            ["2 run directories under .ginkgo/runs"],
        ),
        # An initialized workspace with no runs in it is not a fault.
        (lambda: _mkdir("runs"), 0, ["nothing has run in this workspace"]),
    ],
    ids=[
        "row-without-output",
        "orphan-cache-directory",
        "missing-cache-artifact",
        "truncated-output-then-checks-go-on",
        "retired-codec",
        "orphan-run-directory",
        "staged-without-bytes",
        "orphan-blob",
        "env-drift-between-hosts",
        "one-host-reinstalling",
        "consistent-cache",
        "run-with-directory",
        "save-in-flight",
        "empty-database",
        "runs-outlived-database",
        "every-orphaned-run-counted",
        "initialized-workspace",
    ],
)
def test_db_check_reports_how_the_ledger_and_the_bytes_disagree(capsys, setup, code, phrases):
    setup()

    exit_code, output = _db(capsys, "check")

    assert exit_code == code, output
    for phrase in phrases:
        if phrase.startswith("!"):
            assert phrase[1:] not in output
        else:
            assert phrase in output


def test_db_check_finds_a_relocated_staging_root(capsys, monkeypatch):
    """`db check` must find staged bytes wherever they were configured."""
    _relocated_staging_root(monkeypatch)

    code, output = _db(capsys, "check")
    assert code == 0 and "integrity check passed" in output


def test_db_check_on_an_empty_workspace_creates_nothing(capsys, tmp_path):
    """Creating the database is a run's job. Not a check's."""
    code, output = _db(capsys, "check")

    assert code == 0
    assert "nothing has run in this workspace" in output
    assert not (tmp_path / ".ginkgo").exists()


def test_db_check_never_opens_a_write_connection(monkeypatch):
    """A read path must not be able to change a database out from under a run."""
    open_store(DB).close()
    opened: list[bool] = []
    real_open = sqlite_module.SqliteStore.open

    def _record(cls, path, *, readonly=False, thread_shared=False):
        opened.append(readonly)
        return real_open(path, readonly=readonly, thread_shared=thread_shared)

    monkeypatch.setattr(sqlite_module.SqliteStore, "open", classmethod(_record))

    assert main(["db", "check"]) == 0
    assert opened and all(opened)


def test_db_check_verify_reports_a_blob_corrupted_in_place(capsys):
    open_store(DB).close()
    (_mkdir("artifacts", "blobs") / "abc123").write_bytes(b"not the bytes abc123 names")
    _artifact("abc123", size=26)

    assert _db(capsys, "check")[0] == 0
    code, output = _db(capsys, "check", "--verify")
    assert code == 1
    assert "artifact abc123: bytes do not match its digest" in output


def test_db_check_fix_removes_what_nothing_can_use(capsys):
    """EV5-11/ED5-7: stranded run directories, rows without bytes, orphan cache dirs."""
    _run_with_its_directory()
    stranded = _mkdir("runs", "run-gone", "logs").parent
    _entry("kept")
    _entry("no-bytes", output=None)
    orphan = _mkdir("cache", "orphan")

    code, output = _db(capsys, "check", "--fix")

    assert code == 0
    assert "removed 1 run directory the ledger has no row for" in output
    assert "removed 1 cache entry whose bytes are missing or unreadable" in output
    assert "removed 1 cache directory the ledger has no row for" in output
    assert not stranded.exists() and not orphan.exists()
    assert (WS / "runs" / "run-1").is_dir()
    assert (WS / "cache" / "kept" / "output.json").is_file()
    assert _db(capsys, "check")[0] == 0


def test_db_check_fix_after_a_ledger_was_moved_aside(capsys):
    _mkdir("runs", "run-1")
    (WS / "ginkgo.db-wal").write_bytes(b"stale")
    (WS / "ginkgo.db-shm").write_bytes(b"stale")

    code, output = _db(capsys, "check", "--fix")

    assert code == 0
    assert "removed ginkgo.db-wal, ginkgo.db-shm, left behind without ginkgo.db" in output
    assert "removed 1 run directory the ledger has no row for" in output
    assert list(WS.iterdir()) == [WS / "runs"]


def test_db_path_and_the_database_override(capsys, monkeypatch, tmp_path):
    assert _db(capsys, "path") == (0, ".ginkgo/ginkgo.db\n")

    elsewhere = tmp_path / "local" / "ledger.db"
    monkeypatch.setenv("GINKGO_DB", str(elsewhere))
    assert _db(capsys, "vacuum")[0] == 0

    assert elsewhere.exists()
    assert not (tmp_path / ".ginkgo").exists()


@pytest.mark.parametrize(
    ("finished_at", "argv", "phrase", "remaining"),
    [
        ("2020-01-01T00:00:00+00:00", (), "deleted 3 event rows", 0),
        (None, (), "deleted 0 event rows", 3),  # an unfinished run keeps its events
        ("recent", (), "deleted 0 event rows", 3),
        ("2020-01-01T00:00:00+00:00", ("--dry-run",), "would delete 3 event rows", 3),
    ],
    ids=["finished", "unfinished", "recent", "dry-run"],
)
def test_db_prune_deletes_the_events_of_old_finished_runs(
    capsys, finished_at, argv, phrase, remaining
):
    _run_row("run-1", finished_at=now_iso() if finished_at == "recent" else finished_at, events=3)

    code, output = _db(capsys, "prune", "--events-older-than", "1d", *argv)

    assert code == 0 and phrase in output
    with open_store(DB) as store:
        assert store.query("SELECT count(*) AS n FROM events")[0]["n"] == remaining
        # The projection the events fed is untouched.
        assert store.query("SELECT count(*) AS n FROM runs")[0]["n"] == 1


def test_db_prune_ages_the_digest_memo_on_last_seen(capsys):
    _apply(
        *(
            ProjectionOp(
                sql="INSERT INTO digest_memo (kind, fingerprint, digest, last_seen) "
                "VALUES (?, ?, ?, ?)",
                params=("file", f"fp-{n}", "d", stamp),
            )
            for n, stamp in enumerate(("2020-01-01T00:00:00+00:00", now_iso()))
        )
    )

    code, output = _db(capsys, "prune", "--digest-memo-older-than", "1d")
    assert code == 0 and "deleted 1 digest memo row" in output


def test_db_prune_needs_a_valid_cutoff(capsys):
    assert main(["db", "prune"]) == 2
    assert "Provide --events-older-than" in capsys.readouterr().err

    with pytest.raises(SystemExit) as exited:
        main(["db", "prune", "--events-older-than", "soon"])
    assert exited.value.code == 2
    assert "Invalid duration" in capsys.readouterr().err


def test_db_vacuum_reports_the_size_either_side(capsys):
    assert _db(capsys, "vacuum")[0] == 0
    code, output = _db(capsys, "vacuum")
    # An unchanged size is the only signal SQLite gives that it did nothing.
    assert code == 0 and "no space reclaimed" in output

    _run_row("run-1", finished_at="2020-01-01T00:00:00+00:00", events=2000)
    _db(capsys, "prune", "--events-older-than", "1d")
    code, output = _db(capsys, "vacuum")
    assert code == 0
    assert f"→ {format_bytes(on_disk_bytes(DB))} on disk" in output
    assert "write-ahead log included" in output


def test_a_large_ledger_is_named_with_its_size():
    open_store(DB).close()

    assert large_ledger_warning(DB) is None
    warning = large_ledger_warning(DB, threshold=1)
    assert warning is not None and format_bytes(on_disk_bytes(DB)) in warning


_ONE_TASK = (
    "from ginkgo import flow, task\n\n@task()\ndef one() -> int:\n    return 1\n\n"
    "@flow\ndef main():\n    return one()\n"
)


@pytest.mark.parametrize(
    ("toml", "old_events_kept"),
    [('[ledger]\nkeep_events_for = "30d"\n', False), ("", True)],
    ids=["setting", "no-setting"],
)
def test_a_run_prunes_old_events_only_when_the_workspace_opts_in(tmp_path, toml, old_events_kept):
    """SC5-4: ``[ledger] keep_events_for`` opts a workspace into pruning."""
    _run_row("old", finished_at="2020-01-02T00:00:00+00:00", events=3)
    (tmp_path / "ginkgo.toml").write_text(toml, encoding="utf-8")
    (tmp_path / "workflow.py").write_text(_ONE_TASK, encoding="utf-8")

    result = run_cli("run", "workflow.py")

    assert result.returncode == 0, result.stderr
    with open_store(DB, readonly=True) as store:
        runs = {row["run_id"] for row in store.query("SELECT DISTINCT run_id FROM events")}
    assert ("old" in runs) is old_events_kept
    assert len(runs) == (2 if old_events_kept else 1)


def test_a_malformed_retention_setting_fails_before_running(tmp_path):
    (tmp_path / "ginkgo.toml").write_text('[ledger]\nkeep_events_for = "soon"\n')
    (tmp_path / "workflow.py").write_text(_ONE_TASK, encoding="utf-8")

    result = run_cli("run", "workflow.py")

    assert result.returncode == 1
    assert "[ledger] keep_events_for: Invalid duration 'soon'" in result.stderr
