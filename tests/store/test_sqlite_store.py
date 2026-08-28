"""Opening, migrating and writing the provenance store."""

from __future__ import annotations

import multiprocessing
import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from ginkgo.store import open_store
from ginkgo.store import sqlite as sqlite_module
from ginkgo.store.errors import SchemaVersionError, StoreError, StoreLockedError
from ginkgo.store.protocol import ProjectionOp, ProvenanceStore, StoredEvent
from ginkgo.store.schema import (
    MIGRATIONS,
    SCHEMA_VERSION,
    _statements,
    migrate,
    schema_version,
)
from ginkgo.store.sqlite import SqliteStore


def _conformance(store: SqliteStore) -> ProvenanceStore:
    """``SqliteStore`` satisfies the protocol — checked by ``ty``, not at runtime.

    ``isinstance`` against a protocol compares attribute names and would pass
    for a class whose signatures had drifted; this does not.
    """
    return store


def _event(**overrides: Any) -> StoredEvent:
    fields: dict[str, Any] = {
        "run_id": "run_1",
        "ts": "2026-08-28T09:00:00+00:00",
        "type": "run_started",
        "payload": "{}",
    }
    return StoredEvent(**{**fields, **overrides})


class TestOpening:
    """What a write-mode open leaves behind."""

    def test_a_fresh_database_is_migrated_to_the_current_version(self, tmp_path):
        with open_store(tmp_path / "ginkgo.db") as store:
            assert store.schema_version == SCHEMA_VERSION

    def test_a_path_with_a_uri_delimiter_opens_that_file(self, tmp_path):
        """An f-string URI would read ``?name`` as a query and open ``weird``."""
        directory = tmp_path / "weird?name#1"
        directory.mkdir()

        with open_store(directory / "ginkgo.db") as store:
            assert store.schema_version == SCHEMA_VERSION

        assert (directory / "ginkgo.db").exists()
        assert not (tmp_path / "weird").exists()

    def test_an_unwritable_workspace_names_the_database(self, tmp_path):
        workspace = tmp_path / "readonly"
        workspace.mkdir(mode=0o500)
        path = workspace / ".ginkgo" / "ginkgo.db"

        with pytest.raises(StoreError, match=re.escape(str(path))):
            open_store(path)

    def test_the_parent_directory_is_created(self, tmp_path):
        path = tmp_path / ".ginkgo" / "ginkgo.db"

        with open_store(path):
            assert path.exists()

    def test_reopening_applies_nothing(self, tmp_path):
        path = tmp_path / "ginkgo.db"
        with open_store(path):
            pass

        with open_store(path) as store:
            applied = store.query("SELECT version FROM schema_version")

        assert [row["version"] for row in applied] == [version for version, _ in MIGRATIONS]

    def test_pragmas_are_applied(self, tmp_path):
        with open_store(tmp_path / "ginkgo.db") as store:
            assert store.query("PRAGMA journal_mode")[0][0] == "wal"
            assert store.query("PRAGMA busy_timeout")[0][0] == 5000
            assert store.query("PRAGMA foreign_keys")[0][0] == 1

    def test_closing_twice_is_harmless(self, tmp_path):
        store = open_store(tmp_path / "ginkgo.db")
        store.close()

        store.close()


class TestReadOnlyOpening:
    """A reader must never create, migrate, or lock the database."""

    def test_a_missing_database_is_an_error_rather_than_a_new_file(self, tmp_path):
        path = tmp_path / "ginkgo.db"

        with pytest.raises(StoreError):
            open_store(path, readonly=True)

        assert not path.exists()

    def test_a_behind_schema_database_names_the_command_that_migrates_it(self, tmp_path):
        path = tmp_path / "ginkgo.db"
        sqlite3.connect(path).close()

        with pytest.raises(SchemaVersionError, match="ginkgo db migrate"):
            open_store(path, readonly=True)

    def test_a_reader_refuses_to_write(self, tmp_path):
        path = tmp_path / "ginkgo.db"
        open_store(path).close()

        with open_store(path, readonly=True) as store:
            assert store.readonly
            with pytest.raises(StoreError):
                with store.transaction():
                    pass

    def test_a_reader_sees_what_the_writer_committed(self, tmp_path):
        path = tmp_path / "ginkgo.db"
        with open_store(path) as writer:
            with writer.transaction():
                writer.append([_event()])

        with open_store(path, readonly=True) as reader:
            assert len(reader.query("SELECT seq FROM events")) == 1


class TestMigrations:
    """The schema version is a fact about the file, not about this process."""

    def test_an_unmigrated_database_is_at_version_zero(self, tmp_path):
        connection = sqlite3.connect(tmp_path / "ginkgo.db")

        assert schema_version(connection) == 0

    def test_a_schema_version_table_with_no_version_is_reported_clearly(self, tmp_path):
        path = tmp_path / "ginkgo.db"
        connection = sqlite3.connect(path)
        connection.execute("CREATE TABLE schema_version (version INTEGER, applied_at TEXT)")
        connection.commit()

        with pytest.raises(SchemaVersionError, match="ginkgo db migrate"):
            schema_version(connection)

    def test_migrating_twice_changes_nothing(self, tmp_path):
        connection = sqlite3.connect(tmp_path / "ginkgo.db")
        connection.isolation_level = None

        assert migrate(connection) == SCHEMA_VERSION
        assert migrate(connection) == SCHEMA_VERSION

    def test_every_object_in_the_schema_is_created(self, tmp_path):
        """A snapshot of ``sqlite_master``. Regenerate it deliberately."""
        expected = (Path(__file__).parent / "fixtures" / "schema_v3.txt").read_text(
            encoding="utf-8"
        )

        with open_store(tmp_path / "ginkgo.db") as store:
            assert _schema_snapshot(store) == expected

    def test_a_version_1_database_migrates_forward(self, tmp_path):
        """The step runs on an existing database, not only on a fresh one."""
        connection = sqlite3.connect(tmp_path / "ginkgo.db")
        connection.isolation_level = None
        first_version, first_step = MIGRATIONS[0]
        assert isinstance(first_step, str)
        connection.execute("BEGIN IMMEDIATE")
        for statement in _statements(first_step):
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, '2026-08-28')",
            (first_version,),
        )
        connection.execute("COMMIT")
        connection.execute(
            "INSERT INTO artifacts (artifact_id, kind, digest_algorithm, created_at) "
            "VALUES ('a1', 'blob', 'blake3', '2026-08-28')"
        )

        assert migrate(connection) == SCHEMA_VERSION

        columns = {row[1] for row in connection.execute("PRAGMA table_info(artifacts)")}
        assert "digest_hex" in columns
        assert connection.execute("SELECT digest_hex FROM artifacts").fetchone()[0] == ""
        assert not connection.execute(
            "SELECT name FROM sqlite_master WHERE name = 'cache_key_components'"
        ).fetchall()
        memo_columns = {row[1] for row in connection.execute("PRAGMA table_info(digest_memo)")}
        assert memo_columns == {"kind", "fingerprint", "digest", "last_seen"}


class TestWriting:
    """Events and projections land together, or not at all."""

    def test_appended_events_are_numbered_in_emission_order(self, tmp_path):
        with open_store(tmp_path / "ginkgo.db") as store:
            with store.transaction():
                store.append([_event(type="run_started"), _event(type="run_completed")])

            rows = store.query("SELECT seq, type FROM events ORDER BY seq")

        assert [row["type"] for row in rows] == ["run_started", "run_completed"]
        assert [row["seq"] for row in rows] == [1, 2]

    def test_event_columns_are_populated_from_the_row(self, tmp_path):
        with open_store(tmp_path / "ginkgo.db") as store:
            with store.transaction():
                store.append(
                    [
                        _event(
                            type="task_completed",
                            v=2,
                            task_id="task_0007",
                            attempt=1,
                            cache_key="cache_abc",
                            asset_key="table:rows",
                            payload='{"status": "success"}',
                        )
                    ]
                )

            row = store.query("SELECT * FROM events")[0]

        assert (row["task_id"], row["attempt"], row["v"]) == ("task_0007", 1, 2)
        assert (row["cache_key"], row["asset_key"]) == ("cache_abc", "table:rows")
        assert row["payload"] == '{"status": "success"}'

    def test_projection_ops_run_in_the_same_transaction_as_the_events(self, tmp_path):
        with open_store(tmp_path / "ginkgo.db") as store:
            with store.transaction():
                store.append([_event()])
                store.apply([_insert_run("run_1")])

            assert len(store.query("SELECT run_id FROM runs")) == 1

    def test_transactions_do_not_nest(self, tmp_path):
        with open_store(tmp_path / "ginkgo.db") as store:
            with store.transaction():
                with pytest.raises(StoreError, match="do not nest"):
                    with store.transaction():
                        pass

    def test_a_failed_commit_leaves_the_connection_usable(self, tmp_path, monkeypatch):
        """A commit that fails must not strand the connection mid-transaction."""
        with open_store(tmp_path / "ginkgo.db") as store:
            monkeypatch.setattr(store, "_connection", _CommitFails(store._connection))
            with pytest.raises(StoreError):
                with store.transaction():
                    store.append([_event()])
            monkeypatch.undo()

            with store.transaction():
                store.append([_event()])

            assert len(store.query("SELECT seq FROM events")) == 1

    def test_a_second_writer_is_told_which_database_is_locked(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sqlite_module, "BUSY_TIMEOUT_MS", 50)
        path = tmp_path / "ginkgo.db"

        with open_store(path) as holder, open_store(path) as contender:
            with holder.transaction():
                holder.append([_event()])

                with pytest.raises(StoreLockedError, match=re.escape(str(path))):
                    with contender.transaction():
                        pass

    def test_a_failed_transaction_leaves_no_trace(self, tmp_path):
        with open_store(tmp_path / "ginkgo.db") as store:
            with pytest.raises(RuntimeError):
                with store.transaction():
                    store.append([_event()])
                    store.apply([_insert_run("run_1")])
                    raise RuntimeError("something went wrong mid-run")

            assert store.query("SELECT seq FROM events") == []
            assert store.query("SELECT run_id FROM runs") == []


class _CommitFails:
    """A connection whose ``COMMIT`` fails the way a full disk would."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def __getattr__(self, name: str):
        return getattr(self._connection, name)

    def execute(self, sql: str, *args):
        if sql == "COMMIT":
            error = sqlite3.OperationalError("disk I/O error")
            error.sqlite_errorcode = sqlite3.SQLITE_IOERR
            raise error
        return self._connection.execute(sql, *args)


def _insert_run(run_id: str) -> ProjectionOp:
    return ProjectionOp(
        "INSERT INTO runs (run_id, workflow, status, started_at) VALUES (?, ?, ?, ?)",
        (run_id, "workflow/flow.py", "running", "2026-08-28T09:00:00+00:00"),
    )


def _schema_snapshot(store: SqliteStore) -> str:
    """Render ``sqlite_master`` as stable text, one object per paragraph."""
    rows = store.query(
        "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name"
    )
    objects = (f"{row['type']} {row['name']}\n{row['sql'].strip()}" for row in rows)
    return "\n\n".join(objects) + "\n"


def _open_and_report(path: str, barrier: Any, results: Any) -> None:
    """Open the store the instant every other process is ready to as well."""
    from ginkgo.store.sqlite import open_store as _open_store

    barrier.wait(timeout=30)
    try:
        with _open_store(Path(path)) as store:
            results.append(("ok", store.schema_version))
    except BaseException as exc:  # noqa: BLE001 - reported through the assertion
        results.append(("error", f"{type(exc).__name__}: {exc}"))


class TestConcurrentFirstOpen:
    """Two ginkgo processes may reach an empty workspace at the same moment.

    Three things race on a fresh database: creating the file, switching it into
    WAL, and running the migration. Threads do not reproduce it — one finishes
    the migration before the next is off the barrier — so this uses processes,
    which is also what the race is really between.

    It is a race, so it detects rather than proves: with the WAL retry and the
    re-read under the write lock removed it fails about four runs in five, and
    with them it has never failed. It does not replace the subprocess test in
    ``test_concurrent_runs.py``; it makes the same failure cheap to reproduce.
    """

    def test_processes_opening_one_fresh_database_all_succeed(self, tmp_path: Path) -> None:
        context = multiprocessing.get_context("spawn")
        path = tmp_path / "ginkgo.db"
        workers = 8
        barrier = context.Barrier(workers)
        with context.Manager() as manager:
            results = manager.list()
            processes = [
                context.Process(target=_open_and_report, args=(str(path), barrier, results))
                for _ in range(workers)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=60)
            outcomes = list(results)

        assert [outcome for outcome, _ in outcomes] == ["ok"] * workers, outcomes
        assert {detail for _, detail in outcomes} == {SCHEMA_VERSION}

        with open_store(path, readonly=True) as store:
            applied = store.query("SELECT version FROM schema_version")
        # Migrated once, not once per racing opener.
        assert [row["version"] for row in applied] == [version for version, _ in MIGRATIONS]
