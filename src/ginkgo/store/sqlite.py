"""The SQLite-backed provenance store.

One database per workspace, at :attr:`~ginkgo.workspace_layout.WorkspaceLayout.db`.
A write-mode open creates and migrates it; a read-only open never does either,
so ``ginkgo cache ls`` can run against a workspace mid-run without taking a
lock or racing the writer's migration.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ginkgo.store.errors import SchemaVersionError, StoreError, StoreLockedError
from ginkgo.store.fs import warn_if_network_filesystem
from ginkgo.store.protocol import ProjectionOp, StoredEvent
from ginkgo.store.schema import SCHEMA_VERSION, migrate, schema_version

__all__ = ["BUSY_TIMEOUT_MS", "SqliteStore", "open_store"]


BUSY_TIMEOUT_MS = 5000
"""How long a write waits for another process's lock before giving up."""

_APPEND_EVENT = """
INSERT INTO events (run_id, ts, type, v, task_id, attempt, cache_key, asset_key, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class SqliteStore:
    """A :class:`~ginkgo.store.protocol.ProvenanceStore` over one SQLite file.

    Construct through :meth:`open`, which is where the pragmas, the migration
    and the network-filesystem check happen. The instance owns its connection
    and is not safe to share across threads; a process that writes from a
    background thread gives that thread its own store.
    """

    def __init__(self, *, path: Path, connection: sqlite3.Connection, readonly: bool) -> None:
        self._path = path
        self._connection = connection
        self._readonly = readonly
        self._closed = False

    @classmethod
    def open(cls, path: Path, *, readonly: bool = False) -> SqliteStore:
        """Open the database at *path*.

        A write-mode open creates the parent directory and the file if they are
        missing and brings the schema up to date. A read-only open requires
        both to exist already and refuses a database the current ginkgo would
        have to migrate, rather than reading rows it may misinterpret.

        Parameters
        ----------
        path : Path
            The database file.
        readonly : bool, optional
            Open through a ``mode=ro`` URI, for CLI read paths.

        Returns
        -------
        SqliteStore
            An open store.

        Raises
        ------
        StoreError
            If the database cannot be opened.
        SchemaVersionError
            If a read-only open finds a schema older than this ginkgo's.
        """
        if readonly:
            connection = cls._connect(f"file:{path}?mode=ro", path=path)
            _apply_pragmas(connection, writable=False)
            store = cls(path=path, connection=connection, readonly=True)
            found = schema_version(connection)
            if found < SCHEMA_VERSION:
                store.close()
                raise SchemaVersionError(path=path, found=found, expected=SCHEMA_VERSION)
            return store

        path.parent.mkdir(parents=True, exist_ok=True)
        warn_if_network_filesystem(path)
        connection = cls._connect(f"file:{path}", path=path)
        _apply_pragmas(connection, writable=True)
        migrate(connection)
        return cls(path=path, connection=connection, readonly=False)

    @staticmethod
    def _connect(uri: str, *, path: Path) -> sqlite3.Connection:
        """Connect to *uri*, reporting failure as a :class:`StoreError`."""
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=BUSY_TIMEOUT_MS / 1000)
        except sqlite3.Error as exc:
            raise StoreError(f"Cannot open the provenance store at {path}: {exc}") from exc
        connection.row_factory = sqlite3.Row
        # Transaction control is explicit throughout the store, so the driver's
        # implicit BEGIN before every statement is turned off: it would open a
        # transaction that neither transaction() nor migrate() knows about.
        connection.isolation_level = None
        return connection

    @property
    def path(self) -> Path:
        """The database this store is backed by."""
        return self._path

    @property
    def readonly(self) -> bool:
        """Whether this store refuses writes."""
        return self._readonly

    @property
    def connection(self) -> sqlite3.Connection:
        """The underlying connection, for callers writing their own SQL."""
        return self._connection

    @property
    def schema_version(self) -> int:
        """The schema version the database is at."""
        return schema_version(self._connection)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Commit everything written inside the block, or none of it.

        ``BEGIN IMMEDIATE`` takes the write lock up front, so a concurrent
        writer is reported as a :class:`StoreLockedError` at the start of the
        transaction rather than part-way through it.

        Raises
        ------
        StoreError
            If the store is read-only.
        StoreLockedError
            If another process held the write lock past the busy timeout.
        """
        if self._readonly:
            raise StoreError(f"The provenance store at {self._path} is open read-only.")
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise self._locked_or(exc) from exc
        try:
            yield
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        try:
            self._connection.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            raise self._locked_or(exc) from exc

    def append(self, events: Iterable[StoredEvent]) -> None:
        """Append *events* to the ledger, in the order given.

        Parameters
        ----------
        events : Iterable[StoredEvent]
            The rows to insert. ``seq`` is assigned by SQLite, so insertion
            order is the ledger's order.
        """
        self._connection.executemany(
            _APPEND_EVENT,
            [
                (
                    event.run_id,
                    event.ts,
                    event.type,
                    event.v,
                    event.task_id,
                    event.attempt,
                    event.cache_key,
                    event.asset_key,
                    event.payload,
                )
                for event in events
            ],
        )

    def apply(self, projection_ops: Iterable[ProjectionOp]) -> None:
        """Run *projection_ops* in the order given."""
        for op in projection_ops:
            self._connection.execute(op.sql, tuple(op.params))

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        """Return every row *sql* selects."""
        return self._connection.execute(sql, tuple(params)).fetchall()

    def close(self) -> None:
        """Release the connection. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        self._connection.close()

    def __enter__(self) -> SqliteStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _locked_or(self, exc: sqlite3.OperationalError) -> Exception:
        """Return the error to raise for *exc*, naming the lock when that is it."""
        if "locked" in str(exc) or "busy" in str(exc):
            return StoreLockedError(path=self._path, timeout_ms=BUSY_TIMEOUT_MS)
        return StoreError(f"Provenance store write failed at {self._path}: {exc}")


def open_store(path: Path, *, readonly: bool = False) -> SqliteStore:
    """Open the provenance store at *path*.

    The one entry point the rest of ginkgo uses, so that swapping the backend
    is one edit here rather than at every call site.

    Parameters
    ----------
    path : Path
        The database file, normally ``WorkspaceLayout.db``.
    readonly : bool, optional
        Open a reader that never migrates and never takes a write lock.

    Returns
    -------
    SqliteStore
        An open store.
    """
    return SqliteStore.open(path, readonly=readonly)


def _apply_pragmas(connection: sqlite3.Connection, *, writable: bool) -> None:
    """Configure *connection*.

    ``journal_mode`` is a property of the database file rather than of the
    connection, so a read-only connection cannot set it — the writer that
    created the file already did.
    """
    if writable:
        connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA temp_store=MEMORY")
