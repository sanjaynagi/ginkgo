"""The SQLite-backed provenance store.

One database per workspace, at :attr:`~ginkgo.workspace_layout.WorkspaceLayout.db`.
A write-mode open creates and migrates it; a read-only open never does either,
so ``ginkgo cache ls`` can run against a workspace mid-run without taking a
lock or racing the writer's migration.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ginkgo.store.errors import SchemaVersionError, StoreError, StoreLockedError
from ginkgo.store.fs import warn_if_network_filesystem
from ginkgo.store.protocol import ProjectionOp, StoredEvent
from ginkgo.store.schema import SCHEMA_VERSION, migrate, schema_version

__all__ = ["BUSY_TIMEOUT_MS", "MEMORY", "SqliteStore", "open_store"]


MEMORY = Path(":memory:")
"""A database that exists only for this process, and touches no filesystem.

A remote worker has a workspace's artifacts but not its database, and a reader
of a workspace that has never been run has no database to read. Both want a
store that answers "nothing" and records what it is told without leaving a file
behind.
"""


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
    def open(
        cls, path: Path, *, readonly: bool = False, thread_shared: bool = False
    ) -> SqliteStore:
        """Open the database at *path*.

        A write-mode open creates the parent directory and the file if they are
        missing and brings the schema up to date. A read-only open requires
        both to exist already, never creates or migrates anything, and refuses
        a database the current ginkgo would have to migrate rather than reading
        rows it may misinterpret. :data:`MEMORY` opens a private in-process
        database instead, leaving the filesystem alone.

        Parameters
        ----------
        path : Path
            The database file.
        readonly : bool, optional
            Open through a ``mode=ro`` URI, for CLI read paths.
        thread_shared : bool, optional
            Let threads other than the opener use the connection. The caller
            must then serialise every use of the store itself; SQLite's own
            check is what this turns off, not the reason for it.

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
            connection = cls._connect(
                _uri(path, readonly=True), path=path, thread_shared=thread_shared
            )
            _apply_pragmas(connection, writable=False)
            store = cls(path=path, connection=connection, readonly=True)
            found = schema_version(connection)
            if found < SCHEMA_VERSION:
                store.close()
                raise SchemaVersionError(path=path, found=found, expected=SCHEMA_VERSION)
            return store

        if path != MEMORY:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                warn_if_network_filesystem(path)
            except OSError as exc:
                raise StoreError(f"Cannot open the provenance store at {path}: {exc}") from exc
        connection = cls._connect(
            _uri(path, readonly=False), path=path, thread_shared=thread_shared
        )
        store = cls(path=path, connection=connection, readonly=False)
        try:
            _apply_pragmas(connection, writable=True)
            migrate(connection)
        except sqlite3.OperationalError as exc:
            # A second ginkgo process may be creating the same database right
            # now. Reported as a lock rather than as a raw driver message, so
            # the user is told which database and what to do about it.
            store.close()
            raise store._locked_or(exc) from exc
        return store

    @staticmethod
    def _connect(uri: str, *, path: Path, thread_shared: bool = False) -> sqlite3.Connection:
        """Connect to *uri*, reporting failure as a :class:`StoreError`."""
        try:
            connection = sqlite3.connect(
                uri,
                uri=True,
                timeout=BUSY_TIMEOUT_MS / 1000,
                check_same_thread=not thread_shared,
            )
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
    def schema_version(self) -> int:
        """The schema version the database is at."""
        return schema_version(self._connection)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Commit everything written inside the block, or none of it.

        ``BEGIN IMMEDIATE`` takes the write lock up front, so a concurrent
        writer is reported as a :class:`StoreLockedError` at the start of the
        transaction rather than part-way through it.

        Transactions do not nest: SQLite has no nested commit, so an inner
        block that "committed" would leave the outer one able to roll its work
        back. That is refused rather than approximated.

        Raises
        ------
        StoreError
            If the store is read-only, or a transaction is already open.
        StoreLockedError
            If another process held the write lock past the busy timeout.
        """
        if self._readonly:
            raise StoreError(f"The provenance store at {self._path} is open read-only.")
        if self._connection.in_transaction:
            raise StoreError(
                f"A transaction is already open on the provenance store at {self._path}; "
                "transactions do not nest."
            )
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise self._locked_or(exc) from exc
        try:
            yield
        except BaseException:
            self._rollback()
            raise
        try:
            self._connection.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            # Without this the connection stays inside the failed transaction,
            # and every later transaction() on it fails as a nested one.
            self._rollback()
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

    def restrict_to_reads(self) -> None:
        """Refuse every further write on this connection.

        For the in-memory ledger a reader opens when a workspace has none: the
        schema has to be created, so the open is write-mode, but nothing after
        that may write — least of all the user SQL :meth:`select` carries. A
        read-only open has ``query_only`` from its pragmas already; this is how
        a store that had to be created earns the same guarantee.
        """
        self._connection.execute("PRAGMA query_only=ON")
        self._readonly = True

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        """Return every row *sql* selects."""
        return self._connection.execute(sql, tuple(params)).fetchall()

    def select(
        self, sql: str, params: Sequence[Any] = (), *, limit: int | None = None
    ) -> tuple[tuple[str, ...], list[sqlite3.Row]]:
        """Return the columns *sql* names and at most *limit* of its rows.

        What :meth:`query` cannot answer for SQL the store did not write: an
        empty result still has to say which columns it has, and a statement
        nobody reviewed has to be stopped before it materialises a million
        rows. Ginkgo's own queries know both already and use :meth:`query`.

        Parameters
        ----------
        sql : str
            One statement.
        params : Sequence[Any], optional
            Values for its placeholders.
        limit : int | None, optional
            Stop after this many rows. Unlimited when omitted.

        Returns
        -------
        tuple[tuple[str, ...], list[sqlite3.Row]]
            The column names, and the rows. One row beyond *limit* is fetched
            so the caller can tell a full result from a truncated one.
        """
        cursor = self._connection.execute(sql, tuple(params))
        try:
            rows = cursor.fetchall() if limit is None else cursor.fetchmany(limit + 1)
            columns = tuple(str(column[0]) for column in cursor.description or ())
        finally:
            cursor.close()
        return columns, rows

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

    def _rollback(self) -> None:
        """Abandon the open transaction, if the connection still has one."""
        if self._connection.in_transaction:
            self._connection.execute("ROLLBACK")

    def _locked_or(self, exc: sqlite3.OperationalError) -> Exception:
        """Return the error to raise for *exc*, naming the lock when that is it."""
        if exc.sqlite_errorcode in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            return StoreLockedError(path=self._path, timeout_ms=BUSY_TIMEOUT_MS)
        return StoreError(f"Provenance store write failed at {self._path}: {exc}")


def open_store(path: Path, *, readonly: bool = False, thread_shared: bool = False) -> SqliteStore:
    """Open the provenance store at *path*.

    The one entry point the rest of ginkgo uses, so that swapping the backend
    is one edit here rather than at every call site.

    Parameters
    ----------
    path : Path
        The database file, normally ``WorkspaceLayout.db``.
    readonly : bool, optional
        Open a reader that never migrates and never takes a write lock.
    thread_shared : bool, optional
        Let threads other than the opener use the connection, on the promise
        that the caller serialises them.

    Returns
    -------
    SqliteStore
        An open store.
    """
    return SqliteStore.open(path, readonly=readonly, thread_shared=thread_shared)


def _uri(path: Path, *, readonly: bool) -> str:
    """Return the ``file:`` URI for *path*.

    Built through :meth:`~pathlib.Path.as_uri` rather than by formatting, so a
    workspace under a directory named ``report?draft`` opens that database
    instead of silently opening ``report`` with a query string.
    """
    if Path(path) == MEMORY:
        return "file::memory:"
    uri = Path(path).absolute().as_uri()
    return f"{uri}?mode=ro" if readonly else uri


def _apply_pragmas(connection: sqlite3.Connection, *, writable: bool) -> None:
    """Configure *connection*.

    ``busy_timeout`` goes first so every pragma after it waits for a
    concurrent writer instead of failing on one.

    ``query_only`` closes a read-only connection to writes from the inside;
    see :meth:`SqliteStore.select`.

    ``journal_mode`` is a property of the database file rather than of the
    connection, so a read-only connection cannot set it — the writer that
    created the file already did. Switching it also takes a lock no busy
    handler covers, so it is set only when the file is not already in WAL,
    which is every open after the first.
    """
    connection.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    if writable and _journal_mode(connection) != "wal":
        _enable_wal(connection)
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA temp_store=MEMORY")
    if not writable:
        # A reader carries user-supplied SQL (``ginkgo query``), and mode=ro is
        # a property of this connection's URI alone. query_only refuses a write
        # inside the engine as well, so neither guard is the only one.
        connection.execute("PRAGMA query_only=ON")


def _journal_mode(connection: sqlite3.Connection) -> str:
    """Return the journal mode the database file is in."""
    row = connection.execute("PRAGMA journal_mode").fetchone()
    return str(row[0]).lower() if row is not None else ""


def _enable_wal(connection: sqlite3.Connection) -> None:
    """Switch the database into WAL, waiting out a concurrent first open.

    The switch needs a brief exclusive lock, and SQLite does not run the busy
    handler while it waits for one — so two ginkgo processes starting against
    the same empty workspace would otherwise race, and the loser would fail
    with "database is locked" before either had written anything. The retry
    loop is that busy handler, spelled out.
    """
    deadline = time.monotonic() + BUSY_TIMEOUT_MS / 1000
    while True:
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)
