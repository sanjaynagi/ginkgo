"""The base of every index that writes its own rows.

Most tables in the database are projections: the recorder's writer thread
replays the event ledger into them and nothing else touches them. Two are not.
The cache's rows and the asset catalog's rows are written *directly*, by the
code that owns the fact, at the moment it becomes true — because a cache save
has to be visible to the ``load`` that may follow it microseconds later, and an
asset version's ``data_version`` is computed from parent rows written moments
earlier by a sibling task.

A direct index is therefore a small thing: an open store, a lock so the
evaluator's threads can share one connection, and a transaction helper. Opening
one is a decision — to write (:meth:`open`), to read (:meth:`for_reading`), or
to keep the rows to this process (:meth:`in_memory`) — because the first of
those creates and migrates a database. Constructing one never does.

Two indexes in one process share a connection with :meth:`attached_to` rather
than opening a second: one SQLite connection with one lock in front of it is
what keeps the evaluator's threads from waiting on each other's write locks.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Self, Sequence

from ginkgo.store.protocol import ProjectionOp, ProvenanceStore
from ginkgo.store.sqlite import MEMORY, open_store

__all__ = ["DirectIndex"]


class DirectIndex:
    """A store connection, a lock, and the transaction the two make possible.

    Parameters
    ----------
    store : ProvenanceStore
        An open store. The index owns it and closes it with :meth:`close`,
        unless it was attached to another index's connection.
    lock : threading.RLock | None
        The lock guarding the connection. Passed only by :meth:`attached_to`,
        which shares one lock across the indexes sharing one connection.
    owns_store : bool
        Whether :meth:`close` closes the connection.
    """

    def __init__(
        self,
        *,
        store: ProvenanceStore,
        lock: threading.RLock | None = None,
        owns_store: bool = True,
    ) -> None:
        self._store = store
        self._lock = lock if lock is not None else threading.RLock()
        self._owns_store = owns_store

    @classmethod
    def open(cls, *, path: Path, readonly: bool = False) -> Self:
        """Open the index for writing, creating and migrating the database.

        Parameters
        ----------
        path : Path
            The database file, normally ``WorkspaceLayout.db``.
        readonly : bool, optional
            Open a reader instead: it never creates the file and never
            migrates it, and fails if there is nothing there to read.

        Returns
        -------
        Self
        """
        return cls(store=open_store(Path(path), readonly=readonly, thread_shared=True))

    @classmethod
    def in_memory(cls) -> Self:
        """Return an index over a private database that touches no filesystem.

        What a remote worker gets: it has the artifacts it staged but not the
        workspace that indexes them, and creating a database in a pod's scratch
        directory would record rows nothing will ever read.
        """
        return cls(store=open_store(MEMORY, thread_shared=True))

    @classmethod
    def for_reading(cls, path: Path) -> Self:
        """Return a read-only index over *path*, or an empty one if it is absent.

        A workspace nobody has run anything in has no database and nothing to
        report, which is an answer rather than an error — and a read path must
        never be the thing that creates one.
        """
        if not Path(path).is_file():
            return cls.in_memory()
        return cls.open(path=path, readonly=True)

    @classmethod
    def attached_to(cls, other: DirectIndex) -> Self:
        """Return an index over *other*'s connection, sharing its lock.

        Two indexes over one workspace are two sets of tables, not two
        databases. Sharing the connection means a save in one never waits on
        SQLite's write lock for a save in the other, and closing *other* closes
        them both.
        """
        return cls(store=other._store, lock=other._lock, owns_store=False)

    @property
    def store(self) -> ProvenanceStore:
        """The store this index reads and writes."""
        return self._store

    def close(self) -> None:
        """Release the connection, if this index owns it.

        Safe to call more than once.
        """
        if self._owns_store:
            self._store.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _query(self, sql: str, params: Sequence[Any] = ()) -> list[Any]:
        """Run one read under the index's lock."""
        with self._lock:
            return self._store.query(sql, params)

    def _write(self, *ops: ProjectionOp) -> None:
        """Run *ops* in one transaction, holding the index's lock."""
        with self._lock, self._store.transaction():
            self._store.apply(ops)
