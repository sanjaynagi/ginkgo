"""The base of every index that writes its own rows.

Most tables in the database are projections: the recorder's writer thread
replays the event ledger into them and nothing else touches them. The cache's
rows and the remote-input staging rows are not. They are written *directly*, by
the code that owns the fact, at the moment it becomes true — because a cache
save has to be visible to the ``load`` that may follow it microseconds later.

A direct index is therefore a small thing: an open store, a lock so the
evaluator's threads can share one connection, and a transaction helper. Opening
one is a decision — to write (:meth:`open`), to read (:meth:`for_reading`), or
to keep the rows to this process (:meth:`in_memory`) — because the first of
those creates a database and its schema. Constructing one never does.
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
        An open store. The index owns it and closes it with :meth:`close`.
    """

    def __init__(self, *, store: ProvenanceStore) -> None:
        self._store = store
        self._lock = threading.RLock()

    @classmethod
    def open(cls, *, path: Path, readonly: bool = False) -> Self:
        """Open the index for writing, creating the database if it is missing.

        Parameters
        ----------
        path : Path
            The database file, normally ``WorkspaceLayout.db``.
        readonly : bool, optional
            Open a reader instead: it never creates the file or its
            schema, and fails if there is nothing there to read.

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

    @property
    def store(self) -> ProvenanceStore:
        """The store this index reads and writes."""
        return self._store

    def close(self) -> None:
        """Release the connection.

        Safe to call more than once.
        """
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
