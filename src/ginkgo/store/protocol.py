"""The contract a provenance store satisfies.

Ginkgo writes provenance as an append-only ledger of events plus projection
rows derived from them, and both halves of a write have to land together: a
projection that outlived its event, or an event whose projection never
happened, is a store that cannot be rebuilt. :meth:`ProvenanceStore.transaction`
is what makes that atomic, and :meth:`~ProvenanceStore.append` and
:meth:`~ProvenanceStore.apply` are the two things a write does inside it.

Only :class:`~ginkgo.store.sqlite.SqliteStore` implements this today. The
protocol exists so the rest of ginkgo depends on the contract rather than on
SQLite, not because a second backend is planned.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

__all__ = ["ProjectionOp", "ProvenanceStore", "StoredEvent"]


@dataclass(frozen=True, kw_only=True)
class StoredEvent:
    """One row of the ledger: a runtime event, ready to append.

    The columns mirror what the ``events`` table filters and joins on; every
    other field of the originating :class:`~ginkgo.runtime.events.GinkgoEvent`
    stays in *payload*, so a new event type needs no migration.

    Attributes
    ----------
    run_id : str
        The run that emitted the event.
    ts : str
        ISO-8601 UTC timestamp, taken from the event.
    type : str
        The event's discriminator, e.g. ``"task_completed"``.
    v : int
        The event class's schema version.
    task_id, attempt, cache_key, asset_key
        Set when the event carries them; ``None`` for run-level events.
    payload : str
        JSON text of the whole event payload.
    """

    run_id: str
    ts: str
    type: str
    v: int = 1
    task_id: str | None = None
    attempt: int | None = None
    cache_key: str | None = None
    asset_key: str | None = None
    payload: str


@dataclass(frozen=True)
class ProjectionOp:
    """One statement updating a projection table.

    Projections are written as SQL rather than through an object model because
    every one of them is an upsert of a handful of columns, and the SQL says
    exactly what changes.

    Attributes
    ----------
    sql : str
        A single statement, with ``?`` placeholders.
    params : Sequence[Any]
        Values bound to the placeholders.
    """

    sql: str
    params: Sequence[Any] = ()


class ProvenanceStore(Protocol):
    """A ledger of runtime events and the projections built from it."""

    @property
    def path(self) -> Path:
        """The database this store is backed by."""
        ...

    @property
    def readonly(self) -> bool:
        """Whether this store refuses writes."""
        ...

    def transaction(self) -> AbstractContextManager[None]:
        """Group writes so they commit together, or not at all."""
        ...

    def append(self, events: Iterable[StoredEvent]) -> None:
        """Append *events* to the ledger in the order given."""
        ...

    def apply(self, projection_ops: Iterable[ProjectionOp]) -> None:
        """Run *projection_ops* in the order given."""
        ...

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        """Return every row *sql* selects."""
        ...

    def close(self) -> None:
        """Release the connection. Safe to call more than once."""
        ...
