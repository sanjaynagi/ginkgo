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

import json
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

__all__ = ["ProjectionOp", "ProvenanceStore", "StoredEvent", "event_fields"]


@dataclass(frozen=True, kw_only=True)
class StoredEvent:
    """One row of the ledger: a runtime event, ready to append.

    The columns mirror what the ``events`` table filters and joins on; every
    other field of the originating :class:`~ginkgo.runtime.events.GinkgoEvent`
    stays in *payload*, so a new event type needs no migration. A field a
    column holds is not repeated in *payload*: a cached task writes six events
    a run, and the repeated run id, timestamp and cache key were a fifth of
    them. :meth:`fields` puts the event back together.

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
    task_id, attempt, cache_key
        Set when the event carries them; ``None`` for run-level events.
    payload : str
        JSON text of the event's other fields.
    """

    run_id: str
    ts: str
    type: str
    v: int = 1
    task_id: str | None = None
    attempt: int | None = None
    cache_key: str | None = None
    payload: str

    def fields(self) -> dict[str, Any]:
        """Return every field of the event, the columns folded back into the payload."""
        return event_fields(
            run_id=self.run_id,
            ts=self.ts,
            type=self.type,
            v=self.v,
            task_id=self.task_id,
            attempt=self.attempt,
            cache_key=self.cache_key,
            payload=self.payload,
        )


def event_fields(
    *,
    run_id: str,
    ts: str,
    type: str,
    v: int,
    task_id: str | None,
    attempt: int | None,
    cache_key: str | None,
    payload: str,
) -> dict[str, Any]:
    """Return a stored event's fields from its columns and payload text.

    Parameters
    ----------
    run_id, ts, type, v, task_id, attempt, cache_key
        The ``events`` row's columns. ``type`` is the event's ``event`` field;
        a column that is ``None`` was not lifted out of the payload.
    payload : str
        The row's payload JSON.

    Returns
    -------
    dict[str, Any]
        The event as :meth:`~ginkgo.runtime.events.GinkgoEvent.to_payload`
        gave it.
    """
    lifted = {"task_id": task_id, "attempt": attempt, "cache_key": cache_key}
    return {
        **json.loads(payload),
        "run_id": run_id,
        "ts": ts,
        "event": type,
        "v": v,
        **{name: value for name, value in lifted.items() if value is not None},
    }


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

    def select_with_columns(
        self, sql: str, params: Sequence[Any] = (), *, limit: int | None = None
    ) -> tuple[tuple[str, ...], list[sqlite3.Row]]:
        """Return the columns *sql* names and at most *limit* of its rows.

        For SQL the store did not write; :meth:`query` is for ginkgo's own.
        """
        ...

    def close(self) -> None:
        """Release the connection. Safe to call more than once."""
        ...
