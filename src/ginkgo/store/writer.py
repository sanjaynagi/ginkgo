"""The single thread that writes the ledger.

Events arrive on the event bus from every worker thread in the run. They are
queued here and applied by one background thread, which owns the only
write-mode connection the process holds: SQLite connections are not shareable
across threads, and one writer means transactions never contend with
themselves.

Batching is what keeps the hot path cheap — a batch becomes one transaction —
and it is bounded three ways so nothing waits long to become durable: a
terminal event commits immediately, and otherwise a batch closes at 256 events
or 50 milliseconds, whichever comes first.

A failed write is not survivable: provenance the run cannot record is
provenance nobody can reconstruct. The exception is kept and re-raised on the
next :meth:`StoreWriter.put`, :meth:`StoreWriter.flush` or
:meth:`StoreWriter.close`, so it fails the run rather than the writer thread.
"""

from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.runtime.events import GinkgoEvent
from ginkgo.store.errors import StoreError
from ginkgo.store.projector import TERMINAL_EVENTS, projection_ops
from ginkgo.store.protocol import ProjectionOp, ProvenanceStore, StoredEvent
from ginkgo.store.sqlite import open_store

__all__ = ["MAX_BATCH_EVENTS", "MAX_BATCH_SECONDS", "StoreWriter", "stored_event"]


MAX_BATCH_EVENTS = 256
"""Events a batch accumulates before it commits regardless of the clock."""

MAX_BATCH_SECONDS = 0.05
"""How long the first event of a batch waits for company."""


@dataclass
class _Flush:
    """A request to commit everything queued ahead of it."""

    done: threading.Event = field(default_factory=threading.Event)


_STOP = object()


def stored_event(event: GinkgoEvent) -> StoredEvent:
    """Return the ledger row for one runtime event.

    Parameters
    ----------
    event : GinkgoEvent
        Any event on the bus.

    Returns
    -------
    StoredEvent
        The row, with the filtered columns lifted out of the payload.
    """
    payload = event.to_payload()
    return StoredEvent(
        run_id=payload.get("run_id") or "",
        ts=payload.get("ts") or "",
        type=payload.get("event") or "",
        v=int(payload.get("v") or 1),
        task_id=_column(payload, "task_id"),
        attempt=payload.get("attempt") if isinstance(payload.get("attempt"), int) else None,
        cache_key=_column(payload, "cache_key"),
        asset_key=_column(payload, "asset_key"),
        payload=json.dumps(payload, sort_keys=True, default=str),
    )


class StoreWriter:
    """Queued, batched appends from the event bus into one store.

    Parameters
    ----------
    path : Path
        The database to write, normally ``WorkspaceLayout.db``.
    run_id : str
        The run whose ``timings`` the writer's own cost is recorded against.
    """

    def __init__(self, *, path: Path, run_id: str) -> None:
        self._path = Path(path)
        self._run_id = run_id
        self._queue: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._run, name="ginkgo-store-writer", daemon=True)
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self._failed = False
        self._closed = False
        self._write_seconds = 0.0

    @property
    def path(self) -> Path:
        """The database being written."""
        return self._path

    def start(self) -> None:
        """Open the store on the writer thread and wait until it is usable.

        Raises
        ------
        StoreError
            If the database could not be opened or migrated.
        """
        self._thread.start()
        self._ready.wait()
        self._raise_pending()

    def put(self, event: GinkgoEvent) -> None:
        """Queue one event.

        Raises
        ------
        StoreError
            If an earlier write failed.
        """
        self._raise_pending()
        if self._closed:
            raise StoreError(f"The provenance store at {self._path} is already closed.")
        self._queue.put(stored_event(event))

    def flush(self) -> None:
        """Block until everything queued so far is committed."""
        self._raise_pending()
        if self._closed:
            return
        request = _Flush()
        self._queue.put(request)
        request.done.wait()
        self._raise_pending()

    def close(self) -> None:
        """Drain the queue, record the writer's own cost, and stop. Idempotent."""
        if self._closed:
            self._raise_pending()
            return
        self._closed = True
        if self._thread.is_alive():
            self._queue.put(_STOP)
            self._thread.join()
        self._raise_pending()

    # ------------------------------------------------------------- internals

    def _raise_pending(self) -> None:
        """Re-raise the writer thread's failure, once.

        Once is enough: the first raise fails the run. Raising again from the
        ``close()`` in the caller's ``finally`` would replace that failure with
        a copy of itself and lose the traceback that matters.
        """
        error = self._error
        if error is None:
            return
        self._error = None
        raise error

    def _run(self) -> None:
        """Own the connection and apply batches until asked to stop."""
        try:
            store = open_store(self._path)
        except BaseException as exc:  # noqa: BLE001 - reported to the run
            self._error = exc
            self._failed = True
            self._ready.set()
            self._drain_after_failure()
            return
        self._ready.set()
        try:
            self._loop(store)
            if not self._failed:
                self._record_write_cost(store)
        finally:
            store.close()

    def _loop(self, store: ProvenanceStore) -> None:
        pending: list[StoredEvent] = []
        deadline: float | None = None
        while True:
            timeout = None if deadline is None else max(0.0, deadline - time.monotonic())
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None

            if isinstance(item, StoredEvent):
                pending.append(item)
                if deadline is None:
                    deadline = time.monotonic() + MAX_BATCH_SECONDS
                if item.type in TERMINAL_EVENTS or len(pending) >= MAX_BATCH_EVENTS:
                    self._commit(store, pending)
                    pending, deadline = [], None
                continue

            if pending:
                self._commit(store, pending)
                pending, deadline = [], None
            if isinstance(item, _Flush):
                item.done.set()
            elif item is _STOP:
                return

    def _commit(self, store: ProvenanceStore, batch: list[StoredEvent]) -> None:
        """Append *batch* and apply its projections in one transaction."""
        if self._failed:
            return
        started = time.perf_counter()
        try:
            ops: list[ProjectionOp] = []
            for event in batch:
                ops.extend(projection_ops(event))
            with store.transaction():
                store.append(batch)
                store.apply(ops)
        except BaseException as exc:  # noqa: BLE001 - reported to the run
            self._error = _write_failure(path=self._path, exc=exc)
            self._failed = True
            return
        self._write_seconds += time.perf_counter() - started

    def _record_write_cost(self, store: ProvenanceStore) -> None:
        """Record what writing the ledger cost this run, for the benchmarks."""
        if self._write_seconds <= 0:
            return
        path = '$."provenance_write_seconds"'
        try:
            with store.transaction():
                store.apply(
                    [
                        ProjectionOp(
                            sql=(
                                "UPDATE runs SET timings = json_set("
                                "  coalesce(timings, '{}'), ?,"
                                "  round(coalesce(json_extract(timings, ?), 0) + ?, 6)"
                                ") WHERE run_id = ?"
                            ),
                            params=(path, path, round(self._write_seconds, 6), self._run_id),
                        )
                    ]
                )
        except BaseException as exc:  # noqa: BLE001 - reported to the run
            self._error = _write_failure(path=self._path, exc=exc)
            self._failed = True

    def _drain_after_failure(self) -> None:
        """Release anyone waiting on a flush once the writer cannot run."""
        while True:
            item = self._queue.get()
            if isinstance(item, _Flush):
                item.done.set()
            elif item is _STOP:
                return


def _write_failure(*, path: Path, exc: BaseException) -> BaseException:
    """Return the error to fail the run with, naming the database."""
    if isinstance(exc, StoreError):
        return exc
    return StoreError(f"Could not write the provenance ledger at {path}: {exc}")


def _column(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None
