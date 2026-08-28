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

import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.store.errors import StoreError
from ginkgo.store.projector import TERMINAL_EVENTS, accumulate_seconds, projection_ops
from ginkgo.store.protocol import ProjectionOp, ProvenanceStore, StoredEvent
from ginkgo.store.sqlite import open_store

__all__ = ["MAX_BATCH_EVENTS", "MAX_BATCH_SECONDS", "StoreWriter"]


MAX_BATCH_EVENTS = 256
"""Events a batch accumulates before it commits regardless of the clock."""

MAX_BATCH_SECONDS = 0.05
"""How long the first event of a batch waits for company."""


@dataclass
class _Flush:
    """A request to commit everything queued ahead of it."""

    done: threading.Event = field(default_factory=threading.Event)


_STOP = object()


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
        self._inflight: _Flush | None = None
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

    def put(self, event: StoredEvent) -> None:
        """Queue one ledger row.

        Raises
        ------
        StoreError
            If an earlier write failed.
        """
        self._raise_pending()
        if self._closed:
            raise StoreError(f"The provenance store at {self._path} is already closed.")
        self._queue.put(event)

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
        """Re-raise the writer thread's failure.

        Every time, not once: events reach ``put`` from the resource sampler's
        own thread, where a raised error would die with the thread. Keeping it
        means the ``flush()`` or ``close()`` on the main thread still fails the
        run rather than letting it report success over an incomplete ledger.
        """
        if self._error is not None:
            raise self._error

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
        except BaseException as exc:  # noqa: BLE001 - reported to the run
            self._error = _write_failure(path=self._path, exc=exc)
            self._failed = True
        finally:
            store.close()
            # Whatever went wrong, nobody waits on a thread that has stopped.
            self._release_waiters()

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
                # Held while it is being served, so a thread that dies here
                # still releases the caller waiting on it.
                self._inflight = item
                item.done.set()
                self._inflight = None
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
        try:
            with store.transaction():
                store.apply(
                    [
                        accumulate_seconds(
                            table="runs",
                            column="timings",
                            where="run_id = ?",
                            where_params=(self._run_id,),
                            key="provenance_write_seconds",
                            seconds=round(self._write_seconds, 6),
                        )
                    ]
                )
        except BaseException as exc:  # noqa: BLE001 - reported to the run
            self._error = _write_failure(path=self._path, exc=exc)
            self._failed = True

    def _release_waiters(self) -> None:
        """Wake every flush already queued, so no caller blocks on a dead thread."""
        if self._inflight is not None:
            self._inflight.done.set()
            self._inflight = None
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return
            if isinstance(item, _Flush):
                item.done.set()

    def _drain_after_failure(self) -> None:
        """Release anyone waiting on a flush once the writer cannot start."""
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
