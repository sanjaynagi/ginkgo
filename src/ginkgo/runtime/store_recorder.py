"""The event-bus subscriber that persists a run.

This sits in ``runtime/`` rather than in ``store/`` because it is the piece
that knows the event vocabulary: it translates a :class:`GinkgoEvent` into the
ledger row the store appends. ``store/`` stays below it, knowing only rows and
SQL, so the two layers do not import each other in a circle.

One subscriber, one writer, one fact in one place. Everything the run knows it
says on the bus; this is what turns that into rows and, at the end, into the
manifest the run directory keeps.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Any

from ginkgo.runtime.events import GinkgoEvent
from ginkgo.runtime.run_summary import RunSummary
from ginkgo.runtime.rundir import RunDir
from ginkgo.store.projector import TERMINAL_EVENTS
from ginkgo.store.protocol import ProjectionOp, StoredEvent
from ginkgo.store.sqlite import open_store
from ginkgo.store.writer import StoreWriter

__all__ = ["StoreRecorder", "stored_event"]


# Log chunks are bytes, and bytes stay out of SQL: the log files already hold
# them, and a run that streams megabytes of output would otherwise pay for it
# twice.
_IGNORED = frozenset({"task_log"})

# After these, the recorder commits and hands the events to its committed
# handlers. The terminal events are where a reader has something new to read;
# ``run_started`` is here so a "run began" notification goes out at the start
# of the run rather than at the first task to finish.
_DELIVER_AFTER = TERMINAL_EVENTS | {"run_started"}


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


class StoreRecorder:
    """Persist bus events into the ledger and export the run's manifest.

    Parameters
    ----------
    path : Path
        The database, normally ``WorkspaceLayout.db``.
    run_dir : RunDir
        Where the exported manifest is written when the run completes.
    """

    def __init__(self, *, path: Path, run_dir: RunDir) -> None:
        self._path = Path(path)
        self._run_dir = run_dir
        self._writer = StoreWriter(path=self._path, run_id=run_dir.run_id)
        self._committed: list[Callable[[GinkgoEvent], None]] = []
        self._undelivered: list[GinkgoEvent] = []
        self._manifest_written = False

    @property
    def path(self) -> Path:
        """The database being written."""
        return self._path

    @property
    def completed(self) -> bool:
        """Whether the run has been closed and its manifest written."""
        return self._manifest_written

    def start(self) -> StoreRecorder:
        """Open the ledger, failing the run if it cannot be written."""
        self._writer.start()
        return self

    def on_committed(self, handler: Callable[[GinkgoEvent], None]) -> None:
        """Register a handler that runs only once an event is durable.

        Anything that reads the run back — the notification service asks which
        tasks failed — must be registered here rather than on the bus. A bus
        subscriber can be called before the recorder has committed the event it
        is reacting to; a committed handler cannot.
        """
        self._committed.append(handler)

    def __call__(self, event: GinkgoEvent) -> None:
        """Handle one bus event."""
        if event.event in _IGNORED:
            return
        self._writer.put(stored_event(event))
        self._undelivered.append(event)
        if event.event == "run_completed":
            self._finalize()
        if event.event in _DELIVER_AFTER:
            self._deliver()

    def flush(self) -> None:
        """Block until everything emitted so far is committed and readable."""
        self._writer.flush()

    def close(self) -> None:
        """Drain and stop the writer, then deliver anything outstanding."""
        self._writer.close()
        self._dispatch()

    def __enter__(self) -> StoreRecorder:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------- internals

    def _deliver(self) -> None:
        """Commit, then hand every event since the last delivery to handlers."""
        if not self._committed:
            self._undelivered.clear()
            return
        self._writer.flush()
        self._dispatch()

    def _dispatch(self) -> None:
        pending, self._undelivered = self._undelivered, []
        for event in pending:
            for handler in self._committed:
                handler(event)

    def _finalize(self) -> None:
        """Commit the run, export its manifest, and record that it was written."""
        if self._manifest_written:
            return
        self._manifest_written = True
        # The writer owns its connection on its own thread, so the export reads
        # through a connection of this one — after the writer has stopped, so
        # there is nothing left uncommitted to miss.
        self._writer.close()
        with open_store(self._path) as store:
            summary = RunSummary.load(store, self._run_dir.run_id, runs_root=self._run_dir.root)
            self._run_dir.write_manifest(summary.to_payload())
            with store.transaction():
                store.apply(
                    [
                        ProjectionOp(
                            sql="UPDATE runs SET snapshot_written = 1 WHERE run_id = ?",
                            params=(self._run_dir.run_id,),
                        )
                    ]
                )


def _column(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None
