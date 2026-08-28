"""The event-bus subscriber that persists a run.

One subscriber, one writer, one fact in one place. Everything the run knows it
says on the bus; this is what turns that into rows and, at the end, into the
snapshot the run directory keeps.

Subscribe it before any other handler that reads the store back — the
notification service asks which tasks failed — because a handler only sees what
the recorder has already committed.
"""

from __future__ import annotations

from pathlib import Path
from types import TracebackType

from ginkgo.runtime.events import GinkgoEvent
from ginkgo.runtime.rundir import RunDir
from ginkgo.store.export import export_manifest
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import open_store
from ginkgo.store.writer import StoreWriter

__all__ = ["StoreRecorder"]


# Log chunks are bytes, and bytes stay out of SQL: the log files already hold
# them, and a run that streams megabytes of output would otherwise pay for it
# twice.
_IGNORED = frozenset({"task_log"})

# After these, a later subscriber may query the store for what just happened,
# so the recorder waits for the commit rather than only queueing it.
_FLUSH_AFTER = frozenset({"task_failed"})


class StoreRecorder:
    """Persist bus events into the ledger and export the run's snapshot.

    Parameters
    ----------
    path : Path
        The database, normally ``WorkspaceLayout.db``.
    run_dir : RunDir
        Where the exported snapshot is written when the run completes.
    """

    def __init__(self, *, path: Path, run_dir: RunDir) -> None:
        self._path = Path(path)
        self._run_dir = run_dir
        self._writer = StoreWriter(path=self._path, run_id=run_dir.run_id)
        self._snapshot_written = False

    @property
    def path(self) -> Path:
        """The database being written."""
        return self._path

    def start(self) -> StoreRecorder:
        """Open the ledger, failing the run if it cannot be written."""
        self._writer.start()
        return self

    def __call__(self, event: GinkgoEvent) -> None:
        """Handle one bus event."""
        if event.event in _IGNORED:
            return
        self._writer.put(event)
        if event.event == "run_completed":
            self._finalize()
        elif event.event in _FLUSH_AFTER:
            self._writer.flush()

    def flush(self) -> None:
        """Block until everything emitted so far is committed and readable."""
        self._writer.flush()

    def close(self) -> None:
        """Drain and stop the writer. Idempotent."""
        self._writer.close()

    def __enter__(self) -> StoreRecorder:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _finalize(self) -> None:
        """Commit the run, export its snapshot, and record that it was written."""
        if self._snapshot_written:
            return
        self._snapshot_written = True
        # The writer owns its connection on its own thread, so the export reads
        # through a connection of this one — after the writer has stopped, so
        # there is nothing left uncommitted to miss.
        self._writer.close()
        with open_store(self._path) as store:
            self._run_dir.write_snapshot(export_manifest(store, self._run_dir.run_id))
            with store.transaction():
                store.apply(
                    [
                        ProjectionOp(
                            sql="UPDATE runs SET snapshot_written = 1 WHERE run_id = ?",
                            params=(self._run_dir.run_id,),
                        )
                    ]
                )
