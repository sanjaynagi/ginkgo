"""Convert worker log chunks into ``TaskLog`` events.

Two paths emit ``TaskLog`` events into the evaluator's event bus:

* In-process emitters returned by :py:meth:`LogDrain.make_emitter`, called
  directly by the shell runner and the driver-task wrapper as chunks become
  available on the scheduler process.
* Subprocess workers, which push chunks onto a multiprocessing queue. A
  daemon thread owned by :class:`LogDrain` drains that queue and emits one
  event per chunk.

Both paths share a single per-(node, stream) sequence counter so that the
order of chunks is well-defined across the two sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from queue import Empty
from threading import Event, Thread
from typing import Any, Callable

from ginkgo.runtime.events import EventBus, TaskLog


def _task_id_for_node(node_id: int) -> str:
    return f"task_{node_id:04d}"


@dataclass(kw_only=True)
class LogDrain:
    """Owns the worker log queue, drain thread, and per-task sequence counter."""

    event_bus: EventBus | None
    run_id_provider: Callable[[], str]
    _queue: Any = field(default=None, init=False, repr=False)
    _stop: Event | None = field(default=None, init=False, repr=False)
    _thread: Thread | None = field(default=None, init=False, repr=False)
    _sequences: dict[tuple[int, str], int] = field(default_factory=dict, init=False, repr=False)

    @property
    def queue(self) -> Any:
        """Return the active log event queue, or ``None`` when stopped."""
        return self._queue

    def start(self, *, queue: Any) -> None:
        """Start draining worker log chunks from ``queue``."""
        self._queue = queue
        self._stop = Event()
        self._thread = Thread(
            target=self._drain,
            name="ginkgo-log-drain",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the drain thread and release the queue."""
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self._queue = None
        self._stop = None
        self._thread = None

    def make_emitter(self, *, node: Any, stream: str) -> Callable[[str], None]:
        """Return a per-(node, stream) callback that emits one ``TaskLog`` event."""

        def emit(chunk: str) -> None:
            if not chunk:
                return
            sequence_key = (node.node_id, stream)
            sequence = self._sequences.get(sequence_key, 0) + 1
            self._sequences[sequence_key] = sequence
            self._emit(
                TaskLog(
                    run_id=self.run_id_provider(),
                    task_id=_task_id_for_node(node.node_id),
                    task_name=node.task_def.name,
                    attempt=node.attempt,
                    display_label=node.display_label,
                    stream=stream,
                    chunk=chunk,
                    sequence=sequence,
                )
            )

        return emit

    def _drain(self) -> None:
        if self._queue is None or self._stop is None:
            return

        while True:
            try:
                payload = self._queue.get(timeout=0.1)
            except Empty:
                if self._stop.is_set():
                    return
                continue
            except Exception:
                return

            chunk = payload.get("chunk")
            stream = payload.get("stream")
            task_id = payload.get("task_id")
            if (
                not isinstance(chunk, str)
                or not isinstance(stream, str)
                or not isinstance(task_id, str)
                or not chunk
            ):
                continue
            node_id = int(task_id.split("_")[-1])
            sequence_key = (node_id, stream)
            sequence = self._sequences.get(sequence_key, 0) + 1
            self._sequences[sequence_key] = sequence
            self._emit(
                TaskLog(
                    run_id=str(payload.get("run_id") or self.run_id_provider()),
                    task_id=task_id,
                    task_name=str(payload.get("task_name") or ""),
                    attempt=int(payload.get("attempt") or 0),
                    display_label=payload.get("display_label"),
                    stream=stream,
                    chunk=chunk,
                    sequence=sequence,
                )
            )

    def _emit(self, event: object) -> None:
        if self.event_bus is not None:
            self.event_bus.emit(event)
