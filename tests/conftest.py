"""Shared fixtures for ginkgo tests."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.caching.provenance import make_run_id
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.run_summary import RunSummary
from ginkgo.runtime.store_recorder import StoreRecorder
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout
from ginkgo.runtime.events import (
    EventBus,
    GinkgoEvent,
    RunCompleted,
    RunStarted,
    TaskCacheHit,
    TaskCompleted,
    TaskFailed,
    TaskStaging,
    TaskStarted,
)


def make_download_backend(*, content: bytes = b"hello world", etag: str = "etag1") -> MagicMock:
    """Return a mock ``ObjectStore`` whose download writes fixed bytes.

    ``download`` writes ``content`` to the requested ``dest_path`` and ``head``
    reports the matching size/etag — enough to drive the staging cache and the
    evaluator's remote-input path without touching a real object store.

    Parameters
    ----------
    content : bytes
        Bytes written by ``download`` and reported as the object size.
    etag : str
        ETag returned by both ``download`` and ``head``.
    """
    backend = MagicMock()

    def _download(*, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(content)
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=len(content), etag=etag)

    def _head(*, bucket: str, key: str) -> RemoteObjectMeta:
        return RemoteObjectMeta(uri=f"s3://{bucket}/{key}", size=len(content), etag=etag)

    backend.download.side_effect = _download
    backend.head.side_effect = _head
    return backend


@pytest.fixture(autouse=True)
def isolate_working_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Run each test in an isolated working directory.

    This keeps cache entries scoped to a single test and avoids
    cross-test interference from ``.ginkgo/cache``.
    """
    monkeypatch.chdir(tmp_path)


@dataclass
class EventCollector:
    """Test helper that records every event published on a bus.

    Use ``collector.bus`` as the ``event_bus`` argument when constructing
    an evaluator (or calling :func:`ginkgo.evaluate`), then assert against
    ``collector.events`` or the convenience helpers below.
    """

    bus: EventBus = field(default_factory=EventBus)
    events: list[GinkgoEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.bus.subscribe(self._record)

    def _record(self, event: Any) -> None:
        if isinstance(event, GinkgoEvent):
            self.events.append(event)

    # Convenience accessors -------------------------------------------------

    def started(self) -> list[TaskStarted]:
        return [e for e in self.events if isinstance(e, TaskStarted)]

    def staging(self) -> list[TaskStaging]:
        return [e for e in self.events if isinstance(e, TaskStaging)]

    def cached(self) -> list[GinkgoEvent]:
        """Return every cache-hit signal (both ``TaskCacheHit`` and ``TaskCompleted(status='cached')``)."""
        cached_completed = [
            e for e in self.events if isinstance(e, TaskCompleted) and e.status == "cached"
        ]
        return [e for e in self.events if isinstance(e, TaskCacheHit)] + cached_completed

    def succeeded(self) -> list[TaskCompleted]:
        return [e for e in self.events if isinstance(e, TaskCompleted) and e.status == "success"]

    def failed(self) -> list[TaskFailed]:
        return [e for e in self.events if isinstance(e, TaskFailed)]


@pytest.fixture
def event_collector() -> EventCollector:
    """Return a fresh ``EventCollector`` whose bus can be passed to evaluator."""
    return EventCollector()


@dataclass
class Ledger:
    """A live ledger for one test run: a bus, a run directory, and a recorder.

    Construct through :meth:`start`, pass ``ledger.bus`` and ``ledger.run_dir``
    to the evaluator, and read the result back with :meth:`summary` — the same
    path the CLI takes, so a test asserts on what a user would see.
    """

    run_id: str
    run_dir: RunDir
    bus: EventBus
    recorder: StoreRecorder
    db: Path

    @classmethod
    def start(
        cls,
        *,
        root: Path,
        workflow: str = "workflow/flow.py",
        run_id: str | None = None,
        bus: EventBus | None = None,
        **run_started: Any,
    ) -> "Ledger":
        """Open a ledger under *root* and emit ``RunStarted``."""
        layout = WorkspaceLayout(root=Path(root) / ".ginkgo")
        run_id = run_id or make_run_id(workflow_path=workflow)
        run_dir = RunDir.create(run_id=run_id, root=layout.runs)
        bus = bus if bus is not None else EventBus()
        recorder = StoreRecorder(path=layout.db, run_dir=run_dir).start()
        bus.subscribe(recorder)
        bus.emit(RunStarted(run_id=run_id, workflow=workflow, **run_started))
        return cls(run_id=run_id, run_dir=run_dir, bus=bus, recorder=recorder, db=layout.db)

    def finish(self, *, status: str = "success", error: str | None = None) -> RunSummary:
        """Emit ``RunCompleted``, which exports the snapshot, and read the run back."""
        self.bus.emit(RunCompleted(run_id=self.run_id, status=status, error=error))
        return self.summary()

    def task(self, task_id: str = "task_0000") -> dict[str, Any]:
        """Return one task's projection row, with its ``extra`` fields merged in.

        A test asserting on an open-ended annotation — a notebook's render
        status, a container image digest — should not have to know whether it
        earned a column.
        """
        import json

        self.recorder.flush()
        with open_store(self.db, readonly=True) as store:
            rows = store.query(
                "SELECT * FROM tasks WHERE run_id = ? AND task_id = ?",
                (self.run_id, task_id),
            )
        assert rows, f"no task {task_id} in run {self.run_id}"
        row = dict(rows[0])
        row["cached"] = None if row["cached"] is None else bool(row["cached"])
        for column in ("extra", "failure", "output_summary", "resource_usage", "timings"):
            value = row.get(column)
            row[column] = json.loads(value) if isinstance(value, str) else value
        return {**row, **(row.get("extra") or {})}

    def tasks(self) -> dict[str, dict[str, Any]]:
        """Return every task row of this run, keyed by task id."""
        self.recorder.flush()
        with open_store(self.db, readonly=True) as store:
            ids = [
                row["task_id"]
                for row in store.query(
                    "SELECT task_id FROM tasks WHERE run_id = ? ORDER BY node_id",
                    (self.run_id,),
                )
            ]
        return {task_id: self.task(task_id) for task_id in ids}

    @property
    def path(self) -> Path:
        """The run's directory on disk."""
        return self.run_dir.path

    def summary(self) -> RunSummary:
        """Return the run as every presenter sees it."""
        self.recorder.flush()
        with open_store(self.db, readonly=True) as store:
            return RunSummary.load(store, self.run_id, runs_root=self.run_dir.root)

    def close(self) -> None:
        """Stop the writer."""
        self.recorder.close()


@pytest.fixture
def ledger(tmp_path: Path):
    """Yield a started :class:`Ledger` rooted at ``tmp_path``."""
    started = Ledger.start(root=tmp_path)
    try:
        yield started
    finally:
        started.close()


def latest_run_view(*, root: Path) -> tuple[Path, dict[str, Any]]:
    """Return the newest run under *root* as a plain mapping of its projections.

    A convenience for whole-workflow tests, which assert on statuses across
    every task rather than on one typed field: ``view["tasks"]`` maps task id
    to that task's row with its ``extra`` fields merged in.
    """
    import ginkgo.query as query

    with query.open(WorkspaceLayout(root=Path(root) / ".ginkgo")) as store:
        run = store.runs(limit=1)[0]
        summary = store.run(run.run_id)
        rows = store.store.query(
            "SELECT * FROM tasks WHERE run_id = ? ORDER BY node_id", (run.run_id,)
        )
    tasks: dict[str, Any] = {}
    for row in rows:
        task = dict(row)
        for column in ("extra", "failure", "output_summary", "resource_usage", "timings"):
            value = task.get(column)
            task[column] = json.loads(value) if isinstance(value, str) else value
        tasks[task["task_id"]] = {**task, **(task.get("extra") or {})}
    for entry in summary.tasks:
        tasks[entry.task_key]["dependency_ids"] = list(entry.dependency_ids)
        tasks[entry.task_key]["dynamic_dependency_ids"] = list(entry.dynamic_dependency_ids)
        tasks[entry.task_key]["outputs"] = list(entry.outputs)
    return summary.run_dir, {
        "run_id": run.run_id,
        "status": run.status,
        "workflow": run.workflow,
        "tasks": tasks,
    }
