"""Shared fixtures for ginkgo tests."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.events import (
    EventBus,
    GinkgoEvent,
    TaskCacheHit,
    TaskCompleted,
    TaskFailed,
    TaskStaging,
    TaskStarted,
)


def make_download_backend(*, content: bytes = b"hello world", etag: str = "etag1") -> MagicMock:
    """Return a mock ``RemoteStorageBackend`` whose download writes fixed bytes.

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
