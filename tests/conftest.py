"""Shared fixtures for ginkgo tests."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from ginkgo.runtime.events import (
    EventBus,
    GinkgoEvent,
    TaskCacheHit,
    TaskCompleted,
    TaskFailed,
    TaskStaging,
    TaskStarted,
)


@pytest.fixture(autouse=True)
def isolate_working_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Run each test in an isolated working directory.

    This keeps phase 3 cache entries scoped to a single test and avoids
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
