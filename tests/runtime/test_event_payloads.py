"""Golden payloads for every runtime event.

The event stream is a wire format: agents parse it, and from the provenance
ledger onwards it is also the storage format. A field renamed or dropped by
accident is a silent break in both, so every event type has a checked-in
payload, and each event built from its golden fields must serialise back to
exactly that payload: a renamed or removed field fails to construct, and an
added field or a bumped ``v`` shows up as a difference. Regenerate a fixture
only when the change is deliberate. ``tests/store/test_projector.py`` replays
the same fixtures through the projector.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
from pathlib import Path
from typing import Any

import pytest

from ginkgo.runtime import events as events_module
from ginkgo.runtime.events import GinkgoEvent, TaskFailed

FIXTURES = Path(__file__).parent / "fixtures" / "events"
NAMES = sorted(path.stem for path in FIXTURES.glob("*.json"))


def _event_classes() -> dict[str, type[GinkgoEvent]]:
    """Every concrete event type declared in ``runtime/events.py``, by event name."""
    abstract = {GinkgoEvent.__name__, "RunEvent", "TaskEvent"}
    return {
        next(f.default for f in dataclasses.fields(member) if f.name == "event"): member
        for _, member in inspect.getmembers(events_module, inspect.isclass)
        if issubclass(member, GinkgoEvent) and member.__name__ not in abstract
    }


def _golden(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _from_golden(name: str, **changes: Any) -> GinkgoEvent:
    fields = {k: v for k, v in _golden(name).items() if k not in ("event", "v")}
    return _event_classes()[name](**{**fields, **changes})


def test_every_event_type_has_a_golden_payload() -> None:
    assert set(_event_classes()) == set(NAMES)


@pytest.mark.parametrize("name", NAMES)
def test_payload_matches_the_golden_fixture(name: str) -> None:
    assert _from_golden(name).to_payload() == _golden(name)


def test_an_ignored_failure_is_the_same_payload_plus_one_flag() -> None:
    """The golden ``task_failed`` is the fatal shape; ``ignored`` is the variant."""
    fatal = _golden("task_failed")
    ignored = _from_golden("task_failed", ignored=True)

    assert isinstance(ignored, TaskFailed)
    assert fatal["ignored"] is False
    assert ignored.to_payload() == {**fatal, "ignored": True}
