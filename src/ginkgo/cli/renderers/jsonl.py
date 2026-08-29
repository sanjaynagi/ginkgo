"""JSONL event renderer for agent-oriented run output."""

from __future__ import annotations

import json
from typing import Any, TextIO

from ginkgo.runtime.events import GinkgoEvent

__all__ = ["JsonlEventRenderer", "event_line"]


def event_line(payload: dict[str, Any]) -> str:
    """Return one event's JSONL line, newline included.

    The wire shape of ``--agent-output``. The ledger stores the same payloads,
    so ``ginkgo export events`` replays a finished run through this function and
    an agent reading either stream reads the same bytes.
    """
    return json.dumps(payload, sort_keys=True) + "\n"


class JsonlEventRenderer:
    """Write one runtime event per JSONL line."""

    def __init__(self, *, stream: TextIO, include_task_logs: bool = True) -> None:
        self._stream = stream
        self._include_task_logs = include_task_logs

    def __call__(self, event: GinkgoEvent) -> None:
        if not self._include_task_logs and event.event == "task_log":
            return
        self._stream.write(event_line(event.to_payload()))
        self._stream.flush()
