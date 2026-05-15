"""JSONL event renderer for agent-oriented run output."""

from __future__ import annotations

import json
from typing import TextIO

from ginkgo.runtime.events import GinkgoEvent


class JsonlEventRenderer:
    """Write one runtime event per JSONL line."""

    def __init__(self, *, stream: TextIO, include_task_logs: bool = True) -> None:
        self._stream = stream
        self._include_task_logs = include_task_logs

    def __call__(self, event: GinkgoEvent) -> None:
        if not self._include_task_logs and event.event == "task_log":
            return
        self._stream.write(json.dumps(event.to_payload(), sort_keys=True))
        self._stream.write("\n")
        self._stream.flush()
