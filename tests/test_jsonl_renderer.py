"""Focused tests for JSONL runtime event rendering."""

from __future__ import annotations

import io
import json

from ginkgo.cli.renderers.jsonl import JsonlEventRenderer
from ginkgo.runtime.events import TaskStaging


def test_jsonl_renderer_emits_task_staging_event() -> None:
    stream = io.StringIO()
    renderer = JsonlEventRenderer(stream=stream)

    renderer(
        TaskStaging(
            run_id="run_123",
            task_id="task_0001",
            task_name="example.task",
            attempt=1,
            status="staging",
            remote_input_count=2,
        )
    )

    payload = json.loads(stream.getvalue().strip())
    assert payload["event"] == "task_staging"
    assert payload["status"] == "staging"
    assert payload["remote_input_count"] == 2
