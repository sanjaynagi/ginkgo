"""Focused tests for JSONL runtime event rendering."""

from __future__ import annotations

import io
import json

from ginkgo.cli.renderers.jsonl import JsonlEventRenderer
from ginkgo.runtime.events import PhaseTimed, TaskPlanned, TaskStaging


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


def test_jsonl_renderer_emits_the_ledger_event_types() -> None:
    """The new store-facing events reach agents like any other event."""
    stream = io.StringIO()
    renderer = JsonlEventRenderer(stream=stream)

    renderer(
        TaskPlanned(
            run_id="run_123",
            task_id="task_0001",
            task_name="example.task",
            cache_key="cache_abc",
            inputs={"rows": 3},
        )
    )
    renderer(PhaseTimed(run_id="run_123", scope="run", phase="validate", seconds=0.5))

    payloads = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [payload["event"] for payload in payloads] == ["task_planned", "phase_timed"]
    assert payloads[0]["cache_key"] == "cache_abc"
    assert payloads[1]["seconds"] == 0.5
