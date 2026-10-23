"""JSONL event rendering: every event an agent reads arrives as one JSON line."""

from __future__ import annotations

import io
import json

from ginkgo.cli.renderers.jsonl import JsonlEventRenderer
from ginkgo.runtime.events import PhaseTimed, TaskFailed, TaskPlanned, TaskSkipped, TaskStaging

_TASK = {"run_id": "run_123", "task_id": "task_0001", "task_name": "example.task"}


def test_events_are_written_one_json_line_each_with_their_fields() -> None:
    stream = io.StringIO()
    renderer = JsonlEventRenderer(stream=stream)

    for event in (
        TaskStaging(**_TASK, attempt=1, status="staging", remote_input_count=2),
        TaskPlanned(**_TASK, cache_key="cache_abc", inputs={"rows": 3}),
        PhaseTimed(run_id="run_123", phase="validate", seconds=0.5),
        # An agent reads which failures were ignored, and what they skipped.
        TaskFailed(**_TASK, failure={"kind": "exception", "message": "bad"}, ignored=True),
        TaskSkipped(
            run_id="run_123",
            task_id="task_0002",
            task_name="example.analyse",
            blocked_by_task_id="task_0001",
            blocked_by_task_name="example.task",
        ),
    ):
        renderer(event)

    staging, planned, phase, failed, skipped = map(json.loads, stream.getvalue().splitlines())
    assert (staging["event"], staging["status"], staging["remote_input_count"]) == (
        "task_staging",
        "staging",
        2,
    )
    assert (planned["event"], planned["cache_key"]) == ("task_planned", "cache_abc")
    assert (phase["event"], phase["seconds"]) == ("phase_timed", 0.5)
    assert (failed["event"], failed["ignored"], failed["v"]) == ("task_failed", True, 2)
    assert (skipped["event"], skipped["blocked_by_task_name"]) == ("task_skipped", "example.task")
