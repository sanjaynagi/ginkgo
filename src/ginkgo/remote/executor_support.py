"""Helpers every remote executor plugin needs to speak the worker protocol.

An executor ships an attempt payload to ``python -m ginkgo.remote.worker`` and
reads back the JSON line the worker prints last. These helpers encode the
payload, name the job, detect whether it needs FUSE, and parse the result, so
each executor plugin implements only its own submission and polling.
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any


def encode_payload(attempt: dict[str, Any]) -> str:
    """Encode a worker payload as a base64 JSON string."""
    payload_json = json.dumps(attempt, default=str)
    return base64.b64encode(payload_json.encode()).decode()


def generate_job_name(attempt: dict[str, Any]) -> str:
    """Generate a unique remote-job identifier from the attempt payload.

    Produces a name compatible with both Kubernetes Jobs and GCP Batch
    jobs: lowercase, starts with a letter, alphanumeric + hyphens, no
    longer than 63 characters. Appends a short content-hash suffix so
    resubmissions of the same (run_id, task_id, attempt) do not collide.
    """
    run_id = attempt.get("run_id", "unknown")
    task_id = attempt.get("task_id", "unknown")
    attempt_num = attempt.get("attempt", 0)
    digest = hashlib.sha256(json.dumps(attempt, sort_keys=True, default=str).encode())
    suffix = digest.hexdigest()[:6]
    name = f"ginkgo-{run_id}-{task_id}-{attempt_num}-{suffix}"
    name = name.lower().replace("_", "-")
    if len(name) > 63:
        name = name[:63]
    return name.rstrip("-")


def payload_requires_fuse(attempt: dict[str, Any]) -> bool:
    """Return True when the payload holds any input the worker mounts."""
    from ginkgo.remote.access.protocol import is_fuse_input

    def walk(value: Any) -> bool:
        if isinstance(value, dict):
            if is_fuse_input(value):
                return True
            return any(walk(item) for item in value.values())
        if isinstance(value, (list, tuple)):
            return any(walk(item) for item in value)
        return False

    return walk(attempt.get("args"))


def parse_worker_output(logs: str, *, source_label: str = "job logs") -> dict[str, Any]:
    """Parse the worker result from container log output.

    The remote worker prints a single JSON line to stdout as its last
    output. Search backwards from the end to find it. ``source_label``
    is interpolated into the fallback error payload so callers can
    surface a more specific origin (e.g. ``"pod logs"``).
    """
    for line in reversed(logs.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    message = f"No worker output found in {source_label}"
    return {
        "ok": False,
        "error": {
            "type": "RuntimeError",
            "module": "builtins",
            "message": message,
            "args": [message],
        },
    }
