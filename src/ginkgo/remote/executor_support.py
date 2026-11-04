"""Helpers every remote executor plugin needs to speak the worker protocol.

An executor ships an attempt payload to ``python -m ginkgo.remote.worker`` and
reads back the JSON line the worker prints last. These helpers lay out the
job's containers, encode the payload, name the job, detect whether it needs
FUSE, and parse the result, so each executor plugin implements only its own
submission and polling.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

WORKER_COMMAND = ("python", "-m", "ginkgo.remote.worker")
"""How a worker container runs the worker."""

SHARED_SCRATCH = "/ginkgo-scratch"
"""Where every container of a job with several steps mounts the volume they share.

The worker steps use it as their scratch root, so the code, inputs and outputs
a task's command names are at the same paths in the task's own image.
"""

COMMAND_DIR = "ginkgo-command"
"""The directory, under the scratch root, holding a task's command for its image.

Stage-in leaves the command there as ``command.sh``, which writes the
command's ``stdout``, ``stderr`` and ``exit_code`` beside itself.
"""


@dataclass(frozen=True, kw_only=True)
class JobStep:
    """One container of a remote job, whose steps run one after another.

    Parameters
    ----------
    name : str
        The container's name.
    image : str | None
        The image to run, or ``None`` for the executor's worker image. Only a
        worker step gets the attempt payload and the executor's own
        environment variables.
    command : tuple[str, ...]
        The command to run. A worker step's replaces the worker image's
        entrypoint. The task step's is passed to its image's own entrypoint,
        as ``docker run image cmd`` passes it locally, so an entrypoint that
        sets up the image's environment (micromamba's, say) still runs.
    env : dict[str, str]
        Variables this step needs on top of those.
    """

    name: str
    image: str | None
    command: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)


def job_steps(attempt: dict[str, Any]) -> list[JobStep]:
    """Return the containers that run *attempt*, in the order they run.

    A task runs in one worker container, unless its command runs in an image
    of its own, the attempt's ``image``. That takes three containers sharing
    a volume at :data:`SHARED_SCRATCH`: the worker stages the inputs in and
    leaves the command there, the task's image runs it with ``sh``, and the
    worker stages the outputs out and reports the result. Each runs once the
    one before it has exited. Stage-in exits 0 even when the task fails, and
    the task's step records its command's exit status rather than exiting
    with it, so stage-out always runs to report the result.
    """
    image = attempt.get("image")
    if image is None:
        return [JobStep(name="ginkgo-worker", image=None, command=WORKER_COMMAND)]

    def worker_step(phase: str) -> JobStep:
        env = {"GINKGO_SCRATCH_ROOT": SHARED_SCRATCH, "GINKGO_WORKER_PHASE": phase}
        return JobStep(name=phase, image=None, command=WORKER_COMMAND, env=env)

    return [
        worker_step("stage-in"),
        JobStep(
            name="task", image=image, command=("sh", f"{SHARED_SCRATCH}/{COMMAND_DIR}/command.sh")
        ),
        worker_step("stage-out"),
    ]


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
