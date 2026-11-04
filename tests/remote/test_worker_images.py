"""The repository's worker images, read as text: building them needs Docker and the network."""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _worker_parts(dockerfile: str) -> set[str]:
    """Return the plugins *dockerfile* installs, and ``pixi`` if it copies the binary in."""
    text = (REPO / dockerfile).read_text(encoding="utf-8")
    parts = set(re.findall(r"\./plugins/([\w-]+)", text))
    return parts | ({"pixi"} if "/usr/local/bin/pixi" in text else set())


def test_the_fuse_image_runs_whatever_the_worker_image_does() -> None:
    # An executor runs any task with a streamed input in its fuse_image, so
    # a Pixi task's included. Only a driver uses the Kubernetes executor.
    worker = _worker_parts("Dockerfile") - {"ginkgo-k8s"}

    assert {"pixi", "ginkgo-pixi"} <= worker
    assert worker <= _worker_parts("Dockerfile.worker-fuse")
