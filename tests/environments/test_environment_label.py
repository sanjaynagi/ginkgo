"""Unit tests for the CLI environment label."""

from __future__ import annotations

import pytest

from ginkgo.cli.renderers.common import environment_label
from ginkgo.envs.container import _CONTAINER_SCHEMES


@pytest.mark.parametrize(
    ("env", "label"),
    [
        (None, "local"),
        # ginkgo-pixi labels bare names ``pixi:<name>``; core alone has no label.
        ("analysis_tools", "analysis_tools"),
        *[(f"{scheme}ubuntu:24.04", f"{scheme}ubuntu:24.04") for scheme in _CONTAINER_SCHEMES],
    ],
)
def test_environment_label(env: str | None, label: str) -> None:
    assert environment_label(env) == label
