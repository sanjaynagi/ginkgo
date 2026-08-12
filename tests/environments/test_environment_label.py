"""Unit tests for the CLI environment label."""

from __future__ import annotations

import pytest

from ginkgo.cli.renderers.common import environment_label
from ginkgo.envs.container import _CONTAINER_SCHEMES


def test_none_env_is_local() -> None:
    assert environment_label(None) == "local"


def test_named_env_is_pixi_prefixed() -> None:
    assert environment_label("analysis_tools") == "pixi:analysis_tools"


@pytest.mark.parametrize("scheme", _CONTAINER_SCHEMES)
def test_container_env_keeps_its_uri(scheme: str) -> None:
    env = f"{scheme}ubuntu:24.04"
    assert environment_label(env) == env
