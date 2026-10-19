"""ginkgo-gcp registers the batch executor type and builds it from settings."""

from __future__ import annotations

import pytest

from ginkgo.plugins.api import ExecutorType
from ginkgo.plugins.registry import plugin_registry
from ginkgo_gcp.batch import GCPBatchExecutor, build_gcp_batch_executor


def test_the_installed_plugin_provides_the_batch_executor_type() -> None:
    executor_type = plugin_registry().get(ExecutorType, "batch")

    assert executor_type.label == "GCP Batch"
    assert executor_type.required_settings == ("project", "image")


def test_settings_map_onto_the_executor_with_defaults() -> None:
    executor = build_gcp_batch_executor({"project": "p", "image": "reg/worker:1"})

    assert isinstance(executor, GCPBatchExecutor)
    assert (executor.project, executor.image, executor.region) == (
        "p",
        "reg/worker:1",
        "europe-west2",
    )
    assert executor.max_run_duration == "3600s"


def test_a_missing_required_setting_fails_to_build() -> None:
    with pytest.raises(KeyError, match="project"):
        build_gcp_batch_executor({"image": "reg/worker:1"})
