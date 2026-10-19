"""ginkgo-gcp: Google Cloud integrations, starting with the ``batch`` executor type."""

from __future__ import annotations

from ginkgo.plugins.api import ExecutorType, Plugin
from ginkgo_gcp.batch import build_gcp_batch_executor

__all__ = ["PLUGIN"]

PLUGIN = Plugin(
    name="ginkgo-gcp",
    contributions=(
        ExecutorType(
            name="batch",
            label="GCP Batch",
            required_settings=("project", "image"),
            build=build_gcp_batch_executor,
        ),
    ),
)
