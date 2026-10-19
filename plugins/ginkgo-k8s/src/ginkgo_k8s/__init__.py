"""ginkgo-k8s: the ``k8s`` executor type, which runs tasks as Kubernetes Jobs."""

from __future__ import annotations

from ginkgo.plugins.api import ExecutorType, Plugin
from ginkgo_k8s.executor import build_kubernetes_executor

__all__ = ["PLUGIN"]

PLUGIN = Plugin(
    name="ginkgo-k8s",
    contributions=(
        ExecutorType(
            name="k8s",
            label="Kubernetes",
            required_settings=("image",),
            build=build_kubernetes_executor,
        ),
    ),
)
