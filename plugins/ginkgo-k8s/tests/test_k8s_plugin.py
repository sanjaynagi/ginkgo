"""ginkgo-k8s registers the k8s executor type and builds it from settings."""

from __future__ import annotations

from ginkgo.plugins.api import ExecutorType
from ginkgo.plugins.registry import plugin_registry
from ginkgo_k8s.executor import KubernetesExecutor, build_kubernetes_executor


def test_the_installed_plugin_provides_the_k8s_executor_type() -> None:
    executor_type = plugin_registry().get(ExecutorType, "k8s")

    assert executor_type.label == "Kubernetes"
    assert executor_type.required_settings == ("image",)


def test_settings_map_onto_the_executor_with_defaults() -> None:
    executor = build_kubernetes_executor({"image": "reg/worker:1", "namespace": "ml"})

    assert isinstance(executor, KubernetesExecutor)
    assert (executor.image, executor.namespace) == ("reg/worker:1", "ml")
    assert executor.backoff_limit == 0
