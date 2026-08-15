"""Named executors: config parsing, routing, and dispatch wiring."""

from __future__ import annotations

import pytest

from ginkgo import task
from ginkgo.core.task import TaskDef
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.remote_executor import RemoteDispatchStats, RemoteExecutor, RemoteJobHandle


class _FakeRemoteExecutor(RemoteExecutor):
    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        raise AssertionError("routing tests never submit")


CONFIG = {
    "remote": {
        "executors": {
            "gpu-k8s": {"type": "k8s", "namespace": "ml", "image": "reg/worker:v2"},
            "cheap-batch": {
                "type": "batch",
                "project": "proj",
                "image": "reg/worker:v2",
                "code": {"mode": "sync", "package": "my_workflow"},
            },
        }
    }
}


@task(executor="gpu-k8s")
def trains(x: int) -> int:
    return x


@task(executor="cheap-batch")
def crunches(x: int) -> int:
    return x


@task(remote=True)
def follows_default(x: int) -> int:
    return x


@task(executor="nope")
def stray(x: int) -> int:
    return x


class TestExecutorRegistryConfig:
    def test_named_executors_are_parsed(self) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default="gpu-k8s")
        assert sorted(registry.specs) == ["cheap-batch", "gpu-k8s"]
        assert registry.specs["gpu-k8s"].type == "k8s"
        assert registry.specs["gpu-k8s"].settings["namespace"] == "ml"
        assert registry.default_name == "gpu-k8s"

    def test_code_table_is_split_out_of_settings(self) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default=None)
        assert registry.code_config("cheap-batch") == {"mode": "sync", "package": "my_workflow"}
        assert "code" not in registry.specs["cheap-batch"].settings
        assert registry.code_config("gpu-k8s") is None

    def test_legacy_sections_become_implicit_names(self) -> None:
        registry = ExecutorRegistry.from_config(
            {"remote": {"k8s": {"image": "reg/worker:v2"}}},
            default="k8s",
        )
        assert registry.specs["k8s"].type == "k8s"
        assert registry.has_default

    def test_local_default_leaves_no_default_executor(self) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default="local")
        assert not registry.has_default
        # Pinned tasks still route, which is the point of naming executors.
        assert registry.resolve("gpu-k8s", task_name="t") == "gpu-k8s"

    def test_unknown_default_lists_configured_names(self) -> None:
        with pytest.raises(ValueError, match="cheap-batch, gpu-k8s"):
            ExecutorRegistry.from_config(CONFIG, default="typo")

    def test_unknown_type_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="needs a type"):
            ExecutorRegistry.from_config(
                {"remote": {"executors": {"weird": {"type": "slurm"}}}},
                default=None,
            )

    def test_reserved_local_name_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="reserved executor name"):
            ExecutorRegistry.from_config(
                {"remote": {"executors": {"local": {"type": "k8s"}}}},
                default=None,
            )

    def test_empty_config_has_no_executors(self) -> None:
        registry = ExecutorRegistry.from_config({}, default=None)
        assert registry.specs == {}
        assert "No executors are configured" in registry.available_hint()

    def test_backend_is_built_lazily_and_once(self, monkeypatch) -> None:
        import ginkgo.runtime.executor_registry as module

        builds: list[str] = []

        def fake_build(spec):
            builds.append(spec.name)
            return _FakeRemoteExecutor()

        monkeypatch.setattr(module, "_build_executor", fake_build)
        registry = ExecutorRegistry.from_config(CONFIG, default="gpu-k8s")
        assert builds == []  # parsing config builds nothing

        first = registry.get("gpu-k8s")
        second = registry.get("gpu-k8s")
        assert first is second
        assert builds == ["gpu-k8s"]  # the unused executor is never constructed

    def test_label_names_the_backend(self) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default=None)
        assert registry.label("gpu-k8s") == "gpu-k8s (Kubernetes)"
        assert registry.label("cheap-batch") == "cheap-batch (GCP Batch)"
        legacy = ExecutorRegistry.from_config(
            {"remote": {"batch": {"project": "p", "image": "i"}}}, default=None
        )
        assert legacy.label("batch") == "GCP Batch"


class TestExecutorRouting:
    def _evaluator(self, *, default: str | None) -> ConcurrentEvaluator:
        registry = ExecutorRegistry.from_config(CONFIG, default=default)
        for name in registry.specs:
            registry._built[name] = _FakeRemoteExecutor()
        return ConcurrentEvaluator(jobs=1, executor_registry=registry)

    def test_task_routes_to_the_executor_it_names(self) -> None:
        evaluator = self._evaluator(default=None)
        assert evaluator._resolve_placement(task_def=trains) == "gpu-k8s"
        assert evaluator._resolve_placement(task_def=crunches) == "cheap-batch"

    def test_pinned_executor_beats_the_run_default(self) -> None:
        evaluator = self._evaluator(default="cheap-batch")
        assert evaluator._resolve_placement(task_def=trains) == "gpu-k8s"
        assert evaluator._resolve_placement(task_def=follows_default) == "cheap-batch"

    def test_unknown_executor_name_fails_at_build(self) -> None:
        evaluator = self._evaluator(default="gpu-k8s")
        with pytest.raises(ValueError, match="executor='nope', which is not configured"):
            evaluator.build_and_validate(stray(x=1))

    def test_non_python_kind_with_executor_is_rejected(self) -> None:
        @task(kind="shell", executor="gpu-k8s")
        def shelled() -> str:
            raise AssertionError("never runs")

        evaluator = self._evaluator(default=None)
        with pytest.raises(ValueError, match="only supports python tasks"):
            evaluator._resolve_placement(task_def=shelled)


class TestTaskDeclaration:
    def test_executor_lands_on_the_task_def(self) -> None:
        assert trains.executor == "gpu-k8s"
        assert trains.remote is False

    def test_executor_and_remote_together_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="drop remote=True"):

            @task(executor="gpu-k8s", remote=True)
            def both(x: int) -> int:
                return x

    def test_empty_executor_name_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty name"):
            TaskDef(fn=lambda: None, executor="  ")


class TestDispatchStats:
    def test_summary_splits_multi_executor_runs(self) -> None:
        stats = RemoteDispatchStats()
        stats.record_submit(executor="gpu-k8s")
        stats.record_submit(executor="gpu-k8s")
        stats.record_submit(executor="cheap-batch")
        summary = stats.summary()
        assert summary is not None
        assert "3 remote (gpu-k8s 2, cheap-batch 1)" in summary

    def test_single_executor_summary_stays_terse(self) -> None:
        stats = RemoteDispatchStats()
        stats.record_submit(executor="gpu-k8s")
        summary = stats.summary()
        assert summary is not None
        assert summary.startswith("1 remote")
        assert "gpu-k8s" not in summary
