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


# Same executors without code sync, for dispatch tests that would otherwise
# try to tar and upload a workflow package.
NO_CODE_CONFIG = {
    "remote": {
        "executors": {
            name: {key: value for key, value in table.items() if key != "code"}
            for name, table in CONFIG["remote"]["executors"].items()
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


@task()
def local_step(x: int) -> int:
    return x * 2


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


class _RecordingHandle:
    """Job handle that succeeds immediately with a fixed worker payload."""

    def __init__(self, *, job_id: str, result_value: object) -> None:
        self._job_id = job_id
        self._result_value = result_value

    @property
    def job_id(self) -> str:
        return self._job_id

    def state(self):
        from ginkgo.runtime.remote_executor import RemoteJobState

        return RemoteJobState.SUCCEEDED

    def result(self):
        from ginkgo.runtime.remote_executor import RemoteJobResult, RemoteJobState

        return RemoteJobResult(
            state=RemoteJobState.SUCCEEDED,
            payload={"ok": True, "result": self._result_value, "result_encoding": "raw"},
            exit_code=0,
        )

    def cancel(self) -> None:  # pragma: no cover - not reached in these tests
        raise AssertionError("no cancellation in these tests")

    def logs_tail(self, *, lines: int = 100) -> str:
        return ""


class _RecordingExecutor(RemoteExecutor):
    """Executor that records every payload it is handed."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.submitted: list[dict] = []

    def submit(self, *, attempt: dict) -> RemoteJobHandle:
        self.submitted.append(attempt)
        return _RecordingHandle(
            job_id=f"{self.name}-{len(self.submitted)}",
            result_value=attempt["task_name"],
        )


class TestDispatchManagerRouting:
    """Each task reaches the executor placement chose, and only that one."""

    def _manager(self, tmp_path, registry):
        from ginkgo.runtime.caching.digest_registry import DigestRegistry
        from ginkgo.runtime.remote_dispatch import RemoteDispatchManager

        return RemoteDispatchManager(
            registry=registry,
            digests=DigestRegistry(),
            local_artifact_store=None,
            staging_cache_path=tmp_path / "staged.json",
            run_id_provider=lambda: "run",
            emit_event=lambda event: None,
        )

    def _node(self, *, node_id: int, task_def):
        from ginkgo.core.expr import Expr
        from ginkgo.runtime.evaluator import NodeRun, TaskNode

        expr = Expr(task_def=task_def, args={"x": 1})
        return NodeRun(node=TaskNode(node_id=node_id, expr=expr, dependency_ids=frozenset()))

    def test_tasks_reach_only_the_executor_they_name(self, tmp_path, monkeypatch) -> None:
        from concurrent.futures import ThreadPoolExecutor

        import ginkgo.runtime.executor_registry as registry_module
        import ginkgo.runtime.remote_dispatch as dispatch_module

        backends = {"gpu-k8s": _RecordingExecutor("gpu-k8s")}
        backends["cheap-batch"] = _RecordingExecutor("cheap-batch")
        monkeypatch.setattr(registry_module, "_build_executor", lambda spec: backends[spec.name])
        # The artifact store needs project config; these payloads carry no
        # file arguments, so skip it.
        monkeypatch.setattr(
            dispatch_module.RemoteDispatchManager, "_ensure_artifact_store", lambda self: None
        )

        registry = ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s")
        manager = self._manager(tmp_path, registry)

        with ThreadPoolExecutor(max_workers=2) as watcher:
            futures = [
                manager.dispatch(
                    node=self._node(node_id=1, task_def=trains),
                    executor_name="gpu-k8s",
                    payload={"args": {}, "task_name": "trains"},
                    gpu_type="nvidia-l4",
                    watcher=watcher,
                ),
                manager.dispatch(
                    node=self._node(node_id=2, task_def=crunches),
                    executor_name="cheap-batch",
                    payload={"args": {}, "task_name": "crunches"},
                    gpu_type=None,
                    watcher=watcher,
                ),
            ]
            results = [future.result(timeout=30) for future in futures]

        assert [payload["task_name"] for payload in backends["gpu-k8s"].submitted] == ["trains"]
        assert [payload["task_name"] for payload in backends["cheap-batch"].submitted] == [
            "crunches"
        ]
        assert sorted(result["result"] for result in results) == ["crunches", "trains"]
        # Handles from both executors coexist, keyed by node.
        assert manager.handle_for(1) is not None
        assert manager.handle_for(2) is not None
        assert manager.stats.submitted_by_executor == {"gpu-k8s": 1, "cheap-batch": 1}

    def test_gpu_type_rides_along_per_task(self, tmp_path, monkeypatch) -> None:
        from concurrent.futures import ThreadPoolExecutor

        import ginkgo.runtime.executor_registry as registry_module
        import ginkgo.runtime.remote_dispatch as dispatch_module

        backend = _RecordingExecutor("gpu-k8s")
        monkeypatch.setattr(registry_module, "_build_executor", lambda spec: backend)
        monkeypatch.setattr(
            dispatch_module.RemoteDispatchManager, "_ensure_artifact_store", lambda self: None
        )

        registry = ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s")
        manager = self._manager(tmp_path, registry)
        with ThreadPoolExecutor(max_workers=1) as watcher:
            manager.dispatch(
                node=self._node(node_id=1, task_def=trains),
                executor_name="gpu-k8s",
                payload={"args": {}, "task_name": "trains"},
                gpu_type="nvidia-l4",
                watcher=watcher,
            ).result(timeout=30)

        assert backend.submitted[0]["resources"]["gpu_type"] == "nvidia-l4"


class TestCodeBundlePerExecutor:
    """Code sync is per executor, with one upload shared by identical configs."""

    def _manager(self, tmp_path, registry):
        from ginkgo.runtime.caching.digest_registry import DigestRegistry
        from ginkgo.runtime.remote_dispatch import RemoteDispatchManager

        return RemoteDispatchManager(
            registry=registry,
            digests=DigestRegistry(),
            local_artifact_store=None,
            staging_cache_path=tmp_path / "staged.json",
            run_id_provider=lambda: "run",
            emit_event=lambda event: None,
        )

    def test_identical_code_configs_share_one_upload(self, tmp_path, monkeypatch) -> None:
        config = {
            "remote": {
                "executors": {
                    "a": {
                        "type": "k8s",
                        "image": "i",
                        "code": {"mode": "sync", "package": "pkg"},
                    },
                    "b": {
                        "type": "batch",
                        "project": "p",
                        "image": "i",
                        "code": {"mode": "sync", "package": "pkg"},
                    },
                    "c": {
                        "type": "k8s",
                        "image": "i",
                        "code": {"mode": "sync", "package": "other"},
                    },
                    "baked": {"type": "k8s", "image": "i"},
                }
            }
        }
        registry = ExecutorRegistry.from_config(config, default=None)
        manager = self._manager(tmp_path, registry)

        uploads: list[str] = []

        def fake_publish(self, *, config):
            if config is None or config.get("mode") != "sync":
                return None
            key = (config["package"], ())
            cached = self._bundles.get(key)
            if cached is not None:
                return cached
            uploads.append(config["package"])
            meta = {"package": config["package"], "digest": f"sha-{config['package']}"}
            self._bundles[key] = meta
            return meta

        monkeypatch.setattr(type(manager), "_publish_code_bundle", fake_publish, raising=True)

        first = manager._ensure_code_bundle(executor_name="a")
        second = manager._ensure_code_bundle(executor_name="b")
        third = manager._ensure_code_bundle(executor_name="c")
        baked = manager._ensure_code_bundle(executor_name="baked")

        assert first == second  # same package -> one bundle
        assert uploads == ["pkg", "other"]  # uploaded once each, not per executor
        assert third["package"] == "other"
        assert baked is None
        # Resolved bundles are memoized per executor.
        assert manager._ensure_code_bundle(executor_name="a") is first
        assert uploads == ["pkg", "other"]

    def test_baked_mode_attaches_no_bundle(self, tmp_path) -> None:
        registry = ExecutorRegistry.from_config(
            {"remote": {"executors": {"a": {"type": "k8s", "image": "i"}}}}, default=None
        )
        manager = self._manager(tmp_path, registry)
        assert manager._ensure_code_bundle(executor_name="a") is None


class TestExecutionBackendContract:
    """execution_backend carries the executor name, and 'local' when local."""

    def test_local_task_reports_local(self) -> None:
        from ginkgo import evaluate
        from ginkgo.runtime.events import EventBus, TaskStarted

        seen: list[str | None] = []
        bus = EventBus()
        bus.subscribe(
            lambda event: (
                seen.append(event.execution_backend) if isinstance(event, TaskStarted) else None
            )
        )
        assert evaluate(local_step(x=2), jobs=1, event_bus=bus) == 4
        assert seen == ["local"]

    def test_remote_task_reports_the_executor_name(self, monkeypatch) -> None:
        import ginkgo.runtime.executor_registry as registry_module
        import ginkgo.runtime.remote_dispatch as dispatch_module
        from ginkgo.runtime.events import EventBus, TaskStarted

        backends: dict[str, _RecordingExecutor] = {}

        def fake_build(spec):
            backends[spec.name] = _RecordingExecutor(spec.name)
            return backends[spec.name]

        monkeypatch.setattr(registry_module, "_build_executor", fake_build)
        monkeypatch.setattr(
            dispatch_module.RemoteDispatchManager, "_ensure_artifact_store", lambda self: None
        )

        seen: list[str | None] = []
        bus = EventBus()
        bus.subscribe(
            lambda event: (
                seen.append(event.execution_backend) if isinstance(event, TaskStarted) else None
            )
        )
        evaluator = ConcurrentEvaluator(
            jobs=1,
            event_bus=bus,
            executor_registry=ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s"),
        )
        evaluator.evaluate(trains(x=1))
        assert seen == ["gpu-k8s"]
        # The event name is only trustworthy if the task really was submitted.
        assert list(backends) == ["gpu-k8s"]
        assert len(backends["gpu-k8s"].submitted) == 1
