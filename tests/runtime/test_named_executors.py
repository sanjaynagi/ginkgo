"""Named executors: config parsing, routing, and dispatch wiring."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor

import pytest

from ginkgo import evaluate, task
from ginkgo.core.expr import Expr
from ginkgo.core.task import TaskDef
from ginkgo.plugins.registry import PluginNotFoundError
from ginkgo.runtime.caching.digest_registry import DigestRegistry
from ginkgo.runtime.evaluator import ConcurrentEvaluator, NodeRun
from ginkgo.runtime.events import EventBus, TaskStarted
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.graph import TaskNode
from ginkgo.runtime.remote_dispatch import RemoteDispatchManager
from ginkgo.runtime.remote_executor import RemoteDispatchStats
from tests.runtime.conftest import prebuilt_registry

pytestmark = pytest.mark.usefixtures("stub_executor_types")


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


def _executors(executors: dict) -> dict:
    return {"remote": {"executors": executors}}


@task(executor="gpu-k8s")
def trains(x: int) -> str:
    return str(x)


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


@task()
def local_fails(x: int) -> int:
    raise ValueError("local failure")


@task(kind="shell", executor="gpu-k8s")
def shelled() -> str:
    raise AssertionError("never runs")


class TestExecutorRegistryConfig:
    def test_named_executors_are_parsed_with_code_split_out(self) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default="gpu-k8s")
        assert sorted(registry.specs) == ["cheap-batch", "gpu-k8s"]
        assert registry.specs["gpu-k8s"].type == "k8s"
        assert registry.specs["gpu-k8s"].settings["namespace"] == "ml"
        assert registry.default_name == "gpu-k8s"
        assert registry.code_config("cheap-batch") == {"mode": "sync", "package": "my_workflow"}
        assert "code" not in registry.specs["cheap-batch"].settings
        assert registry.code_config("gpu-k8s") is None

    def test_a_legacy_type_section_or_empty_config_has_no_executors(self) -> None:
        legacy = ExecutorRegistry.from_config({"remote": {"k8s": {"image": "i"}}}, default=None)
        assert legacy.specs == {}
        assert "No executors are configured" in legacy.available_hint()

    def test_local_default_leaves_no_default_executor(self) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default="local")
        assert not registry.has_default
        # Pinned tasks still route, which is the point of naming executors.
        assert registry.resolve("gpu-k8s", task_name="t") == "gpu-k8s"

    @pytest.mark.parametrize(
        ("config", "default", "error", "match"),
        [
            (CONFIG, "typo", ValueError, "cheap-batch, gpu-k8s"),
            (
                _executors({"weird": {"type": "slurm"}}),
                None,
                PluginNotFoundError,
                r"\[remote.executors.weird\].*'slurm'.*'batch', 'k8s'",
            ),
            (_executors({"local": {"type": "k8s"}}), None, ValueError, "reserved executor name"),
        ],
        ids=["unknown-default", "unknown-type", "reserved-local"],
    )
    def test_bad_config_is_refused(self, config, default, error, match: str) -> None:
        with pytest.raises(error, match=match):
            ExecutorRegistry.from_config(config, default=default)

    def test_backend_is_built_lazily_and_once(self, recording_executors) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default="gpu-k8s")
        assert recording_executors == {}  # parsing config builds nothing

        assert registry.get("gpu-k8s") is registry.get("gpu-k8s")
        assert list(recording_executors) == ["gpu-k8s"]  # the unused one is never built

    def test_label_names_the_backend(self) -> None:
        registry = ExecutorRegistry.from_config(CONFIG, default=None)
        assert registry.label("gpu-k8s") == "gpu-k8s (Kubernetes)"
        assert registry.label("cheap-batch") == "cheap-batch (GCP Batch)"
        same_name = ExecutorRegistry.from_config(
            _executors({"batch": {"type": "batch", "project": "p", "image": "i"}}), default=None
        )
        assert same_name.label("batch") == "GCP Batch"


class TestSettingsValidation:
    """Missing required settings fail before the run, not at first dispatch."""

    @pytest.mark.parametrize(
        ("table", "match"),
        [
            ({"type": "k8s", "namespace": "ml"}, r"requires a image.*\[remote.executors.half\]"),
            ({"type": "batch", "image": "i"}, "requires a project"),
        ],
    )
    def test_missing_settings_are_reported_for_a_dispatch_target(self, table, match) -> None:
        registry = ExecutorRegistry.from_config(_executors({"half": table}), default="half")
        with pytest.raises(ValueError, match=match):
            registry.validate_settings(names=["half"])
        # A local run must not fail over a section it never dispatches to.
        registry.validate_settings(names=[])


class TestCodeSyncGaps:
    """Executors that ship no code while a configured peer syncs its own."""

    SPLIT = _executors(
        {
            "syncer": {"type": "k8s", "image": "i", "code": {"mode": "sync", "package": "wf"}},
            "cheap-batch": {"type": "batch", "project": "p", "image": "i"},
        }
    )

    def test_executor_without_code_table_is_reported(self) -> None:
        registry = ExecutorRegistry.from_config(self.SPLIT, default="cheap-batch")
        (message,) = registry.code_sync_gaps(names=["cheap-batch"])
        for section in ("cheap-batch]", "syncer]", "cheap-batch.code]"):
            assert f"[remote.executors.{section}" in message
        assert registry.code_sync_gaps(names=["syncer"]) == []

    def test_nothing_is_reported_when_no_executor_syncs(self) -> None:
        registry = ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s")
        assert registry.code_sync_gaps(names=["gpu-k8s", "cheap-batch"]) == []


class TestExecutorRouting:
    def _evaluator(self, *, default: str | None) -> ConcurrentEvaluator:
        registry = prebuilt_registry(CONFIG, default=default)
        return ConcurrentEvaluator(jobs=1, executor_registry=registry)

    def test_pinned_executor_beats_the_run_default(self) -> None:
        unset = self._evaluator(default=None)
        assert unset.placement.resolve(task_def=trains) == "gpu-k8s"
        assert unset.placement.resolve(task_def=crunches) == "cheap-batch"
        evaluator = self._evaluator(default="cheap-batch")
        assert evaluator.placement.resolve(task_def=trains) == "gpu-k8s"
        assert evaluator.placement.resolve(task_def=follows_default) == "cheap-batch"

    def test_unknown_executor_name_fails_at_build(self) -> None:
        evaluator = self._evaluator(default="gpu-k8s")
        with pytest.raises(ValueError, match="executor='nope', which is not configured"):
            evaluator.build_graph(stray(x=1))
            evaluator.validate_graph()

    def test_non_python_kind_with_executor_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot be dispatched remotely"):
            self._evaluator(default=None).placement.resolve(task_def=shelled)


class TestTaskDeclaration:
    def test_executor_lands_on_the_task_def(self) -> None:
        assert (trains.executor, trains.remote) == ("gpu-k8s", False)

    def test_executor_and_remote_together_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="drop remote=True"):
            task(executor="gpu-k8s", remote=True)(lambda x: x)

    def test_empty_executor_name_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty name"):
            TaskDef(fn=lambda: None, executor="  ")


def test_dispatch_summary_splits_multi_executor_runs_only() -> None:
    stats = RemoteDispatchStats()
    stats.record_submit(executor="gpu-k8s")
    assert stats.summary().startswith("1 remote")
    assert "gpu-k8s" not in stats.summary()

    stats.record_submit(executor="gpu-k8s")
    stats.record_submit(executor="cheap-batch")
    assert "3 remote (gpu-k8s 2, cheap-batch 1)" in stats.summary()


def _manager(registry: ExecutorRegistry) -> RemoteDispatchManager:
    return RemoteDispatchManager(
        registry=registry,
        digests=DigestRegistry(),
        local_artifact_store=None,
        run_id_provider=lambda: "run",
        emit_event=lambda event: None,
    )


def _node(*, node_id: int, task_def) -> NodeRun:
    expr = Expr(task_def=task_def, args={"x": 1})
    return NodeRun(node=TaskNode(node_id=node_id, expr=expr, dependency_ids=frozenset()))


def test_tasks_reach_only_the_executor_they_name(recording_executors) -> None:
    manager = _manager(ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s"))

    with ThreadPoolExecutor(max_workers=2) as watcher:
        futures = [
            manager.dispatch(
                node=_node(node_id=node_id, task_def=task_def),
                executor_name=executor,
                payload={"args": {}, "task_name": task_def.base_name},
                gpu_type=gpu_type,
                watcher=watcher,
            )
            for node_id, task_def, executor, gpu_type in [
                (1, trains, "gpu-k8s", "nvidia-l4"),
                (2, crunches, "cheap-batch", None),
            ]
        ]
        results = [future.result(timeout=30) for future in futures]

    gpu, batch = recording_executors["gpu-k8s"], recording_executors["cheap-batch"]
    assert [payload["task_name"] for payload in gpu.submitted] == ["trains"]
    assert [payload["task_name"] for payload in batch.submitted] == ["crunches"]
    # The GPU type rides along per task.
    assert gpu.submitted[0]["resources"]["gpu_type"] == "nvidia-l4"
    assert sorted(result["result"] for result in results) == ["crunches", "trains"]
    # Handles from both executors coexist, keyed by node.
    assert manager.handle_for(1) is not None and manager.handle_for(2) is not None
    assert manager.stats.submitted_by_executor == {"gpu-k8s": 1, "cheap-batch": 1}


def test_identical_code_configs_share_one_upload(tmp_path, monkeypatch) -> None:
    """Code sync is per executor, with one upload shared by identical configs."""
    import ginkgo.remote.code_bundle as code_bundle
    import ginkgo.remote.resolve as resolve

    (tmp_path / "ginkgo.toml").write_text('[remote.artifacts]\nstore = "s3://bucket/code"\n')
    for package in ("pkg", "other"):
        (tmp_path / package).mkdir()
        (tmp_path / package / "__init__.py").write_text("")
    uploads: list[str] = []

    def record_upload(*, bundle_path, digest, **_):
        uploads.append(digest)
        return f"code/{digest}.tar.gz"

    monkeypatch.setattr(code_bundle, "publish_code_bundle", record_upload)
    monkeypatch.setattr(resolve, "resolve_backend", lambda scheme: object())

    def sync(package: str) -> dict:
        return {"mode": "sync", "package": package}

    manager = _manager(
        ExecutorRegistry.from_config(
            _executors(
                {
                    "a": {"type": "k8s", "image": "i", "code": sync("pkg")},
                    "b": {"type": "batch", "project": "p", "image": "i", "code": sync("pkg")},
                    "c": {"type": "k8s", "image": "i", "code": sync("other")},
                    "baked": {"type": "k8s", "image": "i"},
                }
            ),
            default=None,
        )
    )

    first = manager._ensure_code_bundle(executor_name="a")
    assert manager._ensure_code_bundle(executor_name="b") == first  # same package, one bundle
    assert manager._ensure_code_bundle(executor_name="c")["package"] == "other"
    assert manager._ensure_code_bundle(executor_name="baked") is None
    assert manager._ensure_code_bundle(executor_name="a") is first
    assert len(uploads) == 2  # once per distinct package, not per executor


def _started_backends() -> tuple[list[str | None], EventBus]:
    seen: list[str | None] = []
    bus = EventBus()
    bus.subscribe(
        lambda e: seen.append(e.execution_backend) if isinstance(e, TaskStarted) else None
    )
    return seen, bus


class TestExecutionBackendContract:
    """execution_backend carries the executor name, and 'local' when local."""

    def test_local_task_reports_local(self) -> None:
        seen, bus = _started_backends()
        assert evaluate(local_step(x=2), jobs=1, event_bus=bus) == 4
        assert seen == ["local"]

    def test_remote_task_reports_the_executor_name(self, recording_executors) -> None:
        seen, bus = _started_backends()
        registry = ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s")
        ConcurrentEvaluator(jobs=1, event_bus=bus, executor_registry=registry).evaluate(
            trains(x=1)
        )
        assert seen == ["gpu-k8s"]
        # The event name is only trustworthy if the task really was submitted.
        assert list(recording_executors) == ["gpu-k8s"]
        assert len(recording_executors["gpu-k8s"].submitted) == 1


class _QueuedFuture(Future):
    """Future of a work item that no pool thread has picked up yet.

    A pool worker notifies waiters of a cancelled item when it dequeues it;
    this future does so on cancellation, as no worker ever will.
    """

    def cancel(self) -> bool:
        if self.cancelled():
            return True
        if not super().cancel():
            return False
        self.set_running_or_notify_cancel()
        return True


class _QueuedWatcher:
    """Watcher pool whose futures stay queued, never picked up by a thread."""

    def submit(self, fn, *args, **kwargs) -> _QueuedFuture:
        return _QueuedFuture()


def test_fail_fast_cancels_a_queued_remote_job_on_its_executor(
    recording_executors, monkeypatch
) -> None:
    from ginkgo.runtime.executors import Executors

    # The job is submitted but its watcher never starts, so fail-fast
    # cancels the watcher future while the job is in flight.
    monkeypatch.setattr(Executors, "get_or_create_remote_watcher", lambda self: _QueuedWatcher())
    evaluator = ConcurrentEvaluator(
        jobs=2,
        executor_registry=ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s"),
    )

    with pytest.raises(ValueError, match="local failure"):
        evaluator.evaluate([trains(x=1), local_fails(x=0)])

    assert recording_executors["gpu-k8s"].cancelled == [evaluator._nodes[0].task_def.name]


def test_a_remote_task_without_an_artifact_store_fails_before_submitting(monkeypatch) -> None:
    import ginkgo.runtime.executor_registry as registry_module
    from ginkgo.runtime.remote_dispatch import RemoteArtifactStoreMissingError
    from tests.runtime.conftest import RecordingExecutor

    backend = RecordingExecutor("gpu-k8s")
    monkeypatch.setattr(registry_module, "_build_executor", lambda spec: backend)
    evaluator = ConcurrentEvaluator(
        jobs=1, executor_registry=ExecutorRegistry.from_config(NO_CODE_CONFIG, default="gpu-k8s")
    )

    with pytest.raises(RemoteArtifactStoreMissingError, match=r"\[remote.artifacts\] store"):
        evaluator.evaluate(trains(x=1))
    assert backend.submitted == []
