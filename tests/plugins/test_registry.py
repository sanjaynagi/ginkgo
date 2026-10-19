"""Tests for plugin discovery and lookup."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ginkgo.core.directive import ExecutionDirective
from ginkgo.core.secret import SecretRef
from ginkgo.core.task import task
from ginkgo.plugins import registry as registry_module
from ginkgo.plugins.api import (
    EventSubscriberType,
    ExecutorType,
    MountDriverType,
    ObjectStoreType,
    Plugin,
    SecretBackendType,
    TaskKind,
)
from ginkgo.plugins.builtin import BUILTIN
from ginkgo.plugins.registry import (
    PluginConflictError,
    PluginLoadError,
    PluginNotFoundError,
    PluginRegistry,
    use_plugin_registry,
)
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.executor_registry import ExecutorRegistry


class _FakeExecutor:
    def __init__(self, settings: Mapping[str, Any]) -> None:
        self.settings = dict(settings)

    def submit(self, *, attempt: dict) -> Any:
        raise NotImplementedError


@dataclass
class _VaultBackend:
    def resolve(self, *, ref: SecretRef) -> str | None:
        return f"vault:{ref.name}"


def _vault_backends(**_: Any) -> tuple[_VaultBackend, ...]:
    return (_VaultBackend(),)


_FAKE = Plugin(
    name="ginkgo-fake",
    contributions=(
        ExecutorType(name="fake", label="Fake", required_settings=("queue",), build=_FakeExecutor),
        SecretBackendType(name="vault", build=_vault_backends),
    ),
)


@dataclass
class _EntryPoint:
    name: str
    value: str
    loaded: Any

    def load(self) -> Any:
        if isinstance(self.loaded, Exception):
            raise self.loaded
        return self.loaded


@pytest.fixture
def with_fake_plugin() -> Iterator[PluginRegistry]:
    with use_plugin_registry(PluginRegistry(plugins=(BUILTIN, _FAKE))) as registry:
        yield registry


def _discover_with(monkeypatch: pytest.MonkeyPatch, *entries: _EntryPoint) -> PluginRegistry:
    monkeypatch.setattr(registry_module, "entry_points", lambda *, group: list(entries))
    return PluginRegistry.discover()


class TestBuiltin:
    def test_core_contributes_every_shipped_implementation(self) -> None:
        registry = PluginRegistry(plugins=(BUILTIN,))

        assert registry.keys(ExecutorType) == []
        assert registry.keys(ObjectStoreType) == ["gs", "oci", "s3"]
        assert registry.keys(MountDriverType) == ["gs", "oci", "s3"]
        assert registry.keys(SecretBackendType) == ["env"]
        assert registry.keys(EventSubscriberType) == ["slack"]


class TestLookup:
    def test_an_unknown_key_names_what_is_available(self) -> None:
        registry = PluginRegistry(plugins=(BUILTIN,))

        with pytest.raises(PluginNotFoundError, match="object store for scheme 'az'.*'gs', 'oci'"):
            registry.get(ObjectStoreType, "az")

    def test_a_first_party_key_names_the_package_that_provides_it(self) -> None:
        registry = PluginRegistry(plugins=(BUILTIN,))

        with pytest.raises(PluginNotFoundError, match="Install ginkgo-k8s to add it"):
            registry.get(ExecutorType, "k8s")

    def test_two_plugins_claiming_one_key_is_an_error_naming_both(self) -> None:
        clash = Plugin(
            name="ginkgo-other-s3",
            contributions=(ObjectStoreType(scheme="s3", build=lambda **_: None),),
        )

        with pytest.raises(PluginConflictError, match="'ginkgo' and 'ginkgo-other-s3'"):
            PluginRegistry(plugins=(BUILTIN, clash))

    def test_the_same_key_on_different_points_does_not_clash(self) -> None:
        registry = PluginRegistry(plugins=(BUILTIN,))

        assert registry.get(ObjectStoreType, "gs").scheme == "gs"
        assert registry.get(MountDriverType, "gs").scheme == "gs"


class TestDiscovery:
    def test_entry_point_plugins_load_after_core(self, monkeypatch: pytest.MonkeyPatch) -> None:
        registry = _discover_with(monkeypatch, _EntryPoint("fake", "fake:PLUGIN", _FAKE))

        assert [plugin.name for plugin in registry.plugins] == ["ginkgo", "ginkgo-fake"]
        assert registry.get(ExecutorType, "fake").label == "Fake"

    def test_an_entry_point_that_is_not_a_plugin_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(PluginLoadError, match="'broken'.*is a dict, not a ginkgo Plugin"):
            _discover_with(monkeypatch, _EntryPoint("broken", "broken:PLUGIN", {}))

    def test_an_entry_point_that_fails_to_import_is_reported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        failing = _EntryPoint("missing", "missing:PLUGIN", ImportError("no module 'missing'"))

        with pytest.raises(PluginLoadError, match="'missing'.*failed to load: no module"):
            _discover_with(monkeypatch, failing)


class TestPluginContributionsReachCore:
    def test_a_plugin_executor_type_is_configurable_and_built(
        self, with_fake_plugin: PluginRegistry
    ) -> None:
        registry = ExecutorRegistry.from_config(
            {"remote": {"executors": {"queue": {"type": "fake", "queue": "q1"}}}},
            default="queue",
        )

        executor = registry.get("queue")

        assert isinstance(executor, _FakeExecutor)
        assert executor.settings == {"queue": "q1"}
        assert registry.label("queue") == "queue (Fake)"

    def test_a_plugin_executor_type_checks_its_required_settings(
        self, with_fake_plugin: PluginRegistry
    ) -> None:
        registry = ExecutorRegistry.from_config(
            {"remote": {"executors": {"queue": {"type": "fake"}}}}, default="queue"
        )

        with pytest.raises(ValueError, match="Fake executor 'queue' requires a queue"):
            registry.validate_settings(names=["queue"])

    def test_a_plugin_secret_backend_resolves_its_refs(
        self, with_fake_plugin: PluginRegistry, tmp_path: Path
    ) -> None:
        resolver = build_secret_resolver(project_root=tmp_path, config={}, environ={"A": "1"})

        assert resolver.resolve(ref=SecretRef(name="token", backend="vault")) == "vault:token"
        assert resolver.resolve(ref=SecretRef(name="A", backend="env")) == "1"


@dataclass
class _Recorder:
    seen: list[str]
    closed: list[bool]

    def handle(self, event: Any) -> None:
        self.seen.append(event.event)

    def close(self) -> None:
        self.closed.append(True)


class TestEventSubscribers:
    def test_a_plugin_subscriber_sees_a_runs_committed_events(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from ginkgo.cli.app import main

        recorder = _Recorder(seen=[], closed=[])
        observer = Plugin(
            name="ginkgo-observer",
            contributions=(EventSubscriberType(name="observer", build=lambda _: recorder),),
        )
        monkeypatch.chdir(tmp_path)
        Path("wf.py").write_text(
            "from ginkgo import flow, task\n\n"
            "@task()\ndef one() -> int:\n    return 1\n\n"
            "@flow\ndef main():\n    return one()\n",
            encoding="utf-8",
        )

        with use_plugin_registry(PluginRegistry(plugins=(BUILTIN, observer))):
            assert main(["run", "wf.py"]) == 0

        assert recorder.seen[0] == "run_started"
        assert recorder.seen[-1] == "run_completed"
        assert "task_completed" in recorder.seen
        assert recorder.closed == [True]


@dataclass(frozen=True)
class _Greeting(ExecutionDirective):
    name: str


@dataclass
class _GreetingRunner:
    replayed: list[str]

    def run(self, *, node: Any, directive: _Greeting) -> str:
        return f"hello {directive.name}"

    def replay_cached(self, *, node: Any, cache_key: str) -> None:
        self.replayed.append(cache_key)


@task(kind="greeting")
def greet(name: str) -> Any:
    """A task of a kind only the test's plugin provides."""
    return _Greeting(name=name)


class TestTaskKinds:
    def test_a_plugin_task_kind_runs_its_directive_and_replays_on_a_hit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from ginkgo import evaluate

        runner = _GreetingRunner(replayed=[])
        greeting_kind = Plugin(
            name="ginkgo-greeting",
            contributions=(
                TaskKind(
                    name="greeting",
                    directive=_Greeting,
                    call_form="greet(...)",
                    execution_mode="driver",
                    allows_env=False,
                    remote_capable=False,
                    runner=lambda _: runner,
                ),
            ),
        )
        monkeypatch.chdir(tmp_path)

        with use_plugin_registry(PluginRegistry(plugins=(BUILTIN, greeting_kind))):
            assert evaluate(greet(name="ginkgo")) == "hello ginkgo"
            assert evaluate(greet(name="ginkgo")) == "hello ginkgo"
        assert len(runner.replayed) == 1
