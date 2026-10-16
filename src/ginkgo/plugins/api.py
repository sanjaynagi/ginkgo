"""The plugin contract: a ``Plugin`` and the extension points it contributes to.

A plugin is a named bundle of contributions. Each contribution is a frozen
spec for exactly one extension point, keyed by the name config or user code
uses to select it — an executor ``type``, a URI scheme, a secret backend name.
Core looks a contribution up by its key and calls the spec's ``build``; it
never names a concrete implementation.

The set of extension points is fixed. Adding one is a deliberate change to
core, not something a plugin can do, so every dispatch site stays typed and
greppable.

The API is provisional until 1.0: first-party plugins pin the exact core
version, and the specs here may change in step with them.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

if TYPE_CHECKING:
    from ginkgo.remote.access.drivers.base import MountDriver
    from ginkgo.remote.backend import ObjectStore
    from ginkgo.runtime.backend import ExecutionEnvironment
    from ginkgo.runtime.environment.secrets import SecretBackend, SecretResolver
    from ginkgo.runtime.events import GinkgoEvent
    from ginkgo.runtime.remote_executor import RemoteExecutor
    from ginkgo.store.protocol import ProvenanceStore

__all__ = [
    "Contribution",
    "EnvironmentContext",
    "EnvironmentType",
    "EventSubscriber",
    "EventSubscriberContext",
    "EventSubscriberType",
    "ExecutorType",
    "MountDriverType",
    "ObjectStoreType",
    "Plugin",
    "SecretBackendType",
]


@dataclass(frozen=True, kw_only=True)
class ExecutorType:
    """A remote executor backend, selected by ``type`` in an executor table.

    Parameters
    ----------
    name : str
        The ``type`` value in ``[remote.executors.<name>]``.
    label : str
        Human-readable backend name for run headers and error messages.
    required_settings : tuple[str, ...]
        Settings the backend cannot be built without, checked before any task
        runs.
    build : Callable[[Mapping[str, Any]], RemoteExecutor]
        Construct the backend from the executor table's settings.
    """

    point_name: ClassVar[str] = "executor type"

    name: str
    label: str
    required_settings: tuple[str, ...]
    build: Callable[[Mapping[str, Any]], RemoteExecutor]

    @property
    def key(self) -> str:
        """The lookup key: the executor ``type``."""
        return self.name


@dataclass(frozen=True, kw_only=True)
class ObjectStoreType:
    """An object store for one URI scheme.

    Parameters
    ----------
    scheme : str
        URI scheme, e.g. ``"s3"``.
    build : Callable[..., ObjectStore]
        ``build(*, config, region)`` constructs the store from the merged
        runtime config and an optional region override.
    """

    point_name: ClassVar[str] = "object store for scheme"

    scheme: str
    build: Callable[..., ObjectStore]

    @property
    def key(self) -> str:
        """The lookup key: the URI scheme."""
        return self.scheme


@dataclass(frozen=True, kw_only=True)
class MountDriverType:
    """A FUSE driver that mounts buckets of one URI scheme.

    Parameters
    ----------
    scheme : str
        URI scheme, e.g. ``"gs"``.
    build : Callable[[], MountDriver]
        Construct the driver.
    """

    point_name: ClassVar[str] = "FUSE driver for scheme"

    scheme: str
    build: Callable[[], MountDriver]

    @property
    def key(self) -> str:
        """The lookup key: the URI scheme."""
        return self.scheme


@dataclass(frozen=True, kw_only=True)
class SecretBackendType:
    """A secret backend, selected by ``secret(name, backend=...)``.

    Parameters
    ----------
    name : str
        The backend name a ``SecretRef`` carries.
    build : Callable[..., tuple[SecretBackend, ...]]
        ``build(*, project_root, config, environ)`` returns the chain of
        backends tried in order for this name.
    """

    point_name: ClassVar[str] = "secret backend"

    name: str
    build: Callable[..., tuple[SecretBackend, ...]]

    @property
    def key(self) -> str:
        """The lookup key: the backend name."""
        return self.name


@dataclass(frozen=True, kw_only=True)
class EventSubscriberContext:
    """What a run hands an event subscriber when building it.

    Parameters
    ----------
    config : Mapping[str, Any] | None
        Merged runtime config; each subscriber reads its own table.
    secret_resolver : SecretResolver | None
        The run's secret resolver.
    store : ProvenanceStore
        A read connection to the ledger.
    run_id : str
        The run being observed.
    run_dir : Path
        The run's directory.
    workflow_path : Path
        The workflow file being run.
    warn : Callable[[str], None]
        Report a non-fatal problem to the user.
    """

    config: Mapping[str, Any] | None
    secret_resolver: SecretResolver | None
    store: ProvenanceStore
    run_id: str
    run_dir: Path
    workflow_path: Path
    warn: Callable[[str], None]


class EventSubscriber(Protocol):
    """Receives a run's events once they are committed to the ledger."""

    def handle(self, event: GinkgoEvent) -> None:
        """React to one committed event."""
        ...

    def close(self) -> None:
        """Finish outstanding work at the end of the run."""
        ...


@dataclass(frozen=True, kw_only=True)
class EventSubscriberType:
    """A run observer, such as a notification channel.

    Parameters
    ----------
    name : str
        Identifies the subscriber, e.g. ``"slack"``.
    build : Callable[[EventSubscriberContext], EventSubscriber | None]
        Return a subscriber for this run, or ``None`` when its config leaves
        it disabled.
    """

    point_name: ClassVar[str] = "event subscriber"

    name: str
    build: Callable[[EventSubscriberContext], EventSubscriber | None]

    @property
    def key(self) -> str:
        """The lookup key: the subscriber name."""
        return self.name


@dataclass(frozen=True, kw_only=True)
class EnvironmentContext:
    """What a run hands an environment backend when building it.

    Parameters
    ----------
    project_root : Path
        The workflow project's root directory.
    workflow_root : Path | None
        Where named environment manifests are searched, when known.
    config : Mapping[str, Any]
        Merged runtime config; each backend reads its own table.
    probe_registry : bool
        Whether validation may ask image registries about missing images.
        Only a dry run and ``ginkgo doctor`` set it.
    """

    project_root: Path
    workflow_root: Path | None
    config: Mapping[str, Any]
    probe_registry: bool = False


@dataclass(frozen=True, kw_only=True)
class EnvironmentType:
    """An execution environment for ``env=`` values of one URI scheme.

    Parameters
    ----------
    scheme : str
        The scheme of the ``env=`` value, e.g. ``"docker"`` for
        ``"docker://image:tag"``. The empty string claims bare names with no
        scheme, such as a Pixi environment name.
    label : str
        Short name for the backend in run tables, e.g. ``"pixi"``.
    build : Callable[[EnvironmentContext], ExecutionEnvironment]
        Construct the backend for a run. Schemes contributed with the same
        ``build`` share one backend instance per run.
    """

    point_name: ClassVar[str] = "environment for scheme"

    scheme: str
    label: str
    build: Callable[[EnvironmentContext], ExecutionEnvironment]

    @property
    def key(self) -> str:
        """The lookup key: the ``env=`` scheme."""
        return self.scheme


Contribution = (
    ExecutorType
    | ObjectStoreType
    | MountDriverType
    | SecretBackendType
    | EventSubscriberType
    | EnvironmentType
)
"""Every extension point a plugin can contribute to."""


@dataclass(frozen=True, kw_only=True)
class Plugin:
    """A named bundle of contributions, exposed through an entry point.

    Parameters
    ----------
    name : str
        Distribution-style name, e.g. ``"ginkgo-k8s"``.
    contributions : tuple[Contribution, ...]
        What the plugin adds.
    """

    name: str
    contributions: tuple[Contribution, ...]
