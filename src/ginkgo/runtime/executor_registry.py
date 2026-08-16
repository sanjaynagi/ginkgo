"""Named remote executors: config parsing, validation, lazy construction.

A run may have several remote executors configured — a GPU Kubernetes
namespace, a cheap batch queue — each named in the runtime config:

.. code-block:: toml

    [remote.executors.gpu-k8s]
    type = "k8s"
    namespace = "ml"
    image = "..."

    [remote.executors.cheap-batch]
    type = "batch"
    project = "my-project"
    image = "..."

Tasks route to one by name (``@task(executor="gpu-k8s")``); ``remote=True``
and GPU overflow route to the run's *default* executor, selected with
``--executor``. The registry owns the name → backend mapping so the
evaluator never parses config and never constructs backends itself, and
so unknown names fail at build time with the configured names listed.

The legacy single-executor sections ``[remote.k8s]`` and ``[remote.batch]``
are read as implicitly named executors ``k8s`` and ``batch``, which is what
makes ``--executor k8s`` keep working unchanged.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.runtime.remote_executor import RemoteExecutor

# Executor backend types and the config sections that define them implicitly.
_EXECUTOR_TYPES = ("k8s", "batch")

# Name reserved for "no remote executor" — the default run placement.
LOCAL = "local"


@dataclass(frozen=True, kw_only=True)
class ExecutorSpec:
    """One named executor: its backend type and backend settings.

    Parameters
    ----------
    name : str
        Executor name as written in config and in ``@task(executor=...)``.
    type : str
        Backend type (``"k8s"`` or ``"batch"``).
    settings : dict[str, Any]
        Backend-specific configuration (image, namespace, project, ...),
        excluding the ``type`` key and the nested ``code`` table.
    code : dict[str, Any] | None
        Code-sync configuration from the executor's ``code`` sub-table.
    """

    name: str
    type: str
    settings: dict[str, Any]
    code: dict[str, Any] | None = None


@dataclass
class ExecutorRegistry:
    """Named executors available to a run, built on first use.

    An empty registry means no remote dispatch is configured at all, which
    is the default for a local run.
    """

    specs: dict[str, ExecutorSpec] = field(default_factory=dict)
    default_name: str | None = None
    _built: dict[str, RemoteExecutor] = field(default_factory=dict, init=False, repr=False)

    @classmethod
    def from_config(
        cls,
        runtime_config: dict[str, Any] | None,
        *,
        default: str | None = None,
    ) -> ExecutorRegistry:
        """Build a registry from the ``[remote]`` config tables.

        Parameters
        ----------
        runtime_config : dict[str, Any] | None
            Merged runtime config.
        default : str | None
            Name selected by ``--executor``. ``None`` or ``"local"`` leaves
            the run without a default executor, so only tasks that name an
            executor explicitly dispatch remotely.

        Raises
        ------
        ValueError
            If an executor table is malformed, declares an unknown ``type``,
            or if *default* names an executor that is not configured.
        """
        remote = (runtime_config or {}).get("remote", {})
        if not isinstance(remote, dict):
            raise ValueError("[remote] must be a table")

        specs: dict[str, ExecutorSpec] = {}
        # Legacy sections first so an explicit [remote.executors.k8s] wins.
        for type_name in _EXECUTOR_TYPES:
            section = remote.get(type_name)
            if isinstance(section, dict) and section:
                specs[type_name] = _spec_from_table(
                    name=type_name,
                    table=section,
                    type_name=type_name,
                    source=f"[remote.{type_name}]",
                )

        executors = remote.get("executors", {})
        if not isinstance(executors, dict):
            raise ValueError("[remote.executors] must be a table of named executors")
        for name, table in executors.items():
            source = f"[remote.executors.{name}]"
            if not isinstance(table, dict):
                raise ValueError(f"{source} must be a table of executor settings")
            if name == LOCAL:
                raise ValueError(
                    f"{source} uses the reserved executor name {LOCAL!r}; "
                    "pick a name that does not collide with local placement"
                )
            type_name = table.get("type")
            if not isinstance(type_name, str) or type_name not in _EXECUTOR_TYPES:
                supported = ", ".join(_EXECUTOR_TYPES)
                raise ValueError(f"{source} needs a type from {{{supported}}}, got {type_name!r}")
            specs[name] = _spec_from_table(
                name=name,
                table=table,
                type_name=type_name,
                source=source,
            )

        default_name = None if default in (None, LOCAL) else default
        if default_name is not None and default_name not in specs:
            raise ValueError(
                f"--executor {default_name!r} is not configured. {_available_hint(specs)}"
            )
        return cls(specs=specs, default_name=default_name)

    @classmethod
    def for_validation(
        cls,
        *,
        project_root: Path,
        config_paths: Sequence[str | Path] | None = None,
    ) -> ExecutorRegistry:
        """Registry for commands that validate a graph without running it.

        ``ginkgo doctor``, ``inspect workflow``, and ``secrets list`` build the
        graph but choose no executor, so the registry carries every configured
        name with no default. A task pinned with ``executor=`` then validates
        exactly as it would under ``ginkgo run``, while ``remote=True`` still
        reports the missing default — which is genuinely unknown here, since
        it comes from the ``--executor`` flag of a run that has not happened.
        """
        from ginkgo.config import load_runtime_config

        config = load_runtime_config(project_root=project_root, override_paths=config_paths)
        return cls.from_config(config, default=None)

    @classmethod
    def for_executor(cls, executor: RemoteExecutor, *, name: str = "remote") -> ExecutorRegistry:
        """Wrap an already-constructed executor as the run's default.

        Used by programmatic callers and tests that hold a backend object
        rather than a config table.
        """
        registry = cls(
            specs={name: ExecutorSpec(name=name, type="prebuilt", settings={})},
            default_name=name,
        )
        registry._built[name] = executor
        return registry

    def available_hint(self) -> str:
        """Sentence naming the configured executors, for error messages."""
        return _available_hint(self.specs)

    @property
    def has_default(self) -> bool:
        """Whether the run has a default executor for ``remote=True`` tasks."""
        return self.default_name is not None

    def resolve(self, name: str, *, task_name: str) -> str:
        """Validate that *name* is a configured executor and return it.

        Raises
        ------
        ValueError
            If no executor by that name is configured.
        """
        if name not in self.specs:
            raise ValueError(
                f"{task_name} declares executor={name!r}, which is not configured. "
                f"{_available_hint(self.specs)}"
            )
        return name

    def get(self, name: str) -> RemoteExecutor:
        """Return the backend for *name*, constructing it on first use."""
        built = self._built.get(name)
        if built is None:
            built = _build_executor(self.specs[name])
            self._built[name] = built
        return built

    def code_config(self, name: str) -> dict[str, Any] | None:
        """Return the code-sync config for *name*, if it declares one."""
        spec = self.specs.get(name)
        return None if spec is None or spec.code is None else dict(spec.code)

    def label(self, name: str) -> str:
        """Human-readable label for a run header line."""
        spec = self.specs.get(name)
        friendly = (
            None if spec is None else {"k8s": "Kubernetes", "batch": "GCP Batch"}.get(spec.type)
        )
        if friendly is None:
            return name
        return friendly if name == spec.type else f"{name} ({friendly})"


def _spec_from_table(
    *,
    name: str,
    table: dict[str, Any],
    type_name: str,
    source: str,
) -> ExecutorSpec:
    """Split one executor config table into settings and code-sync config."""
    code = table.get("code")
    if code is not None and not isinstance(code, dict):
        raise ValueError(f"{source} code must be a table")
    settings = {key: value for key, value in table.items() if key not in {"type", "code"}}
    return ExecutorSpec(name=name, type=type_name, settings=settings, code=code)


def _available_hint(specs: dict[str, ExecutorSpec]) -> str:
    """Phrase listing the configured executor names for an error message."""
    if not specs:
        return "No executors are configured; add a [remote.executors.<name>] table to ginkgo.toml."
    names = ", ".join(sorted(specs))
    return f"Configured executors: {names}."


def _build_executor(spec: ExecutorSpec) -> RemoteExecutor:
    """Construct the backend object for one executor spec."""
    if spec.type == "k8s":
        return _build_k8s_executor(spec)
    if spec.type == "batch":
        return _build_batch_executor(spec)
    raise ValueError(f"executor {spec.name!r} has no buildable backend type {spec.type!r}")


def _build_k8s_executor(spec: ExecutorSpec) -> RemoteExecutor:
    """Construct a ``KubernetesExecutor`` from an executor spec."""
    from ginkgo.remote.kubernetes import KubernetesExecutor

    config = spec.settings
    image = config.get("image")
    if not image:
        raise ValueError(
            f"Kubernetes executor {spec.name!r} requires an image. "
            f"Set image in {_source_hint(spec)}."
        )

    return KubernetesExecutor(
        namespace=config.get("namespace", "default"),
        image=image,
        service_account=config.get("service_account"),
        pull_policy=config.get("pull_policy", "IfNotPresent"),
        gpu_type=config.get("gpu_type"),
        node_selector=config.get("node_selector"),
        tolerations=config.get("tolerations"),
        ttl_seconds_after_finished=int(config.get("ttl_seconds_after_finished", 3600)),
        unschedulable_timeout=float(config.get("unschedulable_timeout", 300.0)),
        ephemeral_storage=config.get("ephemeral_storage", "10Gi"),
        backoff_limit=int(config.get("backoff_limit", 2)),
        fuse_image=config.get("fuse_image"),
        fuse_annotations=config.get("fuse_annotations"),
        fuse_privileged=bool(config.get("fuse_privileged", False)),
    )


def _build_batch_executor(spec: ExecutorSpec) -> RemoteExecutor:
    """Construct a ``GCPBatchExecutor`` from an executor spec."""
    from ginkgo.remote.gcp_batch import GCPBatchExecutor

    config = spec.settings
    project = config.get("project")
    if not project:
        raise ValueError(
            f"GCP Batch executor {spec.name!r} requires a project. "
            f"Set project in {_source_hint(spec)}."
        )

    image = config.get("image")
    if not image:
        raise ValueError(
            f"GCP Batch executor {spec.name!r} requires an image. "
            f"Set image in {_source_hint(spec)}."
        )

    return GCPBatchExecutor(
        project=project,
        region=config.get("region", "europe-west2"),
        image=image,
        service_account=config.get("service_account"),
        gpu_type=config.get("gpu_type"),
        gpu_driver_version=config.get("gpu_driver_version", "LATEST"),
        max_run_duration=config.get("max_run_duration", "3600s"),
        fuse_image=config.get("fuse_image"),
        fuse_privileged=bool(config.get("fuse_privileged", False)),
    )


def _source_hint(spec: ExecutorSpec) -> str:
    """Config section a spec came from, for error messages."""
    if spec.name == spec.type:
        return f"[remote.{spec.type}]"
    return f"[remote.executors.{spec.name}]"
