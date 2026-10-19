"""Execution environment protocol and the router over its implementations.

The ``ExecutionEnvironment`` protocol decouples the evaluator from a specific
execution environment. Implementations are contributed by plugins as
:class:`~ginkgo.plugins.api.EnvironmentType`, keyed by the scheme of the
``env=`` value; ``EnvironmentRouter`` builds each one on first use and routes
every call to it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, Sequence, runtime_checkable

from ginkgo.envs.mounts import Mount
from ginkgo.plugins.api import EnvironmentContext, EnvironmentType
from ginkgo.plugins.registry import plugin_registry


@runtime_checkable
class ExecutionEnvironment(Protocol):
    """Contract for environment-backed task execution.

    The evaluator consults an execution environment whenever a task declares
    ``env=...``.  Implementations handle environment validation, materialisation,
    subprocess argument construction, and identity hashing for cache keys.
    """

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Raise if any environment name cannot be resolved."""
        ...

    def prepare(self, *, env: str) -> None:
        """Ensure the execution environment is ready for dispatch."""
        ...

    def env_identity(self, *, env: str) -> str:
        """Return a stable identity string for cache keying.

        The identity is a function of the *declared* environment: the Pixi
        manifest digest, or the container image reference. It must be resolvable
        before :meth:`prepare` and must read the same afterwards, because the
        cache key is built before the environment is materialised. An identity
        that changed on materialisation re-ran every environment-backed task on
        the run after the environment was first installed (issue #194).

        Returns
        -------
        str
            Identity of the declared environment. Never empty: the cache layer
            refuses to build a key from an unresolved identity.
        """
        ...

    def materialized_digest(self, *, env: str) -> str | None:
        """Return a digest of the environment as materialised here, for provenance.

        Unlike :meth:`env_identity`, this describes local state — the solved
        lock file, the pulled image — so it is only available once
        :meth:`prepare` has run. Never used for cache keys.

        Returns
        -------
        str | None
            Digest, or ``None`` when the environment is not materialised.
        """
        ...

    def exec_argv(
        self,
        *,
        env: str,
        cmd: str,
        mounts: Sequence[Mount] = (),
        env_vars: Sequence[str] = (),
    ) -> list[str]:
        """Build an argument vector to execute *cmd* inside the environment.

        Parameters
        ----------
        env : str
            Environment name, path, or container URI.
        cmd : str
            Shell command string.
        mounts : Sequence[Mount]
            Host paths the command needs, declared by the task. Only container
            environments act on these; host-local environments ignore them.
        env_vars : Sequence[str]
            Names of environment variables Ginkgo computed for this task that
            must reach the command. Host-local environments inherit them
            already and ignore this.

        Returns
        -------
        list[str]
            Argument vector suitable for ``subprocess.run(..., shell=False)``.
        """
        ...

    def exec_failure_hint(self, *, env: str, exit_code: int, output: str) -> str | None:
        """Return a diagnosis to append to a failed command's error, if any.

        Returns
        -------
        str | None
            A message naming a cause the raw output does not, or ``None`` when
            the environment has nothing to add.
        """
        ...

    def env_lock_path(self, *, env: str) -> Path | None:
        """Return the path to an environment lock file for provenance capture.

        Returns
        -------
        Path | None
            Absolute path to the lock file, or ``None`` when the environment
            does not produce a meaningful lock artifact.
        """
        ...

    def provenance_fields(self, *, env: str) -> dict[str, Any]:
        """Return what the run records about the environment a task ran in.

        Called once the environment is prepared. The fields are annotated onto
        the task (its ``extra``); a lock file from :meth:`env_lock_path` is
        copied into the run directory separately.

        Returns
        -------
        dict[str, Any]
            At least ``{"backend": <name>}``.
        """
        ...


def env_scheme(env: str) -> str:
    """Return the scheme of an ``env=`` value, or ``""`` for a bare name.

    ``"docker://ubuntu:24.04"`` has scheme ``"docker"``; ``"analysis"`` and
    ``"envs/analysis/pixi.toml"`` have none.
    """
    scheme, separator, _ = env.partition("://")
    return scheme if separator else ""


@dataclass(kw_only=True)
class EnvironmentRouter:
    """Routes each ``env=`` value to the environment its scheme belongs to.

    Environments are built from the plugin registry on first use, one instance
    per environment type, so a run never constructs a backend none of its
    tasks declares and schemes one backend serves share its state.

    Parameters
    ----------
    context : EnvironmentContext
        Handed to each environment's ``build``.
    """

    context: EnvironmentContext
    _built: dict[str, ExecutionEnvironment] = field(default_factory=dict, init=False, repr=False)

    def _route(self, *, env: str) -> ExecutionEnvironment:
        """Return the environment responsible for *env*, building it if needed.

        Raises
        ------
        PluginNotFoundError
            If no installed plugin provides an environment for the scheme.
        """
        environment_type = plugin_registry().get(EnvironmentType, env_scheme(env))
        built = self._built.get(environment_type.name)
        if built is None:
            built = environment_type.build(self.context)
            self._built[environment_type.name] = built
        return built

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Validate each env name with the environment that owns it."""
        by_environment: dict[int, tuple[ExecutionEnvironment, set[str]]] = {}
        for env in sorted(env_names):
            environment = self._route(env=env)
            by_environment.setdefault(id(environment), (environment, set()))[1].add(env)
        for environment, names in by_environment.values():
            environment.validate_envs(env_names=names)

    def prepare(self, *, env: str) -> None:
        """Delegate to the owning environment."""
        self._route(env=env).prepare(env=env)

    def env_identity(self, *, env: str) -> str:
        """Delegate to the owning environment."""
        return self._route(env=env).env_identity(env=env)

    def materialized_digest(self, *, env: str) -> str | None:
        """Delegate to the owning environment."""
        return self._route(env=env).materialized_digest(env=env)

    def exec_argv(
        self,
        *,
        env: str,
        cmd: str,
        mounts: Sequence[Mount] = (),
        env_vars: Sequence[str] = (),
    ) -> list[str]:
        """Delegate to the owning environment."""
        return self._route(env=env).exec_argv(env=env, cmd=cmd, mounts=mounts, env_vars=env_vars)

    def exec_failure_hint(self, *, env: str, exit_code: int, output: str) -> str | None:
        """Delegate to the owning environment."""
        return self._route(env=env).exec_failure_hint(env=env, exit_code=exit_code, output=output)

    def env_lock_path(self, *, env: str) -> Path | None:
        """Delegate to the owning environment."""
        return self._route(env=env).env_lock_path(env=env)

    def provenance_fields(self, *, env: str) -> dict[str, Any]:
        """Delegate to the owning environment."""
        return self._route(env=env).provenance_fields(env=env)
