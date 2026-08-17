"""Execution environment protocol and implementations.

The ``ExecutionEnvironment`` protocol decouples the evaluator from a specific
execution environment.  ``LocalEnvironment`` wraps an existing ``PixiRegistry``,
``ContainerBackend`` wraps Docker/Podman, and ``CompositeEnvironment`` routes
calls to the correct underlying environment based on the ``env`` string.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence, runtime_checkable

from ginkgo.envs.container import ContainerBackend, is_container_env
from ginkgo.envs.mounts import Mount
from ginkgo.envs.pixi import PixiRegistry


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


@dataclass(kw_only=True)
class LocalEnvironment:
    """Local execution environment backed by Pixi.

    Parameters
    ----------
    pixi_registry : PixiRegistry
        The underlying Pixi registry that discovers and materialises
        environments.
    """

    pixi_registry: PixiRegistry

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Raise for any environment name that cannot be resolved."""
        self.pixi_registry.validate_envs(env_names=env_names)

    def prepare(self, *, env: str) -> None:
        """Materialize the Pixi environment."""
        self.pixi_registry.prepare(env=env)

    def env_identity(self, *, env: str) -> str:
        """Return the BLAKE3 digest of the Pixi manifest."""
        return self.pixi_registry.env_identity(env=env)

    def materialized_digest(self, *, env: str) -> str | None:
        """Return the BLAKE3 digest of the installed environment's lock file."""
        return self.pixi_registry.lock_hash(env=env)

    def exec_argv(
        self,
        *,
        env: str,
        cmd: str,
        mounts: Sequence[Mount] = (),
        env_vars: Sequence[str] = (),
    ) -> list[str]:
        """Build argv to run *cmd* through the Pixi environment.

        Pixi runs on the host, where every declared path is already reachable
        and the subprocess environment is inherited, so *mounts* and *env_vars*
        are accepted for protocol conformance and ignored.
        """
        return self.pixi_registry.exec_argv(env=env, cmd=cmd)

    def exec_failure_hint(self, *, env: str, exit_code: int, output: str) -> str | None:
        """Pixi failures surface the tool's own error; nothing to add."""
        return None

    def env_lock_path(self, *, env: str) -> Path | None:
        """Return the path to the Pixi lock file for provenance capture."""
        manifest = self.pixi_registry.resolve(env=env)
        lock_path = manifest.parent / "pixi.lock"
        return lock_path if lock_path.is_file() else None


@dataclass(kw_only=True)
class CompositeEnvironment:
    """Routes calls to the correct environment based on the ``env`` string.

    Container URIs (``docker://...``, ``oci://...``) are dispatched to the
    container environment.  All other env values go to the local environment.

    Parameters
    ----------
    local : LocalEnvironment
        Environment for Pixi and bare-host tasks.
    container : ContainerBackend | None
        Environment for container-isolated tasks.  When ``None``, container
        env URIs will raise at validation time.
    """

    local: LocalEnvironment
    container: ContainerBackend | None = None

    def _route(self, *, env: str) -> ExecutionEnvironment:
        """Return the environment responsible for *env*."""
        if is_container_env(env):
            if self.container is None:
                raise RuntimeError(
                    f"Container env {env!r} requires a container backend, but none is configured."
                )
            return self.container
        return self.local

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Partition env names by type and validate with each environment."""
        local_envs = {e for e in env_names if not is_container_env(e)}
        container_envs = {e for e in env_names if is_container_env(e)}

        if local_envs:
            self.local.validate_envs(env_names=local_envs)
        if container_envs:
            if self.container is None:
                raise RuntimeError(
                    f"Container envs {sorted(container_envs)} require a container backend, "
                    "but none is configured."
                )
            self.container.validate_envs(env_names=container_envs)

    def prepare(self, *, env: str) -> None:
        """Delegate to the correct environment."""
        self._route(env=env).prepare(env=env)

    def env_identity(self, *, env: str) -> str:
        """Delegate to the correct environment."""
        return self._route(env=env).env_identity(env=env)

    def materialized_digest(self, *, env: str) -> str | None:
        """Delegate to the correct environment."""
        return self._route(env=env).materialized_digest(env=env)

    def exec_argv(
        self,
        *,
        env: str,
        cmd: str,
        mounts: Sequence[Mount] = (),
        env_vars: Sequence[str] = (),
    ) -> list[str]:
        """Delegate to the correct environment."""
        return self._route(env=env).exec_argv(env=env, cmd=cmd, mounts=mounts, env_vars=env_vars)

    def exec_failure_hint(self, *, env: str, exit_code: int, output: str) -> str | None:
        """Delegate to the correct environment."""
        return self._route(env=env).exec_failure_hint(env=env, exit_code=exit_code, output=output)

    def env_lock_path(self, *, env: str) -> Path | None:
        """Delegate to the correct environment."""
        return self._route(env=env).env_lock_path(env=env)
