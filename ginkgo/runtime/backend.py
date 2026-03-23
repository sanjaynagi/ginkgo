"""Task execution backend protocol and implementations.

The backend protocol decouples the evaluator from a specific execution
environment.  ``LocalBackend`` wraps an existing ``PixiRegistry``,
``ContainerBackend`` wraps Docker/Podman, and ``CompositeBackend`` routes
calls to the correct underlying backend based on the ``env`` string.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence, runtime_checkable

from ginkgo.envs.container import ContainerBackend, is_container_env
from ginkgo.envs.pixi import PixiRegistry


@runtime_checkable
class TaskBackend(Protocol):
    """Contract for environment-backed task execution.

    The evaluator consults a backend whenever a task declares ``env=...``.
    Implementations handle environment validation, materialisation, subprocess
    argument construction, and identity hashing for cache keys.
    """

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Raise if any environment name cannot be resolved."""
        ...

    def prepare(self, *, env: str) -> None:
        """Ensure the execution environment is ready for dispatch."""
        ...

    def env_identity(self, *, env: str) -> str | None:
        """Return a stable identity string for cache keying.

        For Pixi environments this is the lock-file digest.  For container
        backends it would be the image digest.

        Returns
        -------
        str | None
            Hex digest or ``None`` when identity cannot be determined.
        """
        ...

    def shell_argv(self, *, env: str, cmd: str) -> list[str]:
        """Build an argument vector to execute *cmd* inside the environment.

        Returns
        -------
        list[str]
            Argument vector suitable for ``subprocess.run(..., shell=False)``.
        """
        ...

    def python_argv_m(
        self,
        *,
        env: str,
        module: str,
        args: Sequence[str] = (),
    ) -> list[str]:
        """Build an argument vector to run ``python -m`` inside the environment.

        Returns
        -------
        list[str]
            Argument vector suitable for ``subprocess.run(..., shell=False)``.
        """
        ...

    def env_lock_path(self, *, env: str) -> Path | None:
        """Return the path to an environment lock file for provenance capture.

        Returns
        -------
        Path | None
            Absolute path to the lock file, or ``None`` when the backend does
            not produce a meaningful lock artifact.
        """
        ...


@dataclass(kw_only=True)
class LocalBackend:
    """Local execution backend backed by Pixi environments.

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

    def env_identity(self, *, env: str) -> str | None:
        """Return the Pixi lock-file SHA-256 digest."""
        return self.pixi_registry.lock_hash(env=env)

    def shell_argv(self, *, env: str, cmd: str) -> list[str]:
        """Build argv to run *cmd* through the Pixi environment."""
        return self.pixi_registry.shell_argv(env=env, cmd=cmd)

    def python_argv_m(
        self,
        *,
        env: str,
        module: str,
        args: Sequence[str] = (),
    ) -> list[str]:
        """Build argv to run ``python -m`` through the Pixi environment."""
        return self.pixi_registry.python_argv_m(env=env, module=module, args=args)

    def env_lock_path(self, *, env: str) -> Path | None:
        """Return the path to the Pixi lock file for provenance capture."""
        manifest = self.pixi_registry.resolve(env=env)
        lock_path = manifest.parent / "pixi.lock"
        return lock_path if lock_path.is_file() else None


@dataclass(kw_only=True)
class CompositeBackend:
    """Routes calls to the correct backend based on the ``env`` string.

    Container URIs (``docker://...``, ``oci://...``) are dispatched to the
    container backend.  All other env values go to the local backend.

    Parameters
    ----------
    local : LocalBackend
        Backend for Pixi and bare-host tasks.
    container : ContainerBackend | None
        Backend for container-isolated tasks.  When ``None``, container
        env URIs will raise at validation time.
    """

    local: LocalBackend
    container: ContainerBackend | None = None

    def _route(self, *, env: str) -> TaskBackend:
        """Return the backend responsible for *env*."""
        if is_container_env(env):
            if self.container is None:
                raise RuntimeError(
                    f"Container env {env!r} requires a container backend, but none is configured."
                )
            return self.container
        return self.local

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Partition env names by type and validate with each backend."""
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
        """Delegate to the correct backend."""
        self._route(env=env).prepare(env=env)

    def env_identity(self, *, env: str) -> str | None:
        """Delegate to the correct backend."""
        return self._route(env=env).env_identity(env=env)

    def shell_argv(self, *, env: str, cmd: str) -> list[str]:
        """Delegate to the correct backend."""
        return self._route(env=env).shell_argv(env=env, cmd=cmd)

    def python_argv_m(
        self,
        *,
        env: str,
        module: str,
        args: Sequence[str] = (),
    ) -> list[str]:
        """Delegate to the correct backend."""
        return self._route(env=env).python_argv_m(env=env, module=module, args=args)

    def env_lock_path(self, *, env: str) -> Path | None:
        """Delegate to the correct backend."""
        return self._route(env=env).env_lock_path(env=env)
