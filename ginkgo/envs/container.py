"""Container execution backend for Ginkgo.

Tasks declare container execution via ``env="docker://image:tag"`` or
``env="oci://image:tag"``.  The backend builds ``docker run`` (or
``podman run``) argument vectors that the evaluator executes through
its existing subprocess infrastructure.

Shell tasks use a same-path bind mount: the project root is mounted at its
host-side absolute path inside the container, so path strings baked into the
shell command by the task wrapper resolve correctly without rewriting.
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence


# ------------------------------------------------------------------
# URI parsing
# ------------------------------------------------------------------

_CONTAINER_SCHEMES = ("docker://", "oci://")


def is_container_env(env: str) -> bool:
    """Return whether *env* uses a container URI scheme."""
    return env.startswith(_CONTAINER_SCHEMES)


@dataclass(frozen=True, kw_only=True)
class ContainerRef:
    """Parsed container image reference.

    Parameters
    ----------
    scheme : str
        URI scheme (``"docker"`` or ``"oci"``).
    image : str
        Image reference (e.g. ``"myorg/ginkgo-scipy:3.11"``).
    """

    scheme: str
    image: str


def parse_container_uri(env: str) -> ContainerRef:
    """Parse a container URI into a ``ContainerRef``.

    Parameters
    ----------
    env : str
        Environment string starting with ``docker://`` or ``oci://``.

    Returns
    -------
    ContainerRef

    Raises
    ------
    ValueError
        If *env* is not a valid container URI.
    """
    for prefix in _CONTAINER_SCHEMES:
        if env.startswith(prefix):
            image = env[len(prefix) :]
            if not image:
                raise ValueError(f"Container URI {env!r} has no image reference after scheme")
            return ContainerRef(scheme=prefix.rstrip(":/"), image=image)

    raise ValueError(f"Not a container URI: {env!r}")


# ------------------------------------------------------------------
# Errors
# ------------------------------------------------------------------


class ContainerRuntimeNotFoundError(RuntimeError):
    """Raised when the container runtime binary is not on PATH."""

    def __init__(self, *, runtime: str) -> None:
        super().__init__(
            f"Container runtime {runtime!r} is not installed or not found on PATH. "
            f"Install {runtime} to run container-isolated tasks."
        )


class ContainerPrepareError(RuntimeError):
    """Raised when an image cannot be pulled."""

    def __init__(self, *, image: str, output: str) -> None:
        details = output.strip() or "no output from container runtime"
        super().__init__(f"Failed to pull container image {image!r}: {details}")


# ------------------------------------------------------------------
# Container backend
# ------------------------------------------------------------------


@dataclass(kw_only=True)
class ContainerBackend:
    """Container execution backend using Docker or Podman.

    Parameters
    ----------
    runtime : str
        Container runtime command (``"docker"`` or ``"podman"``).
    project_root : Path
        Host directory mounted into the container at the same absolute path.
        Defaults to the current working directory.
    pull_policy : str
        When to pull images: ``"if-not-present"``, ``"always"``, or
        ``"never"``.
    """

    runtime: str = "docker"
    project_root: Path = field(default_factory=Path.cwd)
    pull_policy: str = "if-not-present"
    _pulled_images: set[str] = field(default_factory=set, init=False, repr=False)
    _digest_cache: dict[str, str | None] = field(default_factory=dict, init=False, repr=False)

    # ------------------------------------------------------------------
    # TaskBackend protocol
    # ------------------------------------------------------------------

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Validate container env URIs and runtime availability."""
        for env in sorted(env_names):
            parse_container_uri(env)

        _require_container_runtime(self.runtime)

    def prepare(self, *, env: str) -> None:
        """Pull the container image according to the pull policy."""
        ref = parse_container_uri(env)

        if ref.image in self._pulled_images:
            return

        if self.pull_policy == "never":
            self._pulled_images.add(ref.image)
            return

        if self.pull_policy == "if-not-present" and self._image_exists_locally(ref.image):
            self._pulled_images.add(ref.image)
            return

        # Pull the image.
        _require_container_runtime(self.runtime)
        completed = subprocess.run(
            [self.runtime, "pull", ref.image],
            check=False,
            text=True,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise ContainerPrepareError(
                image=ref.image,
                output=(completed.stdout or "") + (completed.stderr or ""),
            )
        self._pulled_images.add(ref.image)

        # Invalidate digest cache after a pull since the image may have changed.
        self._digest_cache.pop(env, None)

    def env_identity(self, *, env: str) -> str | None:
        """Return the image digest for cache keying.

        Returns
        -------
        str | None
            Image ID (``sha256:...``), or ``None`` if the image cannot be
            inspected.
        """
        if env in self._digest_cache:
            return self._digest_cache[env]

        ref = parse_container_uri(env)
        digest = self._resolve_digest(ref.image)
        self._digest_cache[env] = digest
        return digest

    def shell_argv(self, *, env: str, cmd: str) -> list[str]:
        """Build an argument vector to run *cmd* inside a container.

        The project root is bind-mounted at its host-side absolute path so
        that paths baked into the shell command resolve correctly.

        Returns
        -------
        list[str]
            Argument vector for ``subprocess.run(..., shell=False)``.
        """
        ref = parse_container_uri(env)
        project = str(self.project_root)

        return [
            self.runtime,
            "run",
            "--rm",
            "-v",
            f"{project}:{project}",
            "-w",
            project,
            ref.image,
            "bash",
            "-c",
            cmd,
        ]

    def python_argv_m(
        self,
        *,
        env: str,
        module: str,
        args: Sequence[str] = (),
    ) -> list[str]:
        """Container environments only support shell tasks.

        Raises
        ------
        NotImplementedError
            Always — Python tasks cannot run in container environments.
        """
        raise NotImplementedError(
            "Container environments only support shell tasks. "
            "Use @task(kind='shell') for tasks with a container env."
        )

    def env_lock_path(self, *, env: str) -> Path | None:
        """Containers have no lock file artifact."""
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _image_exists_locally(self, image: str) -> bool:
        """Return whether *image* is present in the local image store."""
        completed = subprocess.run(
            [self.runtime, "image", "inspect", image],
            check=False,
            capture_output=True,
        )
        return completed.returncode == 0

    def _resolve_digest(self, image: str) -> str | None:
        """Return the image ID via ``docker image inspect``."""
        completed = subprocess.run(
            [self.runtime, "image", "inspect", "--format", "{{.Id}}", image],
            check=False,
            text=True,
            capture_output=True,
        )
        if completed.returncode != 0:
            return None

        digest = (completed.stdout or "").strip()
        return digest if digest else None


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _require_container_runtime(runtime: str) -> None:
    """Raise if the container runtime binary is not on PATH."""
    if shutil.which(runtime) is None:
        raise ContainerRuntimeNotFoundError(runtime=runtime)
