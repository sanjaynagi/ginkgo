"""Container execution backend for Ginkgo.

Tasks declare container execution via ``env="docker://image:tag"`` or
``env="oci://image:tag"``.  The backend builds ``docker run`` (or
``podman run``) argument vectors that the evaluator executes through
its existing subprocess infrastructure.

Shell tasks use same-path bind mounts: the project root, every declared
``file``/``folder`` input, and every declared output are mounted at their
host-side absolute paths inside the container, so path strings baked into the
shell command by the task wrapper resolve correctly without rewriting.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from ginkgo.envs.mounts import Mount, parse_extra_mount, resolve_mounts


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


# Docker and Podman both report a missing entry binary as an exec error naming
# the executable, with an exit code in the runtime's own reserved range. The
# message never says which of the runtime's assumptions failed, so a container
# task that fails this way gets the diagnosis added.
_MISSING_EXECUTABLE_MARKERS = (
    "executable file not found",
    "no such file or directory",
    "not found in $path",
)
_RUNTIME_EXIT_CODES = (125, 126, 127)


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
    user : str
        Who the container runs as: ``"auto"`` (the invoking user's uid:gid,
        so outputs are not root-owned), ``"root"``, or an explicit
        ``"uid:gid"`` pair.
    shell : str
        Shell used to run the task command inside the image.
    auto_mount : bool
        Whether to bind-mount declared path inputs and outputs that fall
        outside the project root.
    extra_mounts : tuple[str, ...]
        Escape hatch for paths that cannot be declared task inputs, as
        ``"/path"``, ``"/path:rw"``, or ``"/host:/container:rw"``.
    """

    runtime: str = "docker"
    project_root: Path = field(default_factory=Path.cwd)
    pull_policy: str = "if-not-present"
    user: str = "auto"
    shell: str = "bash"
    auto_mount: bool = True
    extra_mounts: tuple[str, ...] = ()
    _pulled_images: set[str] = field(default_factory=set, init=False, repr=False)
    _digest_cache: dict[str, str | None] = field(default_factory=dict, init=False, repr=False)

    # ------------------------------------------------------------------
    # ExecutionEnvironment protocol
    # ------------------------------------------------------------------

    def validate_envs(self, *, env_names: set[str]) -> None:
        """Validate container env URIs, config, and runtime availability."""
        for env in sorted(env_names):
            parse_container_uri(env)

        # Fail on a malformed [container] table before any image is pulled.
        for spec in self.extra_mounts:
            parse_extra_mount(spec)
        self._user_argv()

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

    def exec_argv(
        self,
        *,
        env: str,
        cmd: str,
        mounts: Sequence[Mount] = (),
        env_vars: Sequence[str] = (),
    ) -> list[str]:
        """Build an argument vector to run *cmd* inside a container.

        The project root is bind-mounted at its host-side absolute path, as is
        every mount in *mounts* that falls outside it, so that paths baked into
        the shell command resolve correctly.

        Parameters
        ----------
        env : str
            Container env URI.
        cmd : str
            Shell command string, already interpolated by the task body.
        mounts : Sequence[Mount]
            Declared path inputs and outputs for this task.  Ignored when
            ``auto_mount`` is off; ``extra_mounts`` are always applied.
        env_vars : Sequence[str]
            Names of environment variables to forward into the container.
            Emitted as bare ``-e NAME``, which both runtimes resolve from the
            client process's own environment — the value never reaches the host
            process table, so a forwarded secret stays out of it.  The image's
            own environment is otherwise left intact.

        Returns
        -------
        list[str]
            Argument vector for ``subprocess.run(..., shell=False)``.
        """
        ref = parse_container_uri(env)
        project = str(self.project_root)

        argv = [self.runtime, "run", "--rm", "-v", f"{project}:{project}"]
        for resolved in self.resolve_task_mounts(mounts=mounts):
            argv += resolved.as_argv()
        argv += self._user_argv()
        for name in env_vars:
            argv += ["-e", name]
        argv += ["-w", project, ref.image, self.shell, "-c", cmd]
        return argv

    def exec_failure_hint(self, *, env: str, exit_code: int, output: str) -> str | None:
        """Return a diagnosis for a container failure the raw output does not name.

        Alpine- and distroless-based images often ship no ``bash``, and the
        runtime reports that as an opaque exec error. Naming the image, the
        shell, and the setting that changes it turns a dead end into a fix.
        """
        if exit_code not in _RUNTIME_EXIT_CODES:
            return None

        lowered = output.lower()
        if self.shell not in output:
            return None
        if not any(marker in lowered for marker in _MISSING_EXECUTABLE_MARKERS):
            return None

        image = parse_container_uri(env).image
        return (
            f"Container image {image!r} appears not to ship {self.shell!r}. "
            f'Set [container] shell in ginkgo.toml (for example shell = "sh") '
            f"to the shell this image provides."
        )

    def resolve_task_mounts(self, *, mounts: Sequence[Mount] = ()) -> list[Mount]:
        """Return the bind mounts to add alongside the project root."""
        requested = list(mounts) if self.auto_mount else []
        requested += [parse_extra_mount(spec) for spec in self.extra_mounts]
        return resolve_mounts(project_root=self.project_root, mounts=requested)

    def env_lock_path(self, *, env: str) -> Path | None:
        """Containers have no lock file artifact."""
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _user_argv(self) -> list[str]:
        """Return the ``-u``/``-e HOME`` flags implied by ``user``.

        Defaulting to the invoking user keeps task outputs writable by
        downstream Python tasks and by the person who started the run.  Images
        that resolve ``$HOME`` or want a writable home would otherwise break on
        the missing passwd entry, so a home inside the container is supplied
        alongside.
        """
        if self.user == "root":
            return []

        if self.user == "auto":
            if not hasattr(os, "getuid"):  # Non-POSIX host; let the image decide.
                return []
            spec = f"{os.getuid()}:{os.getgid()}"
        else:
            spec = self.user
            if not all(part.isdigit() for part in spec.split(":")) or spec.count(":") != 1:
                raise ValueError(
                    f'Invalid [container] user {self.user!r}. Expected "auto", "root", '
                    'or a numeric "uid:gid" pair.'
                )

        return ["-u", spec, "-e", "HOME=/tmp"]

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


def container_backend_from_config(
    *,
    project_root: Path,
    config: dict[str, Any] | None = None,
) -> ContainerBackend:
    """Build a ``ContainerBackend`` from the ``[container]`` config table.

    Parameters
    ----------
    project_root : Path
        Host directory mounted into every container.
    config : dict[str, Any] | None
        Merged runtime config.  A missing or non-mapping ``[container]`` table
        yields backend defaults.

    Returns
    -------
    ContainerBackend
    """
    table = (config or {}).get("container")
    if not isinstance(table, dict):
        table = {}

    extra_mounts = table.get("extra_mounts", ())
    if isinstance(extra_mounts, str):
        extra_mounts = (extra_mounts,)

    return ContainerBackend(
        project_root=project_root,
        runtime=str(table.get("runtime", "docker")),
        pull_policy=str(table.get("pull_policy", "if-not-present")),
        user=str(table.get("user", "auto")),
        shell=str(table.get("shell", "bash")),
        auto_mount=bool(table.get("auto_mount", True)),
        extra_mounts=tuple(str(spec) for spec in extra_mounts),
    )
