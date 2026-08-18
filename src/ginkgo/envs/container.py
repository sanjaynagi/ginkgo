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
from ginkgo.errors import GinkgoError


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


class ContainerRuntimeNotFoundError(GinkgoError, RuntimeError):
    """Raised when the container runtime binary is not on PATH."""

    def __init__(self, *, runtime: str) -> None:
        super().__init__(
            f"Container runtime {runtime!r} is not installed or not found on PATH. "
            f"Install {runtime} to run container-isolated tasks."
        )


class ContainerPrepareError(GinkgoError, RuntimeError):
    """Raised when an image cannot be pulled."""

    def __init__(self, *, image: str, output: str) -> None:
        details = output.strip() or "no output from container runtime"
        super().__init__(f"Failed to pull container image {image!r}: {details}")


# Markers the *runtime* itself emits when it cannot exec the entry binary, as
# distinct from anything the command may print once it is running. A shell that
# started and then failed to find a file reports "no such file or directory"
# too, so that phrase is deliberately not here: it would turn a CRLF script or
# a missing input into a bogus "your image has no bash".
_EXEC_FAILURE_MARKERS = (
    "executable file not found",
    "exec format error",
    "oci runtime",
    "exec:",
)
# Exit codes the runtime reserves for its own failures rather than the command's.
_RUNTIME_EXIT_CODES = (125, 126, 127)

# Shells to suggest, in order, when the configured one is missing.
_FALLBACK_SHELLS = ("sh", "bash", "ash")


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
        Who the container runs as. ``"auto"`` picks whatever leaves outputs
        owned by the invoking user, which differs by runtime: Docker needs an
        explicit ``uid:gid``, while rootless Podman already maps the container's
        own root to the invoking user and needs nothing. ``"root"`` lets the
        image decide, and an explicit ``"uid:gid"`` pair overrides both.
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
    _digest_cache: dict[str, str] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        # Resolved once, so the path mounted and the path mounts are compared
        # against are the same one. A project reached through a symlink would
        # otherwise have every declared input mounted a second time, read-only,
        # over the read-write project mount.
        self.project_root = self.project_root.resolve()

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

    def env_identity(self, *, env: str) -> str:
        """Return the declared image reference, which names the environment.

        Cache keys fold this in, so it must not depend on whether the image has
        been pulled yet: the local image ID only exists after :meth:`prepare`,
        and keying on it re-ran every container task once per fresh image
        (issue #194). A tag is as specific as the workflow author chose to be —
        pin ``image@sha256:...`` for content-level invalidation. The image as
        pulled is recorded separately, by :meth:`materialized_digest`.

        Returns
        -------
        str
            Image reference with the ``docker://`` / ``oci://`` scheme stripped.
        """
        return parse_container_uri(env).image

    def materialized_digest(self, *, env: str) -> str | None:
        """Return the ID of the image as pulled on this machine.

        Recorded in provenance, and compared against the digest a cache entry
        was written with, so an image repointed under a mutable tag stops
        serving stale results. An absent image is not remembered: ``prepare``
        pulls it, and a caller that asked before the pull has to see the ID
        afterwards.

        Returns
        -------
        str | None
            Image ID (``sha256:...``), or ``None`` if the image cannot be
            inspected — which is the case until :meth:`prepare` has pulled it.
        """
        cached = self._digest_cache.get(env)
        if cached is not None:
            return cached

        ref = parse_container_uri(env)
        digest = self._resolve_digest(ref.image)
        if digest is not None:
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
            client process's own environment, so the values are not spelled out
            in this argument vector.  The image's own environment is otherwise
            left intact.

        Returns
        -------
        list[str]
            Argument vector for ``subprocess.run(..., shell=False)``.
        """
        ref = parse_container_uri(env)
        project = str(self.project_root)

        argv = [self.runtime, "run", "--rm", "-v", f"{project}:{project}"]
        for resolved in self.resolve_bind_mounts(mounts=mounts):
            argv += _mount_argv(resolved)
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

        The test is deliberately narrow. It fires only on a failure the runtime
        itself raised, and only when the shell is named as the binary that could
        not be executed — not when a shell that started reports a missing file.
        A command failing on a CRLF script or an absent input says
        "no such file or directory" too, and sending its author to the shell
        setting would cost them more time than saying nothing.
        """
        if exit_code not in _RUNTIME_EXIT_CODES:
            return None

        lowered = output.lower()
        if not any(marker in lowered for marker in _EXEC_FAILURE_MARKERS):
            return None
        # Both runtimes quote the binary they could not exec. Matching the
        # quoted token keeps "sh" from matching inside "bash".
        if f'"{self.shell}"' not in output and f"`{self.shell}`" not in output:
            return None

        image = parse_container_uri(env).image
        suggestion = next(
            (candidate for candidate in _FALLBACK_SHELLS if candidate != self.shell),
            None,
        )
        advice = (
            f'Set [container] shell in ginkgo.toml (for example shell = "{suggestion}") '
            "to a shell this image provides."
            if suggestion is not None
            else "Set [container] shell in ginkgo.toml to a shell this image provides."
        )
        return f"Container image {image!r} appears not to ship {self.shell!r}. {advice}"

    def resolve_bind_mounts(self, *, mounts: Sequence[Mount] = ()) -> list[Mount]:
        """Return every bind mount to add alongside the project root.

        Combines the task's declared mounts with the user's ``extra_mounts``.
        ``auto_mount = false`` suppresses the declared ones only: an entry the
        user wrote down is not something to second-guess.
        """
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
        """Return the flags that decide who the container runs as.

        Task outputs have to stay writable by downstream Python tasks and by the
        person who started the run, and the flag that achieves that is not the
        same for both runtimes. Docker runs the container as the image's own
        user, so it needs an explicit ``uid:gid``. Rootless Podman already maps
        the container's root to the invoking user, and passing ``-u`` there
        makes ownership *worse*: the uid maps into the subordinate range
        instead, leaving files the user cannot chmod or delete.

        The explicit ``uid:gid`` form is passed through as given on either
        runtime, for the cases where the caller knows better.
        """
        if self.user == "root":
            return []

        if self.user == "auto":
            if self.runtime == "podman" or not hasattr(os, "getuid"):
                return []
            return _uid_gid_argv(f"{os.getuid()}:{os.getgid()}")

        parts = self.user.split(":")
        if len(parts) != 2 or not all(part.isdigit() for part in parts):
            raise ValueError(
                f'Invalid [container] user {self.user!r}. Expected "auto", "root", '
                'or a numeric "uid:gid" pair.'
            )
        return _uid_gid_argv(self.user)

    def _image_exists_locally(self, image: str) -> bool:
        """Return whether *image* is present in the local image store."""
        completed = subprocess.run(
            [self.runtime, "image", "inspect", image],
            check=False,
            capture_output=True,
        )
        return completed.returncode == 0

    def _resolve_digest(self, image: str) -> str | None:
        """Return the image ID via ``docker image inspect``.

        Cache lookups ask for this, so a runtime that is not installed reads as
        "no local image" rather than raising: an absent runtime cannot be
        evidence about an image either way.
        """
        try:
            completed = subprocess.run(
                [self.runtime, "image", "inspect", "--format", "{{.Id}}", image],
                check=False,
                text=True,
                capture_output=True,
            )
        except OSError:
            return None
        if completed.returncode != 0:
            return None

        digest = (completed.stdout or "").strip()
        return digest if digest else None


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _mount_argv(mount: Mount) -> list[str]:
    """Return the runtime flag pair that establishes *mount*."""
    return ["-v", f"{mount.host_path}:{mount.container_path}:{mount.mode}"]


def _uid_gid_argv(spec: str) -> list[str]:
    """Return the flags that run a container as *spec*.

    A container running as a uid with no passwd entry has no home directory
    either, and images that resolve ``$HOME`` fail on that rather than on
    anything the task did, so one is supplied.
    """
    return ["-u", spec, "-e", "HOME=/tmp"]


def _require_container_runtime(runtime: str) -> None:
    """Raise if the container runtime binary is not on PATH."""
    if shutil.which(runtime) is None:
        raise ContainerRuntimeNotFoundError(runtime=runtime)


_CONTAINER_CONFIG_KEYS = frozenset(
    {"runtime", "pull_policy", "user", "shell", "auto_mount", "extra_mounts"}
)


def container_backend_from_config(
    *,
    project_root: Path,
    config: dict[str, Any] | None = None,
) -> ContainerBackend:
    """Build a ``ContainerBackend`` from the ``[container]`` config table.

    Unknown keys and mistyped values are rejected rather than ignored: a
    typo in this table would otherwise leave a workflow running under
    defaults that look nothing like what the file says.

    Parameters
    ----------
    project_root : Path
        Host directory mounted into every container.
    config : dict[str, Any] | None
        Merged runtime config.  A missing ``[container]`` table yields backend
        defaults.

    Returns
    -------
    ContainerBackend

    Raises
    ------
    ValueError
        If the table is not a mapping, names an unknown key, or gives a value
        of the wrong type.
    """
    table = (config or {}).get("container", {})
    if not isinstance(table, dict):
        raise ValueError("[container] must be a table of container settings")

    unknown = set(table) - _CONTAINER_CONFIG_KEYS
    if unknown:
        supported = ", ".join(sorted(_CONTAINER_CONFIG_KEYS))
        raise ValueError(
            f"[container] has unknown keys {sorted(unknown)}; supported keys are {{{supported}}}"
        )

    return ContainerBackend(
        project_root=project_root,
        runtime=_config_str(table, "runtime", "docker"),
        pull_policy=_config_str(table, "pull_policy", "if-not-present"),
        user=_config_str(table, "user", "auto"),
        shell=_config_str(table, "shell", "bash"),
        auto_mount=_config_bool(table, "auto_mount", True),
        extra_mounts=_config_mounts(table),
    )


def _config_str(table: dict[str, Any], key: str, default: str) -> str:
    value = table.get(key, default)
    if not isinstance(value, str):
        raise ValueError(f"[container] {key} must be a string, got {value!r}")
    return value


def _config_bool(table: dict[str, Any], key: str, default: bool) -> bool:
    value = table.get(key, default)
    # `bool("false")` is True, so coercing a string here would invert what the
    # file says. TOML has a real boolean; require it.
    if not isinstance(value, bool):
        raise ValueError(f"[container] {key} must be true or false, got {value!r}")
    return value


def _config_mounts(table: dict[str, Any]) -> tuple[str, ...]:
    value = table.get("extra_mounts", ())
    if isinstance(value, str):
        value = (value,)
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"[container] extra_mounts must be a list of strings, got {value!r}")
    for spec in value:
        if not isinstance(spec, str):
            raise ValueError(f"[container] extra_mounts entries must be strings, got {spec!r}")
    return tuple(value)
