"""Bind-mount model for container-backed execution.

A container task's shell command carries host absolute paths, so every path the
command touches must appear inside the container at the same absolute path it
has on the host.  ``Mount`` records one such correspondence, and
``resolve_mounts`` turns the raw set a task declares into the minimal, safe list
the backend asks the runtime for.  Rendering a mount as runtime flags belongs to
the backend, not here.

Mounts arrive from two places, and the difference matters. A *declared* mount is
derived from what a task annotates, so it is resolved conservatively: it may be
widened from read-only to read-write by another declared mount, and it may not
name a directory broad enough to be dangerous. A *configured* mount comes from
``[container] extra_mounts``, where the user has said what they want: its mode is
authoritative and nothing may quietly widen it.
"""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Sequence

from ginkgo.errors import GinkgoError

MountMode = Literal["ro", "rw"]
MountOrigin = Literal["declared", "configured"]

DEFAULT_MOUNT_MODE: MountMode = "ro"

# Mounting one of these would expose or shadow host system state, and no
# declared workflow path legitimately names one. The names are resolved as well
# as taken literally, because on macOS the system directories are symlinks into
# /private and so answer to two spellings; resolving here means the check does
# not need a per-platform list of aliases.
_REFUSED_NAMES = (
    "/bin",
    "/boot",
    "/dev",
    "/etc",
    "/home",
    "/lib",
    "/lib32",
    "/lib64",
    "/libx32",
    "/proc",
    "/run",
    "/sbin",
    "/sys",
    "/usr",
    "/var",
    "/Applications",
    "/Library",
    "/System",
    "/Users",
)


def _refused_paths() -> frozenset[Path]:
    """Return the refused directories under every name they answer to."""
    refused: set[Path] = set()
    for name in _REFUSED_NAMES:
        path = Path(name)
        refused.add(path)
        with suppress(OSError):
            refused.add(path.resolve())
    return frozenset(refused)


_REFUSED_PATHS = _refused_paths()


class UnsafeMountError(GinkgoError, ValueError):
    """Raised when a path is too broad or too sensitive to bind-mount."""

    def __init__(self, *, path: Path, reason: str) -> None:
        super().__init__(
            f"Refusing to bind-mount {str(path)!r} into a container: {reason}. "
            "Point the declared path at a directory of its own, or name the mount "
            "explicitly in [container] extra_mounts if that is really what you want."
        )


class MountModeConflictError(GinkgoError, ValueError):
    """Raised when a declared mount would widen a configured read-only mount."""

    def __init__(self, *, path: Path) -> None:
        super().__init__(
            f"[container] extra_mounts declares {str(path)!r} read-only, but a task "
            "declares an output under it, which needs write access. Change that entry "
            "to ':rw' or move the output elsewhere."
        )


class MissingMountError(GinkgoError, ValueError):
    """Raised when a configured mount names a path that does not exist."""

    def __init__(self, *, path: Path) -> None:
        super().__init__(
            f"[container] extra_mounts names {str(path)!r}, which does not exist. "
            "The container runtime would create it as an empty directory on the host; "
            "create it first or remove the entry."
        )


@dataclass(frozen=True, kw_only=True)
class Mount:
    """One host path made visible inside a container.

    Parameters
    ----------
    host_path : Path
        Path on the host, as given.  Symlinks are resolved by
        :func:`resolve_mounts`, not here.
    container_path : Path
        Path the mount appears at inside the container.  :func:`mount` defaults
        it to ``host_path``, since task commands carry host paths.
    mode : MountMode
        ``"ro"`` for inputs, ``"rw"`` for declared outputs and scratch space.
    origin : MountOrigin
        Whether the mount was derived from a task declaration or written by the
        user in ``[container] extra_mounts``.
    """

    host_path: Path
    container_path: Path
    mode: MountMode = DEFAULT_MOUNT_MODE
    origin: MountOrigin = "declared"


def mount(
    path: str | Path,
    *,
    mode: MountMode = DEFAULT_MOUNT_MODE,
    origin: MountOrigin = "declared",
) -> Mount:
    """Return a same-path ``Mount`` for *path*."""
    same = Path(path)
    return Mount(host_path=same, container_path=same, mode=mode, origin=origin)


def parse_extra_mount(spec: str) -> Mount:
    """Parse one ``[container] extra_mounts`` entry.

    Accepts ``"/path"``, ``"/path:rw"``, ``"/host:/container"``, and
    ``"/host:/container:rw"``.  The mode defaults to read-only.

    Raises
    ------
    ValueError
        If *spec* is empty, has too many fields, or has a second field that is
        neither a mode nor an absolute container path.
    """
    parts = [part.strip() for part in spec.split(":")]
    if not parts[0]:
        raise ValueError(f"Empty container mount specification: {spec!r}")

    if len(parts) == 1:
        host, container, mode = parts[0], parts[0], DEFAULT_MOUNT_MODE
    elif len(parts) == 2 and parts[1] in ("ro", "rw"):
        host, container, mode = parts[0], parts[0], parts[1]
    elif len(parts) == 2 and parts[1].startswith("/"):
        host, container, mode = parts[0], parts[1], DEFAULT_MOUNT_MODE
    elif len(parts) == 2:
        # Neither a mode nor a container path. Guessing which was meant would
        # mount at a relative path or with the wrong mode, in silence.
        raise ValueError(
            f"Invalid container mount {spec!r}: {parts[1]!r} is neither a mode "
            '("ro"/"rw") nor an absolute container path.'
        )
    elif len(parts) == 3:
        host, container, mode = parts
    else:
        raise ValueError(
            f"Invalid container mount specification: {spec!r}. "
            'Expected "/path", "/path:rw", or "/host:/container:rw".'
        )

    if mode not in ("ro", "rw"):
        raise ValueError(f"Invalid container mount mode {mode!r} in {spec!r}")

    return Mount(
        host_path=Path(host),
        container_path=Path(container),
        mode=mode,
        origin="configured",
    )


def resolve_mounts(*, project_root: Path, mounts: Iterable[Mount]) -> list[Mount]:
    """Return the minimal safe mount list for *mounts*.

    Symlinks resolve on the host side while the container side keeps the path as
    given, so a command written against a symlink still resolves.  Mounts the
    project-root mount already covers are dropped, descendants of an equal-mode
    mount collapse into it, and read-write wins over read-only for the same
    container path — except where the read-only mount was configured by the
    user, which raises instead.

    Raises
    ------
    UnsafeMountError
        If a path is the filesystem root, a system directory, or a home
        directory.
    MissingMountError
        If a configured mount names a path that does not exist.
    MountModeConflictError
        If a declared mount would widen a configured read-only mount.
    """
    project = project_root.resolve()

    by_container: dict[Path, Mount] = {}
    for candidate in mounts:
        given = _absolute(candidate.host_path)
        if not given.exists():
            if candidate.origin == "configured":
                raise MissingMountError(path=given)
            # A declared path that vanished between resolution and dispatch is
            # the command's problem to report, not a mount to invent.
            continue

        host = given.resolve()
        container = _absolute(candidate.container_path)
        # Both spellings are checked: a refused directory is often reached
        # through a symlink under another name.
        _require_safe(given)
        _require_safe(host)

        # The project root is already mounted at its own path, so a path that
        # resolves inside it needs nothing further. A path *under* the project
        # root that resolves elsewhere still needs its own mount.
        if container == host and _is_within(path=host, ancestor=project):
            continue

        resolved = Mount(
            host_path=host,
            container_path=container,
            mode=candidate.mode,
            origin=candidate.origin,
        )
        existing = by_container.get(container)
        if existing is None:
            by_container[container] = resolved
        else:
            by_container[container] = _merge(existing=existing, candidate=resolved)

    return _drop_covered(list(by_container.values()))


def _merge(*, existing: Mount, candidate: Mount) -> Mount:
    """Return the mount that satisfies both requests for one container path."""
    if existing.mode == candidate.mode:
        return existing
    # One asks for rw and the other for ro. A user-written ro is a decision, not
    # a default, so widening it silently would discard what they asked for.
    read_only = existing if existing.mode == "ro" else candidate
    if read_only.origin == "configured":
        raise MountModeConflictError(path=read_only.container_path)
    return existing if existing.mode == "rw" else candidate


def _absolute(path: Path) -> Path:
    return path if path.is_absolute() else Path.cwd() / path


def _require_safe(path: Path) -> None:
    """Raise if *path* is too broad or too sensitive to mount."""
    if path == Path(path.anchor):
        raise UnsafeMountError(path=path, reason="it is the filesystem root")
    if path in _REFUSED_PATHS:
        raise UnsafeMountError(path=path, reason="it is a system directory")
    # A declared path whose directory is the whole home directory would hand the
    # image ~/.ssh, ~/.aws, and ~/.config along with the one file it wanted.
    with suppress(OSError, RuntimeError):
        home = Path.home().resolve()
        if path == home or path == Path.home():
            raise UnsafeMountError(path=path, reason="it is your home directory")


def _is_within(*, path: Path, ancestor: Path) -> bool:
    return path == ancestor or ancestor in path.parents


def _drop_covered(mounts: Sequence[Mount]) -> list[Mount]:
    """Drop mounts an enclosing mount of the same mode already provides."""
    kept: list[Mount] = []
    for candidate in mounts:
        if any(
            other is not candidate
            and other.mode == candidate.mode
            and other.container_path in candidate.container_path.parents
            and other.host_path in candidate.host_path.parents
            for other in mounts
        ):
            continue
        kept.append(candidate)
    return sorted(kept, key=lambda item: str(item.container_path))
