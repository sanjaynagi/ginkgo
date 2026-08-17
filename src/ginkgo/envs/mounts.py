"""Bind-mount model shared by the container backend and its callers.

A container task's shell command carries host absolute paths, so every path the
command touches must appear inside the container at the same absolute path it
has on the host.  ``Mount`` records one such correspondence, and
``resolve_mounts`` turns the raw set a task declares into the minimal, safe list
the runtime is asked for.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

MOUNT_MODES = ("ro", "rw")

# Mounting these would expose or shadow host system state, and no declared
# workflow input legitimately lives in them.  ``/`` is refused outright.
_REFUSED_ROOTS = frozenset(
    {
        "/bin",
        "/boot",
        "/dev",
        "/etc",
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
        # macOS hosts, where the system directories above are symlinks into
        # /private and so resolve to a second set of names.
        "/Applications",
        "/Library",
        "/System",
        "/private/etc",
        "/private/var",
    }
)


class UnsafeMountError(ValueError):
    """Raised when a resolved path may not be bind-mounted."""

    def __init__(self, *, path: Path) -> None:
        super().__init__(
            f"Refusing to bind-mount {str(path)!r} into a container: it is the host "
            "root or a system directory. Point the declared input at a path inside "
            "your data or project tree."
        )


@dataclass(frozen=True, kw_only=True)
class Mount:
    """One host path made visible inside a container.

    Parameters
    ----------
    host_path : Path
        Path on the host, as declared.  Symlinks are resolved by
        ``resolve_mounts``, not here.
    container_path : Path
        Path the mount appears at inside the container.  Defaults to
        ``host_path`` via :func:`mount`, since commands carry host paths.
    mode : str
        ``"ro"`` for inputs, ``"rw"`` for declared outputs and scratch space.
    """

    host_path: Path
    container_path: Path
    mode: str = "ro"

    def as_argv(self) -> list[str]:
        """Return the ``-v`` flag pair for this mount."""
        return ["-v", f"{self.host_path}:{self.container_path}:{self.mode}"]


def mount(path: str | Path, *, mode: str = "ro") -> Mount:
    """Return a same-path ``Mount`` for *path*."""
    if mode not in MOUNT_MODES:
        raise ValueError(f"Mount mode must be one of {MOUNT_MODES}, got {mode!r}")
    resolved = Path(path)
    return Mount(host_path=resolved, container_path=resolved, mode=mode)


def parse_extra_mount(spec: str) -> Mount:
    """Parse a ``[container] extra_mounts`` entry.

    Accepts ``"/path"``, ``"/path:rw"``, and ``"/host:/container:rw"``.  The
    mode defaults to ``"ro"``.

    Raises
    ------
    ValueError
        If *spec* is empty, has too many fields, or names an unknown mode.
    """
    parts = [part.strip() for part in spec.split(":")]
    if not parts or not parts[0]:
        raise ValueError(f"Empty container mount specification: {spec!r}")

    if len(parts) == 1:
        host, container, mode = parts[0], parts[0], "ro"
    elif len(parts) == 2 and parts[1] in MOUNT_MODES:
        host, container, mode = parts[0], parts[0], parts[1]
    elif len(parts) == 2 and parts[1].startswith("/"):
        host, container, mode = parts[0], parts[1], "ro"
    elif len(parts) == 2:
        # Neither a mode nor a container path; guessing which was meant would
        # silently mount at a relative path or with the wrong mode.
        raise ValueError(
            f"Invalid container mount mode {parts[1]!r} in {spec!r}. Expected one of "
            f"{MOUNT_MODES}, or an absolute container path."
        )
    elif len(parts) == 3:
        host, container, mode = parts
    else:
        raise ValueError(
            f"Invalid container mount specification: {spec!r}. "
            'Expected "/path", "/path:rw", or "/host:/container:rw".'
        )

    if mode not in MOUNT_MODES:
        raise ValueError(f"Invalid container mount mode {mode!r} in {spec!r}")

    return Mount(host_path=Path(host), container_path=Path(container), mode=mode)


def resolve_mounts(*, project_root: Path, mounts: Iterable[Mount]) -> list[Mount]:
    """Return the minimal safe mount list for *mounts*.

    Symlinks are resolved on the host side while the container side keeps the
    declared path, so a command written against a symlink still resolves.
    Mounts already covered by the project-root mount are dropped, descendants
    of an equal-mode mount are collapsed into it, and read-write wins over
    read-only for the same container path.

    Raises
    ------
    UnsafeMountError
        If a resolved host path is the filesystem root or a system directory.
    """
    project = project_root.resolve()

    by_container: dict[Path, Mount] = {}
    for candidate in mounts:
        declared = _absolute(candidate.host_path)
        host = _resolve_host_path(candidate.host_path)
        container = _absolute(candidate.container_path)
        # Both names are checked: a refused directory is often a symlink to a
        # path under another name (/etc -> /private/etc on macOS), and either
        # spelling would otherwise get through.
        _require_safe(declared)
        _require_safe(host)

        # The project root is already mounted at its own path; a declared path
        # that resolves inside it needs nothing further. A path *under* the
        # project root that resolves elsewhere still needs its own mount.
        if container == host and _is_within(path=host, ancestor=project):
            continue

        existing = by_container.get(container)
        if existing is None or _stronger(candidate.mode, existing.mode):
            by_container[container] = Mount(
                host_path=host, container_path=container, mode=candidate.mode
            )

    return _drop_covered(list(by_container.values()))


def _resolve_host_path(path: Path) -> Path:
    """Resolve *path* to a real host path, tolerating a not-yet-created leaf."""
    absolute = _absolute(path)
    if absolute.exists():
        return absolute.resolve()
    # A declared output does not exist yet; its parent has been created by the
    # runner, so resolve that and re-attach the leaf name.
    parent = absolute.parent
    return (parent.resolve() if parent.exists() else parent) / absolute.name


def _absolute(path: Path) -> Path:
    return path if path.is_absolute() else Path.cwd() / path


def _require_safe(path: Path) -> None:
    if path == Path(path.anchor) or str(path) in _REFUSED_ROOTS:
        raise UnsafeMountError(path=path)


def _stronger(mode: str, than: str) -> bool:
    return mode == "rw" and than == "ro"


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
