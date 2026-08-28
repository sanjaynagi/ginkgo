"""Whether the ledger is about to be written to a network filesystem.

SQLite's locking is only as reliable as the filesystem's, and NFS, Lustre, SMB
and FUSE mounts do not all honour the primitives it needs. Ginkgo does not
refuse to run there — a shared cluster home directory is a perfectly ordinary
place to keep a workspace — but it says so once, and points at ``GINKGO_DB``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

__all__ = ["is_network_filesystem", "warn_if_network_filesystem"]


_NETWORK_FILESYSTEMS = ("nfs", "lustre", "cifs", "smbfs", "fuse", "9p", "afs", "gpfs")
"""Filesystem-type prefixes whose locking SQLite cannot rely on."""

_warned = False
"""Whether this process has already printed the warning."""


def is_network_filesystem(path: Path) -> str | None:
    """Return the filesystem type of *path* when it is a network mount.

    The mount table is consulted rather than the path itself, so *path* need
    not exist yet: the longest mount point that prefixes it is the one it will
    be created under.

    Parameters
    ----------
    path : Path
        The file or directory to classify.

    Returns
    -------
    str | None
        The filesystem type, e.g. ``"nfs4"``, or ``None`` when the mount is
        local — or when the mount table could not be read, since an
        unanswerable question is not a reason to warn.
    """
    fstype = _mount_type(path)
    if fstype is None:
        return None
    lowered = fstype.lower()
    if any(lowered.startswith(prefix) for prefix in _NETWORK_FILESYSTEMS):
        return fstype
    return None


def warn_if_network_filesystem(path: Path) -> None:
    """Warn on stderr, at most once per process, if *path* is on a network mount.

    Parameters
    ----------
    path : Path
        The database file about to be opened for writing.
    """
    global _warned
    if _warned:
        return
    fstype = is_network_filesystem(path)
    _warned = True
    if fstype is None:
        return
    print(
        f"⚠ .ginkgo is on {fstype}; SQLite locking may be unreliable. "
        "Set GINKGO_DB to a local path.",
        file=sys.stderr,
    )


def _mount_type(path: Path) -> str | None:
    """Return the filesystem type of the mount *path* falls under."""
    try:
        mounts = _mount_table()
        target = str(Path(path).resolve())
    except OSError:
        return None
    best: tuple[int, str] | None = None
    for mount_point, fstype in mounts:
        if target == mount_point or target.startswith(mount_point.rstrip("/") + "/"):
            if best is None or len(mount_point) > best[0]:
                best = (len(mount_point), fstype)
    return None if best is None else best[1]


def _mount_table() -> list[tuple[str, str]]:
    """Return ``(mount point, filesystem type)`` for every current mount."""
    proc_mounts = Path("/proc/self/mounts")
    if proc_mounts.exists():
        return _parse_proc_mounts(proc_mounts.read_text(encoding="utf-8", errors="replace"))
    return _parse_mount_output(_run_mount())


def _parse_proc_mounts(text: str) -> list[tuple[str, str]]:
    """Parse Linux ``/proc/self/mounts`` lines: ``device point type opts``."""
    table: list[tuple[str, str]] = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 3:
            # Mount points are octal-escaped for spaces and tabs.
            table.append((fields[1].encode().decode("unicode_escape"), fields[2]))
    return table


def _parse_mount_output(text: str) -> list[tuple[str, str]]:
    """Parse BSD/macOS ``mount`` lines: ``dev on /point (type, opts...)``."""
    table: list[tuple[str, str]] = []
    for line in text.splitlines():
        _, separator, tail = line.partition(" on ")
        if not separator or "(" not in tail:
            continue
        point, _, options = tail.rpartition(" (")
        fstype = options.split(",")[0].strip().rstrip(")").strip()
        if point and fstype:
            table.append((point, fstype))
    return table


def _run_mount() -> str:
    """Return the output of ``mount``, or an empty string if it cannot be run."""
    try:
        result = subprocess.run(["mount"], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout
