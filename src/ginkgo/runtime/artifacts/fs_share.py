"""Filesystem-shared copy helpers for the artifact CAS.

When importing bytes into the content-addressed artifact store, we often
already have a copy on disk under a different path — for example, the
remote-input :class:`~ginkgo.remote.staging.StagingCache` downloads into
``.ginkgo/staging/blobs/<digest>`` before the artifact store would
``shutil.copy2`` the same bytes into ``.ginkgo/artifacts/blobs/<digest>``
for transport to a remote worker. That doubles disk usage for every
staged input, which is cheap at 1 MB and painful at 100 GB.

This module encapsulates a progressive strategy for sharing bytes across
paths instead of duplicating them:

1. **Reflink (copy-on-write clone).** On APFS (macOS) and btrfs / XFS
   (Linux) the kernel can create a second inode that shares the
   underlying data blocks until either side is modified. Independent
   inode identity means chmod, permissions, and subsequent writes do
   not bleed across — it is the semantically cleanest option.
2. **Hardlink.** Two directory entries pointing at the same inode.
   Zero-cost, universally supported on POSIX, but any ``chmod`` on one
   path also changes the other. We only take this path when the caller
   promises the source is immutable (``src_is_readonly=True``) — the
   stage flow guarantees that via the CAS layout.
3. **Full copy.** ``shutil.copy2`` — the historical behaviour. Always
   safe, always the slow path; used when reflink is unsupported and
   hardlink is either disallowed or rejected (for example, because the
   source lives on a different filesystem).
"""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import fcntl
import os
import shutil
import sys
from pathlib import Path
from typing import Literal

ShareMethod = Literal["reflink", "hardlink", "copy"]


# --- Platform-specific reflink bindings ---------------------------------------


def _load_macos_clonefile():
    """Return a bound ``clonefile`` syscall wrapper on macOS, else ``None``."""
    libc_name = ctypes.util.find_library("System") or "libSystem.dylib"
    try:
        libc = ctypes.CDLL(libc_name, use_errno=True)
    except OSError:
        return None
    if not hasattr(libc, "clonefile"):
        return None
    fn = libc.clonefile
    fn.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint32]
    fn.restype = ctypes.c_int
    return fn


_CLONEFILE = _load_macos_clonefile() if sys.platform == "darwin" else None

# Linux ``ioctl`` command code for FICLONE (copy-on-write reflink).
# See ``include/uapi/linux/fs.h``: ``_IOW(0x94, 9, int)``.
_FICLONE = 0x40049409


def _reflink(*, src: Path, dst: Path) -> bool:
    """Attempt a copy-on-write clone from ``src`` to ``dst``.

    Returns ``True`` on success. Returns ``False`` on any failure —
    unsupported filesystem, cross-device, missing syscall, or
    permissions — without raising. The destination is left absent on
    failure so the caller can try the next tier.
    """
    if sys.platform == "darwin" and _CLONEFILE is not None:
        # ``clonefile`` requires the destination not to exist.
        if dst.exists():
            return False
        rc = _CLONEFILE(os.fsencode(src), os.fsencode(dst), 0)
        if rc == 0:
            return True
        return False

    if sys.platform.startswith("linux"):
        # ``FICLONE`` overwrites the destination, which must be a
        # writable regular file. Open with ``O_CREAT|O_EXCL`` so an
        # existing file fails fast — we want the store's existence
        # check to have already excluded that case.
        try:
            src_fd = os.open(src, os.O_RDONLY)
        except OSError:
            return False
        try:
            try:
                dst_fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except OSError:
                return False
            try:
                fcntl.ioctl(dst_fd, _FICLONE, src_fd)
            except OSError:
                # FS doesn't support FICLONE, cross-device, etc.
                # Remove the empty destination so fallback tiers can create it.
                try:
                    os.close(dst_fd)
                finally:
                    try:
                        dst.unlink()
                    except FileNotFoundError:
                        pass
                return False
            os.close(dst_fd)
            return True
        finally:
            os.close(src_fd)

    return False


def _hardlink(*, src: Path, dst: Path) -> bool:
    """Create a hardlink from ``src`` to ``dst``; ``False`` on failure.

    Fails (returns ``False``) when the paths are on different devices
    (``EXDEV``) or when the filesystem does not support hardlinks.
    """
    try:
        os.link(src, dst)
        return True
    except OSError as exc:
        if exc.errno in {errno.EXDEV, errno.EPERM, errno.ENOTSUP}:
            return False
        raise


# --- Public API --------------------------------------------------------------


def share_bytes(
    *,
    src: Path,
    dst: Path,
    allow_hardlink: bool = False,
) -> ShareMethod:
    """Populate ``dst`` with the bytes of ``src``, sharing when possible.

    Parameters
    ----------
    src : Path
        Existing source file. Must be a regular file.
    dst : Path
        Destination path. Must not already exist — the artifact store
        always calls this after a ``blob_path.exists()`` guard because
        CAS blobs are written once and re-used.
    allow_hardlink : bool
        When ``True`` the caller guarantees ``src`` will not be
        mutated for the lifetime of ``dst``. Hardlink becomes the
        second-tier option after reflink, before full copy. When
        ``False`` (default) hardlinking is skipped so that operations
        on the store's file (notably ``chmod`` to read-only) cannot
        bleed back into caller-owned paths.

    Returns
    -------
    ShareMethod
        ``"reflink"``, ``"hardlink"``, or ``"copy"`` describing how
        ``dst`` was populated. Exposed primarily for tests and
        telemetry.
    """
    if _reflink(src=src, dst=dst):
        return "reflink"

    if allow_hardlink and _hardlink(src=src, dst=dst):
        return "hardlink"

    shutil.copy2(src, dst)
    return "copy"
