"""Run-scoped memoization for content hashing.

Within a single evaluator run, file contents rarely change.  ``HashMemo``
caches BLAKE3 digests keyed by filesystem stat metadata so that repeated
hashing of the same file (e.g. a large BAM consumed by many downstream
tasks) reads the bytes only once.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

from ginkgo.runtime.hashing import hash_directory, hash_file, hash_str


@dataclass(frozen=True)
class _StatKey:
    """Filesystem identity for a single file."""

    device: int
    inode: int
    size: int
    mtime_ns: int


class HashMemo:
    """Run-scoped content-hash cache keyed by file stat metadata.

    Thread-safe: all reads and writes are guarded by a lock so the memo
    can be shared across the evaluator's thread pools.
    """

    def __init__(self) -> None:
        self._file_cache: dict[_StatKey, str] = {}
        self._dir_cache: dict[str, str] = {}
        self._lock = threading.Lock()

    # -- public API ----------------------------------------------------------

    def hash_file(self, path: Path) -> str:
        """Return the BLAKE3 digest for *path*, memoized by stat.

        Parameters
        ----------
        path : Path
            File to hash.  Symlinks are resolved.

        Returns
        -------
        str
            Hex-encoded BLAKE3 digest.
        """
        resolved = path.resolve()
        key = _stat_key_for(resolved)
        with self._lock:
            cached = self._file_cache.get(key)
        if cached is not None:
            return cached

        digest = hash_file(path)
        with self._lock:
            self._file_cache[key] = digest
        return digest

    def hash_directory(self, path: Path) -> str:
        """Return the BLAKE3 digest for a directory, memoized by children stats.

        Parameters
        ----------
        path : Path
            Directory to hash.  Symlinks are resolved.

        Returns
        -------
        str
            Hex-encoded BLAKE3 digest.
        """
        fingerprint = self._dir_fingerprint(path)
        with self._lock:
            cached = self._dir_cache.get(fingerprint)
        if cached is not None:
            return cached

        digest = hash_directory(path)
        with self._lock:
            self._dir_cache[fingerprint] = digest
        return digest

    def put_file(self, path: Path, digest: str) -> None:
        """Inject a known digest for *path* without reading it.

        Parameters
        ----------
        path : Path
            The file whose digest is already known.
        digest : str
            The BLAKE3 hex digest.
        """
        resolved = path.resolve()
        key = _stat_key_for(resolved)
        with self._lock:
            self._file_cache[key] = digest

    # -- internals -----------------------------------------------------------

    def _dir_fingerprint(self, path: Path) -> str:
        """Build a stat-based fingerprint for a directory's contents."""
        real_path = path.resolve()
        parts: list[str] = []
        for child in sorted(
            real_path.rglob("*"),
            key=lambda p: str(p.relative_to(real_path)),
        ):
            rel = child.relative_to(real_path).as_posix()
            if child.is_dir():
                parts.append(f"D:{rel}")
            else:
                st = child.stat()
                parts.append(f"F:{rel}:{st.st_dev}:{st.st_ino}:{st.st_size}:{st.st_mtime_ns}")
        return hash_str("\n".join(parts))


def _stat_key_for(resolved_path: Path) -> _StatKey:
    """Build a stat key from a resolved (non-symlink) path."""
    st = resolved_path.stat()
    return _StatKey(
        device=st.st_dev,
        inode=st.st_ino,
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
    )
