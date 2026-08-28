"""Memoization for content hashing.

File contents rarely change between runs, let alone within one. ``HashMemo``
caches BLAKE3 digests keyed by filesystem stat metadata so that hashing the
same file twice — a large BAM consumed by many downstream tasks, or the same
input folder on tomorrow's run — reads the bytes only once.

The memo is two tiers: a dict for the current process, and the ``digest_memo``
table behind it so a second run starts warm (issue #245). Both are keyed by the
same stat fingerprint, so a file whose size or mtime moved misses in both and
is re-hashed.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

from ginkgo.core.hashing import hash_directory, hash_file, hash_str
from ginkgo.runtime.caching.index import CacheIndex


@dataclass(frozen=True)
class _StatKey:
    """Filesystem identity for a single file."""

    device: int
    inode: int
    size: int
    mtime_ns: int


class HashMemo:
    """Content-hash cache keyed by file stat metadata.

    Parameters
    ----------
    index : CacheIndex | None
        Where digests are remembered between runs. Without one the memo is
        in-process only, which is what a library caller or a test that never
        opens a workspace wants.

    Attributes
    ----------
    reads : int
        How many times the memo has had to read content. Counted so a test
        can assert that a warm run reads nothing.

    Notes
    -----
    Thread-safe: all reads and writes are guarded by a lock so the memo can be
    shared across the evaluator's thread pools.
    """

    def __init__(self, *, index: CacheIndex | None = None) -> None:
        self._file_cache: dict[_StatKey, str] = {}
        self._dir_cache: dict[str, str] = {}
        self._index = index
        self._lock = threading.Lock()
        self.reads = 0

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

        fingerprint = _fingerprint(key)
        remembered = self._recall("file", fingerprint)
        if remembered is not None:
            with self._lock:
                self._file_cache[key] = remembered
            return remembered

        self.reads += 1
        digest = hash_file(path)
        with self._lock:
            self._file_cache[key] = digest
        self._remember(
            "file",
            fingerprint,
            digest,
            path=resolved,
            size=key.size,
            mtime_ns=key.mtime_ns,
        )
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

        remembered = self._recall("directory", fingerprint)
        if remembered is not None:
            with self._lock:
                self._dir_cache[fingerprint] = remembered
            return remembered

        self.reads += 1
        digest = hash_directory(path)
        with self._lock:
            self._dir_cache[fingerprint] = digest
        self._remember("directory", fingerprint, digest, path=path.resolve())
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

    def _recall(self, kind: str, fingerprint: str) -> str | None:
        """Return a digest remembered by an earlier run, if there is one."""
        if self._index is None:
            return None
        return self._index.digest(kind=kind, fingerprint=fingerprint)

    def _remember(
        self,
        kind: str,
        fingerprint: str,
        digest: str,
        *,
        path: Path,
        size: int | None = None,
        mtime_ns: int | None = None,
    ) -> None:
        """Persist a digest so the next run does not have to read the bytes."""
        if self._index is None:
            return
        self._index.record_digest(
            kind=kind,
            fingerprint=fingerprint,
            digest=digest,
            path=path,
            size=size,
            mtime_ns=mtime_ns,
        )

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


def _fingerprint(key: _StatKey) -> str:
    """Return the persisted form of a file's stat identity."""
    return f"{key.device}:{key.inode}:{key.size}:{key.mtime_ns}"


def _stat_key_for(resolved_path: Path) -> _StatKey:
    """Build a stat key from a resolved (non-symlink) path."""
    st = resolved_path.stat()
    return _StatKey(
        device=st.st_dev,
        inode=st.st_ino,
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
    )
