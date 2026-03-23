"""Centralised content hashing using BLAKE3.

All content-addressed hashing in Ginkgo (cache keys, artifact IDs, input
hashing, source hashing) flows through the helpers in this module.  Swapping
the underlying algorithm requires changes only here.

BLAKE3 is ~3-5x faster than SHA-256 on a single core and natively parallelises
across cores via tree hashing, which matters for large artifact files.
"""

from __future__ import annotations

from pathlib import Path

import blake3


def hash_bytes(data: bytes) -> str:
    """Return the hex digest of raw bytes.

    Parameters
    ----------
    data : bytes
        Bytes to hash.

    Returns
    -------
    str
        Hex-encoded BLAKE3 digest.
    """
    return blake3.blake3(data).hexdigest()


def hash_str(value: str) -> str:
    """Return the hex digest of a UTF-8 string.

    Parameters
    ----------
    value : str
        String to hash.

    Returns
    -------
    str
        Hex-encoded BLAKE3 digest.
    """
    return hash_bytes(value.encode("utf-8"))


def hash_file(path: Path) -> str:
    """Return the hex digest of a file's contents.

    Parameters
    ----------
    path : Path
        File to hash.  Symlinks are followed.

    Returns
    -------
    str
        Hex-encoded BLAKE3 digest.
    """
    real_path = path.resolve()
    hasher = blake3.blake3()
    with real_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def new_hasher() -> blake3.blake3:
    """Return a fresh incremental BLAKE3 hasher.

    Returns
    -------
    blake3.blake3
        An incremental hasher supporting ``.update()`` and ``.hexdigest()``.
    """
    return blake3.blake3()
