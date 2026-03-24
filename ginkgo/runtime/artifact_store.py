"""Content-addressed artifact storage for Ginkgo.

The ``ArtifactStore`` protocol defines the contract for storing and retrieving
binary artifacts.  ``LocalArtifactStore`` is the default implementation that
stores artifacts on the local filesystem under ``.ginkgo/artifacts/``.
"""

from __future__ import annotations

import shutil
import stat
from pathlib import Path
from typing import Protocol, runtime_checkable

from ginkgo.runtime.hashing import hash_bytes, hash_directory, hash_file


@runtime_checkable
class ArtifactStore(Protocol):
    """Protocol for content-addressed artifact storage.

    Artifact IDs use the format ``<sha256>.<ext>`` for files and ``<sha256>``
    for directories.  This identity scheme is consumed directly by downstream
    phases (remote artifact store, asset catalog, versioned assets).
    """

    def store(self, *, src_path: Path) -> str:
        """Copy bytes into the store and return a content-addressed artifact ID.

        Parameters
        ----------
        src_path : Path
            Source file or directory to store.

        Returns
        -------
        str
            Artifact ID in the form ``<sha256>.<ext>`` (file) or ``<sha256>``
            (directory).
        """
        ...

    def retrieve(self, *, artifact_id: str, dest_path: Path) -> None:
        """Materialise an artifact at *dest_path* as a symlink.

        Parameters
        ----------
        artifact_id : str
            The artifact ID returned by :meth:`store`.
        dest_path : Path
            Location where the symlink should be created.
        """
        ...

    def exists(self, *, artifact_id: str) -> bool:
        """Return whether an artifact exists in the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to check.

        Returns
        -------
        bool
        """
        ...

    def delete(self, *, artifact_id: str) -> None:
        """Remove an artifact from the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to remove.
        """
        ...

    def artifact_path(self, *, artifact_id: str) -> Path:
        """Return the absolute filesystem path for an artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        Path
        """
        ...

    def store_bytes(self, *, data: bytes, extension: str) -> str:
        """Store raw bytes and return a content-addressed artifact ID.

        Parameters
        ----------
        data : bytes
            Raw bytes to store.
        extension : str
            File extension (without leading dot).

        Returns
        -------
        str
            Artifact ID.
        """
        ...

    def read_bytes(self, *, artifact_id: str) -> bytes:
        """Read raw bytes for an artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        bytes
        """
        ...


class LocalArtifactStore:
    """Local filesystem artifact store.

    Parameters
    ----------
    root : Path
        Root directory for artifact storage.  Defaults to
        ``.ginkgo/artifacts`` under the current working directory.
    """

    def __init__(self, *, root: Path | None = None) -> None:
        self._root = root if root is not None else Path.cwd() / ".ginkgo" / "artifacts"
        self._root.mkdir(parents=True, exist_ok=True)

    def store(self, *, src_path: Path) -> str:
        """Copy a file or directory into the store.

        Parameters
        ----------
        src_path : Path
            Source file or directory to store.

        Returns
        -------
        str
            Content-addressed artifact ID.
        """
        if src_path.is_dir():
            return self._store_directory(src_path)
        return self._store_file(src_path)

    def retrieve(self, *, artifact_id: str, dest_path: Path) -> None:
        """Create a symlink at *dest_path* pointing to the stored artifact.

        Parameters
        ----------
        artifact_id : str
            Artifact ID returned by :meth:`store`.
        dest_path : Path
            Target symlink location.  Parent directories are created if needed.
        """
        target = self._root / artifact_id
        if not target.exists():
            raise FileNotFoundError(f"Artifact not found in store: {artifact_id}")

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if dest_path.is_symlink() or dest_path.exists():
            if dest_path.is_symlink():
                dest_path.unlink()
            elif dest_path.is_dir():
                shutil.rmtree(dest_path)
            else:
                dest_path.unlink()
        dest_path.symlink_to(target)

    def exists(self, *, artifact_id: str) -> bool:
        """Check whether an artifact exists in the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to check.

        Returns
        -------
        bool
        """
        return (self._root / artifact_id).exists()

    def delete(self, *, artifact_id: str) -> None:
        """Remove an artifact from the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to remove.
        """
        target = self._root / artifact_id
        if not target.exists():
            return
        if target.is_dir():
            _make_writable_recursive(target)
            shutil.rmtree(target)
        else:
            target.chmod(stat.S_IRUSR | stat.S_IWUSR)
            target.unlink()

    def artifact_path(self, *, artifact_id: str) -> Path:
        """Return the absolute path for an artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        Path
        """
        return self._root / artifact_id

    def _store_file(self, src_path: Path) -> str:
        """Store a single file, returning its artifact ID."""
        digest = hash_file(src_path)
        ext = src_path.suffix.lstrip(".")
        artifact_id = f"{digest}.{ext}" if ext else digest
        dest = self._root / artifact_id

        # Content-addressed dedup: skip if already present.
        if dest.exists():
            return artifact_id

        shutil.copy2(src_path, dest)
        dest.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)  # 0o444
        return artifact_id

    def _store_directory(self, src_path: Path) -> str:
        """Store a directory tree, returning its artifact ID."""
        digest = _hash_directory(src_path)
        artifact_id = digest
        dest = self._root / artifact_id

        # Content-addressed dedup: skip if already present.
        if dest.exists():
            return artifact_id

        shutil.copytree(src_path, dest)
        _set_read_only_recursive(dest)
        return artifact_id

    def store_bytes(self, *, data: bytes, extension: str) -> str:
        """Store raw bytes, returning a content-addressed artifact ID.

        Parameters
        ----------
        data : bytes
            Raw bytes to store.
        extension : str
            File extension (without leading dot).

        Returns
        -------
        str
            Artifact ID in the form ``<sha256>.<ext>``.
        """
        digest = hash_bytes(data)
        artifact_id = f"{digest}.{extension}" if extension else digest
        dest = self._root / artifact_id

        if dest.exists():
            return artifact_id

        dest.write_bytes(data)
        dest.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)  # 0o444
        return artifact_id

    def read_bytes(self, *, artifact_id: str) -> bytes:
        """Read raw bytes for an artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        bytes
        """
        target = self._root / artifact_id
        if not target.exists():
            raise FileNotFoundError(f"Artifact not found in store: {artifact_id}")
        return target.read_bytes()


def _hash_directory(path: Path) -> str:
    """Return a BLAKE3 digest over a directory's recursive contents."""
    return hash_directory(path)


def _set_read_only_recursive(path: Path) -> None:
    """Set a directory tree to read-only (files 0o444, dirs 0o555)."""
    for child in path.rglob("*"):
        if child.is_dir():
            child.chmod(
                stat.S_IRUSR
                | stat.S_IXUSR
                | stat.S_IRGRP
                | stat.S_IXGRP
                | stat.S_IROTH
                | stat.S_IXOTH
            )
        else:
            child.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    # Set the root directory itself.
    path.chmod(
        stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    )


def _make_writable_recursive(path: Path) -> None:
    """Restore write permissions on a read-only directory tree before deletion."""
    for child in path.rglob("*"):
        if child.is_dir():
            child.chmod(stat.S_IRWXU)
        else:
            child.chmod(stat.S_IRUSR | stat.S_IWUSR)
    path.chmod(stat.S_IRWXU)
