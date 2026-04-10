"""Code bundle creation, publishing, and extraction for remote workers.

In *code-sync* mode, the workflow's project package is tarred up, hashed,
uploaded to the remote artifact backend, and downloaded by the worker
before importing the task callable.
"""

from __future__ import annotations

import hashlib
import logging
import tarfile
import tempfile
from pathlib import Path

from ginkgo.remote.backend import RemoteStorageBackend

logger = logging.getLogger(__name__)

# Directories and patterns excluded from the code bundle by default.
_DEFAULT_EXCLUDES: frozenset[str] = frozenset(
    {
        "__pycache__",
        ".git",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "node_modules",
        ".tox",
        ".nox",
        ".eggs",
        "*.egg-info",
    }
)

# File extensions excluded from the bundle.
_EXCLUDED_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".pyc",
        ".pyo",
        ".so",
        ".dylib",
    }
)


def _should_exclude(member: tarfile.TarInfo, *, excludes: frozenset[str]) -> bool:
    """Return True if a tar member should be excluded from the bundle."""
    parts = Path(member.name).parts
    for part in parts:
        if part in excludes:
            return True
        # Glob-style matching for patterns like "*.egg-info".
        for pattern in excludes:
            if "*" in pattern and Path(part).match(pattern):
                return True
    if any(member.name.endswith(ext) for ext in _EXCLUDED_EXTENSIONS):
        return True
    return False


def create_code_bundle(
    *,
    package_path: Path,
    excludes: frozenset[str] | None = None,
) -> tuple[Path, str]:
    """Create a tarball of a Python package directory.

    Parameters
    ----------
    package_path : Path
        Path to the package directory to bundle.
    excludes : frozenset[str] | None
        Directory/file names to exclude. Uses sensible defaults if None.

    Returns
    -------
    tuple[Path, str]
        ``(tarball_path, sha256_hex_digest)`` — the caller is responsible
        for cleaning up the temp file.
    """
    if not package_path.is_dir():
        raise FileNotFoundError(f"Package directory not found: {package_path}")

    if excludes is None:
        excludes = _DEFAULT_EXCLUDES

    tmp = tempfile.NamedTemporaryFile(
        prefix="ginkgo-code-bundle-",
        suffix=".tar.gz",
        delete=False,
    )
    tarball_path = Path(tmp.name)
    tmp.close()

    with tarfile.open(tarball_path, "w:gz") as tar:
        for item in sorted(package_path.rglob("*")):
            arcname = str(item.relative_to(package_path.parent))
            info = tar.gettarinfo(str(item), arcname=arcname)
            if _should_exclude(info, excludes=excludes):
                continue
            if item.is_file():
                with open(item, "rb") as fh:
                    tar.addfile(info, fh)
            elif item.is_dir():
                tar.addfile(info)

    digest = _sha256_file(tarball_path)
    logger.debug("Created code bundle %s (digest=%s)", tarball_path, digest[:12])
    return tarball_path, digest


def publish_code_bundle(
    *,
    backend: RemoteStorageBackend,
    bucket: str,
    prefix: str,
    bundle_path: Path,
    digest: str,
) -> str:
    """Upload a code bundle to remote storage if not already present.

    Parameters
    ----------
    backend : RemoteStorageBackend
        Storage backend for uploads.
    bucket : str
        Target bucket name.
    prefix : str
        Key prefix (should end with ``/``).
    bundle_path : Path
        Local path to the tarball.
    digest : str
        SHA-256 hex digest of the bundle.

    Returns
    -------
    str
        Remote key of the uploaded bundle.
    """
    remote_key = f"{prefix}code-bundles/{digest}.tar.gz"

    # Skip upload if already present.
    try:
        backend.head(bucket=bucket, key=remote_key)
        logger.debug("Code bundle %s already exists remotely", digest[:12])
        return remote_key
    except (FileNotFoundError, OSError):
        pass

    backend.upload(src_path=bundle_path, bucket=bucket, key=remote_key)
    logger.info("Published code bundle %s → %s", digest[:12], remote_key)
    return remote_key


def download_and_extract(
    *,
    backend: RemoteStorageBackend,
    bucket: str,
    key: str,
    dest_dir: Path,
) -> Path:
    """Download a code bundle and extract it into *dest_dir*.

    Parameters
    ----------
    backend : RemoteStorageBackend
        Storage backend for downloads.
    bucket : str
        Source bucket name.
    key : str
        Remote key of the bundle tarball.
    dest_dir : Path
        Directory to extract into.

    Returns
    -------
    Path
        The extraction directory (same as *dest_dir*).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    tarball = dest_dir / "code-bundle.tar.gz"
    backend.download(bucket=bucket, key=key, dest_path=tarball)

    with tarfile.open(tarball, "r:gz") as tar:
        tar.extractall(path=dest_dir)

    tarball.unlink()
    logger.debug("Extracted code bundle to %s", dest_dir)
    return dest_dir


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 16)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
