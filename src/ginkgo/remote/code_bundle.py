"""Code bundle creation, publishing, and extraction for remote workers.

In *code-sync* mode, the workflow's project package is tarred up, hashed,
uploaded to the remote artifact backend, and downloaded by the worker
before importing the task callable.

Filtering respects three layers (most-specific wins):

1. **Built-in defaults** — ``__pycache__``, ``.git``, compiled extensions, etc.
2. **``.gitignore``** — if present in the package or its parent, loaded via
   ``pathspec`` so the bundle mirrors ``git ls-files``.
3. **User ``exclude`` list** — extra glob patterns from
   ``[remote.k8s.code] exclude``.
"""

from __future__ import annotations

import hashlib
import logging
import tarfile
import tempfile
from pathlib import Path
from typing import Any

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


def _load_gitignore_spec(root: Path) -> Any | None:
    """Load ``.gitignore`` from *root* or its parent as a ``pathspec`` matcher.

    Returns ``None`` if pathspec is unavailable or no ``.gitignore`` exists.
    """
    try:
        import pathspec
    except ImportError:
        return None

    for candidate in (root / ".gitignore", root.parent / ".gitignore"):
        if candidate.is_file():
            lines = candidate.read_text(encoding="utf-8").splitlines()
            spec = pathspec.PathSpec.from_lines("gitwildmatch", lines)
            logger.debug("Loaded .gitignore from %s (%d patterns)", candidate, len(lines))
            return spec
    return None


def _should_exclude(
    member: tarfile.TarInfo,
    *,
    excludes: frozenset[str],
    gitignore_spec: Any | None = None,
    package_parent: Path | None = None,
) -> bool:
    """Return True if a tar member should be excluded from the bundle."""
    parts = Path(member.name).parts
    for part in parts:
        if part in excludes:
            return True
        for pattern in excludes:
            if "*" in pattern and Path(part).match(pattern):
                return True
    if any(member.name.endswith(ext) for ext in _EXCLUDED_EXTENSIONS):
        return True

    # .gitignore matching — pathspec expects paths relative to the repo root.
    if gitignore_spec is not None:
        match_path = member.name
        if package_parent is not None:
            # arcname is relative to package_parent; .gitignore patterns are
            # relative to the repo root which is typically package_parent.
            match_path = member.name
        if gitignore_spec.match_file(match_path):
            return True

    return False


def create_code_bundle(
    *,
    package_path: Path,
    excludes: frozenset[str] | None = None,
    extra_excludes: list[str] | None = None,
) -> tuple[Path, str]:
    """Create a tarball of a Python package directory.

    Parameters
    ----------
    package_path : Path
        Path to the package directory to bundle.
    excludes : frozenset[str] | None
        Directory/file names to exclude. Uses sensible defaults if None.
    extra_excludes : list[str] | None
        Additional glob patterns from user config (e.g. ``[remote.k8s.code]
        exclude``). Merged with *excludes*.

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
    if extra_excludes:
        excludes = excludes | frozenset(extra_excludes)

    gitignore_spec = _load_gitignore_spec(package_path)

    tmp = tempfile.NamedTemporaryFile(
        prefix="ginkgo-code-bundle-",
        suffix=".tar.gz",
        delete=False,
    )
    tarball_path = Path(tmp.name)
    tmp.close()

    included = 0
    excluded_count = 0
    with tarfile.open(tarball_path, "w:gz") as tar:
        for item in sorted(package_path.rglob("*")):
            arcname = str(item.relative_to(package_path.parent))
            info = tar.gettarinfo(str(item), arcname=arcname)
            if _should_exclude(
                info,
                excludes=excludes,
                gitignore_spec=gitignore_spec,
                package_parent=package_path.parent,
            ):
                excluded_count += 1
                continue
            if item.is_file():
                with open(item, "rb") as fh:
                    tar.addfile(info, fh)
                included += 1
            elif item.is_dir():
                tar.addfile(info)

    digest = _sha256_file(tarball_path)
    size_mb = tarball_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Code bundle: %d files included, %d excluded, %.1f MiB (digest=%s)",
        included,
        excluded_count,
        size_mb,
        digest[:12],
    )
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
