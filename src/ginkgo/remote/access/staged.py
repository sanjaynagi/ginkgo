"""Staged remote-input access: download to local disk before task runs.

This is the historical behaviour. The strategy delegates to the existing
:class:`~ginkgo.remote.staging.StagingCache` and records byte counters
from the materialised files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef
from ginkgo.remote.access.protocol import AccessStats, PerInputStats
from ginkgo.remote.staging import StagingCache


class StagedAccess:
    """Stage remote inputs via :class:`StagingCache`.

    Parameters
    ----------
    cache : StagingCache | None
        Cache instance to delegate to. A default-rooted cache is created
        when ``None``.
    policy : str
        Label used in :attr:`AccessStats.policy` (``"stage"`` or
        ``"stage (fallback)"``).
    """

    def __init__(
        self,
        *,
        cache: StagingCache | None = None,
        policy: str = "stage",
    ) -> None:
        self._cache = cache if cache is not None else StagingCache()
        self._stats = AccessStats(policy=policy)

    def materialize_file(self, *, ref: RemoteFileRef) -> Path:
        """Stage a remote file and return the cached path."""
        path = self._cache.stage_file(ref=ref)
        self._record(uri=ref.uri, path=path)
        return path

    def materialize_folder(self, *, ref: RemoteFolderRef) -> Path:
        """Stage a remote folder prefix and return the local directory."""
        path = self._cache.stage_folder(ref=ref)
        total = _tree_size(path)
        self._record(uri=ref.uri, size=total)
        return path

    def release(self, *, paths: Iterable[Path]) -> None:  # noqa: ARG002
        """No-op: staged blobs outlive the task by design."""
        return

    def stats(self) -> AccessStats:
        """Return recorded staging statistics."""
        return self._stats

    def _record(self, *, uri: str, path: Path | None = None, size: int | None = None) -> None:
        """Record a materialisation event against the per-input stats table."""
        if size is None and path is not None:
            try:
                size = path.stat().st_size if path.is_file() else _tree_size(path)
            except OSError:
                size = 0
        entry = self._stats.per_input.setdefault(uri, PerInputStats(uri=uri))
        entry.bytes_read += int(size or 0)


def _tree_size(path: Path) -> int:
    """Return total file-byte size under ``path`` (0 for missing)."""
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            try:
                total += child.stat().st_size
            except OSError:
                continue
    return total
