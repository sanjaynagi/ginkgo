"""Registry of known content digests for workspace paths."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path


class DigestRegistry:
    """Content digests of workspace paths, keyed by resolved path string.

    Shared between the evaluator's cache bookkeeping, which records output
    digests as tasks complete or hit cache, and remote dispatch, which
    restores persisted digests and consults them when staging arguments.
    Downstream consumers use the digests to skip re-hashing files whose
    content is already known. The mutable mapping is exposed as
    :attr:`known` because ``build_cache_key`` and ``stage_args_for_remote``
    read and update it directly.
    """

    def __init__(self) -> None:
        self.known: dict[str, str] = {}

    def record_artifacts(self, artifact_ids: Mapping[str, str]) -> None:
        """Record ``path -> artifact_id`` digests under resolved path keys."""
        for path_str, artifact_id in artifact_ids.items():
            self.known[str(Path(path_str).resolve())] = artifact_id

    def update(self, digests: Mapping[str, str]) -> None:
        """Merge digests whose keys are already resolved path strings."""
        self.known.update(digests)
