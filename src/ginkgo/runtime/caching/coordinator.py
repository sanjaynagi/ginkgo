"""Cache lookups for the evaluator: content-addressed and stat-index paths."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ginkgo.core.task import TaskDef
from ginkgo.runtime.caching.cache import MISSING, CacheStore
from ginkgo.runtime.caching.digest_registry import DigestRegistry
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.task_validation import TaskValidator

if TYPE_CHECKING:
    from ginkgo.runtime.evaluator import NodeRun


@dataclass(frozen=True, kw_only=True)
class CacheHit:
    """A validated cached result for one node."""

    value: Any
    cache_key: str


@dataclass(kw_only=True)
class CacheCoordinator:
    """Cache lookups and cache-side bookkeeping for the evaluator.

    Owns the two lookup paths — content-addressed keys and the stat-index
    fast path for ``--trust-mtimes`` runs — plus the bookkeeping both
    share with task completion: recording stat-index entries and
    propagating output digests. Lookups return a :class:`CacheHit` or
    ``None``; marking the node complete (events, provenance, notebook
    replay) stays with the evaluator.
    """

    cache_store: CacheStore
    validator: TaskValidator
    digests: DigestRegistry
    index: CacheIndex

    def content_lookup(self, *, node: NodeRun) -> CacheHit | None:
        """Return a valid content-addressed cached result for a prepared node.

        Computes and stores the node's cache key and input hashes on first
        use, folding in digests already known for upstream outputs.
        """
        assert node.resolved_args is not None
        if node.cache_key is None or node.input_hashes is None:
            cache_key, input_hashes = self.cache_store.build_cache_key(
                task_def=node.task_def,
                resolved_args=node.resolved_args,
                extra_source_hash=node.extra_source_hash,
                known_digests=self.digests.known,
            )
            node.cache_key = cache_key
            node.input_hashes = input_hashes

        cached_result = self.cache_store.load(cache_key=node.cache_key, task_def=node.task_def)
        if cached_result is MISSING or not self._is_valid_cached_result(
            cache_key=node.cache_key,
            task_def=node.task_def,
            value=cached_result,
        ):
            return None
        return CacheHit(value=cached_result, cache_key=node.cache_key)

    def lookup_by_stat(self, *, node: NodeRun) -> CacheHit | None:
        """Return a stat-index cached result for ``--trust-mtimes`` mode.

        On a hit the node's cache key is set to the indexed content key and
        its input hashes are cleared: trust-mtimes mode only checks that
        output files exist, not that their content matches the artifact
        store.
        """
        if node.resolved_args is None:
            # Nothing has been resolved yet, so there is no fingerprint to
            # take — the same guard the recording side has always had.
            return None
        stat_key = self.cache_store.stat_fingerprint(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
            extra_source_hash=node.extra_source_hash,
        )
        content_key = self.index.stat_index_lookup(stat_key)
        if content_key is None:
            return None

        cached_result = self.cache_store.load(cache_key=content_key, task_def=node.task_def)
        if cached_result is MISSING:
            return None

        node.cache_key = content_key
        node.input_hashes = {}
        return CacheHit(value=cached_result, cache_key=content_key)

    def record_stat_index_entry(self, *, node: NodeRun, cache_key: str) -> None:
        """Record a stat-index entry for a completed task."""
        if node.resolved_args is None:
            return
        stat_key = self.cache_store.stat_fingerprint(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
            extra_source_hash=node.extra_source_hash,
        )
        self.index.record_stat_index(stat_key=stat_key, cache_key=cache_key)

    def propagate_known_digests(self, *, cache_key: str) -> None:
        """Populate the digest registry from a cache entry's artifact IDs.

        Called on cache hits so that downstream tasks can skip re-hashing
        file outputs that this task produced.
        """
        artifact_ids = self.cache_store.load_artifact_ids(cache_key=cache_key)
        if artifact_ids is None:
            return
        self.digests.record_artifacts(artifact_ids)

    def _is_valid_cached_result(self, *, cache_key: str, task_def: TaskDef, value: Any) -> bool:
        """Return whether a cached value still satisfies return validation.

        For file/folder outputs, the cache store ensures the working tree has a
        matching writable materialization before standard return validation
        checks run.
        """
        if not self.cache_store.validate_cached_outputs(
            cache_key=cache_key,
            task_def=task_def,
            value=value,
        ):
            return False

        try:
            self.validator.validate_return_value(task_def=task_def, value=value)
        except (FileNotFoundError, ValueError):
            return False

        return True
