"""In-memory cache of live wrapped-asset payloads.

When a task returns a wrapped asset sentinel (``table`` / ``array`` /
``fig`` / ``text``), the evaluator serialises the payload to the artifact
store and replaces the sentinel with an :class:`AssetRef`. Downstream
tasks that consume those refs in the same evaluator process would
otherwise have to re-read the bytes from disk on every receive — defeating
the zero-copy in-process handoff Ginkgo relies on.

The :class:`LivePayloadRegistry` caches the producer's original Python
object keyed by ``artifact_id`` so arg-binding can rehydrate
``AssetRef`` values back into the live object without a disk round trip.
The registry enforces a simple capped-LRU eviction policy to bound
memory — a cleverer ref-counted scheme would require plumbing the
scheduler's consumer counts into the registry, which is not worth the
complexity for the current evaluator.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any


@dataclass(kw_only=True)
class LivePayloadRegistry:
    """Capped-LRU cache mapping ``artifact_id`` to the producer's payload.

    Parameters
    ----------
    capacity : int
        Maximum number of live payloads retained. When the registry is
        full, the least recently inserted entry is evicted.
    """

    capacity: int = 64
    _payloads: OrderedDict[str, Any] = field(default_factory=OrderedDict)

    def put(self, *, artifact_id: str, payload: Any) -> None:
        """Register a live payload for the given artifact id.

        Parameters
        ----------
        artifact_id : str
            Identifier of the artifact the payload corresponds to.
        payload : Any
            The producer's original Python object (DataFrame, ndarray, ...).
        """
        if artifact_id in self._payloads:
            # Refresh recency without replacing — stored payloads are
            # content-addressed, so the object is equivalent.
            self._payloads.move_to_end(artifact_id)
            return
        self._payloads[artifact_id] = payload
        while len(self._payloads) > self.capacity:
            self._payloads.popitem(last=False)

    def get(self, *, artifact_id: str) -> Any | None:
        """Return the cached payload or ``None`` if the entry is absent.

        Parameters
        ----------
        artifact_id : str
            Identifier to look up.

        Returns
        -------
        Any | None
            The cached Python object, or ``None`` when no live entry exists.
        """
        payload = self._payloads.get(artifact_id)
        if payload is not None:
            self._payloads.move_to_end(artifact_id)
        return payload

    def release(self, *, artifact_id: str) -> None:
        """Drop an entry from the registry if present.

        Parameters
        ----------
        artifact_id : str
            Identifier to evict.
        """
        self._payloads.pop(artifact_id, None)

    def clear(self) -> None:
        """Drop every cached payload."""
        self._payloads.clear()

    def __contains__(self, artifact_id: str) -> bool:
        return artifact_id in self._payloads

    def __len__(self) -> int:
        return len(self._payloads)
