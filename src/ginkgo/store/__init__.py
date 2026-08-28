"""The provenance ledger: an event log and the projections built from it.

Ginkgo records what a run did as an append-only sequence of runtime events in
``.ginkgo/ginkgo.db``, alongside projection tables — runs, tasks, cache
entries, assets — derived from those events as they arrive. Bytes stay on
disk; only the index lives in SQL.

This package deals in rows and SQL. It must not import from ``runtime/``,
which is what makes it safe for ``runtime/`` and ``cli/`` to import it.

``open_store(path)`` is the way in. ``ProvenanceStore`` is what it returns,
seen as a contract.
"""

from __future__ import annotations

from ginkgo.store.protocol import ProvenanceStore
from ginkgo.store.sqlite import open_store

__all__ = ["ProvenanceStore", "open_store"]
