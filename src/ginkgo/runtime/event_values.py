"""Rendering user values into a form an event can carry.

A task's arguments and a run's parameters are arbitrary Python. Before either
reaches the bus they pass through here, which does three things: it redacts
secrets, it reduces anything with no JSON form to a description of itself, and
it keeps only the first items of a long container, since every event is stored.

This happens at emit time rather than at the projection site because the ledger
is not the only subscriber. ``--agent-output`` renders the same events straight
to stdout, so a ``SecretRef`` that was only redacted on its way into SQLite
would already have been printed.
"""

from __future__ import annotations

from itertools import islice
from pathlib import Path
from typing import Any

from ginkgo.core.subworkflow import SubWorkflowResult
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.core.secret import SecretRef
from ginkgo.runtime.artifacts.value_codec import SUMMARY_ITEM_LIMIT, summarise_value
from ginkgo.runtime.environment.secrets import redact_value

__all__ = ["render_value"]


def render_value(value: Any, *, item_limit: int | None = SUMMARY_ITEM_LIMIT) -> Any:
    """Return *value* in a form an event can carry: JSON-safe, redacted and bounded.

    Parameters
    ----------
    value : Any
        Any resolved argument, parameter, or task result.
    item_limit : int | None
        How many items of a list, tuple or dict to keep, at every depth.
        ``None`` keeps them all, for values a person typed, such as a run's
        parameters.

    Returns
    -------
    Any
        Secrets replaced by their redaction marker, path-like values by their
        string, and anything else without a JSON form by
        :func:`~ginkgo.runtime.artifacts.value_codec.summarise_value`'s
        description of it. A container longer than ``item_limit`` becomes
        ``{"type", "items", "omitted", "length"}`` holding its first items, so
        a 200,000-element input costs each event and ledger row a few hundred
        bytes. This is what ``runs show --json`` and ``debug`` show as a
        task's inputs and a run's parameters.
    """
    if isinstance(value, SecretRef):
        return redact_value(value)
    if isinstance(value, (file, folder, tmp_dir, Path)):
        return str(value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, SubWorkflowResult):
        return {"type": "subworkflow_result", "run_id": value.run_id, "status": value.status}
    if isinstance(value, (list, tuple)):
        kept = value if item_limit is None else value[:item_limit]
        items = [render_value(item, item_limit=item_limit) for item in kept]
        return _bounded(value, items=items, kept=len(kept))
    if isinstance(value, dict):
        pairs = list(islice(value.items(), item_limit))
        rendered = {
            str(render_value(key, item_limit=item_limit)): render_value(
                item, item_limit=item_limit
            )
            for key, item in pairs
        }
        return _bounded(value, items=rendered, kept=len(pairs))
    return summarise_value(value)


def _bounded(value: Any, *, items: Any, kept: int) -> Any:
    """Return a container's rendered *items*, noting the ones left out."""
    if kept == len(value):
        return items
    kind = "dict" if isinstance(value, dict) else type(value).__name__
    return {"type": kind, "items": items, "omitted": len(value) - kept, "length": len(value)}
