"""Rendering user values into a form an event can carry.

A task's arguments and a run's parameters are arbitrary Python. Before either
reaches the bus they pass through here, which does two things: it redacts
secrets, and it reduces anything with no JSON form to a description of itself.

This happens at emit time rather than at the projection site because the ledger
is not the only subscriber. ``--agent-output`` renders the same events straight
to stdout, so a ``SecretRef`` that was only redacted on its way into SQLite
would already have been printed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ginkgo.core.subworkflow import SubWorkflowResult
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.artifacts.value_codec import summarise_value
from ginkgo.runtime.environment.secrets import redact_value

__all__ = ["render_value"]


def render_value(value: Any) -> Any:
    """Return *value* in a form an event can carry: JSON-safe and redacted.

    Parameters
    ----------
    value : Any
        Any resolved argument, parameter, or task result.

    Returns
    -------
    Any
        Secrets replaced by their redaction marker, path-like values by their
        string, and anything else without a JSON form by
        :func:`~ginkgo.runtime.artifacts.value_codec.summarise_value`'s
        description of it. This is what ``inspect run`` and ``debug`` show as a
        task's inputs and a run's parameters.
    """
    value = redact_value(value)
    if isinstance(value, (file, folder, tmp_dir, Path)):
        return str(value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, SubWorkflowResult):
        return {"type": "subworkflow_result", "run_id": value.run_id, "status": value.status}
    if isinstance(value, (list, tuple)):
        return [render_value(item) for item in value]
    if isinstance(value, dict):
        return {str(render_value(key)): render_value(item) for key, item in value.items()}
    return summarise_value(value)
