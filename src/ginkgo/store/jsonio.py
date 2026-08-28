"""How the store puts objects into text columns and takes them back out.

Several tables keep a fact as JSON because nothing filters or joins on it —
``tasks.extra``, ``cache_entries.input_hashes``, ``runs.params``. Encoding and
decoding those columns is the same operation everywhere, so it is written once
here rather than in each module that touches one.

Both directions are deliberately forgiving. ``dumps`` falls back to ``str`` for
a value no encoder knows, because a run must not fail over a field nobody
queries; ``loads`` returns text it cannot parse unchanged, because a column
holding a bare string is more useful to a reader than an exception.

Key order is the order the object had. Much of what lands in these columns was
written by a user — a task's parameters, an asset's metadata, a model's metrics
— and the order they wrote it in is how ``inspect run`` and the report card
show it back. Sorting the keys made the rows byte-stable, which nothing asked
for, at the cost of rendering ``accuracy, precision, recall`` alphabetically.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = ["dumps", "dumps_or_none", "loads"]


def dumps(value: Any) -> str:
    """Return *value* as JSON text, keys in the order the object had."""
    return json.dumps(value, default=str)


def dumps_or_none(value: Any) -> str | None:
    """Return *value* as JSON text, or ``None`` when there is nothing to store."""
    return None if value is None else dumps(value)


def loads(value: Any) -> Any:
    """Return the object stored in a JSON column.

    A value that is not text is returned as it is, and text that is not JSON is
    returned as text: this reads columns, and a column can hold anything.
    """
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value
