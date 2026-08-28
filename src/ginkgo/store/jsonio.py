"""How the store puts objects into text columns and takes them back out.

Several tables keep a fact as JSON because nothing filters or joins on it —
``tasks.extra``, ``cache_entries.input_hashes``, ``runs.params``. Encoding and
decoding those columns is the same operation everywhere, so it is written once
here rather than in each module that touches one.

Both directions are deliberately forgiving. ``dumps`` falls back to ``str`` for
a value no encoder knows, because a run must not fail over a field nobody
queries; ``loads`` returns text it cannot parse unchanged, because a column
holding a bare string is more useful to a reader than an exception.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = ["dumps", "dumps_or_none", "loads"]


def dumps(value: Any) -> str:
    """Return *value* as JSON text, with keys sorted for stable rows."""
    return json.dumps(value, sort_keys=True, default=str)


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
