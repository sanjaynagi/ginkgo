"""Optional output declarations for driver tasks.

``optional()`` marks one declared output of a ``shell()``, ``script()``, or
``notebook()`` task as permitted to be absent after execution. It wraps a
declaration, not a value, which is why it lives here rather than beside the
``file`` / ``folder`` value markers in :mod:`ginkgo.core.types`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import final


@final
@dataclass(frozen=True)
class OptionalOutput:
    """A declared output path that may legitimately be absent.

    Parameters
    ----------
    payload : str
        The wrapped output path, exactly as it would have been written
        without the wrapper.
    """

    payload: str


def optional(payload: str) -> OptionalOutput:
    """Declare an output path that may be absent after the task runs.

    A present path is hashed, stored, restored, and validated like any other
    file output. An absent one resolves to ``None`` in the task result, so the
    declaring task must annotate that element ``file | None`` and consumers
    must handle absence explicitly.

    Parameters
    ----------
    payload : str
        The output path.

    Returns
    -------
    OptionalOutput
        A marker consumed by ``shell()``, ``script()``, and ``notebook()``.

    Raises
    ------
    TypeError
        If the payload is already optional, or is not a path string.
    """
    if isinstance(payload, OptionalOutput):
        raise TypeError("optional() must not be nested — wrap the path once")

    if isinstance(payload, Path):
        raise TypeError(
            f"optional() takes a path string, not a Path — pass str({payload!r}) instead"
        )

    if not isinstance(payload, str):
        received = f"{type(payload).__module__}.{type(payload).__name__}"
        raise TypeError(f"optional() takes a path string, received a {received}")

    return OptionalOutput(payload=payload)
