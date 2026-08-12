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

from ginkgo.core.asset import AssetResult


@final
@dataclass(frozen=True)
class OptionalOutput:
    """A declared output path that may legitimately be absent.

    Parameters
    ----------
    payload : str | AssetResult
        The wrapped output declaration, exactly as it would have been written
        without the wrapper.
    """

    payload: str | AssetResult


def optional(payload: str | AssetResult) -> OptionalOutput:
    """Declare an output path that may be absent after the task runs.

    A present path is hashed, stored, restored, and validated like any other
    file output. An absent one resolves to ``None`` in the task result, so the
    declaring task must annotate that element ``file | None`` and consumers
    must handle absence explicitly.

    Parameters
    ----------
    payload : str | AssetResult
        The output path, or an asset wrapping one.

    Returns
    -------
    OptionalOutput
        A marker consumed by ``shell()``, ``script()``, and ``notebook()``.

    Raises
    ------
    TypeError
        If the payload is already optional, or is not a path or asset.
    """
    if isinstance(payload, OptionalOutput):
        raise TypeError("optional() must not be nested — wrap the path once")

    if isinstance(payload, Path):
        raise TypeError(
            "optional() takes a path string or an asset, not a Path — "
            f"pass str({payload!r}) instead"
        )

    if not isinstance(payload, (str, AssetResult)):
        received = f"{type(payload).__module__}.{type(payload).__name__}"
        raise TypeError(f"optional() takes a path string or an asset, received a {received}")

    return OptionalOutput(payload=payload)
