"""Ginkgo type markers for task argument and return type annotations.

These are marker classes used as type annotations on task parameters and return
values. They drive argument validation and cache-key contribution; see each
class for details.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, get_args, get_origin


class file(str):
    """A path to a single file.

    Validated to exist on disk before task execution.  Return values declared
    as ``file`` are validated to exist after execution.  Cache key contribution
    is the BLAKE3 digest of file contents.
    """


class folder(str):
    """A path to a directory.

    Validated to exist and be a directory before execution.  Cache key
    contribution is the BLAKE3 digest of sorted recursive contents.
    """


class tmp_dir(str):
    """A ginkgo-managed scratch directory, unique per task execution.

    Created automatically before task execution and deleted on success.
    Kept on failure for debugging.  Does not participate in the cache key.
    """


def annotation_includes(*, annotation: Any, expected: Any) -> bool:
    """Return whether an annotation directly or indirectly allows ``expected``.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly a union or other generic alias.
    expected : Any
        The marker type to look for (typically ``file`` or ``folder``).

    Returns
    -------
    bool
        ``True`` when ``expected`` is the annotation itself or appears among
        the arguments of a generic alias, recursively.
    """
    if annotation is expected:
        return True
    origin = get_origin(annotation)
    if origin is None:
        return False
    return any(
        annotation_includes(annotation=item, expected=expected) for item in get_args(annotation)
    )


def is_path_shaped_annotation(annotation: Any) -> bool:
    """Return whether an annotation binds its value to a filesystem path.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly a union.

    Returns
    -------
    bool
        ``True`` when the annotation is or includes ``file`` or ``folder``.
    """
    return annotation_includes(annotation=annotation, expected=file) or annotation_includes(
        annotation=annotation, expected=folder
    )


def is_path_like(value: Any) -> bool:
    """Return whether a value can be interpreted as a filesystem path.

    Parameters
    ----------
    value : Any
        The candidate value.

    Returns
    -------
    bool
        ``True`` for ``str``, ``Path``, and any other ``os.PathLike``.
    """
    return isinstance(value, (str, Path, os.PathLike))


def require_path_value(*, value: Any, annotation_label: str, label: str) -> None:
    """Reject a value bound to ``file`` / ``folder`` that is not a path.

    This is the single home for the rule, shared by input/return validation
    and by cache-key hashing — both of which would otherwise stringify the
    value and report a path-syntax complaint about an object that was never
    meant to be a path.

    Parameters
    ----------
    value : Any
        The resolved argument or return value.
    annotation_label : str
        ``"file"`` or ``"folder"`` — the annotation the value is bound to.
    label : str
        Diagnostic label for the parameter or return value, e.g.
        ``"summarise.return"``.

    Raises
    ------
    TypeError
        When the value is an asset reference of another kind, or any other
        non-path value.
    """
    # Imported here because ``ginkgo.core.asset`` imports this module.
    from ginkgo.core.asset import AssetRef

    if isinstance(value, AssetRef):
        raise TypeError(
            f"{label} is annotated `{annotation_label}` but is a `{value.kind}` asset "
            f"({value.key}). Annotate it `object` (or the payload type) to receive the "
            f"asset payload, or return `asset(path)` to produce a `{annotation_label}` asset."
        )
    if not is_path_like(value):
        received = f"{type(value).__module__}.{type(value).__name__}"
        raise TypeError(
            f"{label} is annotated `{annotation_label}` but received a {received}. "
            f"Annotate it `object` (or the payload type), or use a path."
        )
