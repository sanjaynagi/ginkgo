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
