"""Ginkgo type markers for task argument and return type annotations.

These are marker classes used as type annotations on task parameters and return
values. They drive argument validation and cache-key contribution; see each
class for details.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import UnionType
from typing import Any, Union, get_args, get_origin


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


def unwrap_optional_annotation(annotation: Any) -> tuple[Any, bool]:
    """Split an ``X | None`` annotation into its inner type and nullability.

    An optional task output is annotated ``file | None``, so every consumer of
    an annotation must be able to ask "does this admit ``None``, and what is it
    otherwise?" without each re-deriving union handling.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly a union.

    Returns
    -------
    tuple[Any, bool]
        The annotation with ``None`` removed, and whether ``None`` was
        admitted. A union of several non-``None`` members is returned
        unchanged, since no single inner type describes it.
    """
    if get_origin(annotation) not in {Union, UnionType}:
        return annotation, False

    args = get_args(annotation)
    if type(None) not in args:
        return annotation, False

    remaining = tuple(arg for arg in args if arg is not type(None))
    if len(remaining) == 1:
        return remaining[0], True
    return Union[remaining], True


def pair_elements_with_annotations(*, annotation: Any, value: Any) -> list[tuple[Any, Any]]:
    """Pair each element of a container value with the annotation governing it.

    A homogeneous container (``list[file]``, ``tuple[file, ...]``) governs every
    element with the same inner annotation. A heterogeneous tuple
    (``tuple[file, file | None]``) governs each element with its own, and
    applying only the first — as every container walk here used to — hands an
    absent optional the annotation ``file`` and loses the fact that it may be
    ``None``.

    Parameters
    ----------
    annotation : Any
        The container's declared annotation.
    value : Any
        The container value, a list or tuple.

    Returns
    -------
    list[tuple[Any, Any]]
        One ``(annotation, element)`` pair per element. Falls back to the
        container annotation itself when the annotation carries no arguments.
    """
    inner_args = get_args(annotation)

    # A fixed-length tuple annotation lines up positionally with its value.
    # Only a genuinely sized value can be paired that way — anything else
    # (an unresolved expression proxy, say) falls through to the homogeneous
    # walk, which is what it got before per-element pairing existed.
    if (
        get_origin(annotation) is tuple
        and inner_args
        and Ellipsis not in inner_args
        and isinstance(value, (list, tuple))
        and len(inner_args) == len(value)
    ):
        return list(zip(inner_args, value, strict=True))

    inner_annotation = inner_args[0] if inner_args else annotation
    return [(inner_annotation, item) for item in value]


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


def path_binding_remedy(*, annotation_label: str, execution_mode: str | None) -> str:
    """Return the way out of a kind/path mismatch, for this kind of consumer.

    A ``driver`` task forwards its arguments to an external command, which
    cannot take a Python payload however the parameter is annotated. Telling
    its author to annotate ``object`` sends them from a clear refusal to a
    DataFrame interpolated into a shell command, so that remedy is offered
    only where it works.

    Parameters
    ----------
    annotation_label : str
        ``"file"`` or ``"folder"`` — the annotation the value is bound to.
    execution_mode : str | None
        ``TaskDef.execution_mode`` of the consuming task, or ``None`` when
        the consumer is not known.

    Returns
    -------
    str
        One sentence naming the available remedies.
    """
    if execution_mode == "driver":
        return (
            f"Produce a `{annotation_label}` asset upstream with `asset(path)`, or write "
            "the payload to the format the command expects in Python first and pass that "
            "path."
        )
    return (
        "Annotate it `object` (or the payload type) to receive the asset payload in a "
        f"Python task, or produce a `{annotation_label}` asset upstream with `asset(path)`."
    )


def require_path_value(
    *,
    value: Any,
    annotation_label: str,
    label: str,
    execution_mode: str | None = None,
) -> None:
    """Reject a value bound to ``file`` / ``folder`` that is not a path.

    This is the single home for the rule, shared by input/return validation
    and by cache-key hashing — both of which would otherwise stringify the
    value and report a path-syntax complaint about an object that was never
    meant to be a path.

    An ``AssetRef`` passes when its artifact holds the payload's own bytes —
    a ``file`` copied verbatim, a ``fig``'s native PNG/SVG/HTML, a ``text``
    asset's raw UTF-8 — since such a path reads as the file it appears to be.
    A kind whose serialiser writes Ginkgo's own encoding (``table`` as
    Parquet, ``array`` as zarr/npy, ``model`` as a framework dump) is
    refused: binding it to a path hands the task a serialized payload rather
    than the bytes the path's name implies. Which side a kind falls on is
    declared once, on its ``AssetKindSpec``.

    Parameters
    ----------
    value : Any
        The resolved argument or return value.
    annotation_label : str
        ``"file"`` or ``"folder"`` — the annotation the value is bound to.
    label : str
        Diagnostic label for the parameter or return value, e.g.
        ``"summarise.return"``.
    execution_mode : str | None
        ``TaskDef.execution_mode`` of the consuming task, used to pick the
        remedies worth offering.

    Raises
    ------
    TypeError
        When the value is an asset reference whose artifact is not readable
        as the annotated path type, or any other non-path value.
    """
    # Imported here because ``ginkgo.core.asset`` imports this module, and
    # because the kind registry lives with the serialisers whose output it
    # describes.
    from ginkgo.core.asset import AssetRef
    from ginkgo.runtime.artifacts.asset_kinds import artifact_encoding_for

    if isinstance(value, AssetRef):
        encoding = artifact_encoding_for(value.kind)
        remedy = path_binding_remedy(
            annotation_label=annotation_label, execution_mode=execution_mode
        )
        if encoding is None:
            if annotation_label == "file" or value.kind == annotation_label:
                return
            # Native bytes, but a single file where a directory was asked for.
            raise TypeError(
                f"{label} is annotated `{annotation_label}` but is a `{value.kind}` asset "
                f"({value.key}), whose artifact is a file rather than a directory. {remedy}"
            )
        raise TypeError(
            f"{label} is annotated `{annotation_label}` but is a `{value.kind}` asset "
            f"({value.key}). Its artifact holds {encoding}, not readable "
            f"`{annotation_label}` bytes, so the task would read a serialized blob. "
            f"{remedy}"
        )
    if not is_path_like(value):
        received = f"{type(value).__module__}.{type(value).__name__}"
        raise TypeError(
            f"{label} is annotated `{annotation_label}` but received a {received}. "
            f"Annotate it `object` (or the payload type), or use a path."
        )
