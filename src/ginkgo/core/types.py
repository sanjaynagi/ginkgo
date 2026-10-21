"""Ginkgo type markers for task argument and return type annotations.

These are marker classes used as type annotations on task parameters and return
values. They drive argument validation and cache-key contribution; see each
class for details.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import UnionType
from typing import Annotated, Any, TypeVar, Union, get_args, get_origin


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


class untracked(str):
    """A path parameter deliberately excluded from content tracking.

    Since #307 phase 2, a ``str`` (or ``Any``/``list[str]``/...) parameter
    naming an existing *file* is content-hashed by default — the annotation
    alone no longer opts a path out of tracking (see :func:`is_str_path_annotation`
    and the root-input hashing rule in ``CacheStore._hash_value``). ``untracked``
    is the explicit replacement: annotate a parameter (or its container,
    ``list[untracked]``, or its optional, ``untracked | None``) with it to key
    the value by its path string only, whatever exists on disk at that path.
    Never content-hashed, never warned about by ``ginkgo doctor``, and labelled
    ``"untracked"`` by ``ginkgo cache explain``.
    """


class _OutputMarker:
    """Sentinel identifying an ``Out[...]`` parameter annotation.

    Never instantiated by users; its only job is to be a unique object that
    ``Annotated`` metadata can carry and later be checked for by identity.
    """

    def __repr__(self) -> str:
        return "Out"


_OUTPUT = _OutputMarker()

_T = TypeVar("_T")

#: Marks a task parameter as a path this task *writes* rather than reads.
#:
#: ``Out[file]`` / ``Out[folder]`` compose with containers and optionals
#: (``Out[list[file]]``, ``Out[file | None]``), but must be the outermost
#: wrapper — ``list[Out[file]]`` is rejected at task-definition time, as is
#: any inner annotation that is not path-shaped (``Out[str]``, ``Out[int]``).
#: Since ``Out`` is ``Annotated[T, _OUTPUT]``, a type checker still sees
#: ``Out[file]`` as plain ``file``.
Out = Annotated[_T, _OUTPUT]


def is_output_annotation(annotation: Any) -> bool:
    """Return whether ``annotation`` is ``Out[...]`` at its outermost level.

    Parameters
    ----------
    annotation : Any
        A type annotation, as returned by ``get_type_hints(..., include_extras=True)``.

    Returns
    -------
    bool
        ``True`` when ``annotation`` is an ``Annotated`` type carrying the
        ``Out`` marker among its metadata.
    """
    if get_origin(annotation) is not Annotated:
        return False
    return any(item is _OUTPUT for item in get_args(annotation)[1:])


def strip_output_annotation(annotation: Any) -> Any:
    """Return the inner annotation of an ``Out[...]`` wrapper.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly ``Out[...]``.

    Returns
    -------
    Any
        The annotation ``Out`` wraps, or ``annotation`` unchanged when it is
        not an ``Out[...]`` annotation.
    """
    if is_output_annotation(annotation):
        return get_args(annotation)[0]
    return annotation


def contains_output_annotation(annotation: Any) -> bool:
    """Return whether ``Out[...]`` appears anywhere within ``annotation``.

    Used to reject a nested ``Out`` — ``list[Out[file]]`` rather than the
    required ``Out[list[file]]`` — since ``Out`` must be the outermost
    wrapper on a parameter annotation.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly nested inside a container or union.

    Returns
    -------
    bool
        ``True`` when ``annotation`` itself, or anything nested inside it,
        is ``Out[...]``.
    """
    if is_output_annotation(annotation):
        return True
    origin = get_origin(annotation)
    if origin is None:
        return False
    return any(contains_output_annotation(item) for item in get_args(annotation))


def _is_path_object_type(candidate: Any) -> bool:
    """Return whether ``candidate`` is ``pathlib.Path`` or a subclass of it."""
    return isinstance(candidate, type) and issubclass(candidate, Path)


def contains_pathlib_annotation(annotation: Any) -> bool:
    """Return whether ``annotation`` is, or contains, a ``pathlib.Path`` type.

    ``pathlib.Path`` is rejected on a task *parameter* at definition time:
    unlike ``file``/``folder`` it is hashed as a pickled object in the cache
    key today, tracked by neither path nor content.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly nested inside a container or union.

    Returns
    -------
    bool
        ``True`` when ``annotation`` itself, or anything nested inside it,
        is ``pathlib.Path`` (or a subclass).
    """
    if _is_path_object_type(annotation):
        return True
    origin = get_origin(annotation)
    if origin is None:
        return False
    return any(contains_pathlib_annotation(item) for item in get_args(annotation))


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


def is_untracked_annotation(annotation: Any) -> bool:
    """Return whether ``annotation`` is, or contains, the ``untracked`` marker.

    Matches ``untracked`` itself, and composed shapes such as
    ``list[untracked]``, ``tuple[untracked, ...]`` and ``untracked | None`` —
    anywhere :class:`untracked` appears among a generic alias's arguments,
    recursively, the same way :func:`annotation_includes` looks for
    ``file``/``folder``. Also usable by phase 2A's edge inference to skip a
    parameter deliberately opted out of tracking.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly a union or generic alias.

    Returns
    -------
    bool
        ``True`` when ``untracked`` is the annotation or appears within it.
    """
    return annotation_includes(annotation=annotation, expected=untracked)


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


#: Exact types of a value that cannot name a path, a remote object or a secret.
#: Walks over task values settle such a leaf with one type check, so a long list
#: of numbers costs little to hash, label or validate.
PLAIN_SCALAR_TYPES = frozenset({type(None), bool, int, float})


def holds_only_plain_scalars(value: Any) -> bool:
    """Return whether *value* is a list, tuple or dict of plain scalars only.

    Such a container names no path, remote object or secret, so a walk looking
    for one can pass over it whole.

    Parameters
    ----------
    value : Any
        The candidate value.

    Returns
    -------
    bool
        ``True`` when every element (every key and value, for a dict) has one
        of the :data:`PLAIN_SCALAR_TYPES`.
    """
    if isinstance(value, dict):
        return all(
            type(key) in PLAIN_SCALAR_TYPES and type(item) in PLAIN_SCALAR_TYPES
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return all(type(item) in PLAIN_SCALAR_TYPES for item in value)
    return False


#: Exact parameter names that read as a path regardless of any suffix.
_PATH_LIKE_PARAM_NAMES = frozenset({"path", "file", "dir"})

#: Suffixes that mark a parameter name as path-like (``output_path``,
#: ``report_files``, ``scratch_dir``, ``assets_folder``, ...).
_PATH_LIKE_PARAM_SUFFIXES = (
    "_path",
    "_paths",
    "_file",
    "_files",
    "_dir",
    "_dirs",
    "_folder",
    "_folders",
)


def looks_path_like_param_name(name: str) -> bool:
    """Return whether a parameter name reads as naming a filesystem path.

    A purely lexical heuristic, deliberately name-only: it has no access to
    the value a parameter receives, only its declared name. Used to decide
    which ``str``-annotated parameters are worth warning about for issue
    #307 — a ``str`` path is legitimate (e.g. a URL, a label), but one named
    ``output_path`` or ``input_dir`` is very likely meant to be a real
    filesystem path that should be content-tracked instead.

    Parameters
    ----------
    name : str
        The parameter name, as it appears in the task's signature.

    Returns
    -------
    bool
        ``True`` when the name is exactly ``path``/``file``/``dir`` or ends
        with a path-like suffix such as ``_path`` or ``_folders``, matched
        case-insensitively.
    """
    lowered = name.lower()
    return lowered in _PATH_LIKE_PARAM_NAMES or lowered.endswith(_PATH_LIKE_PARAM_SUFFIXES)


def is_str_path_annotation(annotation: Any) -> bool:
    """Return whether an annotation is a bare ``str`` shape carrying no path tracking.

    Matches ``str``, ``str | None``, ``list[str]``, and ``tuple[str, ...]`` —
    the shapes a path-like parameter (see :func:`looks_path_like_param_name`)
    would plausibly be declared with when its author reached for the built-in
    ``str`` instead of :class:`file` / :class:`folder`. Anything else,
    including ``file``/``folder`` themselves (``str`` subclasses) and a fixed
    heterogeneous tuple, is left alone.

    Parameters
    ----------
    annotation : Any
        A type annotation, possibly a union or generic alias.

    Returns
    -------
    bool
        ``True`` when the annotation is exactly one of the ``str`` shapes
        above.
    """
    unwrapped, _ = unwrap_optional_annotation(annotation)
    if unwrapped is str:
        return True
    origin = get_origin(unwrapped)
    args = get_args(unwrapped)
    if origin is list:
        return args == (str,)
    if origin is tuple:
        return args == (str, Ellipsis)
    return False


def looks_like_path_string(text: str) -> bool:
    """Return whether *text* is shaped like a real filesystem path, textually.

    The eligibility rule for #307 phase 2's root-input content hashing: a
    ``str``/``PathLike`` value that names an existing regular file is
    content-hashed only when it also *reads* as a path, not a bare word that
    happens to collide with a file name in the working directory. Two shapes
    qualify:

    - it contains a path separator (``/``, or ``os.sep``/``os.altsep`` on
      the current platform) — ``"data/counts.tsv"``, ``"./x"``, ``"../y"``;
    - or it carries a file extension — ``Path(text).suffix`` is non-empty and
      not the bare ``"."`` a trailing dot leaves.

    A bare word with neither, such as ``"alpha"`` or ``"results"``, does not
    qualify even when a same-named file exists on disk: nothing about the
    string itself suggests a path, so treating it as one would silently
    content-hash values that were never meant to be paths (a label, a slug).
    Such a value stays keyed by its own repr, exactly as any other scalar.

    This is purely textual — no filesystem access. Callers pair it with an
    existence check (see ``is_content_trackable_path_value`` in
    ``ginkgo.runtime.task_validation``).

    Parameters
    ----------
    text : str
        The candidate string.

    Returns
    -------
    bool
        ``True`` when the text contains a path separator or a file
        extension.
    """
    if "/" in text or os.sep in text or (os.altsep is not None and os.altsep in text):
        return True
    suffix = Path(text).suffix
    return suffix not in ("", ".")


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


#: Where the staging-task pattern is written out, and why the obvious
#: improvisation is worse than the refusal. Shared by every message that sends
#: a user to that pattern, so they say the same thing.
STAGING_TASK_GUIDANCE = (
    'A worked example is in the guide under Notebook Tasks, "Staging A Python '
    'Value For A Notebook". Passing the path as a plain `str` is not a safe '
    "substitute: a literal path creates no dependency edge, so the task can run "
    "before the file is written and stays cached after it changes."
)


def path_binding_remedy(*, execution_mode: str | None) -> str:
    """Return the way out of a payload bound to a path, for this kind of consumer.

    A ``driver`` task forwards its arguments to an external command, which
    cannot take a Python payload however the parameter is annotated. Telling
    its author to annotate ``object`` sends them from a clear refusal to a
    DataFrame interpolated into a shell command, so that remedy is offered
    only where it works.

    Parameters
    ----------
    execution_mode : str | None
        The consuming task kind's ``execution_mode``, or ``None`` when
        the consumer is not known.

    Returns
    -------
    str
        One sentence naming the available remedies.
    """
    if execution_mode == "driver":
        return (
            "Write the payload to the format the command expects in a separate "
            f"`python` task and pass that path. {STAGING_TASK_GUIDANCE}"
        )
    return "Annotate it `object` (or the payload type), or use a path."


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
        The consuming task kind's ``execution_mode``, used to pick the
        remedies worth offering.

    Raises
    ------
    TypeError
        When the value is not path-like.
    """
    if not is_path_like(value):
        received = f"{type(value).__module__}.{type(value).__name__}"
        raise TypeError(
            f"{label} is annotated `{annotation_label}` but received a {received}. "
            f"{path_binding_remedy(execution_mode=execution_mode)}"
        )
