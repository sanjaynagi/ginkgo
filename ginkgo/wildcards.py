"""Template expansion and wildcard utilities for workflow authoring."""

from __future__ import annotations

from collections.abc import Iterable
from itertools import product
from string import Formatter
from typing import Any


def _placeholder_names(*, template: str, function_name: str) -> list[str]:
    """Return simple named placeholders in first-appearance order."""
    formatter = Formatter()
    names: list[str] = []
    seen_names: set[str] = set()

    for _, field_name, _, _ in formatter.parse(template):
        if field_name is None:
            continue
        if not field_name.isidentifier():
            raise ValueError(
                f"{function_name}() only supports simple named placeholders like "
                f"'{{sample}}'; got {field_name!r} in template {template!r}."
            )
        if field_name not in seen_names:
            seen_names.add(field_name)
            names.append(field_name)

    return names


def _normalize_wildcards(
    *,
    template: str,
    function_name: str,
    wildcards: dict[str, Iterable[Any]],
) -> tuple[list[str], list[list[Any]]]:
    """Validate wildcard names and normalize iterable values to lists."""
    placeholder_names = _placeholder_names(template=template, function_name=function_name)

    missing_names = [name for name in placeholder_names if name not in wildcards]
    if missing_names:
        raise ValueError(
            f"{function_name}() template {template!r} references wildcard(s) "
            f"{missing_names!r} without matching keyword arguments."
        )

    extra_names = sorted(name for name in wildcards if name not in placeholder_names)
    if extra_names:
        raise ValueError(
            f"{function_name}() received wildcard argument(s) {extra_names!r} that do not appear "
            f"in template {template!r}."
        )

    wildcard_values: list[list[Any]] = []
    for name in placeholder_names:
        values = wildcards[name]
        if isinstance(values, str | bytes):
            raise ValueError(
                f"{function_name}() wildcard {name!r} must be an iterable of values, "
                f"not {type(values).__name__}."
            )
        wildcard_values.append(list(values))

    return placeholder_names, wildcard_values


def expand(template: str, **wildcards: Iterable[Any]) -> list[str]:
    """Expand a string template across wildcard combinations.

    Parameters
    ----------
    template : str
        Template containing named ``str.format`` placeholders.
    **wildcards : collections.abc.Iterable[Any]
        Iterable values for each placeholder in ``template``.

    Returns
    -------
    list[str]
        Expanded strings in deterministic Cartesian-product order.
    """
    placeholder_names, wildcard_values = _normalize_wildcards(
        template=template,
        function_name="expand",
        wildcards=wildcards,
    )
    if not placeholder_names:
        return [template]

    return [
        template.format_map(dict(zip(placeholder_names, combination, strict=True)))
        for combination in product(*wildcard_values)
    ]


def zip_expand(template: str, **wildcards: Iterable[Any]) -> list[str]:
    """Expand a string template by zipping wildcard values positionally.

    Parameters
    ----------
    template : str
        Template containing named ``str.format`` placeholders.
    **wildcards : collections.abc.Iterable[Any]
        Iterable values for each placeholder in ``template``.

    Returns
    -------
    list[str]
        Expanded strings in deterministic positional order.
    """
    placeholder_names, wildcard_values = _normalize_wildcards(
        template=template,
        function_name="zip_expand",
        wildcards=wildcards,
    )
    if not placeholder_names:
        return [template]

    lengths = {len(values) for values in wildcard_values}
    if len(lengths) > 1:
        raise ValueError(
            f"zip_expand() wildcard iterables must have equal lengths; got lengths "
            f"{[len(values) for values in wildcard_values]!r} for template {template!r}."
        )

    return [
        template.format_map(dict(zip(placeholder_names, combination, strict=True)))
        for combination in zip(*wildcard_values, strict=True)
    ]


def slug(value: str) -> str:
    """Return a deterministic file-safe slug.

    Parameters
    ----------
    value : str
        Input text to normalize.

    Returns
    -------
    str
        Lowercased slug with non-alphanumeric runs collapsed to underscores.
    """
    characters: list[str] = []
    previous_was_separator = False

    for character in value.lower():
        if character.isalnum():
            characters.append(character)
            previous_was_separator = False
            continue

        if not previous_was_separator:
            characters.append("_")
            previous_was_separator = True

    return "".join(characters).strip("_")


def flatten(items: list[Any] | tuple[Any, ...]) -> list[Any]:
    """Flatten nested lists and tuples into a single list.

    Parameters
    ----------
    items : list[Any] | tuple[Any, ...]
        Nested list or tuple structure.

    Returns
    -------
    list[Any]
        Flat list preserving left-to-right order.
    """
    flattened: list[Any] = []

    # Flatten only explicit sequence containers to keep helper behavior predictable.
    for item in items:
        if isinstance(item, list | tuple):
            flattened.extend(flatten(item))
            continue
        flattened.append(item)

    return flattened
