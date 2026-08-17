"""Template expansion and wildcard utilities for workflow authoring."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import product
from string import Formatter
from typing import Any


class ExpandedTemplate(list[str]):
    """Expanded template strings that remember the template they came from.

    ``expand()`` and ``zip_expand()`` return one string per wildcard
    combination, so the result is a column already aligned row-for-row with
    the values it was built from — never an independent axis to sweep. The
    list behaves exactly like ``list[str]``; remembering the template lets
    ``.product_map()`` reject it by name instead of silently crossing it
    with the axes it was derived from.

    The subclass must never reach ``yaml.safe_dump`` directly — the safe
    dumper represents only exact built-in types and raises on this one.
    Every code path that serialises task arguments already normalises
    sequences into plain lists first, and ``tests/core/test_helpers.py``
    holds that normalisation in place.
    """

    def __init__(
        self,
        values: Iterable[str],
        *,
        template: str,
        function_name: str,
        placeholders: Sequence[str],
    ) -> None:
        super().__init__(values)
        self.template = template
        self.function_name = function_name
        self.placeholders = tuple(placeholders)

    def unresolved_placeholders(self, names: Sequence[str]) -> tuple[str, ...]:
        """Return the placeholders that do not name one of ``names``.

        Resolution is by name, never by position: a template can be reused
        verbatim as a ``per_branch()`` template exactly when every
        placeholder already names an argument of the call. Positional
        correspondence between wildcards and arguments is the assumption
        that produced the mislabelling this type exists to prevent, so it
        is not assumed here either.
        """
        available = set(names)
        return tuple(name for name in self.placeholders if name not in available)


@dataclass(frozen=True)
class PerBranch:
    """A template rendered once per fan-out branch from that branch's values.

    Parameters
    ----------
    template : str
        Template whose placeholders name arguments of the fan-out call.
    """

    template: str

    def placeholder_names(self) -> list[str]:
        """Return the argument names this template reads, in first-use order."""
        return _placeholder_names(template=self.template, function_name="per_branch")

    def render(self, values: dict[str, Any]) -> str:
        """Render the template from one branch's argument values."""
        names = self.placeholder_names()
        return self.template.format_map({name: values[name] for name in names})


def per_branch(template: str) -> PerBranch:
    """Derive one value per fan-out branch from that branch's own arguments.

    Use this for arguments that are a function of the branch — output paths
    above all — rather than an axis to sweep. Placeholders name other
    arguments of the same ``.map()`` / ``.product_map()`` call (or arguments
    fixed on the task call), and are rendered per branch, so the value can
    never drift out of step with the values it describes.

    Parameters
    ----------
    template : str
        Template containing named ``str.format`` placeholders, each naming
        an argument of the fan-out call.

    Returns
    -------
    PerBranch
        Marker consumed by ``.map()`` and ``.product_map()``.

    Examples
    --------
    >>> per_branch("results/{temperature}_{defect_density}.json").template
    'results/{temperature}_{defect_density}.json'
    """
    if not isinstance(template, str):
        raise TypeError(f"per_branch() template must be a string, got {type(template).__name__}.")

    names = _placeholder_names(template=template, function_name="per_branch")
    if not names:
        raise ValueError(
            f"per_branch() template {template!r} has no placeholders, so every branch would "
            "receive the same value. Reference the fan-out arguments it should vary with, "
            "e.g. per_branch('results/{sample}.txt'), or pass a plain value as a fixed "
            "argument on the task call."
        )

    return PerBranch(template=template)


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


def expand(template: str, **wildcards: Iterable[Any]) -> ExpandedTemplate:
    """Expand a string template across wildcard combinations.

    The result is one string per combination, aligned row-for-row with the
    wildcard values, so it pairs with ``.map()``. It is not an axis: passing
    it to ``.product_map()`` is rejected, since that would cross it with the
    very axes it was derived from. For a grid, use
    :func:`per_branch` instead.

    Parameters
    ----------
    template : str
        Template containing named ``str.format`` placeholders.
    **wildcards : collections.abc.Iterable[Any]
        Iterable values for each placeholder in ``template``.

    Returns
    -------
    ExpandedTemplate
        Expanded strings in deterministic Cartesian-product order.
    """
    placeholder_names, wildcard_values = _normalize_wildcards(
        template=template,
        function_name="expand",
        wildcards=wildcards,
    )
    if not placeholder_names:
        return ExpandedTemplate(
            [template], template=template, function_name="expand", placeholders=()
        )

    return ExpandedTemplate(
        (
            template.format_map(dict(zip(placeholder_names, combination, strict=True)))
            for combination in product(*wildcard_values)
        ),
        template=template,
        function_name="expand",
        placeholders=placeholder_names,
    )


def zip_expand(template: str, **wildcards: Iterable[Any]) -> ExpandedTemplate:
    """Expand a string template by zipping wildcard values positionally.

    Like :func:`expand`, the result is a row-aligned column for ``.map()``,
    not an axis for ``.product_map()``.

    Parameters
    ----------
    template : str
        Template containing named ``str.format`` placeholders.
    **wildcards : collections.abc.Iterable[Any]
        Iterable values for each placeholder in ``template``.

    Returns
    -------
    ExpandedTemplate
        Expanded strings in deterministic positional order.
    """
    placeholder_names, wildcard_values = _normalize_wildcards(
        template=template,
        function_name="zip_expand",
        wildcards=wildcards,
    )
    if not placeholder_names:
        return ExpandedTemplate(
            [template], template=template, function_name="zip_expand", placeholders=()
        )

    lengths = {len(values) for values in wildcard_values}
    if len(lengths) > 1:
        raise ValueError(
            f"zip_expand() wildcard iterables must have equal lengths; got lengths "
            f"{[len(values) for values in wildcard_values]!r} for template {template!r}."
        )

    return ExpandedTemplate(
        (
            template.format_map(dict(zip(placeholder_names, combination, strict=True)))
            for combination in zip(*wildcard_values, strict=True)
        ),
        template=template,
        function_name="zip_expand",
        placeholders=placeholder_names,
    )


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
