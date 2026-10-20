"""Compact typed output index for task results.

The provenance manifest and lifecycle events both carry a small JSON
summary of each task's outputs (file paths and the artifacts storing them,
dataframe shapes, ndarray dtypes, etc.). This module owns that rendering.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, get_origin

from ginkgo.core.types import (
    annotation_includes,
    file,
    folder,
    is_path_shaped_annotation,
    pair_elements_with_annotations,
    unwrap_optional_annotation,
)
from ginkgo.runtime.artifacts.value_codec import SUMMARY_ITEM_LIMIT, summarise_value


def output_summary(
    annotation: Any,
    value: Any,
    *,
    name: str = "return",
    artifact_ids: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Return a compact typed output index for a task result.

    Parameters
    ----------
    annotation : Any
        The task's effective return annotation.
    value : Any
        The task result.
    name : str
        Label for the top-level output; nested entries extend it with their
        index.
    artifact_ids : Mapping[str, str] | None
        The cache entry's output path to artifact id mapping, keyed as
        ``CacheStore.save`` keys it. A ``file`` or ``folder`` entry whose path
        is in it carries its ``artifact_id``.

    Returns
    -------
    list[dict[str, Any]]
        One entry per output leaf, except that a list keeps only its first
        value leaves and counts the rest in one ``truncated`` entry; every
        file and folder leaf is kept.
    """
    artifact_ids = artifact_ids or {}
    annotation, admits_none = unwrap_optional_annotation(annotation)
    if value is None:
        # A task with no output indexes nothing, but a declared optional output
        # that came back absent is a result in its own right and must stay
        # visible in the manifest.
        if admits_none and is_path_shaped_annotation(annotation):
            output_type = (
                "folder" if annotation_includes(annotation=annotation, expected=folder) else "file"
            )
            return [{"name": name, "type": output_type, "optional": True, "present": False}]
        return []

    entries = _present_output_summary(annotation, value, name=name, artifact_ids=artifact_ids)
    if admits_none:
        for entry in entries:
            entry["optional"] = True
            entry["present"] = True
    return entries


def _present_output_summary(
    annotation: Any,
    value: Any,
    *,
    name: str,
    artifact_ids: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Return the output index for a value that is known to be present."""
    origin = get_origin(annotation)
    if isinstance(value, (list, tuple)):
        if origin in {list, tuple}:
            paired = pair_elements_with_annotations(annotation=annotation, value=value)
        else:
            paired = ((annotation, item) for item in value)
        outputs: list[dict[str, Any]] = []
        for index, (item_annotation, item) in enumerate(paired):
            outputs.extend(
                output_summary(
                    item_annotation, item, name=f"{name}[{index}]", artifact_ids=artifact_ids
                )
            )
        return _cap_value_entries(outputs, name=name)

    if annotation is file or isinstance(value, file):
        return [_path_entry(name=name, kind="file", value=value, artifact_ids=artifact_ids)]

    if annotation is folder or isinstance(value, folder):
        return [_path_entry(name=name, kind="folder", value=value, artifact_ids=artifact_ids)]

    if isinstance(value, Path):
        return [{"name": name, "type": "path", "path": str(value)}]

    shape = getattr(value, "shape", None)
    dtype = getattr(value, "dtype", None)
    if shape is not None and dtype is not None:
        return [
            {
                "name": name,
                "type": "ndarray",
                "shape": list(shape),
                "dtype": str(dtype),
                "summary": summarise_value(value),
            }
        ]

    if value.__class__.__module__.startswith("pandas") and value.__class__.__name__ == "DataFrame":
        return [
            {
                "name": name,
                "type": "dataframe",
                "shape": [int(value.shape[0]), int(value.shape[1])],
                "summary": summarise_value(value),
            }
        ]

    return [{"name": name, "type": "value", "summary": summarise_value(value)}]


def _cap_value_entries(entries: list[dict[str, Any]], *, name: str) -> list[dict[str, Any]]:
    """Keep every file and folder entry but only the first value entries.

    Each entry becomes a ledger row, so a task returning a long list of plain
    values would otherwise write one row per element. The ones left out are
    counted in a single ``truncated`` entry, which absorbs any a nested list
    already made.
    """
    kept: list[dict[str, Any]] = []
    values = 0
    omitted = 0
    for entry in entries:
        if entry["type"] in {"file", "folder"}:
            kept.append(entry)
        elif entry["type"] == "truncated":
            omitted += entry["omitted"]
        elif values < SUMMARY_ITEM_LIMIT:
            kept.append(entry)
            values += 1
        else:
            omitted += 1
    if omitted:
        kept.append({"name": name, "type": "truncated", "omitted": omitted})
    return kept


def _path_entry(
    *, name: str, kind: str, value: Any, artifact_ids: Mapping[str, str]
) -> dict[str, Any]:
    """Return a ``file``/``folder`` output entry, naming its artifact when stored."""
    entry: dict[str, Any] = {"name": name, "type": kind, "path": str(value)}
    artifact_id = artifact_ids.get(str(Path(str(value))))
    if artifact_id is not None:
        entry["artifact_id"] = artifact_id
    return entry
