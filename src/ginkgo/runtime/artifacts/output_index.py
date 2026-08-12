"""Compact typed output index for task results.

The provenance manifest and lifecycle events both carry a small JSON
summary of each task's outputs (file paths, asset references, dataframe
shapes, ndarray dtypes, etc.). This module owns that rendering.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, get_args, get_origin

from ginkgo.core.asset import AssetRef
from ginkgo.core.types import (
    annotation_includes,
    file,
    folder,
    is_path_shaped_annotation,
    unwrap_optional_annotation,
)
from ginkgo.runtime.artifacts.value_codec import summarise_value


def output_summary(
    annotation: Any,
    value: Any,
    *,
    name: str = "return",
) -> list[dict[str, Any]]:
    """Return a compact typed output index for a task result."""
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

    entries = _present_output_summary(annotation, value, name=name)
    if admits_none:
        for entry in entries:
            entry["optional"] = True
            entry["present"] = True
    return entries


def _present_output_summary(
    annotation: Any,
    value: Any,
    *,
    name: str = "return",
) -> list[dict[str, Any]]:
    """Return the output index for a value that is known to be present."""
    origin = get_origin(annotation)
    if origin in {list, tuple} and isinstance(value, (list, tuple)):
        inner_args = get_args(annotation)
        inner_annotation = inner_args[0] if inner_args else Any
        outputs: list[dict[str, Any]] = []
        for index, item in enumerate(value):
            outputs.extend(output_summary(inner_annotation, item, name=f"{name}[{index}]"))
        return outputs

    if isinstance(value, (list, tuple)):
        outputs: list[dict[str, Any]] = []
        for index, item in enumerate(value):
            outputs.extend(output_summary(annotation, item, name=f"{name}[{index}]"))
        return outputs

    if isinstance(value, AssetRef):
        return [
            {
                "name": name,
                "type": "asset",
                "asset_key": str(value.key),
                "version_id": value.version_id,
                "artifact_id": value.artifact_id,
                "path": value.artifact_path,
            }
        ]

    if annotation is file or isinstance(value, file):
        return [{"name": name, "type": "file", "path": str(value)}]

    if annotation is folder or isinstance(value, folder):
        return [{"name": name, "type": "folder", "path": str(value)}]

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
