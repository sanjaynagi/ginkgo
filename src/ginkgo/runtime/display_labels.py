"""Compact display labels for mapped tasks in run tables and events."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ginkgo.core.task import TaskDef
from ginkgo.core.types import tmp_dir


def first_label_param_name(*, task_def: TaskDef) -> str | None:
    """Return the first user-declared parameter name for CLI labeling."""
    for name, parameter in task_def.signature.parameters.items():
        annotation = task_def.type_hints.get(name, parameter.annotation)
        if annotation is tmp_dir:
            continue
        return name
    return None


def render_label_value(value: Any) -> str | None:
    """Return a compact string for a mapped-task display label."""
    if isinstance(value, Path):
        text = value.name or str(value)
    elif isinstance(value, (str, int, float, bool)):
        text = str(value)
    else:
        text = repr(value)

    compact = " ".join(text.split()).strip()
    if not compact:
        return None
    if len(compact) > 24:
        compact = f"{compact[:21]}..."
    return compact
