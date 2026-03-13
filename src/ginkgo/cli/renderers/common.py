"""Shared CLI formatting helpers."""

from __future__ import annotations

import re
from datetime import datetime

from rich.console import Console
from rich.text import Text

from ginkgo.cli.renderers.models import _TaskRow


def _status_style(status: str) -> str:
    """Return the Rich style name for a task status."""
    return {
        "waiting": "yellow",
        "running": "bold cyan",
        "cached": "bold green",
        "succeeded": "green",
        "failed": "bold red",
    }.get(status, "white")


def _status_icon(status: str) -> str:
    """Return the icon used for a task status."""
    return {
        "waiting": "•",
        "running": "◐",
        "cached": "↺",
        "succeeded": "✓",
        "failed": "✖",
    }.get(status, "?")


def _status_label(status: str) -> str:
    """Return the plain-text status label shown in the task table."""
    return f"{_status_icon(status)} {status}"


def _status_text(status: str) -> Text:
    """Return the styled status cell for the live task table."""
    text = Text()
    text.append(f"{_status_icon(status)} ", style=_status_style(status))
    text.append(status, style=_status_style(status))
    return text


def _task_duration_text(row: _TaskRow, *, now: float) -> Text:
    """Return the duration cell for a task row."""
    return Text(_task_duration_plain(row, now=now), style="dim")


def _task_duration_plain(row: _TaskRow, *, now: float) -> str:
    """Return the plain-text duration shown in the task table."""
    if row.started_at is None:
        return "--"
    finished_at = row.finished_at if row.finished_at is not None else now
    return _format_duration(max(0.0, finished_at - row.started_at))


def _environment_label(env: str | None) -> str:
    """Return the task environment label for the CLI table."""
    return "local" if env is None else f"pixi:{env}"


def _task_base_name(task_name: str) -> str:
    """Return the task name without its module prefix."""
    return task_name.rsplit(".", 1)[-1]


def _task_label_width(console: Console) -> int:
    """Choose a readable max label width for the task table."""
    return max(18, min(42, console.width - 34))


def _truncate_task_label(label: str, *, max_width: int) -> str:
    """Truncate a task label while preserving its suffix and fan-out index."""
    if len(label) <= max_width:
        return label

    match = re.search(r"(\[\d+\])$", label)
    suffix = match.group(1) if match else ""
    base = label[: -len(suffix)] if suffix else label

    reserved = len(suffix) + 3
    if max_width <= reserved + 4:
        return f"{label[: max(1, max_width - 3)]}..."

    body_width = max_width - reserved
    head_width = max(6, body_width // 2)
    tail_width = max(4, body_width - head_width)
    if head_width + tail_width > body_width:
        head_width = body_width - tail_width

    return f"{base[:head_width]}...{base[-tail_width:]}{suffix}"


def _format_duration(seconds: float) -> str:
    """Return a compact human-readable duration string."""
    if seconds < 1:
        return f"{seconds:.2f}s"
    if seconds < 10:
        return f"{seconds:.1f}s"
    return f"{seconds:.0f}s"


def _time_of_day_spinner(now: datetime | None = None) -> str:
    """Return a day/night spinner name based on the local hour."""
    current = now if now is not None else datetime.now().astimezone()
    return "earth" if 6 <= current.hour < 18 else "moon"


def _core_unit_label(cores: int) -> str:
    """Return the singular/plural label for core counts."""
    return "core" if cores == 1 else "Cores"
