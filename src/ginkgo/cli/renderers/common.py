"""Shared CLI formatting helpers."""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime

from rich.console import Console, ConsoleOptions, RenderResult
from rich.text import Text

from ginkgo.cli.renderers.models import _TaskRow


def _status_style(status: str) -> str:
    """Return the Rich style name for a task status."""
    return {
        "waiting": "yellow",
        "staging": "bold magenta",
        "submitted": "bold blue",
        "running": "bold cyan",
        "cached": "bold green",
        "succeeded": "green",
        "failed": "bold red",
    }.get(status, "white")


def _status_icon(status: str) -> str:
    """Return the icon used for a task status."""
    return {
        "waiting": "•",
        "staging": "↓",
        "submitted": "↑",
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


def _format_cpu_percent(value: float | None) -> str:
    """Return a compact CPU percentage string."""
    if value is None:
        return "--"
    if value >= 100:
        return f"{value:.0f}%"
    return f"{value:.1f}%"


_SEGMENT_ORDER = ("succeeded", "cached", "running", "submitted", "staging", "waiting", "failed")
"""Display order for multi-state bar segments (left-to-right)."""

_BAR_FILL_CHAR = "█"
"""Low-profile fill glyph for grouped task progress bars."""


class _MultiStateBar:
    """A Rich renderable that draws a segmented bar coloured by task state.

    Parameters
    ----------
    counts
        Number of invocations in each status.
    total
        Total number of invocations (used for proportional sizing).
    width
        Character width of the bar (excluding the label).
    """

    def __init__(self, *, counts: Counter[str], total: int, width: int) -> None:
        self._counts = counts
        self._total = max(total, 1)
        self._width = max(width, 1)

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        text = Text()

        # Compute proportional segment widths.
        segments = [(status, self._counts.get(status, 0)) for status in _SEGMENT_ORDER]
        segments = [(status, count) for status, count in segments if count > 0]

        if not segments:
            text.append(" " * self._width)
            yield text
            return

        # Allocate widths: guarantee minimum 1 char per non-zero segment.
        raw = [(status, count / self._total * self._width) for status, count in segments]
        widths = [(status, max(1, round(w))) for status, w in raw]

        # Adjust to ensure total equals self._width.
        total_allocated = sum(w for _, w in widths)
        diff = self._width - total_allocated
        if diff != 0:
            # Adjust the largest segment to absorb the rounding error.
            largest_idx = max(range(len(widths)), key=lambda i: widths[i][1])
            status, w = widths[largest_idx]
            widths[largest_idx] = (status, max(1, w + diff))

        for status, w in widths:
            if status == "waiting":
                text.append(" " * w)
                continue
            text.append(_BAR_FILL_CHAR * w, style=_status_style(status))

        yield text


def _format_bytes(value: int | float | None) -> str:
    """Return a compact binary size string."""
    if value is None:
        return "--"

    size = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    if size >= 10:
        return f"{size:.0f} {units[unit_index]}"
    return f"{size:.1f} {units[unit_index]}"
