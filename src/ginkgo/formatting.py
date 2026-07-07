"""Canonical value formatters shared by every read-only presenter.

The CLI run-finish output and the static HTML report both turn the same
provenance values — durations, byte sizes, timestamps, counts — into
strings. Keeping those transforms here means a formatting change happens in
one place and cannot silently drift between the two surfaces.

Presentation *vocabulary* (Rich styles and icons for the terminal, tone
tokens for HTML) legitimately differs between presenters and stays with each
one; only the value-to-string formatting is shared.
"""

from __future__ import annotations

from datetime import UTC, datetime


def format_duration(seconds: float | None) -> str:
    """Return a compact duration label.

    Parameters
    ----------
    seconds : float | None
        Duration in seconds, or ``None`` for unknown.

    Returns
    -------
    str
        ``"4.2s"``, ``"2m 14s"``, ``"8h 03m 12s"``, or ``"—"`` when
        ``seconds`` is ``None``.
    """
    if seconds is None:
        return "—"
    value = max(0.0, float(seconds))
    if value < 60:
        if value < 10:
            return f"{value:.1f}s"
        return f"{int(round(value))}s"
    if value < 3600:
        minutes = int(value // 60)
        remaining = int(round(value - minutes * 60))
        return f"{minutes}m {remaining:02d}s"
    hours = int(value // 3600)
    minutes = int((value - hours * 3600) // 60)
    remaining = int(round(value - hours * 3600 - minutes * 60))
    return f"{hours}h {minutes:02d}m {remaining:02d}s"


def format_bytes(size_bytes: int | None) -> str:
    """Return a compact byte-size label.

    Parameters
    ----------
    size_bytes : int | None
        Size in bytes, or ``None`` when unknown.

    Returns
    -------
    str
        Human-readable size (``"14.2 MB"``, ``"62 KB"``, ``"1.2 GB"``) or
        ``"—"`` when the size is unknown.
    """
    if size_bytes is None:
        return "—"
    value = float(max(0, int(size_bytes)))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size_bytes} B"


def format_timestamp(value: datetime | None) -> str:
    """Return an ISO-like timestamp string in UTC.

    Parameters
    ----------
    value : datetime | None
        Timestamp to format, or ``None`` for unknown.

    Returns
    -------
    str
        ``"2026-04-18 14:32:07 UTC"`` or ``"—"``.
    """
    if value is None:
        return "—"
    aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return aware.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_int(value: int | None) -> str:
    """Return an integer with thousands separators, or ``"—"``."""
    if value is None:
        return "—"
    return f"{int(value):,}"
