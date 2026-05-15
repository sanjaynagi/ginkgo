"""Formatting helpers and per-kind preview caps for report export.

Every cap defined here surfaces as a visible marker in the rendered report
(``showing N of M rows``, ``N bytes truncated``, ``asset not inlined``).
Silent clipping is an anti-goal — the reader must always see what was
dropped.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


# ----- Caps ----------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class SizingPolicy:
    """Per-kind preview caps applied at export time.

    Parameters
    ----------
    table_rows : int
        Maximum number of rows rendered inline for a ``table`` asset.
    text_bytes : int
        Maximum bytes of text inlined for a ``text`` asset.
    log_lines : int
        Maximum trailing lines retained from a task log.
    array_sample_values : int
        Maximum number of raw values summarised from an array asset.
    embed_full_assets : bool
        When True, the raw artifact bytes are copied into the bundle
        alongside the preview. Previews are unaffected.
    """

    table_rows: int = 50
    text_bytes: int = 4096
    log_lines: int = 80
    array_sample_values: int = 8
    embed_full_assets: bool = False


# ----- Formatting ---------------------------------------------------------


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


# ----- Table preview ------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class TablePreview:
    """Truncated preview of a tabular asset.

    Parameters
    ----------
    columns : tuple[str, ...]
        Column names in source order.
    rows : tuple[dict[str, Any], ...]
        Row dicts, at most ``shown_rows`` long.
    shown_rows : int
        Number of rows actually present in ``rows``.
    total_rows : int | None
        Total row count in the source artifact when known.
    truncated : bool
        True when the preview omits rows beyond ``shown_rows``.
    """

    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    shown_rows: int
    total_rows: int | None
    truncated: bool


def build_table_preview(
    *,
    path: Path,
    extension: str,
    policy: SizingPolicy,
) -> TablePreview | None:
    """Build a row-truncated preview of a dataframe-like artifact.

    Parameters
    ----------
    path : Path
        Artifact path on disk.
    extension : str
        File extension including the leading dot (``".parquet"``).
    policy : SizingPolicy
        Applied cap on number of rows retained.

    Returns
    -------
    TablePreview | None
        ``None`` when the artifact cannot be parsed as a table.
    """
    suffix = extension.lower()
    try:
        if suffix == ".csv":
            head = pd.read_csv(path, nrows=policy.table_rows + 1)
            total = _row_count_csv(path)
        elif suffix == ".tsv":
            head = pd.read_csv(path, sep="\t", nrows=policy.table_rows + 1)
            total = _row_count_csv(path)
        elif suffix == ".parquet":
            full = pd.read_parquet(path)
            total = len(full.index)
            head = full.head(policy.table_rows + 1)
        elif suffix == ".json":
            full = pd.read_json(path)
            total = len(full.index)
            head = full.head(policy.table_rows + 1)
        elif suffix in {".jsonl", ".ndjson"}:
            full = pd.read_json(path, lines=True)
            total = len(full.index)
            head = full.head(policy.table_rows + 1)
        else:
            return None
    except Exception:
        return None

    truncated = len(head.index) > policy.table_rows
    head = head.head(policy.table_rows)
    head = head.where(pd.notnull(head), None)

    return TablePreview(
        columns=tuple(str(column) for column in head.columns),
        rows=tuple(head.to_dict(orient="records")),
        shown_rows=len(head.index),
        total_rows=total if truncated or total is not None else None,
        truncated=truncated,
    )


def _row_count_csv(path: Path) -> int | None:
    """Count rows in a CSV/TSV file without loading it.

    Subtracts one for the header line. Returns ``None`` on read error.
    """
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            count = sum(1 for _ in handle)
    except OSError:
        return None
    return max(0, count - 1)


# ----- Text preview -------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class TextPreview:
    """Byte-truncated preview of a text asset."""

    text: str
    shown_bytes: int
    total_bytes: int
    truncated: bool


def build_text_preview(*, path: Path, policy: SizingPolicy) -> TextPreview | None:
    """Return a byte-truncated preview of a text artifact."""
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    total = len(raw)
    truncated = total > policy.text_bytes
    body = raw[: policy.text_bytes].decode("utf-8", errors="replace")
    return TextPreview(
        text=body,
        shown_bytes=len(raw[: policy.text_bytes]),
        total_bytes=total,
        truncated=truncated,
    )


# ----- Log tail -----------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class LogTail:
    """Tail of a task log."""

    lines: tuple[str, ...]
    shown_lines: int
    total_lines: int
    truncated: bool


def build_log_tail(*, path: Path | None, policy: SizingPolicy) -> LogTail | None:
    """Return the last ``policy.log_lines`` lines of a log file."""
    if path is None or not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    all_lines = raw.splitlines()
    total = len(all_lines)
    tail = all_lines[-policy.log_lines :]
    return LogTail(
        lines=tuple(tail),
        shown_lines=len(tail),
        total_lines=total,
        truncated=total > policy.log_lines,
    )
