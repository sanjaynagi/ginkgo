"""Formatting helpers and per-kind preview caps for report export.

Every cap defined here surfaces as a visible marker in the rendered report
(``showing N of M rows``, ``N bytes truncated``, ``asset not inlined``).
Silent clipping is an anti-goal — the reader must always see what was
dropped.
"""

from __future__ import annotations

from dataclasses import dataclass
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
