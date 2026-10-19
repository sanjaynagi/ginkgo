"""Size caps applied when exporting a report.

Every cap surfaces as a visible marker in the rendered report (``last N of M
lines``). Silent clipping is an anti-goal — the reader must always see what
was dropped.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


# ----- Caps ----------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class SizingPolicy:
    """Size caps applied at export time.

    Parameters
    ----------
    log_lines : int
        Maximum trailing lines retained from a task log.
    """

    log_lines: int = 80


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
