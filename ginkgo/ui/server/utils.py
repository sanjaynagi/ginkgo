"""Shared formatting and signature helpers for the UI server."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ginkgo.runtime.caching.provenance import latest_run_dir


def run_count(runs_root: Path) -> int:
    """Return the number of recorded runs."""
    if not runs_root.exists():
        return 0
    return sum(1 for path in runs_root.iterdir() if path.is_dir())


def latest_run_id(runs_root: Path) -> str | None:
    """Return the latest run id for one workspace."""
    latest = latest_run_dir(runs_root)
    return latest.name if latest is not None else None


def task_base_name(value: Any) -> str:
    """Return the unqualified task name."""
    if not isinstance(value, str) or not value:
        return "unknown"
    return value.rsplit(".", 1)[-1]


def base_name(value: Any) -> str:
    """Return the basename for a path-like string."""
    if not isinstance(value, str) or not value:
        return "unknown"
    return Path(value).name


def status_count(tasks: Any, status: str) -> int:
    """Count tasks in one status."""
    if not isinstance(tasks, dict):
        return 0
    return sum(
        1 for item in tasks.values() if isinstance(item, dict) and item.get("status") == status
    )


def cached_count(tasks: Any) -> int:
    """Count cached tasks."""
    if not isinstance(tasks, dict):
        return 0
    return sum(
        1 for item in tasks.values() if isinstance(item, dict) and item.get("cached") is True
    )


def duration_seconds(started_at: Any, finished_at: Any) -> float | None:
    """Return a run/task duration in seconds when both timestamps are valid."""
    if not isinstance(started_at, str) or not isinstance(finished_at, str):
        return None
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at)
    except ValueError:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    return max(0.0, (end - start).total_seconds())


def runs_signature(runs_root: Path) -> str:
    """Return a change signature for one runs directory."""
    if not runs_root.exists():
        return "no-runs"
    parts: list[str] = []
    for run_dir in sorted(
        (path for path in runs_root.iterdir() if path.is_dir()),
        key=lambda item: item.name,
    ):
        manifest_path = run_dir / "manifest.yaml"
        if manifest_path.is_file():
            stat = manifest_path.stat()
            parts.append(f"{run_dir.name}:{stat.st_mtime_ns}:{stat.st_size}")
        else:
            parts.append(f"{run_dir.name}:missing")
    return "|".join(parts) or "no-runs"


def cache_signature(cache_root: Path) -> str:
    """Return a change signature for one cache directory."""
    if not cache_root.exists():
        return "no-cache"
    parts: list[str] = []
    for entry in sorted(
        (path for path in cache_root.iterdir() if path.is_dir()),
        key=lambda item: item.name,
    ):
        try:
            stat = entry.stat()
        except FileNotFoundError:
            continue
        parts.append(f"{entry.name}:{stat.st_mtime_ns}")
    return "|".join(parts) or "no-cache"


def dir_size(path: Path) -> int:
    """Return a recursive directory size in bytes."""
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            total += file_path.stat().st_size
    return total


def format_size(size_bytes: int) -> str:
    """Format a byte size for display."""
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size_bytes} B"


def parse_timestamp(timestamp: str) -> datetime | None:
    """Parse an ISO timestamp."""
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def format_age(created_at: datetime | None) -> str:
    """Format a timestamp as an age string."""
    if created_at is None:
        return "-"
    delta = datetime.now(UTC) - created_at.astimezone(UTC)
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"
