"""Cache command handlers."""

from __future__ import annotations

import json
import shutil
import sys
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml
from rich import box
from rich.table import Table
from rich.text import Text

from ginkgo.cli.common import CACHE_ROOT, console
from ginkgo.cli.renderers.common import _task_base_name
from ginkgo.runtime.artifact_store import _make_writable_recursive


def command_cache(args) -> int:
    """Handle ``ginkgo cache`` subcommands."""
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    rich_console = console(sys.stdout, width=None if is_tty else 160)
    if args.cache_command == "ls":
        rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]ls[/]\n")
        entries = list_cache_entries(CACHE_ROOT)
        if not entries:
            rich_console.print("[dim]No cache entries found.[/]")
            return 0

        table = Table(
            box=box.SQUARE,
            border_style="#0f766e",
            header_style="bold #134e4a",
            expand=False,
        )
        table.add_column("Cache Key", style="bold", overflow="fold")
        table.add_column("Task", no_wrap=True)
        table.add_column("Size", justify="right")
        table.add_column("Age", justify="right")
        table.add_column("Created", no_wrap=True)
        for row in entries:
            table.add_row(
                row.cache_key,
                row.task,
                row.size,
                row.age,
                row.created,
            )
        rich_console.print(table)
        return 0

    if args.cache_command == "prune":
        rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]prune[/]\n")
        cutoff = _prune_cutoff(args.older_than)
        entries = [
            entry
            for entry in list_cache_entries(CACHE_ROOT)
            if entry.created_at is not None and entry.created_at < cutoff
        ]
        total_bytes = sum(entry.size_bytes for entry in entries)

        if args.dry_run:
            rich_console.print(
                f"[cyan]Preview:[/] {len(entries)} entries older than "
                f"[bold]{args.older_than}[/] "
                f"([bold]{_format_size(total_bytes)}[/]) would be removed."
            )
            for entry in entries:
                rich_console.print(
                    f"[dim]-[/] {entry.cache_key} ({entry.task}, {entry.age}, {entry.size})"
                )
            return 0

        for entry in entries:
            _safe_rmtree(entry.path)

        # Clean up orphaned artifacts after pruning.
        _gc_orphan_artifacts(CACHE_ROOT)

        rich_console.print(
            f"[green]✓[/] Removed [bold]{len(entries)}[/] cache "
            f"{'entry' if len(entries) == 1 else 'entries'} "
            f"([bold]{_format_size(total_bytes)}[/])."
        )
        return 0

    if args.cache_command == "explain":
        from ginkgo.cli.commands.inspect import inspect_run
        from ginkgo.cli.common import resolve_run_dir

        payload = explain_run_cache(
            cache_root=CACHE_ROOT,
            run_snapshot=inspect_run(run_dir=resolve_run_dir(args.run_id)),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    cache_dir = CACHE_ROOT / args.cache_key
    if not cache_dir.is_dir():
        raise FileNotFoundError(f"Cache entry not found: {args.cache_key}")
    _safe_rmtree(cache_dir)

    # Clean up orphaned artifacts after clearing.
    _gc_orphan_artifacts(CACHE_ROOT)
    rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]clear[/]\n")
    message = Text()
    message.append("✓ ", style="green")
    message.append("Removed cache entry ")
    message.append(args.cache_key, style="bold")
    message.no_wrap = True
    rich_console.print(message)
    return 0


@dataclass(frozen=True)
class CacheEntryRow:
    """Display and pruning metadata for a cache entry."""

    path: Path
    cache_key: str
    task: str
    size: str
    size_bytes: int
    age: str
    created: str
    created_at: datetime | None
    function: str


def list_cache_entries(root: Path) -> list[CacheEntryRow]:
    """Return cache entries as structured rows."""
    if not root.exists():
        return []
    return [
        _cache_entry_row(entry)
        for entry in sorted(path for path in root.iterdir() if path.is_dir())
    ]


def _cache_entry_row(entry: Path) -> CacheEntryRow:
    """Return the display row for a cache entry."""
    meta_path = entry / "meta.json"
    if meta_path.is_file():
        try:
            meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
        except Exception:
            meta = {}
    else:
        meta = {}

    function = str(meta.get("function") or "unknown")
    timestamp = str(meta.get("timestamp") or "")
    created_at = _parse_timestamp(timestamp)
    size_bytes = _dir_size(entry)
    return CacheEntryRow(
        path=entry,
        cache_key=entry.name,
        task=_task_base_name(function),
        size=_format_size(size_bytes),
        size_bytes=size_bytes,
        age=_format_age(created_at),
        created=timestamp or "-",
        created_at=created_at,
        function=function,
    )


def _dir_size(path: Path) -> int:
    """Return the total size of files beneath a cache entry directory."""
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            total += file_path.stat().st_size
    return total


def _format_size(size_bytes: int) -> str:
    """Return a human-readable file size."""
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size_bytes} B"


def _parse_timestamp(timestamp: str) -> datetime | None:
    """Parse an ISO-8601 timestamp if present."""
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _format_age(created_at: datetime | None) -> str:
    """Return a compact age string for a cache entry."""
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


def _prune_cutoff(older_than: str) -> datetime:
    """Return the UTC cutoff timestamp implied by a duration string."""
    duration = _parse_duration_seconds(older_than)
    return datetime.now(UTC) - duration


def _parse_duration_seconds(value: str):
    """Parse a compact duration string like ``30d`` or ``12h``."""
    match = re.fullmatch(r"(?P<count>\d+)(?P<unit>[mhd])", value.strip())
    if match is None:
        raise ValueError(
            "Invalid duration for --older-than. Use a positive integer followed by "
            "m, h, or d (for example: 45m, 12h, 30d)."
        )

    count = int(match.group("count"))
    unit = match.group("unit")
    multipliers = {"m": 60, "h": 3600, "d": 86400}
    from datetime import timedelta

    return timedelta(seconds=count * multipliers[unit])


def _safe_rmtree(path: Path) -> None:
    """Remove a cache entry directory, handling read-only artifacts."""
    try:
        shutil.rmtree(path)
    except PermissionError:
        _make_writable_recursive(path)
        shutil.rmtree(path)


def _gc_orphan_artifacts(cache_root: Path) -> None:
    """Remove artifacts not referenced by any remaining cache entry.

    Scans all ``meta.json`` files under *cache_root* to collect referenced
    artifact IDs, then deletes any artifacts in the sibling ``artifacts/``
    directory that are not referenced.
    """
    artifacts_root = cache_root.parent / "artifacts"
    if not artifacts_root.exists():
        return

    # Collect all referenced artifact IDs from surviving cache entries.
    referenced: set[str] = set()
    for entry_dir in cache_root.iterdir():
        if not entry_dir.is_dir():
            continue
        meta_path = entry_dir / "meta.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        for artifact_id in meta.get("artifact_ids", {}).values():
            referenced.add(artifact_id)

    # Remove orphaned artifacts.
    from ginkgo.runtime.artifact_store import LocalArtifactStore

    store = LocalArtifactStore(root=artifacts_root)
    for child in list(artifacts_root.iterdir()):
        if child.name not in referenced:
            store.delete(artifact_id=child.name)


def explain_run_cache(*, cache_root: Path, run_snapshot: dict[str, object]) -> dict[str, object]:
    """Return cache explanations for each task in a run snapshot."""
    explanations = []
    tasks = run_snapshot.get("tasks", [])
    if isinstance(tasks, list):
        for task in tasks:
            if isinstance(task, dict):
                explanations.append(_explain_task_cache(cache_root=cache_root, task=task))
    return {
        "run_id": run_snapshot.get("run_id"),
        "workflow": run_snapshot.get("workflow"),
        "tasks": explanations,
    }


def _explain_task_cache(*, cache_root: Path, task: dict[str, object]) -> dict[str, object]:
    """Return a best-effort cache explanation for one task."""
    cache_key = task.get("cache_key")
    task_name = str(task.get("task_name") or task.get("task") or "unknown")
    if task.get("status") == "cached":
        return {
            "task_id": task.get("task_id"),
            "task_name": task_name,
            "cache_key": cache_key,
            "reason": "all_inputs_match",
        }

    current_meta = _read_cache_meta(cache_root=cache_root, cache_key=cache_key)
    sibling_entries = _entries_for_function(
        cache_root=cache_root, function=task_name, exclude=cache_key
    )
    if not sibling_entries:
        return {
            "task_id": task.get("task_id"),
            "task_name": task_name,
            "cache_key": cache_key,
            "reason": "no_prior_entry",
        }

    prior_meta = sibling_entries[-1]
    reasons = []
    if current_meta.get("source_hash") != prior_meta.get("source_hash"):
        reasons.append("source_hash_changed")
    if current_meta.get("version") != prior_meta.get("version"):
        reasons.append("version_bump")
    if current_meta.get("env") != prior_meta.get("env"):
        reasons.append("env_lock_changed")
    if current_meta.get("input_hashes") != prior_meta.get("input_hashes"):
        reasons.append("input_changed")
    if not reasons:
        reasons.append("cache_key_changed")

    return {
        "task_id": task.get("task_id"),
        "task_name": task_name,
        "cache_key": cache_key,
        "reason": reasons[0],
        "details": reasons,
    }


def _read_cache_meta(*, cache_root: Path, cache_key: object) -> dict[str, object]:
    """Return cache metadata for one key."""
    if not isinstance(cache_key, str):
        return {}
    meta_path = cache_root / cache_key / "meta.json"
    if not meta_path.is_file():
        return {}
    try:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def _entries_for_function(
    *,
    cache_root: Path,
    function: str,
    exclude: object,
) -> list[dict[str, object]]:
    """Return cache metadata entries for the same function."""
    entries: list[dict[str, object]] = []
    if not cache_root.exists():
        return entries
    for entry in sorted(path for path in cache_root.iterdir() if path.is_dir()):
        if exclude == entry.name:
            continue
        meta = _read_cache_meta(cache_root=cache_root, cache_key=entry.name)
        if _task_base_name(str(meta.get("function", "unknown"))) == _task_base_name(function):
            entries.append(meta)
    return entries
