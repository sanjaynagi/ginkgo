"""Cache command handlers.

``ls``, ``explain`` and ``stats`` read the ledger and nothing else, through a
read-only connection, so they answer while a run is writing. ``prune`` and
``clear`` remove bytes and the rows that point at them, in that order: a row
without bytes is a miss the next run pays for once, while bytes without a row
are an orphan nothing ever collects.
"""

from __future__ import annotations


import json
import shutil
import sys
import re
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from rich import box
from rich.table import Table
from rich.text import Text

from ginkgo import query
from ginkgo.cli.common import CACHE_ROOT, console
from ginkgo.cli.renderers.common import task_base_name
from ginkgo.formatting import format_bytes, format_int, parse_timestamp
from ginkgo.query import CacheEntryRow, CacheStats, Query
from ginkgo.runtime.artifacts.artifact_store import make_writable_recursive
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.workspace_layout import WorkspaceLayout


def command_cache(args) -> int:
    """Handle ``ginkgo cache`` subcommands."""
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    rich_console = console(sys.stdout, width=None if is_tty else 160)
    if args.cache_command == "ls":
        rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]ls[/]\n")
        entries: list[CacheEntryDisplay] = []
        if _database_exists():
            with _reader() as reader:
                entries = list_cache_entries(reader)
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

    if args.cache_command == "stats":
        return _render_stats(rich_console, as_json=args.json)

    if args.cache_command == "prune":
        rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]prune[/]\n")
        if args.older_than is None and args.max_size is None and args.max_entries is None:
            rich_console.print(
                "[red]Error:[/] provide at least one of --older-than, --max-size, "
                "or --max-entries."
            )
            return 2

        max_size_bytes = _parse_size_bytes(args.max_size) if args.max_size else None
        if args.max_entries is not None and args.max_entries < 0:
            rich_console.print("[red]Error:[/] --max-entries must be at least 0.")
            return 2

        with _index() as index:
            all_entries = list_cache_entries(Query(index.store, layout=WorkspaceLayout.relative()))
            entries = select_prune_entries(
                entries=all_entries,
                older_than=args.older_than,
                max_size_bytes=max_size_bytes,
                max_entries=args.max_entries,
                least_recently_hit=args.least_recently_hit,
            )
            total_bytes = sum(entry.size_bytes for entry in entries)

            if args.dry_run:
                reason = _describe_prune_policy(
                    older_than=args.older_than,
                    max_size=args.max_size,
                    max_entries=args.max_entries,
                    least_recently_hit=args.least_recently_hit,
                )
                rich_console.print(
                    f"[cyan]Preview:[/] {len(entries)} entries {reason} "
                    f"([bold]{format_bytes(total_bytes)}[/]) would be removed."
                )
                for entry in entries:
                    rich_console.print(
                        f"[dim]-[/] {entry.cache_key} ({entry.task}, {entry.age}, {entry.created})"
                    )
                return 0

            for entry in entries:
                _safe_rmtree(entry.path)
            index.forget_entries([entry.cache_key for entry in entries])

            # Clean up orphaned artifacts after pruning.
            _gc_orphan_artifacts(CACHE_ROOT, index=index)

        rich_console.print(
            f"[green]✓[/] Removed [bold]{len(entries)}[/] cache "
            f"{'entry' if len(entries) == 1 else 'entries'} "
            f"([bold]{format_bytes(total_bytes)}[/])."
        )
        return 0

    if args.cache_command == "explain":
        from ginkgo.cli.common import open_run

        if args.run_id is not None and args.run_flag is not None and args.run_id != args.run_flag:
            rich_console.print(
                "[red]Error:[/] conflicting run ids "
                f"({args.run_id!r} and --run {args.run_flag!r}). Pass the run id once."
            )
            return 2

        run_id = args.run_id or args.run_flag
        if run_id is None:
            rich_console.print(
                "[red]Error:[/] provide a run id, e.g. ginkgo cache explain RUN_ID."
            )
            return 2

        with open_run(run_id) as (reader, resolved):
            payload = explain_run_cache(reader=reader, run_id=resolved)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    return _clear(args, rich_console)


def _clear(args, rich_console) -> int:
    """Remove one cache entry, or every entry directory with no row."""
    rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]clear[/]\n")
    with _index() as index:
        if args.orphans:
            orphans = _orphan_entry_dirs(cache_root=CACHE_ROOT, index=index)
            for entry_dir in orphans:
                _safe_rmtree(entry_dir)
            _gc_orphan_artifacts(CACHE_ROOT, index=index)
            rich_console.print(
                f"[green]✓[/] Removed [bold]{len(orphans)}[/] orphaned cache "
                f"{'directory' if len(orphans) == 1 else 'directories'}."
            )
            return 0

        if args.cache_key is None:
            rich_console.print(
                "[red]Error:[/] provide a cache key, or --orphans to remove entry "
                "directories the database has no row for."
            )
            return 2

        cache_dir = CACHE_ROOT / args.cache_key
        if not cache_dir.is_dir():
            raise FileNotFoundError(f"Cache entry not found: {args.cache_key}")
        _safe_rmtree(cache_dir)
        index.forget_entries([args.cache_key])
        _gc_orphan_artifacts(CACHE_ROOT, index=index)

    message = Text()
    message.append("✓ ", style="green")
    message.append("Removed cache entry ")
    message.append(args.cache_key, style="bold")
    message.no_wrap = True
    rich_console.print(message)
    return 0


def _render_stats(rich_console, *, as_json: bool) -> int:
    """Print what the cache holds, in aggregate."""
    if not _database_exists():
        stats = CacheStats(
            entries=0,
            total_bytes=0,
            never_hit=0,
            never_hit_bytes=0,
            hit_histogram={},
            top_functions=[],
        )
    else:
        with _reader() as reader:
            stats = reader.cache_stats()
    if as_json:
        print(
            json.dumps(
                {
                    "entries": stats.entries,
                    "total_bytes": stats.total_bytes,
                    "never_hit": stats.never_hit,
                    "never_hit_bytes": stats.never_hit_bytes,
                    "hit_histogram": {str(k): v for k, v in stats.hit_histogram.items()},
                    "top_functions": [
                        {"function": name, "entries": count, "bytes": size}
                        for name, count, size in stats.top_functions
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]stats[/]\n")
    rich_console.print(f"Entries: [bold]{format_int(stats.entries)}[/]")
    rich_console.print(f"Total size: [bold]{format_bytes(stats.total_bytes)}[/]")
    rich_console.print(
        f"Never hit: [bold]{format_int(stats.never_hit)}[/] "
        f"([bold]{format_bytes(stats.never_hit_bytes)}[/])"
    )
    if stats.hit_histogram:
        histogram = Table(box=box.SQUARE, border_style="#0f766e", header_style="bold #134e4a")
        histogram.add_column("Hits", justify="right")
        histogram.add_column("Entries", justify="right")
        for hits, count in sorted(stats.hit_histogram.items()):
            histogram.add_row(str(hits), format_int(count))
        rich_console.print(histogram)
    if stats.top_functions:
        functions = Table(box=box.SQUARE, border_style="#0f766e", header_style="bold #134e4a")
        functions.add_column("Task", no_wrap=True)
        functions.add_column("Entries", justify="right")
        functions.add_column("Size", justify="right")
        for name, count, size in stats.top_functions:
            functions.add_row(task_base_name(name), format_int(count), format_bytes(size))
        rich_console.print(functions)
    return 0


def _reader() -> Query:
    """Open the ledger read-only, so a running workflow is not disturbed."""
    return query.open()


def _database_exists() -> bool:
    """Return whether this workspace has a ledger yet.

    A workspace nobody has run anything in has no database and an empty cache,
    which is an answer rather than an error.
    """
    return Path(WorkspaceLayout.relative().db).is_file()


def _index() -> CacheIndex:
    """Open the cache index for writing, for the commands that remove things."""
    return CacheIndex.open(path=WorkspaceLayout.relative().db)


@dataclass(frozen=True)
class CacheEntryDisplay:
    """Display and pruning metadata for one cache entry."""

    path: Path
    cache_key: str
    task: str
    size: str
    size_bytes: int
    age: str
    created: str
    created_at: datetime | None
    last_hit_at: datetime | None
    function: str


def list_cache_entries(reader: Query) -> list[CacheEntryDisplay]:
    """Return cache entries as display rows, oldest key first.

    Parameters
    ----------
    reader : Query
        An open view of the ledger.

    Returns
    -------
    list[CacheEntryDisplay]
    """
    return sorted(
        (_cache_entry_row(row) for row in reader.cache_entries()),
        key=lambda entry: entry.cache_key,
    )


def _cache_entry_row(row: CacheEntryRow) -> CacheEntryDisplay:
    """Return the display row for one indexed cache entry."""
    created_at = parse_timestamp(row.created_at)
    return CacheEntryDisplay(
        path=CACHE_ROOT / row.cache_key,
        cache_key=row.cache_key,
        task=task_base_name(row.function),
        size=format_bytes(row.size_bytes),
        size_bytes=row.size_bytes,
        age=_format_age(created_at),
        created=row.created_at or "-",
        created_at=created_at,
        last_hit_at=parse_timestamp(row.last_hit_at),
        function=row.function,
    )


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


def select_prune_entries(
    *,
    entries: list[CacheEntryDisplay],
    older_than: str | None,
    max_size_bytes: int | None,
    max_entries: int | None,
    least_recently_hit: bool = False,
) -> list[CacheEntryDisplay]:
    """Return the cache entries that satisfy the combined prune policy.

    Parameters
    ----------
    entries : list[CacheEntryDisplay]
        Existing cache entries; may be unordered.
    older_than : str | None
        Optional duration string. Entries with ``created_at`` older than the
        cutoff are always selected.
    max_size_bytes : int | None
        When set, additional entries are selected until total cache size drops
        to or below this target.
    max_entries : int | None
        When set, additional entries are selected until the remaining entry
        count drops to or below this target.
    least_recently_hit : bool
        Give up the entries nobody has used lately first, rather than the
        oldest. An entry never hit sorts as never used; ties break on age.

    Returns
    -------
    list[CacheEntryDisplay]
        Entries to remove. Order follows the original iteration order for
        display stability.
    """
    oldest = datetime.min.replace(tzinfo=UTC)
    if least_recently_hit:

        def sort_key(entry: CacheEntryDisplay) -> tuple[datetime, datetime]:
            return (entry.last_hit_at or oldest, entry.created_at or oldest)
    else:

        def sort_key(entry: CacheEntryDisplay) -> tuple[datetime, datetime]:
            return (entry.created_at or oldest, entry.created_at or oldest)

    give_up_first = sorted(entries, key=sort_key)
    selected: set[str] = set()

    if older_than is not None:
        cutoff = _prune_cutoff(older_than)
        for entry in give_up_first:
            if entry.created_at is not None and entry.created_at < cutoff:
                selected.add(entry.cache_key)

    if max_size_bytes is not None or max_entries is not None:
        remaining = [entry for entry in give_up_first if entry.cache_key not in selected]
        remaining_size = sum(entry.size_bytes for entry in remaining)
        remaining_count = len(remaining)
        for entry in remaining:
            size_ok = max_size_bytes is None or remaining_size <= max_size_bytes
            count_ok = max_entries is None or remaining_count <= max_entries
            if size_ok and count_ok:
                break
            selected.add(entry.cache_key)
            remaining_size -= entry.size_bytes
            remaining_count -= 1

    return [entry for entry in entries if entry.cache_key in selected]


def _describe_prune_policy(
    *,
    older_than: str | None,
    max_size: str | None,
    max_entries: int | None,
    least_recently_hit: bool = False,
) -> str:
    """Describe the active prune policy for dry-run output."""
    parts = []
    if older_than is not None:
        parts.append(f"older than {older_than}")
    if max_size is not None:
        parts.append(f"over {max_size} cache size")
    if max_entries is not None:
        parts.append(f"over {max_entries} entries")
    if least_recently_hit:
        parts.append("least recently hit first")
    return f"matching policy ({'; '.join(parts)})"


_SIZE_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB|K|M|G|T)?\s*$", re.IGNORECASE)


def _parse_size_bytes(value: str) -> int:
    """Parse a compact size string (e.g. ``2GB``) to an integer byte count."""
    match = _SIZE_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError(f"Invalid --max-size {value!r}. Use e.g. 500MB, 2GB, 10GB.")
    count = float(match.group(1))
    unit = (match.group(2) or "B").upper().rstrip("B") or "B"
    multipliers = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(count * multipliers[unit])


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
        make_writable_recursive(path)
        shutil.rmtree(path)


def _orphan_entry_dirs(*, cache_root: Path, index: CacheIndex) -> list[Path]:
    """Return entry directories the database has no row for.

    A lost database leaves the bytes behind, and nothing else will ever look
    at them: the key that would find them is only in the row that is gone.
    """
    if not cache_root.exists():
        return []
    known = {
        row.cache_key
        for row in Query(index.store, layout=WorkspaceLayout.relative()).cache_entries()
    }
    return sorted(
        entry for entry in cache_root.iterdir() if entry.is_dir() and entry.name not in known
    )


def _gc_orphan_artifacts(cache_root: Path, *, index: CacheIndex | None = None) -> None:
    """Remove artifacts no cache entry or catalogued asset points at.

    The cache and the asset catalog share one artifact store, so each reports
    the artifact IDs it still references and anything beyond their union is
    deleted. The asset half still comes from the YAML catalog; it joins the
    same query when the catalog moves into the store.
    """
    layout = WorkspaceLayout.sibling_of(cache_root)
    if not layout.artifacts.exists():
        return

    from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
    from ginkgo.runtime.artifacts.asset_store import AssetStore

    with ExitStack() as stack:
        if index is None:
            index = stack.enter_context(CacheIndex.open(path=layout.db))
        referenced = index.referenced_artifact_ids()
        referenced |= AssetStore(root=layout.assets).referenced_artifact_ids()

        store = LocalArtifactStore(root=layout.artifacts, index=index)
        for artifact_id in store.list_artifact_ids():
            if artifact_id not in referenced:
                store.delete(artifact_id=artifact_id)


def explain_run_cache(*, reader: Query, run_id: str) -> dict[str, object]:
    """Return cache explanations for each task in a run."""
    summary = reader.run(run_id).to_payload()
    tasks = summary.get("tasks", [])
    explanations = [
        _explain_task_cache(reader=reader, run_id=run_id, task=task)
        for task in tasks
        if isinstance(task, dict)
    ]
    return {
        "run_id": summary.get("run_id"),
        "workflow": summary.get("workflow"),
        "tasks": explanations,
    }


def _explain_task_cache(
    *, reader: Query, run_id: str, task: dict[str, object]
) -> dict[str, object]:
    """Return a best-effort cache explanation for one task."""
    cache_key = task.get("cache_key")
    task_name = str(task.get("task_name") or task.get("task") or "unknown")
    identity: dict[str, object] = {
        "task_id": task.get("task_id"),
        "task_name": task_name,
        "cache_key": cache_key,
    }
    if task.get("status") == "cached":
        return identity | {"reason": "all_inputs_match"}

    if not isinstance(cache_key, str):
        return identity | {"reason": "no_entry_for_key"}
    current = reader.cache_key_components(cache_key)
    if not current:
        return identity | {"reason": "no_entry_for_key"}

    task_id = task.get("task_id")
    prior = (
        reader.previous_cache_key(run_id=run_id, task_id=str(task_id))
        if isinstance(task_id, str)
        else None
    )
    if prior is None:
        return identity | {"reason": "no_prior_entry"}

    prior_key, strategy = prior
    components = _diff_key_components(
        current=current, prior=reader.cache_key_components(prior_key)
    )
    reasons = _coarse_reasons(components)
    return identity | {
        "compared_with": {"cache_key": prior_key, "strategy": strategy},
        "reason": reasons[0],
        "details": reasons,
        "components": components,
    }


def _diff_key_components(
    *, current: dict[str, object], prior: dict[str, object]
) -> list[dict[str, object]]:
    """Return the cache-key components that differ between two entries."""
    differences: list[dict[str, object]] = []
    for name in sorted(set(current) | set(prior)):
        # A component absent from one side is a parameter that came or went.
        if name not in current:
            differences.append({"component": name, "status": "removed", "prior": prior[name]})
        elif name not in prior:
            differences.append({"component": name, "status": "added", "current": current[name]})
        elif current[name] != prior[name]:
            differences.append(
                {
                    "component": name,
                    "status": "changed",
                    "current": current[name],
                    "prior": prior[name],
                }
            )
    return differences


def _coarse_reasons(components: list[dict[str, object]]) -> list[str]:
    """Return the summary reason codes implied by a component diff.

    A moved component no code covers falls back to ``cache_key_changed``; the
    component list names it either way.
    """
    moved = {str(component["component"]) for component in components}
    reasons = []
    if moved & {"source_hash", "extra_source_hash"}:
        reasons.append("source_hash_changed")
    if "version" in moved:
        reasons.append("version_bump")
    if moved & {"env", "env_hash.pixi_lock"}:
        reasons.append("env_changed")
    if any(name.startswith("inputs") for name in moved):
        reasons.append("input_changed")
    return reasons or ["cache_key_changed"]
