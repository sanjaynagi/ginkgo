"""Cache command handlers.

``ls``, ``explain`` and ``stats`` read the ledger and nothing else, through a
read-only connection, so they answer while a run is writing. ``prune`` and
``clear`` remove bytes and the rows that point at them, in that order: a row
without bytes is a miss the next run pays for once, while bytes without a row
are an orphan nothing ever collects.
"""

from __future__ import annotations

import json
import re
import shutil
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich.markup import escape
from rich.text import Text

from ginkgo import query
from ginkgo.cli.common import CACHE_ROOT, console, new_table
from ginkgo.cli.errors import UsageError
from ginkgo.formatting import cutoff_before, format_bytes, format_int, parse_timestamp
from ginkgo.store.maintenance import disk_usage
from ginkgo.runtime.module_loader import task_base_name
from ginkgo.query import CacheEntryRow, CacheStats, Query
from ginkgo.runtime.artifacts.artifact_store import make_writable_recursive
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.workspace_layout import WorkspaceLayout


def command_cache(args) -> int:
    """Handle ``ginkgo cache`` subcommands."""
    rich_console = console(sys.stdout)
    if args.cache_command == "ls":
        rows: list[CacheEntryRow] = []
        if _database_exists():
            with _reader() as reader:
                rows = reader.cache_entries(task=args.task)
        if args.json:
            print(json.dumps([row.to_payload() for row in rows], indent=2, sort_keys=True))
            return 0

        rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]ls[/]\n")
        entries = sorted((_cache_entry_row(row) for row in rows), key=lambda e: e.cache_key)
        if not entries:
            rich_console.print("[dim]No cache entries found.[/]")
            return 0

        table = new_table()
        # Never wrapped: a key is copied whole into `cache clear`, and a key
        # folded over two lines does not paste as one.
        table.add_column(
            "Cache Key",
            style="bold",
            no_wrap=True,
            min_width=max(len(row.cache_key) for row in entries),
        )
        table.add_column("Task", no_wrap=True)
        table.add_column("Label", no_wrap=True)
        table.add_column("Size", justify="right")
        table.add_column("Age", justify="right")
        # Apparent bytes: a file smaller than a filesystem block still takes one.
        table.caption = "Sizes are apparent; `ginkgo cache stats` gives the space on disk."

        table.add_column("Created", no_wrap=True)
        for row in entries:
            table.add_row(
                row.cache_key,
                row.task,
                # A label's brackets are text, not Rich markup.
                Text(row.label),
                row.size,
                row.age,
                row.created,
            )
        rich_console.print(table)
        return 0

    if args.cache_command == "stats":
        return _render_stats(rich_console, as_json=args.json)

    if args.cache_command == "prune":
        if args.older_than is None and args.max_size is None and args.max_entries is None:
            raise UsageError("Provide at least one of --older-than, --max-size, or --max-entries.")
        rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]prune[/]\n")
        max_size_bytes = parse_size_bytes(args.max_size) if args.max_size else None

        all_entries: list[CacheEntryDisplay] = []
        if _database_exists():
            with _reader() as reader:
                all_entries = list_cache_entries(reader)
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
                f"([bold]{format_bytes(total_bytes)}[/] apparent size) would be removed."
            )
            for entry in entries:
                rich_console.print(
                    f"[dim]-[/] {entry.cache_key} ({entry.task}, {entry.age}, {entry.created})"
                )
            return 0

        freed_bytes = 0
        orphans = 0
        if entries or _database_exists():
            with _cache_store() as cache_store:
                for entry in entries:
                    freed_bytes += remove_entry(cache_store, entry.cache_key)
                # Directories no row names are bytes nothing will ever hit,
                # which `db check` would otherwise go on flagging.
                orphan_dirs = cache_store.orphan_entry_dirs()
                orphans = len(orphan_dirs)
                freed_bytes += sum(_remove_tree(path) for path in orphan_dirs)
                # An artifact a surviving entry still names stays, so what was
                # freed is counted from what the collector actually deleted.
                freed_bytes += gc_orphan_artifacts(cache_store)

        removed = f"{len(entries)} cache {'entry' if len(entries) == 1 else 'entries'}"
        if orphans:
            removed += f" and {orphans} orphaned {'directory' if orphans == 1 else 'directories'}"
        rich_console.print(
            f"[green]✓[/] Removed [bold]{removed}[/] ([bold]{format_bytes(freed_bytes)}[/] "
            "freed on disk)."
        )
        if entries:
            rich_console.print(_LEDGER_SPACE_NOTE)
        return 0

    if args.cache_command == "explain":
        from ginkgo.cli.common import open_run

        if args.run_id is not None and args.run_flag is not None and args.run_id != args.run_flag:
            raise UsageError(
                f"Conflicting run ids ({args.run_id!r} and --run {args.run_flag!r}). "
                "Pass the run id once."
            )

        run_id = args.run_id or args.run_flag
        with open_run(run_id) as (reader, resolved):
            payload = explain_run_cache(reader=reader, run_id=resolved)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _render_explain_text(rich_console, payload)
        return 0

    return _clear(args, rich_console)


def _clear(args, rich_console) -> int:
    """Remove one cache entry, a task's entries, every entry, or the orphaned directories."""
    selectors = [args.cache_key is not None, args.task is not None, args.clear_all, args.orphans]
    if sum(selectors) != 1:
        raise UsageError(
            "Provide exactly one of: a cache key, --task NAME to remove a task's entries, "
            "--all to remove every entry, or --orphans to remove entry directories the "
            "database has no row for."
        )
    rich_console.print("[bold green]🌿 ginkgo cache[/] [bold]clear[/]\n")
    if args.cache_key is not None:
        if not (CACHE_ROOT / args.cache_key).is_dir():
            raise FileNotFoundError(f"Cache entry not found: {args.cache_key}")
        with _cache_store() as cache_store:
            freed_bytes = remove_entry(cache_store, args.cache_key)
            freed_bytes += gc_orphan_artifacts(cache_store)

        message = Text()
        message.append("✓ ", style="green")
        message.append("Removed cache entry ")
        message.append(args.cache_key, style="bold")
        message.append(f" ({format_bytes(freed_bytes)} freed on disk).")
        message.no_wrap = True
        rich_console.print(message)
        rich_console.print(_LEDGER_SPACE_NOTE)
        return 0

    if not _database_exists():
        # Nothing has been cached here, and removing nothing is not a
        # reason to create the database that would record it.
        rich_console.print("[dim]No cache entries found.[/]")
        return 0

    if args.orphans:
        with _cache_store() as cache_store:
            orphans = _remove_orphans(cache_store)
            gc_orphan_artifacts(cache_store)
        rich_console.print(
            f"[green]✓[/] Removed [bold]{orphans}[/] orphaned cache "
            f"{'directory' if orphans == 1 else 'directories'}."
        )
        return 0

    with _reader() as reader:
        rows = reader.cache_entries(task=args.task)
    if args.task is not None and not rows:
        raise FileNotFoundError(f"No cache entries found for task: {args.task}")
    with _cache_store() as cache_store:
        freed_bytes = sum(remove_entry(cache_store, row.cache_key) for row in rows)
        orphans = _remove_orphans(cache_store) if args.clear_all else 0
        # As prune counts it: an artifact another entry still names stays.
        freed_bytes += gc_orphan_artifacts(cache_store)
    removed = f"{len(rows)} cache {'entry' if len(rows) == 1 else 'entries'}"
    if orphans:
        removed += f" and {orphans} orphaned {'directory' if orphans == 1 else 'directories'}"
    rich_console.print(
        f"[green]✓[/] Removed [bold]{removed}[/] ({format_bytes(freed_bytes)} freed on disk)."
    )
    if rows:
        rich_console.print(_LEDGER_SPACE_NOTE)
    return 0


_LEDGER_SPACE_NOTE = (
    "[dim]The entries' rows in .ginkgo/ginkgo.db are freed for reuse; "
    "`ginkgo db vacuum` returns that space to the disk.[/]"
)
"""What ``clear`` and ``prune`` say about bytes their freed count leaves out."""


def _remove_orphans(cache_store: CacheStore) -> int:
    """Remove the entry directories with no row.

    The artifacts they named are the caller's to collect, with
    :func:`gc_orphan_artifacts`.

    Returns
    -------
    int
        How many entry directories were removed.
    """
    orphans = cache_store.orphan_entry_dirs()
    for entry_dir in orphans:
        _remove_tree(entry_dir)
    return len(orphans)


def _render_stats(rich_console, *, as_json: bool) -> int:
    """Print what the cache holds, in aggregate."""
    stats = CacheStats.empty()
    if _database_exists():
        with _reader() as reader:
            stats = reader.cache_stats()
    layout = WorkspaceLayout.relative()
    disk_bytes = disk_usage(layout.cache) + disk_usage(layout.artifacts)
    if as_json:
        print(
            json.dumps(
                {
                    "entries": stats.entries,
                    "total_bytes": stats.total_bytes,
                    "disk_bytes": disk_bytes,
                    "artifact_bytes": stats.artifact_bytes,
                    "never_hit": stats.never_hit,
                    "never_hit_bytes": stats.never_hit_bytes,
                    "hit_histogram": {str(k): v for k, v in stats.hit_histogram.items()},
                    "top_functions": [
                        {
                            "task_name": task_base_name(name),
                            "qualified_name": name,
                            "entries": count,
                            "bytes": size,
                        }
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
    rich_console.print(f"Total size: [bold]{format_bytes(stats.total_bytes)}[/] apparent")
    rich_console.print(f"Artifact store: [bold]{format_bytes(stats.artifact_bytes)}[/] apparent")
    rich_console.print(
        f"On disk: [bold]{format_bytes(disk_bytes)}[/] [dim](cache entries and artifact "
        "store, in allocated blocks as du counts them)[/]"
    )
    rich_console.print(
        f"Never hit: [bold]{format_int(stats.never_hit)}[/] "
        f"([bold]{format_bytes(stats.never_hit_bytes)}[/])"
    )
    if stats.hit_histogram:
        histogram = new_table()
        histogram.add_column("Hits", justify="right")
        histogram.add_column("Entries", justify="right")
        for hits, count in sorted(stats.hit_histogram.items()):
            histogram.add_row(str(hits), format_int(count))
        rich_console.print(histogram)
    if stats.top_functions:
        functions = new_table()
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


@contextmanager
def _cache_store() -> Iterator[CacheStore]:
    """Open the cache for writing, for the commands that remove things.

    Yields
    ------
    CacheStore
        Backed by a write-mode index, closed when the block ends.
    """
    layout = WorkspaceLayout.relative()
    with CacheIndex.open(path=layout.db) as index:
        yield CacheStore(index=index, root=layout.cache)


def remove_entry(cache_store: CacheStore, cache_key: str) -> int:
    """Remove one entry's bytes, then its rows — in that order.

    A row without bytes is a miss the next run pays for once; bytes without a
    row are an orphan nothing collects. Doing it the other way round leaves the
    worse of the two behind if the process dies between the halves.

    Returns
    -------
    int
        The disk space the entry's directory occupied.
    """
    freed = _remove_tree(cache_store.output_path(cache_key).parent)
    cache_store.index.forget_entries([cache_key])
    return freed


def _database_exists() -> bool:
    """Return whether this workspace has a ledger yet.

    A workspace nobody has run anything in has no database and an empty cache,
    which is an answer rather than an error.
    """
    return Path(WorkspaceLayout.relative().db).is_file()


@dataclass(frozen=True)
class CacheEntryDisplay:
    """Display and pruning metadata for one cache entry."""

    path: Path
    cache_key: str
    task: str
    label: str
    """The fan-out branch that last recorded the key, or ``-``."""
    size: str
    size_bytes: int
    """The entry's apparent size: its output, its value's blobs and the artifacts it names."""
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
        label=row.display_label or "-",
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

    def give_up_order(entry: CacheEntryDisplay) -> tuple[datetime, datetime]:
        last_used = entry.last_hit_at if least_recently_hit else entry.created_at
        return (last_used or oldest, entry.created_at or oldest)

    give_up_first = sorted(entries, key=give_up_order)
    selected: set[str] = set()

    if older_than is not None:
        cutoff = cutoff_before(older_than)
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


_SIZE_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(?:([KMGT])(I)?)?B?\s*$", re.IGNORECASE)


def parse_size_bytes(value: str) -> int:
    """Parse a compact size string (e.g. ``2GB``) to an integer byte count.

    ``KB``/``MB``/``GB``/``TB`` (or ``K``/``M``/``G``/``T``) are decimal and
    ``KiB``/``MiB``/``GiB``/``TiB`` binary, the units sizes are printed in.

    Raises
    ------
    ValueError
        If *value* is not a number with an optional unit.
    """
    match = _SIZE_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError(f"Invalid size {value!r}. Use e.g. 500MB, 2GB, 10GiB.")
    count, prefix, binary = match.groups()
    exponent = " KMGT".index(prefix.upper()) if prefix else 0
    return int(float(count) * (1024 if binary else 1000) ** exponent)


def _remove_tree(path: Path) -> int:
    """Remove a cache entry directory, handling read-only artifacts.

    Returns
    -------
    int
        The disk space it occupied — allocated blocks, as ``du`` counts them,
        not the apparent size of its files — or ``0`` if it was not there.
    """
    if not path.exists():
        return 0
    freed = disk_usage(path)
    try:
        shutil.rmtree(path)
    except PermissionError:
        make_writable_recursive(path)
        shutil.rmtree(path)
    return freed


def gc_orphan_artifacts(cache_store: CacheStore) -> int:
    """Remove artifacts no cache entry points at, returning the bytes freed."""
    layout = WorkspaceLayout.relative()
    if not layout.artifacts.exists():
        return 0

    # An entry names its output paths' artifacts in rows, and a large value's
    # (a DataFrame's Parquet) from inside its encoded value; both are kept.
    referenced = cache_store.index.referenced_artifact_ids() | cache_store.value_artifact_ids()

    store = cache_store.artifact_store_view
    freed = 0
    for artifact_id in store.list_artifact_ids():
        if artifact_id in referenced:
            continue
        record = store.load_record(artifact_id=artifact_id)
        path = store.artifact_path(artifact_id=artifact_id)
        if record is not None and record.kind == "blob" and path.is_file():
            freed += disk_usage(path)
        store.delete(artifact_id=artifact_id)
    # A tree's member blobs outlive its row, since another tree may share
    # them; the ones nothing reaches any more go here.
    return freed + store.remove_orphan_files()


#: Rich style for each tracking label ``cache explain`` prints, chosen to
#: draw the eye to ``path`` — the one label that names a silent-staleness
#: risk (issues #121/#281) rather than a deliberate design choice.
_TRACKING_STYLES = {
    "path": "yellow",
    "content": "green",
    "version": "cyan",
    "output": "blue",
    "value": "dim",
    "untracked": "dim",
}

#: A short hint printed next to a label that names a real tracking gap.
_TRACKING_HINTS = {
    "path": (
        "tracked by path string only — a directory needs `folder` to track its contents; "
        "use `untracked` if that is deliberate"
    ),
}

_REASON_TEXT = {
    "all_inputs_match": "cache hit — every input matched the stored entry",
    "rerun_requested": "forced by --rerun; the cache was not consulted",
    "no_entry_for_key": (
        "no cache entry exists for this key — nothing it is keyed on changed, but the "
        "entry was removed (cache prune or clear) or never written (the earlier run failed)"
    ),
    "prior_entry_removed": (
        "the cache key changed, and the previous entry has since been removed, so the "
        "component that moved cannot be named"
    ),
    "no_prior_entry": "no earlier run of this call to compare against",
    "task_moved": "the task moved or was renamed, which starts it without a cache entry",
    "source_hash_changed": "the task's source changed",
    "module_source_changed": (
        "the task's own code is unchanged, but its module or a local module it imports "
        "changed — for example another task defined in the same file"
    ),
    "version_bump": "the task's declared version changed",
    "env_changed": "the declared environment changed",
    "input_changed": "one or more inputs changed",
    "cache_key_changed": "the cache key changed",
}


def _render_explain_text(rich_console, payload: dict[str, Any]) -> None:
    """Render ``ginkgo cache explain``'s payload as formatted text.

    The JSON form (``--json``) is the same data; this is the other reading of
    it, one task at a time, with each input's tracking label named and a
    ``path`` label highlighted — the trap of issues #121/#281, where a task
    input names an existing path but is annotated as a plain scalar, so the
    cache key tracks the string, never the bytes it points at.
    """
    run_id = str(payload.get("run_id") or "")
    workflow = payload.get("workflow")
    header = f"[bold green]🌿 ginkgo cache[/] [bold]explain[/] [dim]{escape(run_id)}[/]"
    if workflow:
        header += f" [dim]({escape(str(workflow))})[/]"
    rich_console.print(header + "\n")

    tasks = [task for task in (payload.get("tasks") or []) if isinstance(task, dict)]
    if not tasks:
        rich_console.print("[dim]No tasks found for this run.[/]")
        return
    for task in tasks:
        _render_task_explanation(rich_console, task)


def _render_task_explanation(rich_console, task: dict[str, Any]) -> None:
    """Render one task's cache explanation: identity, reason, inputs, diff."""
    name = str(task.get("display_label") or task.get("task_name") or "unknown")
    task_id = task.get("task_id")
    header = f"[bold]{escape(name)}[/]"
    if task_id:
        header += f" [dim]({escape(str(task_id))})[/]"
    rich_console.print(header)

    cache_key = task.get("cache_key")
    if cache_key:
        rich_console.print(f"  cache key: [bold]{escape(str(cache_key))}[/]")

    reason = str(task.get("reason") or "")
    components = [item for item in (task.get("components") or []) if isinstance(item, dict)]
    rich_console.print(f"  reason: {_reason_text(reason=reason, components=components)}")
    moved_from = task.get("moved_from")
    if moved_from:
        rich_console.print(
            f"  earlier runs knew it as [bold]{escape(str(moved_from))}[/]; a task's cache "
            "identity includes its module and qualified name"
        )

    input_labels = task.get("input_labels")
    if isinstance(input_labels, dict) and input_labels:
        rich_console.print("  inputs:")
        for param, label in sorted(input_labels.items()):
            rich_console.print(f"    {_render_tracking_label(param=param, label=label)}")

    compared_with = task.get("compared_with")
    if isinstance(compared_with, dict):
        # A sibling's entry is context for a new branch, not its history.
        against = (
            "the most similar other entry of the same task"
            if compared_with.get("strategy") == "most_similar_by_function"
            else "its previous entry"
        )
        rich_console.print(
            f"  compared with {against}, [bold]{escape(str(compared_with.get('cache_key')))}[/]"
        )
        for component in components:
            rich_console.print(f"    {_component_line(component)}")
    rich_console.print()


def _reason_text(*, reason: str, components: list[dict[str, Any]]) -> str:
    """Return the reason line, naming the inputs that moved as the JSON does."""
    inputs = [
        str(item["component"]).removeprefix("inputs.")
        for item in components
        if str(item.get("component", "")).startswith("inputs.")
    ]
    if reason == "input_changed" and inputs:
        noun = "input" if len(inputs) == 1 else "inputs"
        return f"{noun} {escape(', '.join(inputs))} changed"
    return _REASON_TEXT.get(reason, escape(reason))


def _component_line(component: dict[str, Any]) -> str:
    """Return one moved component with its prior and current values, shortened."""
    name = escape(str(component.get("component")))
    values = [_short_value(component[side]) for side in ("prior", "current") if side in component]
    status = str(component.get("status"))
    detail = f"{status}: {' → '.join(values)}" if values else status
    return f"{name} [dim]({escape(detail)})[/]"


def _short_value(value: object) -> str:
    """Return a component value short enough to read beside its name."""
    text = str(value)
    return text if len(text) <= 16 else f"{text[:12]}…"


def _render_tracking_label(*, param: object, label: object) -> str:
    """Return one ``  param: label`` line, styled and escaped for Rich.

    ``escape`` guards against a param name or value that happens to look
    like Rich markup (``[file]``, say) being swallowed as a tag rather than
    printed.
    """
    label_str = str(label)
    style = _TRACKING_STYLES.get(label_str)
    text = f"[{style}]{escape(label_str)}[/{style}]" if style else escape(label_str)
    line = f"[bold]{escape(str(param))}[/]: {text}"
    hint = _TRACKING_HINTS.get(label_str)
    if hint:
        line += f" [dim]({hint})[/dim]"
    return line


def explain_run_cache(*, reader: Query, run_id: str) -> dict[str, object]:
    """Return the cache explanation for every task in a run.

    The explaining is :meth:`~ginkgo.query.Query.explain_rerun`'s; this walks
    the run's tasks and renders what it returns.
    """
    summary = reader.run(run_id).to_payload()
    tasks = summary.get("tasks", [])
    explanations = [
        reader.explain_rerun(run_id, str(task["task_id"])).to_payload()
        for task in tasks
        if isinstance(task, dict) and task.get("task_id")
    ]
    return {
        "run_id": summary.get("run_id"),
        "workflow": summary.get("workflow"),
        "tasks": explanations,
    }
