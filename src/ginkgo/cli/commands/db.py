"""Provenance-database command handlers.

``ginkgo db`` is the maintenance surface for the ledger at
``.ginkgo/ginkgo.db``: create or upgrade it, check that it is intact, delete
history nothing reads any more, reclaim the space that frees, and say where the
file is.

``check`` reports rather than repairs, and it asks each owner about its own
half: SQLite for the file's integrity, the cache and the artifact store for
whether their rows and their bytes still agree, the staging cache for its
downloads, and this module for whether the ledger's runs and the run
directories beside it still describe the same set of runs.

Every subcommand but ``path`` opens the database in write mode, so each needs
write access to the workspace: ``check`` reports on the database a run would
use, which means creating it if it is not there yet.
"""

from __future__ import annotations

from ginkgo.cli.common import stdout_console
from ginkgo.formatting import cutoff_before, format_int
from ginkgo.remote.staging import StagingCache
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.maintenance import prune_digest_memo, prune_events, vacuum
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


def command_db(args) -> int:
    """Handle ``ginkgo db`` subcommands."""
    layout = WorkspaceLayout.relative()
    path = layout.db
    rich_console = stdout_console()

    if args.db_command == "path":
        rich_console.print(str(path))
        return 0

    if args.db_command == "migrate":
        rich_console.print("[bold green]🌿 ginkgo db migrate[/]\n")
        with open_store(path) as store:
            rich_console.print(f"[green]✓[/] {path} at schema version {store.schema_version}")
        return 0

    if args.db_command == "vacuum":
        rich_console.print("[bold green]🌿 ginkgo db vacuum[/]\n")
        before = path.stat().st_size if path.is_file() else 0
        with open_store(path) as store:
            vacuum(store)
        after = path.stat().st_size if path.is_file() else 0
        rich_console.print(f"[green]✓[/] {path}: {format_int(before)} → {format_int(after)} bytes")
        return 0

    if args.db_command == "prune":
        return _prune(args, path=path, rich_console=rich_console)

    return _check(layout, rich_console=rich_console)


def _prune(args, *, path, rich_console) -> int:
    """Delete history older than the cutoffs the user named."""
    if args.events_older_than is None and args.digest_memo_older_than is None:
        rich_console.print(
            "[red]Error:[/] provide --events-older-than or --digest-memo-older-than."
        )
        return 1

    rich_console.print("[bold green]🌿 ginkgo db prune[/]\n")
    try:
        cutoffs = {
            "events": (
                None
                if args.events_older_than is None
                else cutoff_before(args.events_older_than, option="--events-older-than")
            ),
            "memo": (
                None
                if args.digest_memo_older_than is None
                else cutoff_before(args.digest_memo_older_than, option="--digest-memo-older-than")
            ),
        }
    except ValueError as exc:
        rich_console.print(f"[red]Error:[/] {exc}")
        return 1

    with open_store(path) as store:
        if cutoffs["events"] is not None:
            count = prune_events(store, before=cutoffs["events"], dry_run=args.dry_run)
            rich_console.print(_line("event", count, dry_run=args.dry_run))
        if cutoffs["memo"] is not None:
            count = prune_digest_memo(store, before=cutoffs["memo"], dry_run=args.dry_run)
            rich_console.print(_line("digest memo", count, dry_run=args.dry_run))

    if not args.dry_run:
        rich_console.print("\nRun [bold]ginkgo db vacuum[/] to release the freed space.")
    return 0


def _line(noun: str, count: int, *, dry_run: bool) -> str:
    """Describe what a prune did, or would do."""
    verb = "would delete" if dry_run else "deleted"
    return f"[green]✓[/] {verb} {format_int(count)} {noun} row{'' if count == 1 else 's'}"


def _check(layout, *, rich_console) -> int:
    """Report every way the ledger and the bytes beside it disagree."""
    rich_console.print("[bold green]🌿 ginkgo db check[/]\n")
    path = layout.db
    # Write mode, so checking an empty workspace creates the database rather
    # than reporting the absence of one as a fault.
    with open_store(path) as store:
        rich_console.print(f"Database: {path}")
        rich_console.print(f"Schema version: {store.schema_version}")
        problems = [row[0] for row in store.query("PRAGMA integrity_check") if row[0] != "ok"]
        problems += _run_directory_problems(store, layout=layout)

    # Each index knows where its own bytes are, so each is what reports on them.
    with CacheIndex.open(path=path) as index:
        cache = CacheStore(index=index, root=layout.cache)
        problems += cache.integrity_problems()
        problems += cache.artifact_store_view.integrity_problems()
        problems += _env_drift(index)

    problems += StagingCache(root=layout.staging, db_path=path).integrity_problems()

    if problems:
        for problem in problems:
            rich_console.print(f"[red]✖[/] {problem}")
        return 1

    rich_console.print("[green]✓[/] integrity check passed")
    return 0


def _run_directory_problems(store, *, layout) -> list[str]:
    """Return the runs and run directories that have no counterpart.

    Both directions matter and mean different things. A row with no directory
    is a run whose logs and manifest were deleted — the record survives, the
    evidence does not. A directory with no row is bytes from a database that is
    gone, which nothing will ever read again.
    """
    recorded = {str(row["run_id"]) for row in store.query("SELECT run_id FROM runs")}
    problems = [
        f"run {run_id} has a row but no run directory"
        for run_id in sorted(recorded)
        if not (layout.runs / run_id).is_dir()
    ]
    if not layout.runs.is_dir():
        return problems
    problems += [
        f"run directory {entry.name} has no row (orphan)"
        for entry in sorted(layout.runs.iterdir())
        if entry.is_dir() and entry.name not in recorded
    ]
    return problems


def _env_drift(index: CacheIndex) -> list[str]:
    """Return the declared environments that materialised differently per host.

    Two machines resolving the same declaration to different dependencies is
    not corruption, but it is why a cache key can be shared and a result not
    be, so it is worth seeing.
    """
    by_env: dict[str, set[str]] = {}
    hosts: dict[str, set[str]] = {}
    for row in index.env_materializations():
        env_hash = str(row["env_hash"])
        by_env.setdefault(env_hash, set()).add(str(row["materialized_digest"]))
        hosts.setdefault(env_hash, set()).add(str(row["host"]))
    return [
        f"environment {env_hash} materialized {len(digests)} different ways "
        f"across {len(hosts[env_hash])} hosts"
        for env_hash, digests in sorted(by_env.items())
        if len(digests) > 1
    ]
