"""Provenance-database command handlers.

``ginkgo db`` is the maintenance surface for the ledger at
``.ginkgo/ginkgo.db``: create or upgrade it, check that it is intact, delete
history nothing reads any more, reclaim the space that frees, and say where the
file is.

``check`` reports rather than repairs, and every half is answered by whoever
owns it: SQLite for the file's integrity, the cache and the artifact store for
whether their rows and their bytes agree, the cache and the asset catalog for
whether a replayed asset still has a row, ``rundir`` for the run directories,
the staging cache for its downloads, the cache index for environment drift.
It is a read path — it opens the database read-only and never creates one.

``migrate``, ``prune`` and ``vacuum`` open in write mode and so need write
access to the workspace; ``check`` and ``path`` do not.
"""

from __future__ import annotations

from ginkgo.cli.common import stdout_console
from ginkgo.formatting import cutoff_before, format_bytes, format_int
from ginkgo.remote.staging import StagingCache
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.rundir import run_directory_problems
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
        if after >= before:
            # SQLite rebuilds into a new file and swaps it in, which it cannot
            # do while another connection holds the database open. It reports
            # no error when that happens, so an unchanged size is the only
            # signal there is.
            rich_console.print(
                f"[yellow]•[/] {path}: no space reclaimed — either there was none to "
                "reclaim, or another process is reading the database"
            )
            return 0
        rich_console.print(f"[green]✓[/] {path}: {format_int(before)} → {format_int(after)} bytes")
        return 0

    if args.db_command == "prune":
        return _prune(args, path=path, rich_console=rich_console)

    return _check(layout, rich_console=rich_console)


def _prune(args, *, path, rich_console) -> int:
    """Delete history older than the cutoffs the user named."""
    if (
        args.events_older_than is None
        and args.digest_memo_older_than is None
        and args.staging_older_than is None
    ):
        rich_console.print(
            "[red]Error:[/] provide --events-older-than, --digest-memo-older-than "
            "or --staging-older-than."
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
            "staging": (
                None
                if args.staging_older_than is None
                else cutoff_before(args.staging_older_than, option="--staging-older-than")
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

    if cutoffs["staging"] is not None:
        # The staging cache owns its bytes as well as its rows, so it prunes
        # itself rather than handing SQL to the maintenance module.
        entries, freed = StagingCache(db_path=path).prune(
            before=cutoffs["staging"].isoformat(), dry_run=args.dry_run
        )
        verb = "would delete" if args.dry_run else "deleted"
        rich_console.print(
            f"[green]✓[/] {verb} {format_int(entries)} staged "
            f"input{'' if entries == 1 else 's'} ({format_bytes(freed)})"
        )

    if not args.dry_run:
        rich_console.print("\nRun [bold]ginkgo db vacuum[/] to release the freed space.")
    return 0


def _line(noun: str, count: int, *, dry_run: bool) -> str:
    """Describe what a prune did, or would do."""
    verb = "would delete" if dry_run else "deleted"
    return f"[green]✓[/] {verb} {format_int(count)} {noun} row{'' if count == 1 else 's'}"


def _check(layout, *, rich_console) -> int:
    """Report every way the ledger and the bytes beside it disagree.

    A read path throughout: it opens the database read-only and never creates
    one. A workspace with no database is not a fault — nothing has run there —
    so it says so and succeeds. Creating the database is ``db migrate``'s job,
    and ``ginkgo run``'s.
    """
    rich_console.print("[bold green]🌿 ginkgo db check[/]\n")
    path = layout.db
    if not path.is_file():
        rich_console.print(f"[green]✓[/] no database at {path}: nothing has run in this workspace")
        return 0

    with open_store(path, readonly=True) as store:
        rich_console.print(f"Database: {path}")
        rich_console.print(f"Schema version: {store.schema_version}")
        problems = [row[0] for row in store.query("PRAGMA integrity_check") if row[0] != "ok"]
        recorded = {str(row["run_id"]) for row in store.query("SELECT run_id FROM runs")}
    problems += run_directory_problems(recorded_run_ids=recorded, root=layout.runs)

    # Each index knows where its own bytes are, so each is what reports on them.
    with CacheIndex.open(path=path, readonly=True) as index:
        cache = CacheStore(index=index, root=layout.cache)
        problems += cache.integrity_problems()
        problems += cache.artifact_store_view.integrity_problems()
        problems += cache.asset_reference_problems(assets=AssetStore.attached_to(index))
        problems += index.env_drift_problems()

    problems += StagingCache(db_path=path).integrity_problems()

    if problems:
        for problem in problems:
            rich_console.print(f"[red]✖[/] {problem}")
        return 1

    rich_console.print("[green]✓[/] integrity check passed")
    return 0
