"""Provenance-database command handlers.

``ginkgo db`` is the maintenance surface for the ledger at
``.ginkgo/ginkgo.db``: create or upgrade it, check that it is intact, delete
history nothing reads any more, reclaim the space that frees, and say where the
file is.

``check`` reports (``check --verify`` also rehashes every stored artifact
byte); ``check --fix`` first removes what nothing can use any more. Every half
is answered by whoever owns it: SQLite for the file's integrity, the cache and the artifact store for
whether their rows and their bytes agree, ``rundir`` for the run directories,
the staging cache for its downloads, the cache index for environment drift.
It is a read path — it opens the database read-only and never creates one.

``migrate``, ``prune``, ``vacuum`` and ``check --fix`` open in write mode and
so need write access to the workspace; ``check`` and ``path`` do not.
"""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from ginkgo.cli.commands.cache import gc_orphan_artifacts, remove_entry
from ginkgo.cli.common import console
from ginkgo.cli.errors import UsageError
from ginkgo.formatting import cutoff_before, format_bytes, format_int
from ginkgo.remote.staging import StagingCache
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.artifacts.artifact_store import make_writable_recursive
from ginkgo.runtime.rundir import (
    orphaned_run_directories,
    run_directory_problems,
    runs_without_ledger_warning,
)
from ginkgo.store.maintenance import on_disk_bytes, prune_digest_memo, prune_events, vacuum
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


def command_db(args) -> int:
    """Handle ``ginkgo db`` subcommands."""
    layout = WorkspaceLayout.relative()
    path = layout.db
    rich_console = console(sys.stdout)

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
        before = on_disk_bytes(path)
        with open_store(path) as store:
            vacuum(store)
        # Measured after the store closes, which checkpoints the write-ahead
        # log the vacuum wrote through and removes it.
        after = on_disk_bytes(path)
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
        rich_console.print(
            f"[green]✓[/] {path}: {format_bytes(before)} → {format_bytes(after)} on disk, "
            "write-ahead log included"
        )
        return 0

    if args.db_command == "prune":
        return _prune(args, path=path, rich_console=rich_console)

    return _check(layout, verify=args.verify, fix=args.fix, rich_console=rich_console)


def _prune(args, *, path, rich_console) -> int:
    """Delete history older than the cutoffs the user named."""
    if (
        args.events_older_than is None
        and args.digest_memo_older_than is None
        and args.staging_older_than is None
    ):
        raise UsageError(
            "Provide --events-older-than, --digest-memo-older-than or --staging-older-than."
        )

    rich_console.print("[bold green]🌿 ginkgo db prune[/]\n")
    # Each duration was validated as the command line was parsed.
    cutoffs = {
        "events": None
        if args.events_older_than is None
        else cutoff_before(args.events_older_than),
        "memo": (
            None
            if args.digest_memo_older_than is None
            else cutoff_before(args.digest_memo_older_than)
        ),
        "staging": (
            None if args.staging_older_than is None else cutoff_before(args.staging_older_than)
        ),
    }

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


@dataclass(frozen=True, kw_only=True)
class LedgerProblems:
    """What ``ginkgo db check`` finds wrong with a workspace.

    Attributes
    ----------
    corrupt : list[str]
        The database's own integrity failures, from SQLite.
    runs : list[str]
        Runs without directories, and run directories without rows.
    drift : list[str]
        Every other way the ledger and the bytes beside it disagree: cache
        directories without rows, rows without bytes, staged inputs, artifacts
        and environments.
    """

    corrupt: list[str] = field(default_factory=list)
    runs: list[str] = field(default_factory=list)
    drift: list[str] = field(default_factory=list)


def ledger_problems(layout: WorkspaceLayout, *, verify: bool = False) -> LedgerProblems:
    """Return every problem ``ginkgo db check`` reports for an existing ledger.

    Read-only: the database is opened read-only, and nothing is repaired.

    Parameters
    ----------
    layout : WorkspaceLayout
        The workspace; its database must exist.
    verify : bool
        Also rehash every stored artifact byte.

    Returns
    -------
    LedgerProblems
        One sentence per problem.

    Raises
    ------
    StoreError
        If the database cannot be opened at all.
    """
    path = layout.db
    with open_store(path, readonly=True) as store:
        corrupt = [row[0] for row in store.query("PRAGMA integrity_check") if row[0] != "ok"]
        recorded = {str(row["run_id"]) for row in store.query("SELECT run_id FROM runs")}
    runs = run_directory_problems(recorded_run_ids=recorded, root=layout.runs)

    # Each index knows where its own bytes are, so each is what reports on them.
    with CacheIndex.open(path=path, readonly=True) as index:
        cache = CacheStore(index=index, root=layout.cache)
        drift = cache.integrity_problems()
        drift += cache.artifact_store_view.integrity_problems(verify=verify)
        drift += index.env_drift_problems()

    drift += StagingCache(db_path=path).integrity_problems()
    return LedgerProblems(corrupt=corrupt, runs=runs, drift=drift)


def _check(layout, *, verify: bool, fix: bool, rich_console) -> int:
    """Report every way the ledger and the bytes beside it disagree.

    A read path throughout unless *fix* is set: it opens the database
    read-only and never creates one. A workspace with no database is usually
    not a fault — nothing has run there — so it says so and succeeds. It only
    says that once the run directories agree: a workspace holding runs whose
    ledger is gone gets the warning instead, and still exits 0, because
    nothing here is corrupt and a missing ledger is not something ``db`` can
    repair. Creating the database is ``db migrate``'s job, and ``ginkgo run``'s.

    With *verify* the artifact store also rehashes every stored byte, which
    catches a blob corrupted in place — something no row comparison can see.
    With *fix* the bytes nothing can use any more are removed first (see
    :func:`_repair`), and what is left is reported.
    """
    rich_console.print("[bold green]🌿 ginkgo db check[/]\n")
    path = layout.db
    if fix:
        for line in _repair(layout):
            rich_console.print(f"[green]✓[/] {line}")
    if not path.is_file():
        warning = runs_without_ledger_warning(layout=layout)
        if warning is not None:
            rich_console.print(f"[yellow]⚠[/] {warning}")
            rich_console.print(
                "[dim]Restore the database if their history is wanted, or remove them with "
                "`ginkgo db check --fix`.[/]"
            )
            return 0
        rich_console.print(f"[green]✓[/] no database at {path}: nothing has run in this workspace")
        return 0

    with open_store(path, readonly=True) as store:
        rich_console.print(f"Database: {path}")
        rich_console.print(f"Schema version: {store.schema_version}")
    found = ledger_problems(layout, verify=verify)
    problems = [*found.corrupt, *found.runs, *found.drift]
    if problems:
        for problem in problems:
            rich_console.print(f"[red]✖[/] {problem}")
        if (found.runs or found.drift) and not fix:
            rich_console.print(
                "[dim]`ginkgo db check --fix` removes the directories, rows and files nothing "
                "can use any more; a run row without its directory is kept.[/]"
            )
        return 1

    rich_console.print("[green]✓[/] integrity check passed")
    return 0


def _repair(layout: WorkspaceLayout) -> list[str]:
    """Remove what no ledger row reaches and no run can use, and say what went.

    Run directories the ledger has no row for, cache directories without a
    row, cache rows whose bytes are gone or unreadable (each would only miss),
    the artifacts no entry names, and a write-ahead log left behind by a
    database that was moved or deleted. A run row whose directory is gone is
    kept: the ledger is the only record of that run left.

    Returns
    -------
    list[str]
        One line per kind of thing removed; empty when nothing was.
    """
    lines: list[str] = []
    if not layout.db.is_file():
        sidecars = [
            sidecar
            for suffix in ("-wal", "-shm")
            if (sidecar := layout.db.with_name(f"{layout.db.name}{suffix}")).is_file()
        ]
        for sidecar in sidecars:
            sidecar.unlink()
        if sidecars:
            names = ", ".join(sidecar.name for sidecar in sidecars)
            lines.append(f"removed {names}, left behind without {layout.db.name}")
        recorded: set[str] = set()
    else:
        with open_store(layout.db, readonly=True) as store:
            recorded = {str(row["run_id"]) for row in store.query("SELECT run_id FROM runs")}

    stranded = orphaned_run_directories(recorded_run_ids=recorded, root=layout.runs)
    for name in stranded:
        _remove_tree(layout.runs / name)
    if stranded:
        noun = "directory" if len(stranded) == 1 else "directories"
        lines.append(f"removed {len(stranded)} run {noun} the ledger has no row for")

    if not layout.db.is_file():
        return lines

    with CacheIndex.open(path=layout.db) as index:
        cache = CacheStore(index=index, root=layout.cache)
        unusable = cache.unusable_entry_keys()
        for key in unusable:
            remove_entry(cache, key)
        orphans = cache.orphan_entry_dirs()
        for entry_dir in orphans:
            _remove_tree(entry_dir)
        gc_orphan_artifacts(cache)
    if unusable:
        noun = "entry" if len(unusable) == 1 else "entries"
        lines.append(f"removed {len(unusable)} cache {noun} whose bytes are missing or unreadable")
    if orphans:
        noun = "directory" if len(orphans) == 1 else "directories"
        lines.append(f"removed {len(orphans)} cache {noun} the ledger has no row for")
    return lines


def _remove_tree(path: Path) -> None:
    """Remove a directory whose files may have been made read-only."""
    make_writable_recursive(path)
    shutil.rmtree(path)
