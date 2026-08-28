"""Provenance-database command handlers.

``ginkgo db`` is the maintenance surface for the ledger at
``.ginkgo/ginkgo.db``: create or upgrade it, check that it is intact, and say
where it is. Later phases add ``vacuum`` and ``prune`` beside these.

Every subcommand but ``path`` opens the database in write mode, so each needs
write access to the workspace: ``check`` reports on the database a run would
use, which means creating it if it is not there yet.
"""

from __future__ import annotations

import sys

from ginkgo.cli.commands.cache import orphan_cache_dirs
from ginkgo.cli.common import console
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


def command_db(args) -> int:
    """Handle ``ginkgo db`` subcommands."""
    path = WorkspaceLayout.relative().db
    rich_console = console(sys.stdout)

    if args.db_command == "path":
        rich_console.print(str(path))
        return 0

    if args.db_command == "migrate":
        rich_console.print("[bold green]🌿 ginkgo db migrate[/]\n")
        with open_store(path) as store:
            rich_console.print(f"[green]✓[/] {path} at schema version {store.schema_version}")
        return 0

    rich_console.print("[bold green]🌿 ginkgo db check[/]\n")
    # Write mode, so checking an empty workspace creates the database rather
    # than reporting the absence of one as a fault.
    with open_store(path) as store:
        rich_console.print(f"Database: {path}")
        rich_console.print(f"Schema version: {store.schema_version}")
        problems = [row[0] for row in store.query("PRAGMA integrity_check") if row[0] != "ok"]
        problems += _cache_problems(store)

    if problems:
        for problem in problems:
            rich_console.print(f"[red]✖[/] {problem}")
        return 1

    rich_console.print("[green]✓[/] integrity check passed")
    return 0


def _cache_problems(store) -> list[str]:
    """Return the ways the cache index and the bytes on disk disagree.

    The database is the only cache index, so the two can only drift by losing
    one side: an entry whose ``output.json`` was deleted is a row that will
    never hit, a directory with no row is bytes nothing will ever find, and an
    artifact a cache entry names but the store does not hold is a restore that
    will fail. Each is reported rather than repaired — ``ginkgo cache clear
    --orphans`` removes the second, and re-running the workflow fixes the rest.
    """
    layout = WorkspaceLayout.relative()
    cache_root = layout.cache
    blobs = layout.artifacts / "blobs"
    trees = layout.artifacts / "trees"
    problems: list[str] = []

    keys = [str(row["cache_key"]) for row in store.query("SELECT cache_key FROM cache_entries")]
    missing_bytes = [key for key in keys if not (cache_root / key / "output.json").is_file()]
    for key in sorted(missing_bytes):
        problems.append(f"cache entry {key} has a row but no output.json")

    for entry in orphan_cache_dirs(cache_root=cache_root, known_keys=keys):
        problems.append(f"cache directory {entry.name} has no row (orphan)")

    for row in store.query(
        "SELECT DISTINCT a.artifact_id, a.kind FROM artifacts a "
        "JOIN cache_artifacts c ON c.artifact_id = a.artifact_id"
    ):
        artifact_id = str(row["artifact_id"])
        bytes_path = (
            trees / f"{artifact_id}.json" if row["kind"] == "tree" else blobs / artifact_id
        )
        if not bytes_path.exists():
            problems.append(f"cache artifact {artifact_id} is missing from the artifact store")

    for row in store.query(
        "SELECT DISTINCT c.artifact_id FROM cache_artifacts c "
        "LEFT JOIN artifacts a ON a.artifact_id = c.artifact_id WHERE a.artifact_id IS NULL"
    ):
        problems.append(f"cache artifact {row['artifact_id']} has no artifact row")

    return problems
