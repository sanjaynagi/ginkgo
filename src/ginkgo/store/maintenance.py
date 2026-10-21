"""Reclaiming space in the ledger.

Two facts in the database grow without bound and neither is needed forever:
the raw ``events`` of a finished run, which the run tables already project
into the shape every reader wants, and the digest memo, whose rows are a
speed-up that costs nothing to lose.

Pruning them is the user's decision, never ginkgo's — a run's events are the
only place its full detail lives, and ``ginkgo export events`` reads them. So
these are functions the ``db`` command calls, not something a run does on its
way past.

Projections are never touched. Deleting the events of a finished run leaves
``runs``, ``tasks`` and the rest exactly as they were; what is lost is the
per-event detail, not the run.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ginkgo.formatting import format_bytes
from ginkgo.store.protocol import ProjectionOp, ProvenanceStore

__all__ = [
    "LARGE_LEDGER_BYTES",
    "LARGE_LEDGER_CODE",
    "large_ledger_warning",
    "on_disk_bytes",
    "prune_digest_memo",
    "prune_events",
    "vacuum",
]

LARGE_LEDGER_BYTES = 1024**3
"""The ledger size past which ``ginkgo doctor`` suggests pruning.

A fully cached run writes about 3.5 KiB per task, so this is a nightly
1,000-task workflow's ledger after roughly ten months."""

LARGE_LEDGER_CODE = "large_ledger"

_FINISHED_RUNS_BEFORE = "SELECT run_id FROM runs WHERE finished_at IS NOT NULL AND finished_at < ?"


def prune_events(store: ProvenanceStore, *, before: datetime, dry_run: bool = False) -> int:
    """Delete the events of runs that finished before *before*.

    A run still in flight is never touched, whatever its start time: its events
    are what the recorder is still writing.

    Parameters
    ----------
    store : ProvenanceStore
        An open write-mode store.
    before : datetime
        The cutoff. Runs that finished at or after it keep their events.
    dry_run : bool, optional
        Count what would go without deleting it.

    Returns
    -------
    int
        The number of event rows deleted, or that would be.
    """
    cutoff = before.isoformat()
    rows = store.query(
        f"SELECT count(*) AS n FROM events WHERE run_id IN ({_FINISHED_RUNS_BEFORE})",
        (cutoff,),
    )
    count = int(rows[0]["n"]) if rows else 0
    if count and not dry_run:
        with store.transaction():
            store.apply(
                [
                    ProjectionOp(
                        sql=f"DELETE FROM events WHERE run_id IN ({_FINISHED_RUNS_BEFORE})",
                        params=(cutoff,),
                    )
                ]
            )
    return count


def prune_digest_memo(store: ProvenanceStore, *, before: datetime, dry_run: bool = False) -> int:
    """Delete memoised digests not seen since *before*.

    Losing one costs a re-hash of the file it described, so the cutoff can be
    aggressive.

    Parameters
    ----------
    store : ProvenanceStore
        An open write-mode store.
    before : datetime
        Rows whose ``last_seen`` is older than this go.
    dry_run : bool, optional
        Count what would go without deleting it.

    Returns
    -------
    int
        The number of memo rows deleted, or that would be.
    """
    cutoff = before.isoformat()
    rows = store.query("SELECT count(*) AS n FROM digest_memo WHERE last_seen < ?", (cutoff,))
    count = int(rows[0]["n"]) if rows else 0
    if count and not dry_run:
        with store.transaction():
            store.apply(
                [ProjectionOp(sql="DELETE FROM digest_memo WHERE last_seen < ?", params=(cutoff,))]
            )
    return count


def vacuum(store: ProvenanceStore) -> None:
    """Rebuild the database file, returning freed pages to the filesystem.

    Deleting rows leaves the file the size it was; this is what shrinks it.
    It rewrites the whole database, so it wants exclusive access and a moment.
    """
    store.query("VACUUM")


def on_disk_bytes(path: Path) -> int:
    """Return what the database at *path* occupies, its write-ahead log included."""
    files = (path, path.with_name(f"{path.name}-wal"), path.with_name(f"{path.name}-shm"))
    return sum(file.stat().st_size for file in files if file.is_file())


def large_ledger_warning(path: Path, *, threshold: int = LARGE_LEDGER_BYTES) -> str | None:
    """Return a warning when the ledger at *path* has outgrown *threshold*.

    Parameters
    ----------
    path : Path
        The ledger database.
    threshold : int, optional
        Size in bytes, write-ahead log included, above which to warn.

    Returns
    -------
    str | None
        The warning, or ``None`` when the ledger is smaller.
    """
    size = on_disk_bytes(path)
    if size <= threshold:
        return None
    return (
        f"The ledger {path} is {format_bytes(size)}. Every run adds to it, cached "
        "tasks included, and nothing is removed unless you prune it."
    )
