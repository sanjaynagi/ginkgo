"""Rebuild the projections from the snapshots the run directories kept.

The database is the index, not the archive: lose it and the runs are still on
disk, one ``manifest.yaml`` each. Because that snapshot is the projection rows
serialised (:mod:`ginkgo.store.export`), rebuilding is a re-insert — there is
no parsing of an older shape and no reconstruction of facts, and a rebuilt
database is byte-for-byte the same projection the run wrote.

Only snapshots this exporter wrote are read. A run directory holding anything
else is skipped with one warning; nothing is migrated and nothing is guessed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ginkgo.store.export import SNAPSHOT_VERSION, TABLES, encode
from ginkgo.store.protocol import ProjectionOp, ProvenanceStore
from ginkgo.workspace_layout import WorkspaceLayout

__all__ = ["RebuildResult", "rebuild"]


@dataclass(kw_only=True)
class RebuildResult:
    """What one ``db rebuild`` pass did.

    Parameters
    ----------
    runs : list[str]
        Run ids re-inserted, in the order they were read.
    skipped : list[str]
        One message per run directory that held no usable snapshot.
    """

    runs: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


def rebuild(
    store: ProvenanceStore,
    *,
    layout: WorkspaceLayout,
    runs: bool = True,
    dry_run: bool = False,
) -> RebuildResult:
    """Re-insert projection rows from every run snapshot under *layout*.

    Idempotent: a run already present is replaced by its snapshot, so running
    this twice leaves the same rows as running it once.

    Parameters
    ----------
    store : ProvenanceStore
        A write-mode store. Ignored when *dry_run* is set.
    layout : WorkspaceLayout
        The workspace whose ``runs/`` directory is read.
    runs : bool, optional
        Rebuild the run projections. The only source Phase 1 has; the cache and
        asset flags arrive with the phases that populate those tables.
    dry_run : bool, optional
        Report what would be rebuilt without writing.

    Returns
    -------
    RebuildResult
        The runs found and the directories skipped.
    """
    result = RebuildResult()
    if not runs or not layout.runs.is_dir():
        return result

    for run_dir in sorted(path for path in layout.runs.iterdir() if path.is_dir()):
        snapshot = _load_snapshot(run_dir)
        if isinstance(snapshot, str):
            result.skipped.append(snapshot)
            continue
        run_id = str(snapshot["runs"][0].get("run_id") or run_dir.name)
        result.runs.append(run_id)
        if dry_run:
            continue
        with store.transaction():
            store.apply(_operations(run_id=run_id, snapshot=snapshot))
    return result


def _load_snapshot(run_dir: Path) -> dict[str, Any] | str:
    """Return the snapshot in *run_dir*, or the reason it cannot be used."""
    path = run_dir / "manifest.yaml"
    if not path.is_file():
        return f"{run_dir}: no manifest.yaml"
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        return f"{run_dir}: manifest.yaml is not readable ({exc})"
    if not isinstance(data, dict) or data.get("ginkgo_snapshot") != SNAPSHOT_VERSION:
        return f"{run_dir}: not a ginkgo snapshot of version {SNAPSHOT_VERSION}"
    if not isinstance(data.get("runs"), list) or not data["runs"]:
        return f"{run_dir}: snapshot has no run row"
    return data


def _operations(*, run_id: str, snapshot: dict[str, Any]) -> list[ProjectionOp]:
    """Return the statements that replace *run_id*'s rows with the snapshot's."""
    ops: list[ProjectionOp] = []
    for table, (columns, _) in TABLES.items():
        ops.append(
            ProjectionOp(
                sql=f"DELETE FROM {table} WHERE run_id = ?",  # noqa: S608 - fixed names
                params=(run_id,),
            )
        )
        placeholders = ", ".join("?" for _ in columns)
        insert = (
            f"INSERT INTO {table} ({', '.join(columns)}) "  # noqa: S608 - fixed names
            f"VALUES ({placeholders})"
        )
        for row in snapshot.get(table) or []:
            if not isinstance(row, dict):
                continue
            encoded = encode(row)
            ops.append(
                ProjectionOp(sql=insert, params=tuple(encoded.get(name) for name in columns))
            )
    ops.append(
        ProjectionOp(
            sql="UPDATE runs SET snapshot_written = 1 WHERE run_id = ?",
            params=(run_id,),
        )
    )
    return ops
