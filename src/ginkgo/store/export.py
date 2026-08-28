"""Export a run's projections as the snapshot its directory keeps.

``runs/<id>/manifest.yaml`` is written once, when the run finishes, and ginkgo
never reads it again except through ``ginkgo db rebuild``. That makes its shape
free: it is the projection tables serialised, table by table, rather than a
second model of a run that would have to be kept in step with the first.

Because the snapshot *is* the projections, rebuild is the inverse of this
module and nothing else — see :mod:`ginkgo.store.rebuild`.
"""

from __future__ import annotations

import json
from typing import Any

from ginkgo.store.protocol import ProvenanceStore

__all__ = ["SNAPSHOT_VERSION", "TABLES", "export_manifest"]


SNAPSHOT_VERSION = 1
"""Bumped when the snapshot's shape changes in a way rebuild must notice."""

RUN_COLUMNS = (
    "run_id",
    "workflow",
    "status",
    "started_at",
    "finished_at",
    "error",
    "jobs",
    "cores",
    "memory",
    "params",
    "param_sources",
    "resources",
    "timings",
    "parent_run_id",
    "parent_task_id",
    "ginkgo_version",
)

TASK_COLUMNS = (
    "run_id",
    "task_id",
    "node_id",
    "name",
    "display_label",
    "kind",
    "execution_mode",
    "env",
    "status",
    "cached",
    "cache_key",
    "source_hash",
    "version",
    "env_hash",
    "extra_source_hash",
    "started_at",
    "finished_at",
    "attempts",
    "max_attempts",
    "exit_code",
    "failure",
    "output_summary",
    "resource_usage",
    "timings",
    "extra",
    "stdout_log",
    "stderr_log",
    "execution_backend",
    "remote_job_id",
)

ATTEMPT_COLUMNS = (
    "run_id",
    "task_id",
    "attempt",
    "started_at",
    "finished_at",
    "status",
    "exit_code",
    "failure",
    "retry_delay_s",
    "execution_backend",
    "remote_job_id",
)

INPUT_COLUMNS = (
    "run_id",
    "task_id",
    "param",
    "position",
    "value_type",
    "value_summary",
    "digest",
    "artifact_id",
    "asset_key",
    "asset_version_id",
    "remote_uri",
)

OUTPUT_COLUMNS = (
    "run_id",
    "task_id",
    "position",
    "name",
    "value_type",
    "path",
    "artifact_id",
    "asset_key",
    "asset_version_id",
)

EDGE_COLUMNS = ("run_id", "src_kind", "src_id", "dst_kind", "dst_id", "edge")

TABLES: dict[str, tuple[tuple[str, ...], str]] = {
    "runs": (RUN_COLUMNS, "run_id = ?"),
    "tasks": (TASK_COLUMNS, "run_id = ? ORDER BY node_id"),
    "attempts": (ATTEMPT_COLUMNS, "run_id = ? ORDER BY task_id, attempt"),
    "task_inputs": (INPUT_COLUMNS, "run_id = ? ORDER BY task_id, param, position"),
    "task_outputs": (OUTPUT_COLUMNS, "run_id = ? ORDER BY task_id, position"),
    "edges": (EDGE_COLUMNS, "run_id = ? ORDER BY edge, src_id, dst_id"),
}
"""Each projection table: the columns the snapshot carries, and how to order them."""

# Columns whose value is JSON in SQLite. The snapshot decodes them so a human
# reading manifest.yaml sees a mapping rather than a quoted string; rebuild
# re-encodes on the way back in.
JSON_COLUMNS = frozenset(
    {
        "params",
        "param_sources",
        "resources",
        "timings",
        "failure",
        "output_summary",
        "resource_usage",
        "extra",
        "env_hash",
    }
)


def export_manifest(store: ProvenanceStore, run_id: str) -> dict[str, Any]:
    """Return the snapshot for *run_id*.

    Parameters
    ----------
    store : ProvenanceStore
        Any open store; only reads are performed.
    run_id : str
        The run to export.

    Returns
    -------
    dict[str, Any]
        ``{"ginkgo_snapshot": 1, "runs": [...], "tasks": [...], ...}`` — one
        key per projection table, each a list of row mappings.

    Raises
    ------
    KeyError
        If the run has no row.
    """
    snapshot: dict[str, Any] = {"ginkgo_snapshot": SNAPSHOT_VERSION}
    for table, (columns, where) in TABLES.items():
        rows = store.query(
            f"SELECT {', '.join(columns)} FROM {table} WHERE {where}",  # noqa: S608 - fixed names
            (run_id,),
        )
        snapshot[table] = [_decode(dict(zip(columns, row))) for row in rows]
    if not snapshot["runs"]:
        raise KeyError(run_id)
    return snapshot


def _decode(row: dict[str, Any]) -> dict[str, Any]:
    """Return *row* with its JSON columns parsed."""
    for column in JSON_COLUMNS & row.keys():
        value = row[column]
        if isinstance(value, str):
            try:
                row[column] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return row


def encode(row: dict[str, Any]) -> dict[str, Any]:
    """Return *row* with its JSON columns re-encoded, ready to insert."""
    encoded = dict(row)
    for column in JSON_COLUMNS & encoded.keys():
        value = encoded[column]
        if value is not None and not isinstance(value, str):
            encoded[column] = json.dumps(value, sort_keys=True, default=str)
    return encoded
