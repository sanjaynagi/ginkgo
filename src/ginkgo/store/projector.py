"""Turn one ledger event into the projection rows it implies.

The ``events`` table is the record of what happened; every other run table is
a view of it maintained forward. This module is the whole of that maintenance:
one pure function per event type, each returning the
:class:`~ginkgo.store.protocol.ProjectionOp` list that event causes. Nothing
here touches a connection, so a projector can be tested by comparing SQL.

Events reach this module as :class:`~ginkgo.store.protocol.StoredEvent` rows,
never as ``GinkgoEvent`` objects: ``store/`` sits below ``runtime/`` and must
not import from it.

Facts that the ledger records but no reader filters or joins on stay inside the
JSON columns (``tasks.extra``, ``tasks.output_summary``). The narrow columns
beside them — ``task_outputs``, ``edges`` — are indexes over those same facts,
kept so lineage queries do not have to parse JSON.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from ginkgo.store.jsonio import dumps, dumps_or_none
from ginkgo.store.protocol import ProjectionOp, StoredEvent

__all__ = ["TERMINAL_EVENTS", "accumulate_seconds", "projection_ops"]


TERMINAL_EVENTS = frozenset({"task_completed", "task_failed", "run_completed"})
"""Events after which the writer commits, so a reader can see them at once."""

_ABSENT = object()
"""A parameter that was hashed but has no rendered value to summarise."""


def projection_ops(event: StoredEvent) -> list[ProjectionOp]:
    """Return the projection updates *event* implies.

    Parameters
    ----------
    event : StoredEvent
        A stored ledger row; its ``payload`` is the event's JSON.

    Returns
    -------
    list[ProjectionOp]
        Statements to run in the same transaction as the event's append. An
        event with no projection effect returns an empty list.
    """
    handler = _HANDLERS.get(event.type)
    if handler is None:
        return []
    return handler(event, json.loads(event.payload))


# ---------------------------------------------------------------- run events


def _run_started(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    ops = [
        ProjectionOp(
            sql="""
            INSERT INTO runs (
              run_id, workflow, status, started_at, jobs, cores, memory,
              params, param_sources, resources, timings,
              parent_run_id, parent_task_id, ginkgo_version
            ) VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, '{}', '{}', ?, ?, ?)
            ON CONFLICT (run_id) DO UPDATE SET
              workflow=excluded.workflow, started_at=excluded.started_at,
              jobs=excluded.jobs, cores=excluded.cores, memory=excluded.memory,
              params=excluded.params, param_sources=excluded.param_sources,
              parent_run_id=excluded.parent_run_id,
              parent_task_id=excluded.parent_task_id,
              ginkgo_version=excluded.ginkgo_version
            """,
            params=(
                event.run_id,
                payload.get("workflow") or "",
                event.ts,
                payload.get("jobs"),
                payload.get("cores"),
                payload.get("memory"),
                dumps(payload.get("params") or {}),
                dumps(payload.get("param_sources") or {}),
                payload.get("parent_run_id"),
                payload.get("parent_task_id"),
                payload.get("ginkgo_version"),
            ),
        )
    ]
    parent_run_id = payload.get("parent_run_id")
    parent_task_id = payload.get("parent_task_id")
    if parent_run_id and parent_task_id:
        # Recorded under the parent's run id: it is the parent's graph that
        # gains an edge, and the parent is who asks which child it spawned.
        ops.append(
            _edge(
                run_id=str(parent_run_id),
                src=("run", event.run_id),
                dst=("task", str(parent_task_id)),
                edge="child_of",
            )
        )
    return ops


def _run_resources_sampled(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    return [
        ProjectionOp(
            sql="UPDATE runs SET resources = ? WHERE run_id = ?",
            params=(dumps(payload.get("resources") or {}), event.run_id),
        )
    ]


def _run_completed(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    resources = payload.get("resources")
    return [
        ProjectionOp(
            sql="""
            UPDATE runs SET
              status = ?, finished_at = ?, error = ?,
              resources = coalesce(?, resources)
            WHERE run_id = ?
            """,
            params=(
                "succeeded" if payload.get("status") == "success" else "failed",
                payload.get("finished_at") or event.ts,
                payload.get("error"),
                dumps(resources) if resources else None,
                event.run_id,
            ),
        )
    ]


def _phase_timed(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    phase = str(payload.get("phase") or "")
    seconds = float(payload.get("seconds") or 0.0)
    # A cached task's execute phase rounds to zero and is still a phase that
    # ran; only a negative reading is nonsense.
    if not phase or seconds < 0:
        return []
    if event.task_id is None:
        return [
            accumulate_seconds(
                table="runs",
                column="timings",
                where="run_id = ?",
                where_params=(event.run_id,),
                key=phase,
                seconds=seconds,
            )
        ]
    return [
        accumulate_seconds(
            table="tasks",
            column="timings",
            where="run_id = ? AND task_id = ?",
            where_params=(event.run_id, event.task_id),
            key=phase,
            seconds=seconds,
        )
    ]


# --------------------------------------------------------------- graph events


def _graph_node_registered(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    task_id = str(payload.get("task_id") or "")
    retries = int(payload.get("retries") or 0)
    ops = [
        ProjectionOp(
            sql="""
            INSERT INTO tasks (
              run_id, task_id, node_id, name, kind, execution_mode, env,
              status, attempts, max_attempts, stdout_log, stderr_log,
              timings, extra
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, '{}', '{}')
            ON CONFLICT (run_id, task_id) DO NOTHING
            """,
            params=(
                event.run_id,
                task_id,
                int(payload.get("node_id", -1)),
                payload.get("task_name") or "unknown",
                payload.get("kind") or "python",
                payload.get("execution_mode") or "worker",
                payload.get("env"),
                retries + 1,
                payload.get("stdout_log"),
                payload.get("stderr_log"),
            ),
        )
    ]
    ops.extend(
        _edge(
            run_id=event.run_id,
            src=("task", str(dependency)),
            dst=("task", task_id),
            edge="depends_on",
        )
        for dependency in payload.get("dependency_ids") or []
    )
    return ops


def _graph_expanded(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    parent = str(payload.get("parent_task_id") or "")
    return [
        _edge(
            run_id=event.run_id,
            src=("task", str(child)),
            dst=("task", parent),
            edge="dynamic_depends_on",
        )
        for child in payload.get("new_node_ids") or []
    ]


# ---------------------------------------------------------------- task events


def _task_planned(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    task_id = event.task_id or ""
    ops = [
        ProjectionOp(
            sql="""
            UPDATE tasks SET
              cache_key = coalesce(?, cache_key),
              source_hash = coalesce(?, source_hash),
              version = coalesce(?, version),
              env_hash = coalesce(?, env_hash),
              extra_source_hash = coalesce(?, extra_source_hash),
              display_label = coalesce(?, display_label)
            WHERE run_id = ? AND task_id = ?
            """,
            params=(
                payload.get("cache_key"),
                payload.get("source_hash"),
                payload.get("version"),
                payload.get("env_hash"),
                payload.get("extra_source_hash"),
                payload.get("display_label"),
                event.run_id,
                task_id,
            ),
        ),
        # Replaced wholesale: a re-plan after a dynamic expansion resolves the
        # same parameters again, and a stale row would outlive its parameter.
        ProjectionOp(
            sql="DELETE FROM task_inputs WHERE run_id = ? AND task_id = ?",
            params=(event.run_id, task_id),
        ),
    ]

    hashes = {
        str(entry.get("param")): entry
        for entry in payload.get("input_hashes") or []
        if isinstance(entry, dict)
    }
    inputs = payload.get("inputs") or {}
    declared_assets = payload.get("asset_inputs") or {}
    # Every parameter that was hashed gets a row, including one the rendered
    # arguments do not name (a tmp_dir, say): the digest is the fact worth
    # keeping, and a missing summary is not a reason to drop it.
    parameters = {str(param): inputs.get(param, _ABSENT) for param in {**inputs, **hashes}}
    for param, value in parameters.items():
        entry = hashes.get(str(param), {})
        asset = _asset_identity(declared=declared_assets.get(str(param)), entry=entry, value=value)
        ops.append(
            ProjectionOp(
                sql="""
                INSERT INTO task_inputs (
                  run_id, task_id, param, position, value_type, value_summary,
                  digest, artifact_id, asset_key, asset_version_id, remote_uri
                ) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id, task_id, param, position) DO UPDATE SET
                  value_type=excluded.value_type, value_summary=excluded.value_summary,
                  digest=excluded.digest, artifact_id=excluded.artifact_id,
                  asset_key=excluded.asset_key,
                  asset_version_id=excluded.asset_version_id, remote_uri=excluded.remote_uri
                """,
                params=(
                    event.run_id,
                    task_id,
                    str(param),
                    entry.get("type"),
                    None if value is _ABSENT else dumps(value),
                    _digest_of(entry),
                    asset.get("artifact_id"),
                    asset.get("asset"),
                    asset.get("version_id"),
                    _remote_uri(entry),
                ),
            )
        )
        if asset.get("asset"):
            ops.append(
                _edge(
                    run_id=event.run_id,
                    src=("asset_version", str(asset.get("version_id") or asset["asset"])),
                    dst=("task", task_id),
                    edge="consumed",
                )
            )

    for dependency in payload.get("dependency_ids") or []:
        ops.append(
            _edge(
                run_id=event.run_id,
                src=("task", str(dependency)),
                dst=("task", task_id),
                edge="depends_on",
            )
        )
    for dependency in payload.get("dynamic_dependency_ids") or []:
        ops.append(
            _edge(
                run_id=event.run_id,
                src=("task", str(dependency)),
                dst=("task", task_id),
                edge="dynamic_depends_on",
            )
        )
    return ops


def _task_cache_hit(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    # Only the task's own row. The cache tables are not projections: they are a
    # direct-write index owned by CacheIndex, which counts the hit itself.
    return [_set_cached(event, payload, cached=1)]


def _task_cache_miss(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    return [_set_cached(event, payload, cached=0)]


def _task_started(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    attempt = int(payload.get("attempt") or 0)
    backend = payload.get("execution_backend")
    return [
        ProjectionOp(
            sql="""
            UPDATE tasks SET
              status = 'running',
              started_at = coalesce(started_at, ?),
              attempts = max(attempts, ?),
              kind = coalesce(?, kind),
              env = coalesce(?, env),
              execution_backend = coalesce(?, execution_backend),
              remote_job_id = coalesce(?, remote_job_id),
              finished_at = NULL
            WHERE run_id = ? AND task_id = ?
            """,
            params=(
                event.ts,
                attempt,
                payload.get("kind"),
                payload.get("env"),
                backend,
                payload.get("remote_job_id"),
                event.run_id,
                event.task_id,
            ),
        ),
        ProjectionOp(
            sql="""
            INSERT INTO attempts (
              run_id, task_id, attempt, started_at, status, execution_backend, remote_job_id
            ) VALUES (?, ?, ?, ?, 'running', ?, ?)
            ON CONFLICT (run_id, task_id, attempt) DO UPDATE SET
              started_at=excluded.started_at, status='running',
              execution_backend=excluded.execution_backend,
              remote_job_id=excluded.remote_job_id
            """,
            params=(
                event.run_id,
                event.task_id,
                attempt,
                event.ts,
                backend,
                payload.get("remote_job_id"),
            ),
        ),
    ]


def _task_running(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    job_id = payload.get("remote_job_id")
    if job_id is None:
        return []
    return [
        ProjectionOp(
            sql="UPDATE tasks SET remote_job_id = ? WHERE run_id = ? AND task_id = ?",
            params=(job_id, event.run_id, event.task_id),
        ),
        ProjectionOp(
            sql=(
                "UPDATE attempts SET remote_job_id = ? "
                "WHERE run_id = ? AND task_id = ? AND attempt = ?"
            ),
            params=(job_id, event.run_id, event.task_id, int(payload.get("attempt") or 0)),
        ),
    ]


def _task_retrying(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    attempt = int(payload.get("attempt") or 0)
    failure = dumps_or_none(payload.get("failure"))
    return [
        ProjectionOp(
            sql="""
            UPDATE tasks SET
              status = 'pending', cached = 0, finished_at = NULL,
              attempts = max(attempts, ?), failure = ?
            WHERE run_id = ? AND task_id = ?
            """,
            params=(attempt, failure, event.run_id, event.task_id),
        ),
        ProjectionOp(
            sql="""
            INSERT INTO attempts (
              run_id, task_id, attempt, finished_at, status, failure, retry_delay_s
            ) VALUES (?, ?, ?, ?, 'failed', ?, ?)
            ON CONFLICT (run_id, task_id, attempt) DO UPDATE SET
              finished_at=excluded.finished_at, status='failed',
              failure=excluded.failure, retry_delay_s=excluded.retry_delay_s
            """,
            params=(
                event.run_id,
                event.task_id,
                attempt,
                event.ts,
                failure,
                payload.get("delay_seconds"),
            ),
        ),
    ]


def _task_completed(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    task_id = event.task_id or ""
    cached = payload.get("status") == "cached"
    outputs = [entry for entry in payload.get("outputs") or [] if isinstance(entry, dict)]
    assets = [entry for entry in payload.get("assets") or [] if isinstance(entry, dict)]
    resource_usage = payload.get("resource_usage")
    ops = [
        ProjectionOp(
            sql="""
            UPDATE tasks SET
              status = ?, cached = ?, finished_at = ?, exit_code = 0, failure = NULL,
              attempts = max(attempts, ?),
              cache_key = coalesce(?, cache_key),
              remote_job_id = coalesce(?, remote_job_id),
              output_summary = ?,
              resource_usage = coalesce(?, resource_usage),
              extra = json_patch(coalesce(extra, '{}'), ?)
            WHERE run_id = ? AND task_id = ?
            """,
            params=(
                "cached" if cached else "succeeded",
                1 if cached else 0,
                event.ts,
                int(payload.get("attempt") or 0),
                payload.get("cache_key"),
                payload.get("remote_job_id"),
                dumps(outputs),
                dumps_or_none(resource_usage),
                dumps({"assets": assets} if assets else {}),
                event.run_id,
                task_id,
            ),
        ),
        ProjectionOp(
            sql="""
            INSERT INTO attempts (run_id, task_id, attempt, finished_at, status, exit_code)
            VALUES (?, ?, ?, ?, ?, 0)
            ON CONFLICT (run_id, task_id, attempt) DO UPDATE SET
              finished_at=excluded.finished_at, status=excluded.status, exit_code=0
            """,
            params=(
                event.run_id,
                task_id,
                int(payload.get("attempt") or 0),
                event.ts,
                "cached" if cached else "succeeded",
            ),
        ),
        ProjectionOp(
            sql="DELETE FROM task_outputs WHERE run_id = ? AND task_id = ?",
            params=(event.run_id, task_id),
        ),
    ]
    for position, entry in enumerate(outputs):
        ops.append(
            ProjectionOp(
                sql="""
                INSERT INTO task_outputs (
                  run_id, task_id, position, name, value_type, path,
                  artifact_id, asset_key, asset_version_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params=(
                    event.run_id,
                    task_id,
                    position,
                    entry.get("name"),
                    entry.get("type"),
                    entry.get("path"),
                    entry.get("artifact_id"),
                    entry.get("asset_key"),
                    entry.get("version_id"),
                ),
            )
        )
        if entry.get("asset_key"):
            ops.append(
                _edge(
                    run_id=event.run_id,
                    src=("task", task_id),
                    dst=("asset_version", str(entry.get("version_id") or entry["asset_key"])),
                    edge="produced",
                )
            )
        elif entry.get("artifact_id"):
            ops.append(
                _edge(
                    run_id=event.run_id,
                    src=("task", task_id),
                    dst=("artifact", str(entry["artifact_id"])),
                    edge="produced",
                )
            )
    return ops


def _task_failed(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    failure = payload.get("failure") or {}
    attempt = int(payload.get("attempt") or 0)
    return [
        ProjectionOp(
            sql="""
            UPDATE tasks SET
              status = 'failed', cached = 0, finished_at = ?, exit_code = ?,
              failure = ?, attempts = max(attempts, ?)
            WHERE run_id = ? AND task_id = ?
            """,
            params=(
                event.ts,
                payload.get("exit_code"),
                dumps(failure),
                attempt,
                event.run_id,
                event.task_id,
            ),
        ),
        ProjectionOp(
            sql="""
            INSERT INTO attempts (
              run_id, task_id, attempt, finished_at, status, exit_code, failure
            ) VALUES (?, ?, ?, ?, 'failed', ?, ?)
            ON CONFLICT (run_id, task_id, attempt) DO UPDATE SET
              finished_at=excluded.finished_at, status='failed',
              exit_code=excluded.exit_code, failure=excluded.failure
            """,
            params=(
                event.run_id,
                event.task_id,
                attempt,
                event.ts,
                payload.get("exit_code"),
                dumps(failure),
            ),
        ),
    ]


# Fields a reader filters or joins on get a column of their own; the rest stay
# in ``tasks.extra``.
_PROMOTED_ANNOTATIONS = {
    "resource_usage": "resource_usage",
    "remote_job_id": "remote_job_id",
    "execution_backend": "execution_backend",
}


def _task_annotated(event: StoredEvent, payload: dict[str, Any]) -> list[ProjectionOp]:
    fields = payload.get("fields")
    if not isinstance(fields, dict) or not fields:
        return []
    ops: list[ProjectionOp] = []
    remaining = dict(fields)
    for name, column in _PROMOTED_ANNOTATIONS.items():
        if name not in remaining:
            continue
        value = remaining.pop(name)
        ops.append(
            ProjectionOp(
                sql=f"UPDATE tasks SET {column} = ? WHERE run_id = ? AND task_id = ?",
                params=(
                    dumps(value) if isinstance(value, (dict, list)) else value,
                    event.run_id,
                    event.task_id,
                ),
            )
        )
    if remaining:
        # RFC 7386 merge semantics: a field set to null is a field removed,
        # which is exactly what "this run has no render error" means.
        ops.append(
            ProjectionOp(
                sql=(
                    "UPDATE tasks SET extra = json_patch(coalesce(extra, '{}'), ?) "
                    "WHERE run_id = ? AND task_id = ?"
                ),
                params=(dumps(remaining), event.run_id, event.task_id),
            )
        )
    return ops


# ------------------------------------------------------------------- helpers


def _set_cached(event: StoredEvent, payload: dict[str, Any], *, cached: int) -> ProjectionOp:
    return ProjectionOp(
        sql=(
            "UPDATE tasks SET cached = ?, cache_key = coalesce(?, cache_key) "
            "WHERE run_id = ? AND task_id = ?"
        ),
        params=(cached, payload.get("cache_key"), event.run_id, event.task_id),
    )


def _edge(*, run_id: str, src: tuple[str, str], dst: tuple[str, str], edge: str) -> ProjectionOp:
    return ProjectionOp(
        sql="""
        INSERT INTO edges (run_id, src_kind, src_id, dst_kind, dst_id, edge)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        params=(run_id, src[0], src[1], dst[0], dst[1], edge),
    )


def accumulate_seconds(
    *,
    table: str,
    column: str,
    where: str,
    where_params: tuple[Any, ...],
    key: str,
    seconds: float,
) -> ProjectionOp:
    """Add *seconds* to ``column -> key``, creating the bucket if it is absent.

    The one place a timing bucket is accumulated, so the writer's record of its
    own cost lands in the same shape as every phase the run reports.
    """
    path = '$."' + key.replace('"', "") + '"'
    return ProjectionOp(
        sql=f"""
        UPDATE {table} SET {column} = json_set(
          coalesce({column}, '{{}}'), ?,
          round(coalesce(json_extract({column}, ?), 0) + ?, 6)
        ) WHERE {where}
        """,
        params=(path, path, seconds, *where_params),
    )


def _asset_identity(
    *, declared: dict[str, Any] | None, entry: dict[str, Any], value: Any
) -> dict[str, Any]:
    """Return the asset an input names, from whichever half recorded it.

    The evaluator knows the answer at resolution time and puts it on the event
    as ``asset_inputs``; that is the authority, and the only source for a
    parameter typed as the payload rather than as a file — by the time such an
    argument reaches the task the ref has become a DataFrame, and neither the
    hash entry nor the rendered value can say where it came from.

    The other two remain because the event predates them. A task that declares
    a parameter as ``file`` and is handed an ``AssetRef`` is hashed as the file
    it resolves to, because the cache keys on content and changing that payload
    would invalidate every entry; the hash entry names the artifact, and the
    rendered argument still describes the ref.
    """
    if declared and declared.get("asset_key"):
        return {
            "asset": declared["asset_key"],
            "version_id": declared.get("version_id"),
            "artifact_id": declared.get("artifact_id"),
        }
    if entry.get("asset"):
        return {
            "asset": entry["asset"],
            "version_id": entry.get("version_id"),
            "artifact_id": entry.get("artifact_id"),
        }
    if isinstance(value, dict) and value.get("type") == "asset_ref" and value.get("asset"):
        return {
            "asset": value["asset"],
            "version_id": value.get("version_id"),
            "artifact_id": value.get("artifact_id"),
        }
    return {}


def _digest_of(entry: dict[str, Any]) -> str | None:
    """Return the digest an input-hash entry carries, if it is a plain one."""
    digest = entry.get("digest")
    return digest if isinstance(digest, str) else None


def _remote_uri(entry: dict[str, Any]) -> str | None:
    """Return the object-store URI a remote input-hash entry names."""
    scheme, bucket, key = entry.get("scheme"), entry.get("bucket"), entry.get("key")
    if not (scheme and bucket and key):
        return None
    return f"{scheme}://{bucket}/{key}"


_HANDLERS: dict[str, Callable[[StoredEvent, dict[str, Any]], list[ProjectionOp]]] = {
    "run_started": _run_started,
    "run_resources_sampled": _run_resources_sampled,
    "run_completed": _run_completed,
    "phase_timed": _phase_timed,
    "graph_node_registered": _graph_node_registered,
    "graph_expanded": _graph_expanded,
    "task_planned": _task_planned,
    "task_cache_hit": _task_cache_hit,
    "task_cache_miss": _task_cache_miss,
    "task_started": _task_started,
    "task_running": _task_running,
    "task_retrying": _task_retrying,
    "task_completed": _task_completed,
    "task_failed": _task_failed,
    "task_annotated": _task_annotated,
}
