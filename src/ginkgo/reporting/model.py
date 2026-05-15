"""Typed report data model built from provenance and the asset catalog.

The renderer consumes an immutable :class:`ReportData` instance. Every field
is either a fixed label or a value derived from provenance — no generated
prose. Templates read these fields directly without computing anything.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ginkgo.core.asset import AssetKey, AssetVersion
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.run_summary import RunSummary, TaskSummary

from .sizing import (
    LogTail,
    SizingPolicy,
    TablePreview,
    TextPreview,
    build_log_tail,
    build_table_preview,
    build_text_preview,
    format_bytes,
    format_duration,
    format_int,
    format_timestamp,
)


# ----- Masthead and summary ---------------------------------------------


@dataclass(frozen=True, kw_only=True)
class KVEntry:
    """One row in a key-value grid."""

    key: str
    value: str


@dataclass(frozen=True, kw_only=True)
class StatCard:
    """One headline stat in the summary grid.

    Parameters
    ----------
    label : str
        Small-caps label above the value.
    value : str
        Primary value shown in the Fraunces stat numeral.
    sub : str
        Secondary line under the value.
    tone : {"ok", "fail", "cool", "neutral"}
        Accent stripe tone; maps to a CSS class.
    """

    label: str
    value: str
    sub: str
    tone: str = "neutral"


@dataclass(frozen=True, kw_only=True)
class MastheadChip:
    """One pre-formatted chip in the masthead strip."""

    key: str
    value: str


# ----- Tasks and graph --------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class TaskRow:
    """One row in the task table."""

    task_key: str
    node_id: int | None
    name: str
    base_name: str
    kind_label: str
    status_label: str
    status_tone: str
    cache_label: str
    duration_label: str
    attempts_label: str
    failed: bool


@dataclass(frozen=True, kw_only=True)
class GraphNode:
    """One placed node in the task graph SVG."""

    task_key: str
    label: str
    sub_label: str | None
    x: int
    y: int
    width: int
    height: int
    tone: str


@dataclass(frozen=True, kw_only=True)
class GraphEdge:
    """One placed edge in the task graph SVG (straight-line)."""

    source: str
    target: str
    x1: int
    y1: int
    x2: int
    y2: int


@dataclass(frozen=True, kw_only=True)
class Graph:
    """Placed task graph ready for SVG rendering."""

    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    width: int
    height: int


# ----- Failure -----------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class FailureCard:
    """One diagnosed failure."""

    task_key: str
    task_name: str
    base_name: str
    category: str | None
    exit_code: int | None
    attempts_label: str | None
    message: str | None
    log_tail: LogTail | None
    log_relpath: str | None


# ----- Assets ------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class AssetStat:
    """One row in an asset stats grid."""

    key: str
    value: str


@dataclass(frozen=True, kw_only=True)
class AssetPreview:
    """Kind-specific preview payload.

    Exactly one of the preview-kind fields is populated; the ``kind`` field
    selects which.
    """

    kind: str  # "table" | "image" | "text" | "stats" | "iframe" | "missing" | "binary"
    table: TablePreview | None = None
    image_relpath: str | None = None
    image_alt: str | None = None
    text: TextPreview | None = None
    text_format: str | None = None
    iframe_relpath: str | None = None
    stats: tuple[AssetStat, ...] = ()
    message: str | None = None


@dataclass(frozen=True, kw_only=True)
class AssetCard:
    """One asset version produced by the run."""

    asset_key: str
    name: str
    namespace: str
    kind_label: str
    kind_tone: str
    artifact_id: str
    meta_line: str
    version_id: str
    preview: AssetPreview


# ----- Notebook ----------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class NotebookCard:
    """One rendered notebook accompanying the report."""

    task_key: str
    title: str
    sub_line: str
    link_relpath: str
    status_tone: str


# ----- Bundle artifact plan ---------------------------------------------


@dataclass(frozen=True, kw_only=True)
class ArtifactCopy:
    """Instruction to copy one artifact file into the bundle."""

    source: Path
    dest_relpath: str  # relative to bundle root


# ----- Top-level report data --------------------------------------------


@dataclass(frozen=True, kw_only=True)
class ReportData:
    """Everything the renderer needs to produce the bundle HTML.

    Instances are built by :func:`build_report_data` and are treated as
    immutable; templates read fields directly without further computation.
    """

    # Identity
    run_id: str
    workflow: str | None
    workflow_label: str
    workspace_label: str | None

    # Run-level status
    status_raw: str
    status_label: str
    status_tone: str
    has_failures: bool

    # Timings
    started_label: str
    finished_label: str
    duration_label: str

    # Masthead
    masthead_chips: tuple[MastheadChip, ...]
    masthead_kv: tuple[KVEntry, ...]

    # Sidebar
    sidebar_run_id: str
    sidebar_status_label: str
    sidebar_status_tone: str

    # Sections
    summary_cards: tuple[StatCard, ...]
    params: Mapping[str, Any]
    graph: Graph
    tasks: tuple[TaskRow, ...]
    failures: tuple[FailureCard, ...]
    assets: tuple[AssetCard, ...]
    notebooks: tuple[NotebookCard, ...]
    environment: tuple[KVEntry, ...]

    # Footer / determinism
    ginkgo_version: str
    generated_at_label: str

    # Artifacts the renderer must copy into the bundle
    artifact_copies: tuple[ArtifactCopy, ...]


# ------------------------------------------------------------------------
# Builder
# ------------------------------------------------------------------------


_KIND_TONE: dict[str, str] = {
    "table": "table",
    "array": "array",
    "fig": "fig",
    "text": "text",
    "model": "model",
    "file": "file",
}

_STATUS_TONE: dict[str, str] = {
    "succeeded": "ok",
    "cached": "ok",
    "failed": "fail",
    "running": "warn",
    "pending": "warn",
    "unknown": "warn",
}

_STATUS_LABEL: dict[str, str] = {
    "succeeded": "ok",
    "cached": "cached",
    "failed": "fail",
    "running": "running",
    "pending": "pending",
    "unknown": "unknown",
}

_RUN_STATUS_LABEL: dict[str, str] = {
    "succeeded": "completed",
    "failed": "completed_with_errors",
    "running": "running",
    "pending": "pending",
}


def build_report_data(
    *,
    run_dir: Path,
    workspace_label: str | None = None,
    assets_root: Path | None = None,
    artifacts_root: Path | None = None,
    policy: SizingPolicy | None = None,
    ginkgo_version: str | None = None,
    generated_at: datetime | None = None,
) -> ReportData:
    """Build a :class:`ReportData` from a completed run directory.

    Parameters
    ----------
    run_dir : Path
        Directory containing ``manifest.yaml`` and friends.
    workspace_label : str | None
        Display label for the enclosing workspace. Inferred from the run
        directory's grandparent when omitted.
    assets_root : Path | None
        Root of the asset catalog. Defaults to ``<workspace>/.ginkgo/assets``.
    artifacts_root : Path | None
        Root of the artifact store. Defaults to ``<workspace>/.ginkgo/artifacts``.
    policy : SizingPolicy | None
        Per-kind size caps applied at export time. Defaults to
        :class:`SizingPolicy` defaults.
    ginkgo_version : str | None
        Version string to render in the footer. Inferred from installed
        package metadata when omitted.
    generated_at : datetime | None
        Timestamp rendered in the single determinism-sensitive footer
        element. Defaults to ``datetime.now(UTC)``.

    Returns
    -------
    ReportData
    """
    run_dir = Path(run_dir).resolve()
    summary = RunSummary.load(run_dir)

    # Terminal runs only — fail fast if the run is still live.
    if summary.status not in {"succeeded", "failed"}:
        raise ValueError(
            f"Run {summary.run_id!r} is not terminal (status={summary.status!r}); "
            "reports can only be exported for completed runs."
        )

    policy = policy or SizingPolicy()
    workspace_root = run_dir.parents[1] if len(run_dir.parents) >= 2 else run_dir.parent
    assets_root = assets_root if assets_root is not None else workspace_root / "assets"
    artifacts_root = artifacts_root if artifacts_root is not None else workspace_root / "artifacts"
    workspace_label = workspace_label or workspace_root.parent.name

    artifact_store = LocalArtifactStore(root=artifacts_root) if artifacts_root.exists() else None

    # Sections — each returns its typed pieces plus any copy instructions.
    artifact_copies: list[ArtifactCopy] = []

    tasks = _build_task_rows(summary=summary)
    failures = _build_failures(
        summary=summary,
        run_dir=run_dir,
        policy=policy,
        artifact_copies=artifact_copies,
    )
    graph = _build_graph(summary=summary, failures=failures)
    assets = _build_assets(
        summary=summary,
        assets_root=assets_root,
        artifact_store=artifact_store,
        policy=policy,
        artifact_copies=artifact_copies,
    )
    notebooks = _build_notebooks(summary=summary, run_dir=run_dir, artifact_copies=artifact_copies)
    summary_cards = _build_summary_cards(summary=summary, assets=assets)

    status_raw = summary.status
    status_tone = _STATUS_TONE.get(status_raw, "warn")
    status_label = _RUN_STATUS_LABEL.get(status_raw, status_raw)

    failed_count = summary.failed_count
    if failed_count > 0:
        sidebar_status_label = f"completed · {failed_count} fail"
        sidebar_status_tone = "fail"
    elif status_raw == "succeeded":
        sidebar_status_label = "completed"
        sidebar_status_tone = "ok"
    else:
        sidebar_status_label = status_label
        sidebar_status_tone = status_tone

    masthead_chips = _build_masthead_chips(summary=summary)
    masthead_kv = _build_masthead_kv(
        summary=summary,
        workspace_label=workspace_label,
        status_label=status_label,
        status_tone=status_tone,
        ginkgo_version=_resolve_ginkgo_version(ginkgo_version),
    )
    environment = _build_environment_kv(
        summary=summary, ginkgo_version=_resolve_ginkgo_version(ginkgo_version)
    )

    return ReportData(
        run_id=summary.run_id,
        workflow=summary.workflow,
        workflow_label=summary.workflow_label,
        workspace_label=workspace_label,
        status_raw=status_raw,
        status_label=status_label,
        status_tone=status_tone,
        has_failures=len(failures) > 0,
        started_label=format_timestamp(summary.started_at),
        finished_label=format_timestamp(summary.finished_at),
        duration_label=format_duration(summary.duration_s),
        masthead_chips=masthead_chips,
        masthead_kv=masthead_kv,
        sidebar_run_id=summary.run_id,
        sidebar_status_label=sidebar_status_label,
        sidebar_status_tone=sidebar_status_tone,
        summary_cards=summary_cards,
        params=summary.params,
        graph=graph,
        tasks=tasks,
        failures=failures,
        assets=assets,
        notebooks=notebooks,
        environment=environment,
        ginkgo_version=_resolve_ginkgo_version(ginkgo_version),
        generated_at_label=format_timestamp(generated_at or datetime.now(UTC)),
        artifact_copies=tuple(artifact_copies),
    )


# ----- Task rows ----------------------------------------------------------


def _build_task_rows(*, summary: RunSummary) -> tuple[TaskRow, ...]:
    """Build ordered task rows for the task ledger."""
    rows: list[TaskRow] = []
    for task in summary.tasks:
        failed = task.status == "failed"
        status_label = _STATUS_LABEL.get(task.status, task.status)
        status_tone = _STATUS_TONE.get(task.status, "warn")
        kind_label = _task_kind_label(task)
        cache_label = _cache_label(task)
        duration_label = format_duration(task.duration_s)
        attempts_label = _attempts_label(task)

        rows.append(
            TaskRow(
                task_key=task.task_key,
                node_id=task.node_id,
                name=task.name,
                base_name=task.base_name,
                kind_label=kind_label,
                status_label=status_label,
                status_tone=status_tone,
                cache_label=cache_label,
                duration_label=duration_label,
                attempts_label=attempts_label,
                failed=failed,
            )
        )
    return tuple(rows)


def _task_kind_label(task: TaskSummary) -> str:
    """Return a display label for the task kind."""
    raw = task.raw.get("kind")
    if isinstance(raw, str) and raw:
        return raw
    if task.task_type == "notebook":
        return "notebook"
    return "task"


def _cache_label(task: TaskSummary) -> str:
    """Return ``"hit"``, ``"miss"``, or ``"—"``."""
    if task.cached or task.status == "cached":
        return "hit"
    if task.status in {"succeeded", "failed"}:
        return "miss"
    return "—"


def _attempts_label(task: TaskSummary) -> str:
    """Return ``"N"`` or ``"N / M"`` for attempts / max_attempts."""
    attempts = task.raw.get("attempts")
    max_attempts = task.raw.get("max_attempts")
    if task.status == "cached":
        return "—"
    if isinstance(attempts, int) and isinstance(max_attempts, int) and max_attempts > 1:
        return f"{attempts + 1} / {max_attempts}"
    if isinstance(attempts, int):
        return f"{attempts + 1}"
    return "1"


# ----- Graph --------------------------------------------------------------


_GRAPH_COL_WIDTH = 200
_GRAPH_ROW_HEIGHT = 72
_GRAPH_NODE_W = 150
_GRAPH_NODE_H = 40
_GRAPH_MARGIN_X = 24
_GRAPH_MARGIN_Y = 24


def _build_graph(*, summary: RunSummary, failures: tuple[FailureCard, ...]) -> Graph:
    """Lay out a simple layered DAG.

    Layers are assigned by longest path from any root node. Each layer
    becomes a column; within a column, nodes are spaced vertically in
    manifest order.
    """
    if not summary.tasks:
        return Graph(nodes=(), edges=(), width=0, height=0)

    id_to_task: dict[int, TaskSummary] = {}
    for task in summary.tasks:
        if task.node_id is not None:
            id_to_task[task.node_id] = task

    predecessors: dict[int, list[int]] = defaultdict(list)
    successors: dict[int, list[int]] = defaultdict(list)
    for task in summary.tasks:
        if task.node_id is None:
            continue
        for dep in task.dependency_ids:
            if dep in id_to_task:
                predecessors[task.node_id].append(dep)
                successors[dep].append(task.node_id)

    # Layer assignment: longest-path level from roots.
    level: dict[int, int] = {}

    def _level(node_id: int) -> int:
        if node_id in level:
            return level[node_id]
        parents = predecessors.get(node_id, [])
        value = 0 if not parents else 1 + max(_level(parent) for parent in parents)
        level[node_id] = value
        return value

    for node_id in id_to_task:
        _level(node_id)

    columns: dict[int, list[int]] = defaultdict(list)
    for node_id in sorted(id_to_task.keys()):
        columns[level[node_id]].append(node_id)

    failure_keys = {card.task_key for card in failures}

    placed: dict[int, GraphNode] = {}
    max_column_height = 0
    for col_index in sorted(columns.keys()):
        node_ids = columns[col_index]
        column_height = len(node_ids) * _GRAPH_ROW_HEIGHT
        max_column_height = max(max_column_height, column_height)
        x = _GRAPH_MARGIN_X + col_index * _GRAPH_COL_WIDTH
        for row_index, node_id in enumerate(node_ids):
            y = _GRAPH_MARGIN_Y + row_index * _GRAPH_ROW_HEIGHT
            task = id_to_task[node_id]
            tone = _graph_tone(task=task, failure_keys=failure_keys)
            label, sub_label = _graph_label(task)
            placed[node_id] = GraphNode(
                task_key=task.task_key,
                label=label,
                sub_label=sub_label,
                x=x,
                y=y,
                width=_GRAPH_NODE_W,
                height=_GRAPH_NODE_H,
                tone=tone,
            )

    edges: list[GraphEdge] = []
    for task in summary.tasks:
        if task.node_id is None:
            continue
        target = placed.get(task.node_id)
        if target is None:
            continue
        for dep in task.dependency_ids:
            source = placed.get(dep)
            if source is None:
                continue
            edges.append(
                GraphEdge(
                    source=source.task_key,
                    target=target.task_key,
                    x1=source.x + source.width,
                    y1=source.y + source.height // 2,
                    x2=target.x,
                    y2=target.y + target.height // 2,
                )
            )

    width = (
        _GRAPH_MARGIN_X
        + (max(columns) + 1) * _GRAPH_COL_WIDTH
        + _GRAPH_NODE_W
        - _GRAPH_COL_WIDTH
        + _GRAPH_MARGIN_X
    )
    height = max_column_height + 2 * _GRAPH_MARGIN_Y

    return Graph(
        nodes=tuple(placed[node_id] for node_id in sorted(placed)),
        edges=tuple(edges),
        width=width,
        height=height,
    )


def _graph_tone(*, task: TaskSummary, failure_keys: set[str]) -> str:
    """Return the graph tone for one task node."""
    if task.task_key in failure_keys or task.status == "failed":
        return "fail"
    if task.task_type == "notebook":
        return "terminal"
    if task.status in {"succeeded", "cached"}:
        return "ok"
    return "neutral"


def _graph_label(task: TaskSummary) -> tuple[str, str | None]:
    """Return the display label and optional sub-label for a graph node."""
    label = task.base_name
    sub = None
    # Fan-out hint when there are dynamic dependencies beyond one.
    dynamic = len(task.dynamic_dependency_ids)
    if dynamic > 1:
        sub = f"×{dynamic}"
    return label, sub


# ----- Failures -----------------------------------------------------------


def _build_failures(
    *,
    summary: RunSummary,
    run_dir: Path,
    policy: SizingPolicy,
    artifact_copies: list[ArtifactCopy],
) -> tuple[FailureCard, ...]:
    """Build one :class:`FailureCard` per failed task."""
    cards: list[FailureCard] = []
    for task in summary.failed_tasks:
        category = None
        if isinstance(task.failure, dict):
            kind = task.failure.get("kind")
            if isinstance(kind, str):
                category = kind

        log_path = _first_log_path(task=task, run_dir=run_dir)
        log_tail = build_log_tail(path=log_path, policy=policy)
        log_relpath: str | None = None
        if log_path is not None and log_path.is_file():
            dest = f"logs/{log_path.name}"
            artifact_copies.append(ArtifactCopy(source=log_path, dest_relpath=dest))
            log_relpath = dest

        cards.append(
            FailureCard(
                task_key=task.task_key,
                task_name=task.name,
                base_name=task.base_name,
                category=category,
                exit_code=task.exit_code,
                attempts_label=_attempts_label(task),
                message=task.error,
                log_tail=log_tail,
                log_relpath=log_relpath,
            )
        )
    return tuple(cards)


def _first_log_path(*, task: TaskSummary, run_dir: Path) -> Path | None:
    """Return the first existing log path for a task (stderr preferred)."""
    for rel in (task.stderr_log, task.stdout_log):
        if isinstance(rel, str) and rel:
            path = (run_dir / rel).resolve()
            if path.is_file():
                return path
    return None


# ----- Assets -------------------------------------------------------------


def _build_assets(
    *,
    summary: RunSummary,
    assets_root: Path,
    artifact_store: LocalArtifactStore | None,
    policy: SizingPolicy,
    artifact_copies: list[ArtifactCopy],
) -> tuple[AssetCard, ...]:
    """Build asset cards for every asset referenced by this run.

    A task reference cites ``(asset_key, version_id)``. Cached tasks replay
    references to versions produced by earlier runs; the report still wants
    to show those, so we resolve each ``(key, version_id)`` pair directly
    rather than filtering on producer ``run_id``.
    """
    if not assets_root.exists() or artifact_store is None:
        return ()

    store = AssetStore(root=assets_root)
    seen_version_ids: set[str] = set()
    cards: list[AssetCard] = []

    references: list[tuple[str, str]] = []
    for task in summary.tasks:
        for asset in task.assets:
            key = asset.get("asset_key")
            version_id = asset.get("version_id")
            if isinstance(key, str) and isinstance(version_id, str):
                references.append((key, version_id))

    for key_text, version_id in references:
        if version_id in seen_version_ids:
            continue
        seen_version_ids.add(version_id)
        try:
            version = store.get_version(key=_parse_asset_key(key_text), version_id=version_id)
        except FileNotFoundError:
            continue
        card = _build_asset_card(
            version=version,
            artifact_store=artifact_store,
            policy=policy,
            artifact_copies=artifact_copies,
        )
        if card is not None:
            cards.append(card)

    cards.sort(key=lambda card: (card.namespace, card.name))
    return tuple(cards)


def _build_asset_card(
    *,
    version: AssetVersion,
    artifact_store: LocalArtifactStore,
    policy: SizingPolicy,
    artifact_copies: list[ArtifactCopy],
) -> AssetCard | None:
    """Build one :class:`AssetCard` for a stored asset version."""
    record = _artifact_record(artifact_store=artifact_store, artifact_id=version.artifact_id)
    path = _artifact_path(artifact_store=artifact_store, artifact_id=version.artifact_id)

    namespace = version.key.namespace
    preview = _build_preview(
        namespace=namespace,
        record=record,
        path=path,
        metadata=version.metadata,
        policy=policy,
        artifact_id=version.artifact_id,
        artifact_copies=artifact_copies,
    )

    meta_line = _asset_meta_line(namespace=namespace, record=record, metadata=version.metadata)
    kind_tone = _KIND_TONE.get(namespace, "file")

    if policy.embed_full_assets and path is not None and path.is_file():
        dest = f"artifacts/{version.artifact_id}{record.extension if record else ''}"
        artifact_copies.append(ArtifactCopy(source=path, dest_relpath=dest))

    return AssetCard(
        asset_key=str(version.key),
        name=version.key.name,
        namespace=namespace,
        kind_label=namespace,
        kind_tone=kind_tone,
        artifact_id=version.artifact_id,
        meta_line=meta_line,
        version_id=version.version_id,
        preview=preview,
    )


def _build_preview(
    *,
    namespace: str,
    record: ArtifactRecord | None,
    path: Path | None,
    metadata: Mapping[str, Any],
    policy: SizingPolicy,
    artifact_id: str,
    artifact_copies: list[ArtifactCopy],
) -> AssetPreview:
    """Dispatch to a kind-specific preview builder."""
    if path is None or not path.exists():
        return AssetPreview(kind="missing", message="Artifact content is unavailable.")

    extension = (record.extension if record is not None else path.suffix).lower()

    if namespace == "table":
        preview = build_table_preview(path=path, extension=extension, policy=policy)
        if preview is not None:
            return AssetPreview(kind="table", table=preview)
        return AssetPreview(
            kind="binary",
            message=f"Unable to parse {extension or 'artifact'} as a table.",
        )

    if namespace == "fig":
        return _fig_preview(
            path=path,
            extension=extension,
            artifact_id=artifact_id,
            artifact_copies=artifact_copies,
        )

    if namespace == "text":
        text_format = str(metadata.get("format") or "plain")
        preview = build_text_preview(path=path, policy=policy)
        if preview is not None:
            return AssetPreview(kind="text", text=preview, text_format=text_format)
        return AssetPreview(kind="binary", message="Unable to read text asset.")

    if namespace == "array":
        return AssetPreview(
            kind="stats",
            stats=_array_stats(metadata=metadata),
            message="Array payload not inlined.",
        )

    if namespace == "model":
        return AssetPreview(
            kind="stats",
            stats=_model_stats(metadata=metadata),
            message="Model weights not inlined.",
        )

    # Fall-through: file assets get a lightweight preview based on extension.
    return _file_preview(
        path=path,
        extension=extension,
        artifact_id=artifact_id,
        artifact_copies=artifact_copies,
        policy=policy,
    )


def _fig_preview(
    *,
    path: Path,
    extension: str,
    artifact_id: str,
    artifact_copies: list[ArtifactCopy],
) -> AssetPreview:
    """Build a figure preview (image or embedded HTML)."""
    if extension in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        dest = f"figures/{artifact_id}{extension}"
        artifact_copies.append(ArtifactCopy(source=path, dest_relpath=dest))
        return AssetPreview(kind="image", image_relpath=dest, image_alt="figure")
    if extension == ".html":
        dest = f"figures/{artifact_id}.html"
        artifact_copies.append(ArtifactCopy(source=path, dest_relpath=dest))
        return AssetPreview(kind="iframe", iframe_relpath=dest)
    return AssetPreview(kind="binary", message=f"Unsupported figure format: {extension}")


def _file_preview(
    *,
    path: Path,
    extension: str,
    artifact_id: str,
    artifact_copies: list[ArtifactCopy],
    policy: SizingPolicy,
) -> AssetPreview:
    """Preview a generic file asset by extension."""
    if extension in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        dest = f"figures/{artifact_id}{extension}"
        artifact_copies.append(ArtifactCopy(source=path, dest_relpath=dest))
        return AssetPreview(kind="image", image_relpath=dest, image_alt="image")
    if extension in {".csv", ".tsv", ".parquet", ".json", ".jsonl", ".ndjson"}:
        preview = build_table_preview(path=path, extension=extension, policy=policy)
        if preview is not None:
            return AssetPreview(kind="table", table=preview)
    if extension in {".txt", ".log", ".md", ".yaml", ".yml", ".toml"}:
        preview = build_text_preview(path=path, policy=policy)
        if preview is not None:
            return AssetPreview(kind="text", text=preview, text_format="plain")
    return AssetPreview(kind="binary", message="Preview unavailable for this file type.")


def _asset_meta_line(
    *,
    namespace: str,
    record: ArtifactRecord | None,
    metadata: Mapping[str, Any],
) -> str:
    """Build the right-hand meta line shown in the asset header."""
    parts: list[str] = []
    if namespace == "table":
        sub_kind = metadata.get("sub_kind")
        if isinstance(sub_kind, str):
            parts.append(sub_kind)
        rows = metadata.get("row_count")
        cols = _column_count(metadata)
        if rows is not None and cols is not None:
            parts.append(f"{format_int(int(rows))} × {cols}")
        byte_size = metadata.get("byte_size")
        if isinstance(byte_size, int):
            parts.append(format_bytes(byte_size))
    elif namespace == "array":
        sub_kind = metadata.get("sub_kind")
        if isinstance(sub_kind, str):
            parts.append(sub_kind)
        shape = metadata.get("shape")
        if shape is not None:
            parts.append(f"shape {tuple(shape)}")
        dtype = metadata.get("dtype")
        if isinstance(dtype, str):
            parts.append(dtype)
        byte_size = metadata.get("byte_size")
        if isinstance(byte_size, int):
            parts.append(format_bytes(byte_size))
    elif namespace == "fig":
        source_format = metadata.get("source_format")
        if isinstance(source_format, str):
            parts.append(source_format)
        dimensions = metadata.get("dimensions")
        if isinstance(dimensions, (list, tuple)) and len(dimensions) == 2:
            parts.append(f"{int(dimensions[0])} × {int(dimensions[1])}")
        byte_size = metadata.get("byte_size")
        if isinstance(byte_size, int):
            parts.append(format_bytes(byte_size))
    elif namespace == "text":
        fmt = metadata.get("format")
        if isinstance(fmt, str):
            parts.append(fmt)
        byte_size = metadata.get("byte_size")
        if isinstance(byte_size, int):
            parts.append(format_bytes(byte_size))
        lines = metadata.get("line_count")
        if isinstance(lines, int):
            parts.append(f"{format_int(lines)} lines")
    elif namespace == "model":
        framework = metadata.get("framework")
        if isinstance(framework, str):
            parts.append(framework)
        byte_size = metadata.get("byte_size")
        if isinstance(byte_size, int):
            parts.append(format_bytes(byte_size))
    else:
        if record is not None:
            parts.append(format_bytes(record.size))

    prefix = f"artifact_id: {record.artifact_id[:8] if record is not None else '—'}"
    return " · ".join([prefix, *parts]) if parts else prefix


def _column_count(metadata: Mapping[str, Any]) -> int | None:
    """Extract a column count from table metadata when possible."""
    schema = metadata.get("schema")
    if isinstance(schema, list):
        return len(schema)
    if isinstance(schema, dict):
        fields = schema.get("fields")
        if isinstance(fields, list):
            return len(fields)
    return None


def _array_stats(*, metadata: Mapping[str, Any]) -> tuple[AssetStat, ...]:
    """Return a small stats grid for an array asset."""
    stats: list[AssetStat] = []
    for key in ("sub_kind", "shape", "dtype", "chunks", "coordinates", "byte_size"):
        if key not in metadata:
            continue
        value = metadata[key]
        if value is None:
            continue
        if key == "byte_size" and isinstance(value, int):
            stats.append(AssetStat(key=key, value=format_bytes(value)))
        else:
            stats.append(AssetStat(key=key, value=str(value)))
    return tuple(stats)


def _model_stats(*, metadata: Mapping[str, Any]) -> tuple[AssetStat, ...]:
    """Return a small stats grid for a model asset."""
    stats: list[AssetStat] = []
    for key in ("sub_kind", "framework", "byte_size"):
        if key not in metadata:
            continue
        value = metadata[key]
        if value is None:
            continue
        if key == "byte_size" and isinstance(value, int):
            stats.append(AssetStat(key=key, value=format_bytes(value)))
        else:
            stats.append(AssetStat(key=key, value=str(value)))
    metrics = metadata.get("metrics")
    if isinstance(metrics, dict):
        for name, value in metrics.items():
            if isinstance(value, (int, float)):
                stats.append(AssetStat(key=str(name), value=f"{float(value):.4g}"))
    return tuple(stats)


def _parse_asset_key(text: str) -> AssetKey:
    """Parse ``namespace:name`` into an :class:`AssetKey`."""
    namespace, sep, name = text.partition(":")
    if sep and namespace and name:
        return AssetKey(namespace=namespace, name=name)
    return AssetKey(namespace="file", name=text)


def _artifact_record(
    *, artifact_store: LocalArtifactStore, artifact_id: str
) -> ArtifactRecord | None:
    """Return stored artifact metadata when it exists."""
    ref_path = artifact_store._refs_dir / f"{artifact_id}.json"
    if not ref_path.is_file():
        return None
    return ArtifactRecord.from_path(ref_path)


def _artifact_path(*, artifact_store: LocalArtifactStore, artifact_id: str) -> Path | None:
    """Return the local artifact path when it exists."""
    if not artifact_store.exists(artifact_id=artifact_id):
        return None
    return artifact_store.artifact_path(artifact_id=artifact_id)


# ----- Notebooks ----------------------------------------------------------


def _build_notebooks(
    *,
    summary: RunSummary,
    run_dir: Path,
    artifact_copies: list[ArtifactCopy],
) -> tuple[NotebookCard, ...]:
    """Build notebook cards from notebook tasks that produced HTML."""
    cards: list[NotebookCard] = []
    for task in summary.tasks:
        if task.rendered_html is None:
            continue
        source = task.rendered_html_absolute(run_dir=run_dir)
        if source is None or not source.is_file():
            continue
        dest = f"notebooks/{task.base_name}.html"
        artifact_copies.append(ArtifactCopy(source=source, dest_relpath=dest))

        size_bytes = source.stat().st_size
        sub_parts: list[str] = []
        if task.notebook_kind:
            sub_parts.append(task.notebook_kind)
        sub_parts.append(format_duration(task.duration_s))
        sub_parts.append(format_bytes(size_bytes))
        if task.notebook_description:
            sub_parts.append(task.notebook_description)

        title = task.notebook_path or task.base_name
        if isinstance(title, str):
            title = Path(title).name
        status_tone = _STATUS_TONE.get(task.status, "warn")

        cards.append(
            NotebookCard(
                task_key=task.task_key,
                title=title or task.base_name,
                sub_line=" · ".join(sub_parts),
                link_relpath=dest,
                status_tone=status_tone,
            )
        )
    return tuple(cards)


# ----- Masthead / summary / environment ----------------------------------


def _build_summary_cards(
    *, summary: RunSummary, assets: tuple[AssetCard, ...]
) -> tuple[StatCard, ...]:
    """Headline stats shown above the fold."""
    total_tasks = summary.task_count
    succeeded = summary.succeeded_count + summary.cached_count
    failed = summary.failed_count
    cache_hits = sum(1 for task in summary.tasks if task.cached or task.status == "cached")
    cache_pct = (100.0 * cache_hits / total_tasks) if total_tasks else 0.0

    tasks_sub_parts: list[str] = []
    if succeeded:
        tasks_sub_parts.append(f"{succeeded} ok")
    if failed:
        tasks_sub_parts.append(f"{failed} fail")
    tasks_sub = " · ".join(tasks_sub_parts) or "no tasks"

    asset_namespaces: dict[str, int] = defaultdict(int)
    for card in assets:
        asset_namespaces[card.namespace] += 1
    assets_sub = (
        " · ".join(
            f"{count} {name}"
            for name, count in sorted(asset_namespaces.items(), key=lambda item: -item[1])
        )
        or "—"
    )

    failure_categories: list[str] = []
    for task in summary.failed_tasks:
        if isinstance(task.failure, dict):
            kind = task.failure.get("kind")
            if isinstance(kind, str):
                failure_categories.append(kind)
    failure_sub = failure_categories[0] if failure_categories else "—"

    return (
        StatCard(
            label="Tasks",
            value=format_int(total_tasks),
            sub=tasks_sub,
            tone="ok" if failed == 0 else "ok",
        ),
        StatCard(
            label="Failures",
            value=format_int(failed),
            sub=failure_sub,
            tone="fail" if failed else "neutral",
        ),
        StatCard(
            label="Assets",
            value=format_int(len(assets)),
            sub=assets_sub,
            tone="neutral",
        ),
        StatCard(
            label="Cache hits",
            value=f"{cache_hits} / {total_tasks}",
            sub=f"{cache_pct:.1f} % warm",
            tone="cool",
        ),
    )


def _build_masthead_chips(*, summary: RunSummary) -> tuple[MastheadChip, ...]:
    """Return the short chip strip below the H1."""
    chips = [
        MastheadChip(key="started", value=format_timestamp(summary.started_at)),
        MastheadChip(key="duration", value=format_duration(summary.duration_s)),
    ]
    resources = summary.resources
    peak = resources.get("peak") if isinstance(resources, dict) else None
    if isinstance(peak, dict):
        rss = peak.get("rss_bytes")
        if isinstance(rss, int):
            chips.append(MastheadChip(key="peak rss", value=format_bytes(rss)))
        cpu = peak.get("cpu_percent")
        if isinstance(cpu, (int, float)):
            chips.append(MastheadChip(key="peak cpu", value=f"{cpu:.0f} %"))
    return tuple(chips)


def _build_masthead_kv(
    *,
    summary: RunSummary,
    workspace_label: str | None,
    status_label: str,
    status_tone: str,
    ginkgo_version: str,
) -> tuple[KVEntry, ...]:
    """Key-value rows for the main masthead grid."""
    entries = [
        KVEntry(key="run_id", value=summary.run_id),
        KVEntry(key="workflow", value=summary.workflow or "—"),
    ]
    if workspace_label:
        entries.append(KVEntry(key="workspace", value=workspace_label))
    entries.extend(
        [
            KVEntry(key="status", value=f"{status_label} · {status_tone}"),
            KVEntry(key="started", value=format_timestamp(summary.started_at)),
            KVEntry(key="finished", value=format_timestamp(summary.finished_at)),
            KVEntry(key="duration", value=format_duration(summary.duration_s)),
            KVEntry(key="ginkgo", value=ginkgo_version),
        ]
    )
    return tuple(entries)


def _build_environment_kv(*, summary: RunSummary, ginkgo_version: str) -> tuple[KVEntry, ...]:
    """Appendix environment grid."""
    entries = [KVEntry(key="ginkgo", value=ginkgo_version)]
    resources = summary.resources
    if isinstance(resources, dict):
        scope = resources.get("scope")
        if isinstance(scope, str):
            entries.append(KVEntry(key="scope", value=scope))
        average = resources.get("average")
        if isinstance(average, dict):
            cpu = average.get("cpu_percent")
            if isinstance(cpu, (int, float)):
                entries.append(KVEntry(key="avg cpu", value=f"{cpu:.1f} %"))
            rss = average.get("rss_bytes")
            if isinstance(rss, int):
                entries.append(KVEntry(key="avg rss", value=format_bytes(rss)))
        sample_count = resources.get("sample_count")
        if isinstance(sample_count, int):
            entries.append(KVEntry(key="samples", value=format_int(sample_count)))
    timings = summary.raw_manifest.get("timings") if summary.raw_manifest else None
    if isinstance(timings, dict):
        run_timings = timings.get("run")
        if isinstance(run_timings, dict):
            execute = run_timings.get("workflow_execute_seconds")
            if isinstance(execute, (int, float)):
                entries.append(KVEntry(key="execute", value=format_duration(float(execute))))
    return tuple(entries)


def _resolve_ginkgo_version(provided: str | None) -> str:
    """Resolve the ginkgo version string."""
    if provided:
        return provided
    try:
        from importlib.metadata import PackageNotFoundError, version

        return version("ginkgo")
    except (ImportError, PackageNotFoundError):  # pragma: no cover
        return "unknown"
    except Exception:  # pragma: no cover
        return "unknown"
