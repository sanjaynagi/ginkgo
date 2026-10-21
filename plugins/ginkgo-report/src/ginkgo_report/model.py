"""Typed report data model built from a run's provenance.

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

from ginkgo.formatting import format_bytes, format_duration, format_int, format_timestamp
from ginkgo.runtime.run_summary import RunSummary, TaskSummary

from ginkgo_report.notebook_page import static_notebook_page
from ginkgo_report.sizing import LogTail, SizingPolicy, build_log_tail


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
        Primary value shown as the stat numeral.
    sub : str
        Secondary line under the value.
    tone : {"ok", "fail", "cool", "neutral"}
        Accent tone; maps to a CSS class.
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
    label: str
    """The task's fan-out label, or its base name."""
    kind_label: str
    status_label: str
    status_tone: str
    cache_label: str
    duration_label: str
    attempts_label: str
    memory_label: str
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
    label: str
    """The task's fan-out label, or its base name."""
    category: str | None
    exit_code: int | None
    attempts_label: str | None
    message: str | None
    log_tail: LogTail | None
    log_relpath: str | None


@dataclass(frozen=True, kw_only=True)
class AttemptNote:
    """One attempt of a task that ran more than once."""

    number: int
    status: str
    message: str | None
    """The attempt's error, when it failed."""


@dataclass(frozen=True, kw_only=True)
class RetriedTask:
    """A task that ran more than once, with what each attempt ended in.

    A retry that succeeded leaves no failure card, so this is where the error
    it recovered from is shown.
    """

    task_key: str
    label: str
    attempts: tuple[AttemptNote, ...]


# ----- Outputs -------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class OutputRow:
    """One file or folder a task produced."""

    task_key: str
    label: str
    """The producing task's fan-out label, or its base name."""
    name: str
    """Which of the task's outputs this is, such as ``return`` or ``return[1]``."""
    kind: str
    path: str


# ----- Rendered pages ----------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class PageCard:
    """One page a task rendered, such as a notebook's HTML, copied with the report."""

    task_key: str
    title: str
    sub_line: str
    link_relpath: str
    status_tone: str
    embed_html: str
    """What a single-file report embeds: a static copy of a marimo page, which
    otherwise needs the network to display, or the page itself."""
    static_relpath: str | None
    """Where a bundle writes the static copy; ``None`` when the page is not a
    marimo export and so is embedded as it is."""


# ----- Bundle artifact plan ---------------------------------------------


@dataclass(frozen=True, kw_only=True)
class ArtifactCopy:
    """Instruction to copy one artifact file into the bundle."""

    source: Path
    dest_relpath: str  # relative to bundle root


# ----- Document sections -------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class ReportSection:
    """One numbered section of the document.

    Parameters
    ----------
    anchor : str
        Fragment id the heading carries and the sidebar links to.
    title : str
        Heading text.
    number : str
        Zero-padded ordinal among the sections this run actually renders.
    """

    anchor: str
    title: str
    number: str


@dataclass(frozen=True, kw_only=True)
class ReportSectionGroup:
    """One sidebar navigation group and the sections it holds."""

    label: str
    sections: tuple[ReportSection, ...]


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
    """The workflow file, relative to the project root when it sits below it."""
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
    section_groups: tuple[ReportSectionGroup, ...]
    summary_cards: tuple[StatCard, ...]
    notices: tuple[str, ...]
    """Run-level problems the run recorded, such as an undelivered notification."""
    params: Mapping[str, Any]
    graph: Graph
    tasks: tuple[TaskRow, ...]
    failures: tuple[FailureCard, ...]
    retried: tuple[RetriedTask, ...]
    outputs: tuple[OutputRow, ...]
    pages: tuple[PageCard, ...]
    environment: tuple[KVEntry, ...]

    # Footer / determinism
    ginkgo_version: str
    generated_at_label: str

    # Artifacts the renderer must copy into the bundle
    artifact_copies: tuple[ArtifactCopy, ...]

    @property
    def sections(self) -> tuple[ReportSection, ...]:
        """Every rendered section in document order."""
        return tuple(section for group in self.section_groups for section in group.sections)

    def section(self, anchor: str) -> ReportSection:
        """Return the section rendered at ``anchor``.

        Raises
        ------
        KeyError
            When no section with that anchor renders for this run. A template
            asking for one it does not render is a template bug, so this is
            loud rather than silently blank.
        """
        for section in self.sections:
            if section.anchor == anchor:
                return section
        raise KeyError(f"no report section renders at anchor {anchor!r}")


# ------------------------------------------------------------------------
# Builder
# ------------------------------------------------------------------------


_STATUS_TONE: dict[str, str] = {
    "succeeded": "ok",
    "cached": "ok",
    "failed": "fail",
    "cancelled": "neutral",
    "skipped": "neutral",
    "running": "warn",
    "pending": "warn",
    "unknown": "warn",
}

_STATUS_LABEL: dict[str, str] = {
    "succeeded": "ok",
    "cached": "cached",
    "failed": "fail",
    "skipped": "skipped",
    "running": "running",
    "pending": "pending",
    "unknown": "unknown",
}

#: Document order of the report's sections, and the sidebar group each one
#: belongs to. This is the single home for section order, titles, and anchors:
#: both the sidebar and the body headings number themselves from the sections
#: a given run renders, so an omitted section leaves no hole in the numerals.
_SECTION_LAYOUT: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
    (
        "Execution",
        (
            ("summary", "Summary"),
            ("params", "Parameters"),
            ("graph", "Task graph"),
            ("tasks", "Tasks"),
            ("failure", "Failure"),
            ("retries", "Retried attempts"),
        ),
    ),
    (
        "Results",
        (
            ("outputs", "Outputs"),
            ("pages", "Rendered pages"),
        ),
    ),
    (
        "Appendix",
        (("env", "Environment"),),
    ),
)


def build_report_data(
    *,
    summary: RunSummary,
    workspace_label: str | None = None,
    policy: SizingPolicy | None = None,
    ginkgo_version: str | None = None,
    generated_at: datetime | None = None,
) -> ReportData:
    """Build a :class:`ReportData` from a completed run.

    Parameters
    ----------
    summary : RunSummary
        The run to report on, loaded from the ledger.
    workspace_label : str | None
        Display label for the enclosing workspace. Inferred from the run
        directory's grandparent when omitted.
    policy : SizingPolicy | None
        Size caps applied at export time. Defaults to :class:`SizingPolicy`
        defaults.
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
    run_dir = summary.run_dir.resolve()

    # Terminal runs only — fail fast if the run is still live.
    if summary.status not in {"succeeded", "failed", "cancelled"}:
        raise ValueError(
            f"Run {summary.run_id!r} is not terminal (status={summary.status!r}); "
            "reports can only be exported for completed runs."
        )

    policy = policy or SizingPolicy()
    workspace_root = run_dir.parents[1] if len(run_dir.parents) >= 2 else run_dir.parent
    workspace_label = workspace_label or workspace_root.parent.name
    ginkgo_version = _resolve_ginkgo_version(ginkgo_version)

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
    retried = _build_retried(summary=summary)
    outputs = _build_outputs(summary=summary)
    pages = _build_pages(summary=summary, run_dir=run_dir, artifact_copies=artifact_copies)
    summary_cards = _build_summary_cards(summary=summary)

    status_raw = summary.status
    status_tone = _STATUS_TONE.get(status_raw, "warn")
    # The run's status in the words ``ginkgo runs`` uses for it.
    status_label = status_raw

    failed_count = summary.failed_count
    if failed_count > 0:
        sidebar_status_label = f"{status_label} · {failed_count} failed"
        sidebar_status_tone = "fail"
    else:
        sidebar_status_label = status_label
        sidebar_status_tone = status_tone

    workflow = _project_relative(summary.workflow, project_root=workspace_root.parent)
    masthead_chips = _build_masthead_chips(summary=summary)
    masthead_kv = _build_masthead_kv(
        summary=summary,
        workflow=workflow,
        workspace_label=workspace_label,
        status_label=status_label,
        ginkgo_version=ginkgo_version,
    )
    environment = _build_environment_kv(summary=summary, ginkgo_version=ginkgo_version)

    return ReportData(
        run_id=summary.run_id,
        workflow=workflow,
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
        section_groups=_build_section_groups(
            has_failures=len(failures) > 0,
            has_retries=len(retried) > 0,
            has_outputs=len(outputs) > 0,
            has_pages=len(pages) > 0,
        ),
        summary_cards=summary_cards,
        notices=summary.notices,
        params=summary.params,
        graph=graph,
        tasks=tasks,
        failures=failures,
        retried=retried,
        outputs=outputs,
        pages=pages,
        environment=environment,
        ginkgo_version=ginkgo_version,
        generated_at_label=format_timestamp(generated_at or datetime.now(UTC)),
        artifact_copies=tuple(artifact_copies),
    )


# ----- Document sections ---------------------------------------------------


def _build_section_groups(
    *,
    has_failures: bool,
    has_retries: bool,
    has_outputs: bool,
    has_pages: bool,
) -> tuple[ReportSectionGroup, ...]:
    """Number the sections this run renders, in document order.

    Optional sections drop out of the sequence entirely rather than reserving
    a numeral, so the rendered numbers are always contiguous.
    """
    renders = {
        "failure": has_failures,
        "retries": has_retries,
        "outputs": has_outputs,
        "pages": has_pages,
    }
    groups: list[ReportSectionGroup] = []
    number = 0
    for label, entries in _SECTION_LAYOUT:
        sections: list[ReportSection] = []
        for anchor, title in entries:
            if not renders.get(anchor, True):
                continue
            number += 1
            sections.append(ReportSection(anchor=anchor, title=title, number=f"{number:02d}"))
        if sections:
            groups.append(ReportSectionGroup(label=label, sections=tuple(sections)))
    return tuple(groups)


# ----- Task rows ----------------------------------------------------------


def _build_task_rows(*, summary: RunSummary) -> tuple[TaskRow, ...]:
    """Build ordered task rows for the task ledger."""
    rows: list[TaskRow] = []
    for task in summary.tasks:
        rows.append(
            TaskRow(
                task_key=task.task_key,
                node_id=task.node_id,
                name=task.name,
                label=task.label,
                kind_label=task.kind_label,
                status_label=_STATUS_LABEL.get(task.status, task.status),
                status_tone=_STATUS_TONE.get(task.status, "warn"),
                cache_label=task.cache_label,
                duration_label=format_duration(task.duration_s),
                attempts_label=task.attempts_label,
                memory_label=_memory_label(task),
                failed=task.status == "failed",
            )
        )
    return tuple(rows)


def _memory_label(task: TaskSummary) -> str:
    """Return ``measured / declared`` peak memory for the task ledger.

    The measurement is the task's own: a Python task's peak above the
    worker's resident memory as the body started, or the peak of a
    subprocess task's process tree. The declaration is shown as written
    (``64Mi``), not as the whole GiB the scheduler rounds it up to, unless
    a retry escalated it, when the denominator is the budget the attempt
    actually ran against. Shows the measurement alone when the task declared
    no memory, and an em dash when nothing was measured (cached or never-ran
    tasks).
    """
    usage = task.resource_usage or {}
    measured = usage.get("measured")
    if not isinstance(measured, dict):
        return "—"
    peak = measured.get("peak_rss_bytes")
    if not isinstance(peak, int) or peak < 0:
        return "—"
    label = format_bytes(peak)
    declared = usage.get("declared") if isinstance(usage.get("declared"), dict) else {}
    declared_gb = declared.get("memory_gb")
    effective_gb = declared.get("effective_memory_gb")
    if isinstance(effective_gb, int) and effective_gb > (declared_gb or 0):
        return f"{label} / {effective_gb} GiB"
    if isinstance(declared.get("memory"), str):
        return f"{label} / {declared['memory']}"
    if isinstance(declared_gb, int) and declared_gb > 0:
        return f"{label} / {declared_gb} GiB"
    return label


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

    def _level(node_id: int, visiting: frozenset[int] = frozenset()) -> int:
        """Return the longest path from a root to *node_id*.

        A task graph is a DAG, so a cycle here means something upstream is
        wrong. The report is not the place to find out: the recursion would
        run until the stack gave out. An edge that closes a cycle is ignored
        instead, which draws the graph as if the back-edge were not there.
        """
        if node_id in level:
            return level[node_id]
        if node_id in visiting:
            return 0
        parents = predecessors.get(node_id, [])
        reachable = [_level(parent, visiting | {node_id}) for parent in parents]
        value = 1 + max(reachable) if reachable else 0
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
    if task.rendered_html is not None:
        return "terminal"
    if task.status in {"succeeded", "cached"}:
        return "ok"
    return "neutral"


def _graph_label(task: TaskSummary) -> tuple[str, str | None]:
    """Return the display label and optional sub-label for a graph node."""
    label = task.label
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
        category = task.failure_kind

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
                label=task.label,
                category=category,
                exit_code=task.exit_code,
                attempts_label=task.attempts_label,
                message=task.error,
                log_tail=log_tail,
                log_relpath=log_relpath,
            )
        )
    return tuple(cards)


def _build_retried(*, summary: RunSummary) -> tuple[RetriedTask, ...]:
    """List each task that ran more than once, with every attempt's outcome."""
    retried: list[RetriedTask] = []
    for task in summary.tasks:
        if len(task.attempt_history) < 2:
            continue
        attempts = []
        for attempt in task.attempt_history:
            failure = attempt.get("failure")
            message = failure.get("message") if isinstance(failure, dict) else None
            attempts.append(
                AttemptNote(
                    number=int(attempt.get("attempt") or 0),
                    status=str(attempt.get("status") or "unknown"),
                    message=message if isinstance(message, str) else None,
                )
            )
        retried.append(
            RetriedTask(task_key=task.task_key, label=task.label, attempts=tuple(attempts))
        )
    return tuple(retried)


def _build_outputs(*, summary: RunSummary) -> tuple[OutputRow, ...]:
    """List the files and folders each task produced or reused from the cache."""
    rows: list[OutputRow] = []
    for task in summary.tasks:
        for output in task.outputs:
            path = output.get("path")
            if not isinstance(path, str):
                continue
            rows.append(
                OutputRow(
                    task_key=task.task_key,
                    label=task.label,
                    name=str(output.get("name") or "return"),
                    kind=str(output.get("type") or "file"),
                    path=path,
                )
            )
    return tuple(rows)


def _first_log_path(*, task: TaskSummary, run_dir: Path) -> Path | None:
    """Return the first existing log path for a task (stderr preferred)."""
    for rel in (task.stderr_log, task.stdout_log):
        if isinstance(rel, str) and rel:
            path = (run_dir / rel).resolve()
            if path.is_file():
                return path
    return None


# ----- Rendered pages -----------------------------------------------------


def _build_pages(
    *,
    summary: RunSummary,
    run_dir: Path,
    artifact_copies: list[ArtifactCopy],
) -> tuple[PageCard, ...]:
    """Build a card for each page a task rendered, such as a notebook's HTML."""
    cards: list[PageCard] = []
    for task in summary.tasks:
        if task.rendered_html is None:
            continue
        source = task.rendered_html_absolute(run_dir=run_dir)
        if source is None or not source.is_file():
            continue
        # Keyed by task, not name: the branches of a fan-out share a name.
        stem = f"pages/{task.task_key}_{task.base_name}"
        dest = f"{stem}.html"
        artifact_copies.append(ArtifactCopy(source=source, dest_relpath=dest))
        page = source.read_text(encoding="utf-8", errors="replace")
        static = static_notebook_page(page=page, title=task.label)

        size_bytes = source.stat().st_size
        sub_parts = [format_duration(task.duration_s), format_bytes(size_bytes)]
        if task.rendered_html_run_id not in (None, summary.run_id):
            sub_parts.append(f"from run {task.rendered_html_run_id}")
        cards.append(
            PageCard(
                task_key=task.task_key,
                title=task.label,
                sub_line=" · ".join(sub_parts),
                link_relpath=dest,
                status_tone=_STATUS_TONE.get(task.status, "warn"),
                embed_html=static if static is not None else page,
                static_relpath=f"{stem}.static.html" if static is not None else None,
            )
        )
    return tuple(cards)


# ----- Masthead / summary / environment ----------------------------------


def _build_summary_cards(*, summary: RunSummary) -> tuple[StatCard, ...]:
    """Headline stats shown above the fold."""
    total_tasks = summary.task_count
    succeeded = summary.succeeded_count + summary.cached_count
    failed = summary.failed_count
    cache_hits = sum(1 for task in summary.tasks if task.cached or task.status == "cached")

    tasks_sub_parts: list[str] = []
    if succeeded:
        tasks_sub_parts.append(f"{succeeded} ok")
    if failed:
        tasks_sub_parts.append(f"{failed} fail")
    tasks_sub = " · ".join(tasks_sub_parts) or "no tasks"

    failure_categories = [
        task.failure_kind for task in summary.failed_tasks if task.failure_kind is not None
    ]
    failure_sub = failure_categories[0] if failure_categories else "—"

    return (
        StatCard(
            label="Tasks",
            value=format_int(total_tasks),
            sub=tasks_sub,
            tone="ok",
        ),
        StatCard(
            label="Failures",
            value=format_int(failed),
            sub=failure_sub,
            tone="fail" if failed else "neutral",
        ),
        StatCard(
            label="Cache hits",
            value=f"{cache_hits} / {total_tasks}",
            sub=_cache_sub(cache_hits=cache_hits, total_tasks=total_tasks),
            tone="cool",
        ),
    )


def _cache_sub(*, cache_hits: int, total_tasks: int) -> str:
    """Say what share of the run's tasks were reused from the cache."""
    if total_tasks == 0:
        return "no tasks"
    if cache_hits == 0:
        return "every task ran"
    if cache_hits == total_tasks:
        return "every task reused from the cache"
    return f"{100 * cache_hits / total_tasks:.0f} % of tasks reused from the cache"


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
    workflow: str | None,
    workspace_label: str | None,
    status_label: str,
    ginkgo_version: str,
) -> tuple[KVEntry, ...]:
    """Key-value rows for the main masthead grid."""
    entries = [
        KVEntry(key="run_id", value=summary.run_id),
        KVEntry(key="workflow", value=workflow or "—"),
    ]
    if workspace_label:
        entries.append(KVEntry(key="workspace", value=workspace_label))
    entries.extend(
        [
            KVEntry(key="status", value=status_label),
            KVEntry(key="started", value=format_timestamp(summary.started_at)),
            KVEntry(key="finished", value=format_timestamp(summary.finished_at)),
            KVEntry(key="duration", value=format_duration(summary.duration_s)),
            KVEntry(key="ginkgo", value=ginkgo_version),
        ]
    )
    return tuple(entries)


def _project_relative(path: str | None, *, project_root: Path) -> str | None:
    """Return *path* relative to *project_root* when it sits below it.

    A report is read on other machines, where the absolute path the run
    recorded names someone else's checkout.
    """
    if path is None:
        return None
    try:
        return str(Path(path).resolve().relative_to(project_root.resolve()))
    except ValueError:
        return path


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
    execute = summary.timings.get("workflow_execute_seconds")
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
