"""Rich live renderer for ``ginkgo run``."""

from __future__ import annotations

import json
import time
from collections import Counter
from pathlib import Path

import yaml
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.rule import Rule
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from ginkgo.cli.renderers.common import (
    _MultiStateBar,
    _format_cpu_percent,
    _core_unit_label,
    _status_label,
    _status_text,
    task_base_name,
    _task_duration_plain,
    _task_duration_text,
    _task_label_width,
    _time_of_day_spinner,
    _truncate_task_label,
)
from ginkgo.envs.interpreter import EnvironmentFinding, explain_import_failure
from ginkgo.formatting import format_bytes, format_duration
from ginkgo.runtime.run_summary import TERMINAL_STATUSES
from ginkgo.cli.renderers.models import (
    CliAssetSummary,
    FailureDetails,
    CliNotebookSummary,
    ResourceRenderState,
    CliRunSummary,
    _TaskGroup,
    _TaskRow,
)

_GROUP_THRESHOLD = 6
"""Minimum invocation count to collapse same-task rows into a group."""

_LIVE_REFRESH_STATUSES = frozenset(
    {"preparing env", "staging", "submitted", "running", "succeeded", "failed"}
)
"""Status transitions significant enough to force an immediate Live refresh."""

_ENV_PREPARE_STATUS = "preparing env"
"""Display status shown while a task's execution environment is installed."""

_ENV_PREPARE_REPORT_THRESHOLD_SECONDS = 1.0
"""Minimum total environment preparation time worth explaining in the summary."""


class _RunEventState:
    """Track per-node task status as JSON event lines arrive from the evaluator."""

    def __init__(self) -> None:
        self._name_counts: Counter[str] = Counter()
        self.rows: dict[int, _TaskRow] = {}
        self.row_order: list[int] = []
        self.notices: list[str] = []
        self.env_prepare_seconds = 0.0
        self.prepared_envs: list[str] = []
        self._env_prepare_started: dict[int, float] = {}

    def seed(self, *, planned_tasks: list[tuple[int, str, str, str]]) -> None:
        """Register the planned task set before any events arrive.

        Each entry is ``(node_id, task_name, label, env_label)``. The label
        comes from the graph, so a fan-out branch reads the same here —
        before it is dispatched — as it does under ``--dry-run``.
        """
        for node_id, task_name, label, env_label in planned_tasks:
            self.rows[node_id] = _TaskRow(
                node_id=node_id,
                task_name=task_name,
                label=label,
                env_label=env_label,
            )
            self.row_order.append(node_id)

    def label_for_node(self, node_id: int) -> str | None:
        """Return the current display label for a node, if known."""
        row = self.rows.get(node_id)
        return None if row is None else row.label

    def handle_event_line(self, line: str) -> str:
        """Apply one JSON event line to task state.

        Returns
        -------
        str
            The status that should trigger a live refresh, or ``""`` when
            the event does not warrant one.
        """
        payload = json.loads(line)
        node_id = int(payload.get("node_id", -1))
        task_name = str(payload["task"])
        status = str(payload["status"])
        display_label = payload.get("display_label")
        if status == "notice":
            message = payload.get("message")
            if isinstance(message, str) and message:
                self.notices.append(message)
                return "notice"
            return ""

        event_time = time.perf_counter()
        if node_id not in self.rows:
            label = self.label_for(task_name=task_name)
            self.rows[node_id] = _TaskRow(
                node_id=node_id,
                task_name=task_name,
                label=label,
                env_label="local",
            )
            self.row_order.append(node_id)
        row = self.rows[node_id]
        if isinstance(display_label, str):
            self._apply_display_label(node_id=node_id, display_label=display_label)
        prepare_ended = self._track_env_prepare(
            node_id=node_id,
            status=status,
            env=payload.get("env"),
            event_time=event_time,
        )
        row.status = status
        # Environment preparation deliberately does not start the row clock:
        # the reported duration is the task's own work, not its install.
        if status in {"staging", "submitted", "running"}:
            row.started_at = row.started_at or event_time
            row.finished_at = None
        elif status in TERMINAL_STATUSES:
            row.started_at = row.started_at or event_time
            row.finished_at = event_time
        # Only refresh on state transitions that the user needs to see
        # immediately. Rapid cache hits are batched by Rich's internal
        # refresh rate to avoid flicker. Leaving `preparing env` is as visible
        # a transition as entering it, whatever status follows.
        if status in _LIVE_REFRESH_STATUSES or prepare_ended:
            return status
        return ""

    def _track_env_prepare(
        self,
        *,
        node_id: int,
        status: str,
        env: object,
        event_time: float,
    ) -> bool:
        """Accumulate time a node spent preparing its execution environment.

        Returns
        -------
        bool
            True when this event ended an in-progress preparation window,
            whether it completed or failed.
        """
        if status == _ENV_PREPARE_STATUS:
            self._env_prepare_started[node_id] = event_time
            if isinstance(env, str) and env not in self.prepared_envs:
                self.prepared_envs.append(env)
            return False

        started = self._env_prepare_started.pop(node_id, None)
        if started is None:
            return False
        self.env_prepare_seconds += max(0.0, event_time - started)
        return True

    def label_for(self, *, task_name: str) -> str:
        """Return a display label for a node the plan did not announce.

        Only nodes the graph grew mid-run reach this: every planned node is
        seeded with its graph label. Such a node's own label arrives with
        its first event carrying one, so this is a placeholder that only
        has to stay distinct from its siblings.
        """
        base_name = task_name.rsplit(".", 1)[-1]
        self._name_counts[base_name] += 1
        count = self._name_counts[base_name]
        return base_name if count == 1 else f"{base_name}[{count}]"

    def _apply_display_label(self, *, node_id: int, display_label: str) -> None:
        """Replace a fallback duplicate label with a richer runtime label."""
        row = self.rows[node_id]
        if row.label == display_label:
            return
        if any(
            other_id != node_id and other_row.label == display_label
            for other_id, other_row in self.rows.items()
        ):
            return
        row.label = display_label

    def ordered_rows(self) -> list[_TaskRow]:
        """Return task rows in first-seen order."""
        return [self.rows[node_id] for node_id in self.row_order]

    def display_items(self) -> list[_TaskGroup | _TaskRow]:
        """Build the grouped display list from ordered rows.

        Tasks with ≥ _GROUP_THRESHOLD invocations sharing the same task_name
        are collapsed into a single ``_TaskGroup``. Others remain as individual
        ``_TaskRow`` entries. Display order follows first-seen position of each
        task_name.
        """
        # Count invocations per task_name.
        name_counts: Counter[str] = Counter(self.rows[nid].task_name for nid in self.row_order)

        # Build groups for names that meet the threshold.
        groups: dict[str, _TaskGroup] = {}
        items: list[_TaskGroup | _TaskRow] = []
        seen_names: set[str] = set()

        for node_id in self.row_order:
            row = self.rows[node_id]
            name = row.task_name

            if name_counts[name] < _GROUP_THRESHOLD:
                items.append(row)
                continue

            if name not in seen_names:
                # Determine common environment label.
                env_labels = {
                    self.rows[nid].env_label
                    for nid in self.row_order
                    if self.rows[nid].task_name == name
                }
                env_label = env_labels.pop() if len(env_labels) == 1 else "mixed"
                base = task_base_name(name)
                group = _TaskGroup(
                    task_name=name,
                    label=f"{base} (×{name_counts[name]})",
                    env_label=env_label,
                    rows=[],
                )
                groups[name] = group
                items.append(group)
                seen_names.add(name)

            groups[name].rows.append(row)

        return items

    def status_counts(self) -> Counter[str]:
        """Return the count of task rows in each status."""
        counts: Counter[str] = Counter(row.status for row in self.rows.values())
        for status in ("waiting", "running", "cached", "succeeded", "failed"):
            counts.setdefault(status, 0)
        return counts

    def terminal_count(self) -> int:
        """Return the number of task rows in a terminal status."""
        counts = self.status_counts()
        return sum(counts[status] for status in TERMINAL_STATUSES)


class _RunLayoutRenderer:
    """Render the live Rich layout and end-of-run panels for a run."""

    def __init__(
        self,
        *,
        console: Console,
        summary: CliRunSummary,
        resources: ResourceRenderState | None,
        state: _RunEventState,
        activity_spinner: Spinner,
        time_spinner: Spinner,
    ) -> None:
        self._console = console
        self._summary = summary
        self._resources = resources
        self._state = state
        self._activity_spinner = activity_spinner
        self._time_spinner = time_spinner

    def render_run_layout(self, *, now: float) -> Group:
        """Return the full live layout: resource line, notices, table, progress."""
        return Group(
            self.render_resource_info_line(),
            Text(""),
            self.render_notice_lines(),
            self.render_status_line(now=now),
            self.render_task_table(now=now),
            self.render_progress_section(),
        )

    def render_status_line(self, *, now: float) -> Table:
        line = Table.grid(padding=(0, 1))
        line.add_column(no_wrap=True)
        line.add_column(no_wrap=True)
        line.add_column(no_wrap=True)
        line.add_column(no_wrap=True)
        line.add_row(
            " " * self.status_line_padding(now=now),
            self._activity_spinner,
            Text("Running", style="bold #0f766e"),
            self._time_spinner,
        )
        return line

    def render_notice_lines(self) -> Text:
        """Render task-scoped runtime notices above the live table."""
        text = Text()
        for index, notice in enumerate(self._state.notices):
            if index > 0:
                text.append("\n")
            text.append(notice, style="bold")
        return text

    def render_resource_info_line(self) -> Text:
        """Render the live locality and resource summary line."""
        text = Text()
        if self._summary.executor_label != "local":
            text.append("☁️  ", style="cyan")
            text.append(f"Running on {self._summary.executor_label}", style="bold")
        else:
            text.append("💻 ", style="cyan")
            text.append(
                f"Running locally on {self._summary.cores} {_core_unit_label(self._summary.cores)}",
                style="bold",
            )
        # Tasks pinned with executor= dispatch to their own executor whatever
        # the run default is, so a "locally" header would otherwise hide them.
        if self._summary.pinned_executors:
            text.append(f" → {', '.join(self._summary.pinned_executors)}", style="cyan")
        text.append(" ")
        text.append("(")
        text.append(self.resource_label(), style="dim")
        text.append(")", style="dim")
        return text

    def render_task_table(self, *, now: float) -> Table:
        table = Table(
            box=box.SQUARE,
            border_style="#0f766e",
            header_style="bold #134e4a",
            expand=False,
        )
        table.add_column("Task", style="bold", no_wrap=True)
        table.add_column("Status", no_wrap=True)
        table.add_column("Environment", no_wrap=True)
        table.add_column("Time", justify="right", no_wrap=True)
        max_label = _task_label_width(self._console)
        for item in self._state.display_items():
            if isinstance(item, _TaskGroup):
                # Collapsed group row with multi-state progress bar.
                counts = item.status_counts()
                total = len(item.rows)
                terminal = item.terminal_count()
                bar_width = max(16, self.status_column_width() - len(f" {terminal}/{total}") - 1)
                bar = _MultiStateBar(counts=counts, total=total, width=bar_width)
                # Build status cell: bar + count label.
                bar_text = Text()
                for chunk in bar.__rich_console__(self._console, self._console.options):
                    if isinstance(chunk, Text):
                        bar_text.append_text(chunk)
                bar_text.append(f" {terminal}/{total}", style="bold #134e4a")
                elapsed = item.elapsed(now=now)
                time_str = format_duration(elapsed) if elapsed is not None else "--"
                table.add_row(
                    Text(
                        _truncate_task_label(item.label, max_width=max_label),
                        style="bold",
                    ),
                    bar_text,
                    Text(item.env_label, style="bold #134e4a"),
                    Text(time_str, style="dim"),
                )
            else:
                table.add_row(
                    Text(
                        _truncate_task_label(item.label, max_width=max_label),
                        style="bold",
                    ),
                    _status_text(item.status),
                    Text(item.env_label, style="bold #134e4a"),
                    _task_duration_text(item, now=now),
                )
        return table

    def render_progress_section(self) -> Table:
        completed = self._state.terminal_count()
        total = max(1, len(self._state.rows))
        progress = Table.grid(padding=(0, 1))
        progress.add_column()
        progress.add_column(no_wrap=True)
        progress_text = f"{completed}/{len(self._state.rows)} complete"
        progress.add_row(
            ProgressBar(
                total=total,
                completed=completed,
                width=max(16, self.task_table_width() - len(progress_text) - 1),
                complete_style="bold #0f766e",
                finished_style="bold #0f766e",
                pulse_style="#99f6e4",
                style="dim #134e4a",
            ),
            Text(progress_text, style="bold #134e4a"),
        )
        return progress

    def resource_label(self) -> str:
        """Return the compact inline CPU/RSS monitor label."""
        resources = self.resource_summary()
        if resources is None:
            return "CPU --   RSS --   Procs --"

        current = resources.get("current")
        if not isinstance(current, dict):
            return "CPU --   RSS --   Procs --"

        return (
            f"CPU {_format_cpu_percent(_as_float(current.get('cpu_percent')))}   "
            f"RSS {format_bytes(_as_int(current.get('rss_bytes')))}   "
            f"Procs {_format_count(current.get('process_count'))}"
        )

    def render_notebooks(self, notebooks: list[CliNotebookSummary]) -> Text:
        """Render this run's notebooks, separating fresh from replayed.

        A cache hit does not re-render, so its artifact and recorded render
        status both belong to an earlier run. Those rows are still listed —
        the artifact is what a reader will open — but they are counted apart
        from the notebooks this run materialised, so a replayed export
        failure does not read as a new one.
        """
        text = Text()
        replayed = [nb for nb in notebooks if nb.replayed]
        materialised = len(notebooks) - len(replayed)
        failed_count = sum(1 for nb in notebooks if nb.render_failed and not nb.replayed)
        text.append(f"\n📓 Notebooks materialised ({materialised})", style="bold")
        if replayed:
            source_count = len({nb.replayed_from_run_id for nb in replayed})
            source = "an earlier run" if source_count == 1 else "earlier runs"
            text.append(f"  ↺ {len(replayed)} from {source}", style="bold #0f766e")
        if failed_count:
            text.append(f"  ⚠ {failed_count} HTML export failed", style="bold yellow")
        text.append("\n")
        for nb in notebooks:
            url = nb.html_path.as_uri()
            text.append(f"  {nb.task_label}  ", style="bold #134e4a")
            text.append(str(nb.html_path), style=f"link {url} #0f766e")
            if nb.replayed_from_run_id is not None:
                text.append(f"  ↺ from run {nb.replayed_from_run_id}", style="#0f766e")
            if nb.render_failed:
                text.append("  ⚠ HTML export failed", style="bold yellow")
            text.append("\n")
        return text

    def render_assets(self, assets: list[CliAssetSummary]) -> Text:
        """Render the list of assets materialised in this run."""
        text = Text()
        text.append(f"\n📦 Assets materialised ({len(assets)})\n", style="bold")
        for asset in assets:
            text.append(f"  {asset.name}\n", style="bold #134e4a")
        return text

    def render_failure_details(self, details: list[FailureDetails]):
        parts: list[object] = []
        category_summary = self.render_failure_category_summary(details)
        if category_summary is not None:
            parts.append(category_summary)
        hinted, hint = _interpreter_hint(details)
        parts.extend(
            self.render_failure_panel(item, hint=hint if item is hinted else None)
            for item in details
        )
        return Group(*parts)

    def render_failure_category_summary(self, details: list[FailureDetails]) -> Text | None:
        """Return a one-line summary grouping failures by category, if any."""
        categorised = [item for item in details if item.failure_kind]
        if not categorised:
            return None
        counts = Counter(item.failure_kind for item in categorised)
        parts = [f"{kind}×{count}" for kind, count in sorted(counts.items())]
        summary = Text()
        summary.append("Failures by category: ", style="bold #7f1d1d")
        summary.append(", ".join(parts), style="#7f1d1d")
        summary.append("\n")
        return summary

    def render_failure_panel(
        self,
        details: FailureDetails,
        *,
        hint: EnvironmentFinding | None = None,
    ) -> Panel:
        summary = Table.grid(padding=(0, 1))
        summary.add_column(style="bold #7f1d1d", no_wrap=True)
        summary.add_column()
        summary.add_row("Task", details.task_label)
        if details.failure_kind:
            summary.add_row("Category", details.failure_kind)
        summary.add_row(
            "Exit code", str(details.exit_code) if details.exit_code is not None else "?"
        )
        if details.error:
            summary.add_row("Reason", Text(details.error, style="#7f1d1d"))
        if details.log_path is not None:
            summary.add_row("Log", str(details.log_path))

        sections: list[object] = [summary]
        if hint is not None:
            sections.append(Text(""))
            sections.append(Text("\n".join(hint.hint_lines), style="yellow"))
        if self._summary.mode == "verbose" and details.inputs:
            sections.append(Text(""))
            sections.append(Text("Inputs", style="bold #7f1d1d"))
            sections.append(
                Text(
                    yaml.safe_dump(details.inputs, sort_keys=False).rstrip(),
                    style="#7f1d1d",
                )
            )
        if details.log_tail:
            sections.append(Text(""))
            sections.append(Text("Log tail", style="bold #7f1d1d"))
            sections.append(Text("\n".join(details.log_tail), style="#7f1d1d"))

        return Panel(
            Group(*sections),
            title=f"[bold red]Failure Details: {details.task_label}[/]",
            border_style="red",
            box=box.SQUARE,
            expand=False,
        )

    def render_env_prepare_summary(self) -> Text | None:
        """Explain a slow first run caused by environment preparation."""
        if self._state.env_prepare_seconds < _ENV_PREPARE_REPORT_THRESHOLD_SECONDS:
            return None
        envs = ", ".join(self._state.prepared_envs)
        env_part = f" ({envs})" if envs else ""
        return Text(
            f"⚙ Environment preparation took "
            f"{format_duration(self._state.env_prepare_seconds)}{env_part} - "
            f"first runs install environments, later runs reuse them",
            style="dim",
        )

    def render_failure_separator(self) -> Rule:
        """Render a separator before end-of-run failure diagnostics."""
        return Rule(style="dim")

    def status_column_width(self) -> int:
        """Return the effective width available for the status column."""
        rows = self._state.ordered_rows()
        if not rows:
            return len("Status")
        return max(len("Status"), *(len(_status_label(row.status)) for row in rows), 30)

    def task_table_width(self, *, now: float | None = None) -> int:
        items = self._state.display_items()
        if not items:
            return len("Task") + len("Status") + len("Environment") + len("Time") + 13

        max_label = _task_label_width(self._console)
        clock = time.perf_counter() if now is None else now
        task_widths: list[int] = [len("Task")]
        status_widths: list[int] = [len("Status")]
        env_widths: list[int] = [len("Environment")]
        time_widths: list[int] = [len("Time")]

        for item in items:
            if isinstance(item, _TaskGroup):
                task_widths.append(len(_truncate_task_label(item.label, max_width=max_label)))
                total = len(item.rows)
                terminal = item.terminal_count()
                status_widths.append(self.status_column_width() + len(f" {terminal}/{total}") + 1)
                env_widths.append(len(item.env_label))
                elapsed = item.elapsed(now=clock)
                time_widths.append(len(format_duration(elapsed)) if elapsed is not None else 2)
            else:
                task_widths.append(len(_truncate_task_label(item.label, max_width=max_label)))
                status_widths.append(len(_status_label(item.status)))
                env_widths.append(len(item.env_label))
                time_widths.append(len(_task_duration_plain(item, now=clock)))

        column_padding = 8
        separators = 5
        return (
            max(task_widths)
            + max(status_widths)
            + max(env_widths)
            + max(time_widths)
            + column_padding
            + separators
        )

    def status_line_padding(self, *, now: float) -> int:
        status_width = len("Running") + 5
        return max(0, (self.task_table_width(now=now) - status_width) // 2)

    def resource_summary(self) -> dict[str, object] | None:
        """Return the latest available resource summary."""
        if self._resources is None:
            return None
        return self._resources.provider()

    def render_resource_footer(self, resources: dict[str, object]) -> Text | None:
        """Render the final CPU/RSS summary line."""
        average = resources.get("average")
        peak = resources.get("peak")
        if not isinstance(average, dict) or not isinstance(peak, dict):
            return None

        avg_cpu = _format_cpu_percent(_as_float(average.get("cpu_percent")))
        peak_cpu = _format_cpu_percent(_as_float(peak.get("cpu_percent")))
        avg_rss = format_bytes(_as_int(average.get("rss_bytes")))
        peak_rss = format_bytes(_as_int(peak.get("rss_bytes")))
        return Text(
            f"CPU avg {avg_cpu}, peak {peak_cpu} | RSS avg {avg_rss}, peak {peak_rss}",
            style="dim",
        )


class CliRunRenderer:
    """Render human-friendly task lifecycle output from evaluator JSON events.

    Coordinates run lifecycle (start/finish, Live updates) between
    ``_RunEventState`` (task status bookkeeping) and ``_RunLayoutRenderer``
    (Rich rendering), and adapts the file-like ``write``/``flush`` interface
    the evaluator writes JSON events to.
    """

    def __init__(
        self,
        *,
        console: Console,
        summary: CliRunSummary,
        resources: ResourceRenderState | None = None,
    ) -> None:
        self._console = console
        self._summary = summary
        self._buffer = ""
        self._state = _RunEventState()
        self._layout = _RunLayoutRenderer(
            console=console,
            summary=summary,
            resources=resources,
            state=self._state,
            activity_spinner=Spinner("dots", style="bold #0f766e"),
            time_spinner=Spinner(_time_of_day_spinner(), style="bold #0f766e"),
        )
        self._live: Live | None = None
        self._started = False
        self._run_started_at: float | None = None
        self._final_elapsed: float | None = None
        self._success: bool | None = None

    def start(self, *, planned_tasks: list[tuple[int, str, str, str]]) -> None:
        """Begin a CLI run section.

        Each planned task is ``(node_id, task_name, label, env_label)``.
        """
        self._state.seed(planned_tasks=planned_tasks)
        self._started = True
        self._run_started_at = time.perf_counter()
        self._live = Live(self, console=self._console, refresh_per_second=12, transient=False)
        if self._console.is_terminal:
            self._live.start()
        else:
            self._live = None

    def write(self, text: str) -> int:
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self._handle_event_line(line)
        return len(text)

    def flush(self) -> None:
        self._console.file.flush()

    def finish(
        self,
        *,
        elapsed: float,
        success: bool,
        resources: dict[str, object] | None = None,
        failure_details: list[FailureDetails] | None = None,
        notebooks: list[CliNotebookSummary] | None = None,
        assets: list[CliAssetSummary] | None = None,
        remote_summary: str | None = None,
    ) -> None:
        """Print the final run summary."""
        if self._buffer.strip():
            self._handle_event_line(self._buffer.strip())
            self._buffer = ""

        self._final_elapsed = elapsed
        self._success = success
        if self._live is not None:
            self._live.refresh()
            self._live.stop()
        elif self._started:
            self._console.print(self)

        counts = self._state.status_counts()
        cached = counts["cached"]
        executed = counts["succeeded"] + counts["failed"]
        if success:
            self._console.print(
                f"\n[bold cyan]⏱[/] Completed in [bold]{format_duration(elapsed)}[/] - "
                f"{executed} tasks executed, {cached} cached"
            )
        else:
            failed = counts["failed"]
            self._console.print(
                f"\n[bold red]✖[/] Failed in [bold]{format_duration(elapsed)}[/] - "
                f"{executed} tasks executed, {cached} cached, {failed} failed"
            )
        env_prepare_summary = self._layout.render_env_prepare_summary()
        if env_prepare_summary is not None:
            self._console.print(env_prepare_summary)
        resource_summary = resources or self._layout.resource_summary()
        if resource_summary is not None:
            resource_footer = self._layout.render_resource_footer(resource_summary)
            if resource_footer is not None:
                self._console.print(resource_footer)
        if remote_summary is not None:
            self._console.print(f"[dim]☁️  {remote_summary}[/dim]")
        if not success and failure_details:
            self._console.print(self._layout.render_failure_separator())
            self._console.print(self._layout.render_failure_details(failure_details))
        if success:
            if notebooks:
                self._console.print(self._layout.render_notebooks(notebooks))
            if assets:
                self._console.print(self._layout.render_assets(assets))
            self._console.print(f"Run directory: {self._summary.run_dir}")

    def label_for_node(self, node_id: int) -> str | None:
        """Return the current display label for a node, if known."""
        return self._state.label_for_node(node_id)

    def __rich__(self):
        return self._layout.render_run_layout(now=self._elapsed_clock())

    def _handle_event_line(self, line: str) -> None:
        refresh_status = self._state.handle_event_line(line)
        if refresh_status and self._live is not None:
            self._live.refresh()

    def _elapsed_clock(self) -> float:
        if self._final_elapsed is not None and self._run_started_at is not None:
            return self._run_started_at + self._final_elapsed
        return time.perf_counter()


def _as_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _format_count(value: object) -> str:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.1f}"
    return "--"


def _interpreter_hint(
    details: list[FailureDetails],
) -> tuple[FailureDetails | None, EnvironmentFinding | None]:
    """Return the one failure that should carry the environment hint, and it.

    A Python task body imports in the CLI's own interpreter, so a missing
    module there is as likely to mean the wrong interpreter as a missing
    dependency, and the message alone would send the reader to the manifest —
    the one thing that is already right. A shell or script body fails in a
    subprocess of its own, where none of that advice applies, so those are
    left alone.

    The hint is the same for every task in a run, and the manifest is read to
    build it, so it is computed once here and attached to the first failure
    that qualifies rather than repeated down every panel.
    """
    for item in details:
        if not item.ran_in_cli_interpreter:
            continue
        finding = explain_import_failure(message=item.error, project_root=Path.cwd())
        if finding is not None:
            return item, finding
    return None, None
