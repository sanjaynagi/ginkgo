"""Rich live renderer for ``ginkgo run``."""

from __future__ import annotations

import json
import time
from collections import Counter

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
    _format_bytes,
    _format_cpu_percent,
    _core_unit_label,
    _format_duration,
    _status_label,
    _status_text,
    _task_base_name,
    _task_duration_plain,
    _task_duration_text,
    _task_label_width,
    _time_of_day_spinner,
    _truncate_task_label,
)
from ginkgo.cli.renderers.models import (
    _AssetSummary,
    _FailureDetails,
    _NotebookSummary,
    _ResourceRenderState,
    _RunSummary,
    _TaskGroup,
    _TaskRow,
)

_GROUP_THRESHOLD = 6
"""Minimum invocation count to collapse same-task rows into a group."""


class _CliRunRenderer:
    """Render human-friendly task lifecycle output from evaluator JSON events."""

    def __init__(
        self,
        *,
        console: Console,
        summary: _RunSummary,
        resources: _ResourceRenderState | None = None,
    ) -> None:
        self._console = console
        self._summary = summary
        self._resources = resources
        self._buffer = ""
        self._name_counts: Counter[str] = Counter()
        self._rows: dict[int, _TaskRow] = {}
        self._row_order: list[int] = []
        self._live: Live | None = None
        self._started = False
        self._run_started_at: float | None = None
        self._final_elapsed: float | None = None
        self._success: bool | None = None
        self._notices: list[str] = []
        self._activity_spinner = Spinner("dots", style="bold #0f766e")
        self._time_spinner = Spinner(_time_of_day_spinner(), style="bold #0f766e")

    def start(self, *, planned_tasks: list[tuple[int, str, str]]) -> None:
        """Begin a CLI run section."""
        for node_id, task_name, env_label in planned_tasks:
            label = self._label_for(node_id=node_id, task_name=task_name)
            self._rows[node_id] = _TaskRow(
                node_id=node_id,
                task_name=task_name,
                label=label,
                env_label=env_label,
            )
            self._row_order.append(node_id)
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
        failure_details: list[_FailureDetails] | None = None,
        notebooks: list[_NotebookSummary] | None = None,
        assets: list[_AssetSummary] | None = None,
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

        counts = self._status_counts()
        cached = counts["cached"]
        executed = counts["succeeded"] + counts["failed"]
        if success:
            self._console.print(
                f"\n[bold cyan]⏱[/] Completed in [bold]{_format_duration(elapsed)}[/] - "
                f"{executed} tasks executed, {cached} cached"
            )
        else:
            failed = counts["failed"]
            self._console.print(
                f"\n[bold red]✖[/] Failed in [bold]{_format_duration(elapsed)}[/] - "
                f"{executed} tasks executed, {cached} cached, {failed} failed"
            )
        resource_summary = resources or self._resource_summary()
        if resource_summary is not None:
            resource_footer = self._render_resource_footer(resource_summary)
            if resource_footer is not None:
                self._console.print(resource_footer)
        if not success and failure_details:
            self._console.print(self._render_failure_separator())
            self._console.print(self._render_failure_details(failure_details))
        if success:
            if notebooks:
                self._console.print(self._render_notebooks(notebooks))
            if assets:
                self._console.print(self._render_assets(assets))
            self._console.print(f"Run directory: {self._summary.run_dir}")

    def label_for_node(self, node_id: int) -> str | None:
        """Return the current display label for a node, if known."""
        row = self._rows.get(node_id)
        return None if row is None else row.label

    def __rich__(self):
        return self._render_run_layout()

    def _handle_event_line(self, line: str) -> None:
        payload = json.loads(line)
        node_id = int(payload.get("node_id", -1))
        task_name = str(payload["task"])
        status = str(payload["status"])
        display_label = payload.get("display_label")
        if status == "notice":
            message = payload.get("message")
            if isinstance(message, str) and message:
                self._notices.append(message)
                if self._live is not None:
                    self._live.refresh()
            return
        event_time = time.perf_counter()
        if node_id not in self._rows:
            label = self._label_for(node_id=node_id, task_name=task_name)
            self._rows[node_id] = _TaskRow(
                node_id=node_id,
                task_name=task_name,
                label=label,
                env_label="local",
            )
            self._row_order.append(node_id)
        row = self._rows[node_id]
        if isinstance(display_label, str):
            self._apply_display_label(node_id=node_id, display_label=display_label)
        row.status = status
        if status in {"staging", "submitted", "running"}:
            row.started_at = row.started_at or event_time
            row.finished_at = None
        elif status in {"cached", "succeeded", "failed"}:
            row.started_at = row.started_at or event_time
            row.finished_at = event_time
        # Only refresh on state transitions that the user needs to see
        # immediately. Rapid cache hits are batched by Rich's internal
        # refresh rate to avoid flicker.
        if self._live is not None and status in {
            "staging",
            "submitted",
            "running",
            "succeeded",
            "failed",
        }:
            self._live.refresh()

    def _label_for(self, *, node_id: int, task_name: str) -> str:
        if node_id in self._rows:
            return self._rows[node_id].label

        base_name = task_name.rsplit(".", 1)[-1]
        self._name_counts[base_name] += 1
        count = self._name_counts[base_name]
        return base_name if count == 1 else f"{base_name}[{count}]"

    def _apply_display_label(self, *, node_id: int, display_label: str) -> None:
        """Replace a fallback duplicate label with a richer runtime label."""
        row = self._rows[node_id]
        if row.label == display_label:
            return
        if any(
            other_id != node_id and other_row.label == display_label
            for other_id, other_row in self._rows.items()
        ):
            return
        row.label = display_label

    def _render_run_layout(self):
        return Group(
            self._render_resource_info_line(),
            Text(""),
            self._render_notice_lines(),
            self._render_status_line(),
            self._render_task_table(),
            self._render_progress_section(),
        )

    def _render_status_line(self) -> Table:
        line = Table.grid(padding=(0, 1))
        line.add_column(no_wrap=True)
        line.add_column(no_wrap=True)
        line.add_column(no_wrap=True)
        line.add_column(no_wrap=True)
        line.add_row(
            " " * self._status_line_padding(),
            self._activity_spinner,
            Text("Running", style="bold #0f766e"),
            self._time_spinner,
        )
        return line

    def _render_notice_lines(self) -> Text:
        """Render task-scoped runtime notices above the live table."""
        text = Text()
        for index, notice in enumerate(self._notices):
            if index > 0:
                text.append("\n")
            text.append(notice, style="bold")
        return text

    def _render_resource_info_line(self) -> Text:
        """Render the live locality and resource summary line."""
        text = Text()
        if self._summary.executor == "k8s":
            text.append("☁️  ", style="cyan")
            text.append("Running on Kubernetes", style="bold")
        elif self._summary.executor == "batch":
            text.append("☁️  ", style="cyan")
            text.append("Running on GCP Batch", style="bold")
        else:
            text.append("💻 ", style="cyan")
            text.append(
                f"Running locally on {self._summary.cores} {_core_unit_label(self._summary.cores)}",
                style="bold",
            )
        text.append(" ")
        text.append("(")
        text.append(self._resource_label(), style="dim")
        text.append(")", style="dim")
        return text

    def _render_task_table(self) -> Table:
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
        now = self._elapsed_clock()
        max_label = _task_label_width(self._console)
        for item in self._display_items():
            if isinstance(item, _TaskGroup):
                # Collapsed group row with multi-state progress bar.
                counts = item.status_counts()
                total = len(item.rows)
                terminal = item.terminal_count()
                bar_width = max(16, self._status_column_width() - len(f" {terminal}/{total}") - 1)
                bar = _MultiStateBar(counts=counts, total=total, width=bar_width)
                # Build status cell: bar + count label.
                bar_text = Text()
                for chunk in bar.__rich_console__(self._console, self._console.options):
                    if isinstance(chunk, Text):
                        bar_text.append_text(chunk)
                bar_text.append(f" {terminal}/{total}", style="bold #134e4a")
                elapsed = item.elapsed(now=now)
                time_str = _format_duration(elapsed) if elapsed is not None else "--"
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

    def _render_progress_section(self) -> Table:
        completed = self._terminal_count()
        total = max(1, len(self._rows))
        progress = Table.grid(padding=(0, 1))
        progress.add_column()
        progress.add_column(no_wrap=True)
        progress_text = f"{completed}/{len(self._rows)} complete"
        progress.add_row(
            ProgressBar(
                total=total,
                completed=completed,
                width=max(16, self._task_table_width() - len(progress_text) - 1),
                complete_style="bold #0f766e",
                finished_style="bold #0f766e",
                pulse_style="#99f6e4",
                style="dim #134e4a",
            ),
            Text(progress_text, style="bold #134e4a"),
        )
        return progress

    def _resource_label(self) -> str:
        """Return the compact inline CPU/RSS monitor label."""
        resources = self._resource_summary()
        if resources is None:
            return "CPU --   RSS --   Procs --"

        current = resources.get("current")
        if not isinstance(current, dict):
            return "CPU --   RSS --   Procs --"

        return (
            f"CPU {_format_cpu_percent(_as_float(current.get('cpu_percent')))}   "
            f"RSS {_format_bytes(_as_int(current.get('rss_bytes')))}   "
            f"Procs {_format_count(current.get('process_count'))}"
        )

    def _render_notebooks(self, notebooks: list[_NotebookSummary]) -> Text:
        """Render the list of notebooks materialised in this run."""
        text = Text()
        text.append(f"\n📓 Notebooks materialised ({len(notebooks)})\n", style="bold")
        for nb in notebooks:
            url = nb.html_path.as_uri()
            text.append(f"  {nb.task_label}  ", style="bold #134e4a")
            text.append(str(nb.html_path), style=f"link {url} #0f766e")
            text.append("\n")
        return text

    def _render_assets(self, assets: list[_AssetSummary]) -> Text:
        """Render the list of assets materialised in this run."""
        text = Text()
        text.append(f"\n📦 Assets materialised ({len(assets)})\n", style="bold")
        for asset in assets:
            text.append(f"  {asset.name}\n", style="bold #134e4a")
        return text

    def _render_failure_details(self, details: list[_FailureDetails]):
        panels = [self._render_failure_panel(item) for item in details]
        return Group(*panels)

    def _render_failure_panel(self, details: _FailureDetails) -> Panel:
        summary = Table.grid(padding=(0, 1))
        summary.add_column(style="bold #7f1d1d", no_wrap=True)
        summary.add_column()
        summary.add_row("Task", details.task_label)
        summary.add_row(
            "Exit code", str(details.exit_code) if details.exit_code is not None else "?"
        )
        if details.error:
            summary.add_row("Reason", Text(details.error, style="#7f1d1d"))
        if details.log_path is not None:
            summary.add_row("Log", str(details.log_path))

        sections: list[object] = [summary]
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

    def _render_failure_separator(self) -> Rule:
        """Render a separator before end-of-run failure diagnostics."""
        return Rule(style="dim")

    def _ordered_rows(self) -> list[_TaskRow]:
        return [self._rows[node_id] for node_id in self._row_order]

    def _display_items(self) -> list[_TaskGroup | _TaskRow]:
        """Build the grouped display list from ordered rows.

        Tasks with ≥ _GROUP_THRESHOLD invocations sharing the same task_name
        are collapsed into a single ``_TaskGroup``. Others remain as individual
        ``_TaskRow`` entries. Display order follows first-seen position of each
        task_name.
        """
        # Count invocations per task_name.
        name_counts: Counter[str] = Counter(self._rows[nid].task_name for nid in self._row_order)

        # Build groups for names that meet the threshold.
        groups: dict[str, _TaskGroup] = {}
        items: list[_TaskGroup | _TaskRow] = []
        seen_names: set[str] = set()

        for node_id in self._row_order:
            row = self._rows[node_id]
            name = row.task_name

            if name_counts[name] < _GROUP_THRESHOLD:
                items.append(row)
                continue

            if name not in seen_names:
                # Determine common environment label.
                env_labels = {
                    self._rows[nid].env_label
                    for nid in self._row_order
                    if self._rows[nid].task_name == name
                }
                env_label = env_labels.pop() if len(env_labels) == 1 else "mixed"
                base = _task_base_name(name)
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

    def _status_column_width(self) -> int:
        """Return the effective width available for the status column."""
        rows = self._ordered_rows()
        if not rows:
            return len("Status")
        return max(len("Status"), *(len(_status_label(row.status)) for row in rows), 30)

    def _task_table_width(self) -> int:
        items = self._display_items()
        if not items:
            return len("Task") + len("Status") + len("Environment") + len("Time") + 13

        max_label = _task_label_width(self._console)
        now = self._elapsed_clock()
        task_widths: list[int] = [len("Task")]
        status_widths: list[int] = [len("Status")]
        env_widths: list[int] = [len("Environment")]
        time_widths: list[int] = [len("Time")]

        for item in items:
            if isinstance(item, _TaskGroup):
                task_widths.append(len(_truncate_task_label(item.label, max_width=max_label)))
                total = len(item.rows)
                terminal = item.terminal_count()
                status_widths.append(self._status_column_width() + len(f" {terminal}/{total}") + 1)
                env_widths.append(len(item.env_label))
                elapsed = item.elapsed(now=now)
                time_widths.append(len(_format_duration(elapsed)) if elapsed is not None else 2)
            else:
                task_widths.append(len(_truncate_task_label(item.label, max_width=max_label)))
                status_widths.append(len(_status_label(item.status)))
                env_widths.append(len(item.env_label))
                time_widths.append(len(_task_duration_plain(item, now=now)))

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

    def _status_line_padding(self) -> int:
        status_width = len("Running") + 5
        return max(0, (self._task_table_width() - status_width) // 2)

    def _status_counts(self) -> Counter[str]:
        counts: Counter[str] = Counter(row.status for row in self._rows.values())
        for status in ("waiting", "running", "cached", "succeeded", "failed"):
            counts.setdefault(status, 0)
        return counts

    def _terminal_count(self) -> int:
        counts = self._status_counts()
        return counts["cached"] + counts["succeeded"] + counts["failed"]

    def _elapsed_clock(self) -> float:
        if self._final_elapsed is not None and self._run_started_at is not None:
            return self._run_started_at + self._final_elapsed
        return time.perf_counter()

    def _resource_summary(self) -> dict[str, object] | None:
        """Return the latest available resource summary."""
        if self._resources is None:
            return None
        return self._resources.provider()

    def _render_resource_footer(self, resources: dict[str, object]) -> Text | None:
        """Render the final CPU/RSS summary line."""
        average = resources.get("average")
        peak = resources.get("peak")
        if not isinstance(average, dict) or not isinstance(peak, dict):
            return None

        avg_cpu = _format_cpu_percent(_as_float(average.get("cpu_percent")))
        peak_cpu = _format_cpu_percent(_as_float(peak.get("cpu_percent")))
        avg_rss = _format_bytes(_as_int(average.get("rss_bytes")))
        peak_rss = _format_bytes(_as_int(peak.get("rss_bytes")))
        return Text(
            f"CPU avg {avg_cpu}, peak {peak_cpu} | RSS avg {avg_rss}, peak {peak_rss}",
            style="dim",
        )


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
