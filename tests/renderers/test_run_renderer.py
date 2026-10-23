"""The Rich run renderer: event mapping, the live table, and the end-of-run summary."""

from __future__ import annotations

import json
import warnings
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from ginkgo import per_branch, shell, task
from ginkgo.cli.commands.run import planned_task_rows
from ginkgo.cli.renderers.common import _status_icon
from ginkgo.cli.renderers.models import (
    CliRenderedPage,
    CliRunSummary,
    FailureDetails,
    SkipDetails,
)
from ginkgo.cli.renderers.rich import RichEventRenderer
from ginkgo.cli.renderers.run import (
    _ENV_PREPARE_REPORT_THRESHOLD_SECONDS,
    CliRunRenderer,
    _RunEventState,
)
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.events import (
    EnvPrepareCompleted,
    EnvPrepareFailed,
    EnvPrepareStarted,
    EventBus,
    PhaseTimed,
    RunResourcesSampled,
    TaskAnnotated,
    TaskFailed,
    TaskPlanned,
    TaskSkipped,
    TaskStarted,
)
from ginkgo.runtime.run_summary import TERMINAL_STATUSES
from tests.conftest import ShellEnvironment

_SCOPE = {"run_id": "r1", "task_id": "task_0", "task_name": "mod.task_a"}


def _line(status: str, *, node_id: int = 0, **fields: object) -> str:
    return json.dumps(
        {"task": f"mod.task_{node_id}", "status": status, "node_id": node_id, **fields}
    )


def _renderer(
    tmp_path: Path, *, rows: int = 1, width: int = 120, height: int | None = None, **summary
) -> tuple[CliRunRenderer, StringIO]:
    """Return a started renderer over ``rows`` planned tasks, printing to a pipe."""
    output = StringIO()
    console = Console(file=output, width=width, height=height, force_terminal=False)
    summary = CliRunSummary(
        run_id="r1", mode="default", run_dir=tmp_path, **{"cores": 1, **summary}
    )
    renderer = CliRunRenderer(console=console, summary=summary)
    renderer.start(
        planned_tasks=[(i, f"mod.task_{i}", f"task_{i}", "pixi:analysis") for i in range(rows)]
    )
    return renderer, output


def _printed(renderable, *, width: int = 120) -> str:
    console = Console(file=StringIO(), width=width, force_terminal=False)
    console.print(renderable)
    return console.file.getvalue()


def _state() -> _RunEventState:
    state = _RunEventState()
    state.seed(planned_tasks=[(0, "mod.task_0", "task_0", "pixi:analysis")])
    return state


def _backdate(state: _RunEventState, seconds: float) -> None:
    """Move the recorded preparation start of node 0 *seconds* into the past."""
    started, env = state._env_prepare_started[0]
    state._env_prepare_started[0] = (started - seconds, env)


# Event adapter --------------------------------------------------------------


class _Sink:
    """Capture the JSON event lines a ``RichEventRenderer`` would write."""

    def __init__(self) -> None:
        self.lines: list[dict[str, object]] = []

    def write(self, text: str) -> int:
        self.lines.append(json.loads(text))
        return len(text)


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (
            EnvPrepareStarted(**_SCOPE, env="analysis"),
            {"status": "preparing env", "env": "analysis"},
        ),
        (EnvPrepareCompleted(**_SCOPE, env="analysis"), {"status": "waiting", "env": "analysis"}),
        (
            EnvPrepareFailed(**_SCOPE, env="analysis", error="pixi install failed"),
            {"status": "failed", "env": "analysis"},
        ),
        (TaskStarted(**_SCOPE), {"status": "running"}),
        (
            TaskSkipped(**_SCOPE, blocked_by_task_id="task_1", blocked_by_task_name="mod.up"),
            {"status": "skipped", "blocked_by_task_name": "mod.up"},
        ),
        (TaskFailed(**_SCOPE, ignored=True), {"status": "failed", "ignored": True}),
    ],
    ids=["env-started", "env-completed", "env-failed", "started", "skipped", "ignored-failure"],
)
def test_events_map_to_status_lines(event, expected) -> None:
    sink = _Sink()
    RichEventRenderer(renderer=sink)(event)

    (line,) = sink.lines
    assert {key: line[key] for key in expected} == expected


def test_ledger_only_events_are_ignored_silently() -> None:
    """Events that exist for the provenance store must not reach the Rich table."""
    sink = _Sink()
    adapter = RichEventRenderer(renderer=sink)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        adapter(TaskPlanned(**_SCOPE))
        adapter(TaskAnnotated(**_SCOPE, fields={"env_lock": "envs/analysis.lock"}))
        adapter(PhaseTimed(run_id="r1", phase="validate", seconds=0.5))
        adapter(RunResourcesSampled(run_id="r1", resources={"cpu_percent": 1.0}))

    assert sink.lines == []


# Row state ------------------------------------------------------------------


def test_preparation_is_active_but_does_not_start_the_task_clock() -> None:
    state = _state()

    state.handle_event_line(_line("preparing env", env="analysis"))
    assert (state.rows[0].status, state.rows[0].started_at) == ("preparing env", None)

    state.handle_event_line(_line("waiting", env="analysis"))
    assert (state.rows[0].status, state.rows[0].started_at) == ("waiting", None)
    assert list(state.env_prepare_by_env) == ["analysis"]
    assert state.failed_envs == set()

    state.handle_event_line(_line("running"))
    assert state.rows[0].started_at is not None


def test_a_waiting_event_outside_preparation_records_no_prepare_time() -> None:
    state = _state()

    state.handle_event_line(_line("waiting"))

    assert state.env_prepare_seconds == 0.0
    assert state.env_prepare_by_env == {}


def test_a_failed_preparation_is_attributed_to_its_own_environment() -> None:
    state = _state()

    state.handle_event_line(_line("preparing env", env="debian"))
    state.handle_event_line(_line("failed", env="debian"))

    assert state.failed_envs == {"debian"}
    assert set(state.env_prepare_by_env) == {"debian"}


@pytest.mark.parametrize("leaving", ["running", "failed"])
def test_preparation_time_accumulates_when_the_row_leaves_it(leaving: str) -> None:
    state = _state()

    state.handle_event_line(_line("preparing env", env="analysis"))
    _backdate(state, 5.0)
    state.handle_event_line(_line(leaving))

    assert state.rows[0].status == leaving
    assert state.env_prepare_seconds == pytest.approx(5.0, abs=0.05)


def test_a_skipped_task_is_terminal_without_a_duration() -> None:
    state = _state()

    state.handle_event_line(_line("skipped"))

    row = state.rows[0]
    assert (row.status, row.started_at) == ("skipped", None)
    assert row.finished_at is not None
    assert state.done_count() == 0
    assert _status_icon("skipped") == "⊘"


@pytest.mark.parametrize(
    ("node_id", "label", "notice"),
    [
        (0, "task_a[x]", "task_0: FUSE access fell back to staging"),
        # A notice from an unplanned task falls back to its display label.
        (9, "child[b]", "child[b]: FUSE access fell back to staging"),
    ],
    ids=["planned", "unplanned"],
)
def test_a_notice_names_the_task_it_is_about(tmp_path, node_id, label, notice) -> None:
    renderer, _ = _renderer(tmp_path)
    message = "FUSE access fell back to staging"
    renderer.write(_line("notice", node_id=node_id, display_label=label, message=message) + "\n")

    assert renderer._state.notices == [notice]


# The live display -----------------------------------------------------------


def test_events_do_not_repaint_the_live_display(tmp_path: Path) -> None:
    """Events change state only; the Live display owns the repaint cadence.

    Repainting per event stacks erase-and-redraw bursts on top of Rich's own
    refresh thread, which is what the terminal shows as flicker.
    """
    live = MagicMock()
    summary = CliRunSummary(run_id="r1", mode="default", run_dir=tmp_path, cores=1)
    renderer = CliRunRenderer(
        console=Console(file=StringIO(), width=120, force_terminal=True), summary=summary
    )
    with patch("ginkgo.cli.renderers.run._SynchronisedLive", return_value=live):
        renderer.start(planned_tasks=[(0, "mod.task_0", "task_0", "local")])

    for status in ("preparing env", "waiting", "running", "succeeded"):
        renderer.write(_line(status, env="analysis") + "\n")
    renderer.finish(elapsed=1.0, success=True)

    # Stopping the display paints the final frame; nothing else asks for one.
    assert live.refresh.call_count == 0
    assert live.stop.call_count == 1


class TestLiveWindow:
    """A live table taller than the terminal shows a window over its rows."""

    @pytest.fixture
    def live(self, tmp_path: Path):
        """Yield ``make(rows, width=120)`` for a live renderer on a 30-line terminal."""
        started: list[CliRunRenderer] = []

        def make(rows: int, *, width: int = 120) -> tuple[CliRunRenderer, Console]:
            console = Console(file=StringIO(), width=width, height=30, force_terminal=True)
            summary = CliRunSummary(run_id="r1", mode="default", run_dir=tmp_path, cores=8)
            renderer = CliRunRenderer(console=console, summary=summary)
            renderer.start(
                planned_tasks=[(i, f"mod.task_{i}", f"step_{i:02d}", "local") for i in range(rows)]
            )
            started.append(renderer)
            return renderer, console

        yield make
        for renderer in started:
            if renderer._live is not None and renderer._live.is_started:
                renderer._live.stop()

    @staticmethod
    def _lines(renderer: CliRunRenderer, console: Console) -> list[str]:
        return [
            "".join(segment.text for segment in line)
            for line in console.render_lines(renderer.__rich__(), console.options, pad=False)
        ]

    @staticmethod
    def _write(renderer: CliRunRenderer, statuses: dict[int, str]) -> None:
        for node_id, status in statuses.items():
            renderer.write(_line(status, node_id=node_id) + "\n")

    def test_the_block_stays_inside_the_terminal_even_with_a_wrapping_notice(self, live) -> None:
        """A block as tall as the terminal scrolls, and a scrolled block flickers."""
        renderer, console = live(42, width=60)
        lines = self._lines(renderer, console)
        assert len(lines) < console.height
        assert any("below" in line for line in lines)

        # Counting a notice as one line is how a sized block overflows anyway.
        notice = "Task read a path that ginkgo does not track, so a change to it " * 3
        renderer.write(_line("notice", message=notice) + "\n")
        assert len(self._lines(renderer, console)) < console.height

    def test_a_table_that_fits_shows_every_row(self, live) -> None:
        lines = self._lines(*live(5))

        assert all(f"step_{i:02d}" in "".join(lines) for i in range(5))
        assert not any("above" in line or "below" in line for line in lines)

    def test_the_window_follows_the_tasks_in_flight_at_a_steady_width(self, live) -> None:
        """Finished rows scroll off the top; a width read off visible rows would jitter."""
        renderer, console = live(42)
        renderer._state.rows[0].label = "a_task_with_a_very_long_label_indeed"
        width_at_the_top = renderer._layout.task_table_width(now=0.0)

        self._write(renderer, dict.fromkeys(range(30), "running"))

        rendered = "".join(self._lines(renderer, console))
        assert "step_29" in rendered and "step_00" not in rendered
        assert "above" in rendered and "below" in rendered
        assert renderer._layout.task_table_width(now=0.0) == width_at_the_top

    def test_a_fanout_of_skips_leaves_the_window_on_the_work_that_ran(self, live) -> None:
        """A skipped task never dispatched, so it must not drag the window away."""
        renderer, console = live(42)
        self._write(
            renderer,
            {**dict.fromkeys(range(3), "failed"), **dict.fromkeys(range(3, 42), "skipped")},
        )

        lines = self._lines(renderer, console)
        rendered = "".join(lines)
        assert len(lines) < console.height
        assert "step_00" in rendered and "step_41" not in rendered
        assert "above" not in rendered and "below" in rendered
        assert sum(renderer._state.status_counts()[s] for s in TERMINAL_STATUSES) == 42

    def test_the_finished_table_shows_every_row(self, live) -> None:
        """Nothing repaints after the run, so the last frame need not be windowed."""
        renderer, console = live(42)
        renderer.finish(elapsed=1.0, success=True)

        lines = self._lines(renderer, console)
        assert all(f"step_{i:02d}" in "".join(lines) for i in range(42))
        assert not any("above" in line or "below" in line for line in lines)


def test_a_piped_run_prints_every_row(tmp_path: Path) -> None:
    """Nothing repaints into a pipe, so nothing there is worth windowing."""
    renderer, output = _renderer(tmp_path, rows=42, height=30)
    renderer.finish(elapsed=1.0, success=True)

    assert all(f"task_{i}" in output.getvalue() for i in range(42))


def test_each_repaint_is_bracketed_as_one_terminal_update(tmp_path: Path) -> None:
    """DEC 2026 keeps the terminal from drawing a half-erased frame."""
    output = StringIO()
    console = Console(file=output, width=120, height=30, force_terminal=True)
    summary = CliRunSummary(run_id="r1", mode="default", run_dir=tmp_path, cores=1)
    renderer = CliRunRenderer(console=console, summary=summary)
    renderer.start(planned_tasks=[(0, "mod.task_0", "task_0", "local")])
    renderer.finish(elapsed=1.0, success=True)

    painted = output.getvalue()
    assert painted.count("\x1b[?2026h") == painted.count("\x1b[?2026l") > 0
    assert painted.index("\x1b[?2026h") < painted.index("\x1b[?2026l")


# Environment preparation ------------------------------------------------------


@pytest.mark.parametrize(
    ("seconds", "outcome", "explained"),
    [
        (25.0, "succeeded", True),
        (25.0, "failed", True),
        (_ENV_PREPARE_REPORT_THRESHOLD_SECONDS / 2, "succeeded", False),
    ],
    ids=["slow", "failed", "fast"],
)
def test_the_summary_explains_slow_environment_preparation(tmp_path, seconds, outcome, explained):
    renderer, output = _renderer(tmp_path)

    renderer.write(_line("preparing env", env="analysis") + "\n")
    _backdate(renderer._state, seconds)
    renderer.write(_line("running") + "\n")
    renderer.write(_line(outcome) + "\n")
    renderer.finish(elapsed=seconds + 1, success=outcome == "succeeded")

    text = " ".join(output.getvalue().split())
    assert ("Environment preparation took" in text) is explained
    if explained:
        assert "analysis" in text
        # Preparation is only the install: a task's slow first start counts as the task's.
        assert "counts as the task's time" in text


class _FailingEnvironment(ShellEnvironment):
    def prepare(self, *, env: str) -> None:
        raise RuntimeError("install failed")


@task(env="test_env", kind="shell")
def needs_env() -> str:
    return shell(cmd="echo hello")


def test_the_evaluator_closes_the_window_a_failed_preparation_opened() -> None:
    events: list[object] = []
    bus = EventBus()
    bus.subscribe(events.append)

    with pytest.raises(RuntimeError, match="install failed"):
        ConcurrentEvaluator(backend=_FailingEnvironment(), event_bus=bus).evaluate(needs_env())

    assert len([e for e in events if isinstance(e, EnvPrepareStarted)]) == 1
    (failed,) = [e for e in events if isinstance(e, EnvPrepareFailed)]
    assert failed.env == "test_env" and "install failed" in (failed.error or "")
    assert not [e for e in events if isinstance(e, EnvPrepareCompleted)]


# Failure reporting -------------------------------------------------------------


def _failure(label: str = "task_a", **fields) -> FailureDetails:
    return FailureDetails(
        task_label=label, **{"exit_code": 1, "log_path": None, "log_tail": [], **fields}
    )


def test_the_summary_counts_skips_and_ignored_failures(tmp_path: Path) -> None:
    renderer, output = _renderer(tmp_path, rows=2)

    renderer.write(_line("failed") + "\n")
    renderer.write(_line("skipped", node_id=1) + "\n")
    renderer.finish(
        elapsed=1.0,
        success=False,
        failure_details=[_failure(error="bad input", ignored=True)],
        skipped=[SkipDetails(task_label="task_b", cause="blocked by task_a")],
    )

    text = output.getvalue()
    assert "1 failed (1 ignored), 1 skipped" in text
    assert "Failed, run continued (1)" in text
    assert "Left unrun by a failure (1)" in text
    assert "task_b" in text and "blocked by task_a" in text


def test_many_ignored_failures_are_panelled_up_to_a_limit(tmp_path: Path) -> None:
    renderer, _ = _renderer(tmp_path)
    details = [
        _failure(f"branch[{i}]", error="bad", failure_kind="user_code_error", ignored=True)
        for i in range(30)
    ]

    text = _printed(renderer._layout.render_failure_details(details))

    assert "Failed, run continued (30)" in text
    assert "user_code_error×30" in text
    assert "and 20 more" in text
    assert "branch[9]" in text and "branch[10]" not in text


def _fatal(label: str, error: str = "seqtkk: command not found") -> FailureDetails:
    return _failure(label, exit_code=127, error=error, failure_kind="shell_command_error")


def test_identical_fatal_failures_share_one_panel_and_different_ones_do_not(tmp_path) -> None:
    """Seven branches failing the same way are one problem, not seven panels."""
    renderer, _ = _renderer(tmp_path)

    shared = _printed(
        renderer._layout.render_failure_details([_fatal(f"trim[{i}]") for i in range(7)])
    )
    assert shared.count("Failure Details:") == 1
    assert "Failure Details: trim[0] (×7)" in shared
    assert "trim[1], trim[2], trim[3]" in shared
    assert "shell_command_error×7" in shared

    separate = _printed(
        renderer._layout.render_failure_details(
            [_fatal("trim[a]"), _fatal("trim[b]", error="seqtk: input file is empty")]
        )
    )
    assert separate.count("Failure Details:") == 2
    assert "(×" not in separate


def test_the_failure_panel_shows_what_it_has_and_only_that(tmp_path: Path) -> None:
    renderer, _ = _renderer(tmp_path)
    log_path = Path(".ginkgo/runs/20261005_105716_799605_a04c64d3/logs") / (
        "task_0000_ginkgo_user_flow_547d514134.pack.stderr.log"
    )

    # An in-process exception has no exit code, and "?" said nothing.
    assert "Exit code" not in _printed(
        renderer._layout.render_failure_panel(_failure(exit_code=None))
    )
    # A log path wider than the console is folded, not truncated.
    folded = _printed(renderer._layout.render_failure_panel(_failure(log_path=log_path)), width=80)
    assert "…" not in folded and "stderr.log" in folded
    # A multi-line error that repeats the log tail is shown by its last line.
    traceback = ["Traceback (most recent call last):", "ValueError: bad value"]
    reduced = _printed(
        renderer._layout.render_failure_panel(
            _failure(log_tail=traceback, error="\n".join(traceback), task_kind="shell")
        )
    )
    assert "ValueError: bad value" in reduced and traceback[0] not in reduced


@pytest.mark.parametrize("label", ["clean[bad]", "clean[bad[/x]]", "clean[bold]"])
def test_a_bracketed_label_is_printed_verbatim_in_the_failure_panel(tmp_path, label) -> None:
    """A fan-out label is user data, not Rich markup: none of it is swallowed."""
    renderer, _ = _renderer(tmp_path)
    details = _failure(
        label, log_path=Path("logs/task[1].stderr.log"), error=f"bad region {label}"
    )

    text = _printed(renderer._layout.render_failure_panel(details))

    assert f"Failure Details: {label}" in text
    assert f"Task      {label}" in text
    assert "task[1].stderr.log" in text


def test_a_wide_fanout_of_skips_is_counted_not_listed(tmp_path: Path) -> None:
    renderer, _ = _renderer(tmp_path)
    skipped = [SkipDetails(task_label=f"branch[{i}]", cause="blocked by load") for i in range(50)]

    report = renderer._layout.render_skip_report(skipped).plain

    assert "50 blocked by load" in report and "branch[0]" not in report


def test_progress_counts_only_tasks_with_a_result_as_complete(tmp_path: Path) -> None:
    """R4: a failed or skipped task is terminal but not complete."""
    renderer, _ = _renderer(tmp_path, rows=3)
    for node_id, status in enumerate(("succeeded", "failed", "skipped")):
        renderer.write(_line(status, node_id=node_id) + "\n")

    text = _printed(renderer._layout.render_progress_section())

    assert "1/3 complete (1 failed, 1 skipped)" in text


# Products and footers ------------------------------------------------------------


def test_a_keep_going_run_lists_what_it_produced_and_a_failed_one_does_not(tmp_path) -> None:
    """Exit 3 is a finished run; the fatal path reports its directory on stderr."""
    html = tmp_path / "report.html"
    html.write_text("<html></html>", encoding="utf-8")

    renderer, output = _renderer(tmp_path)
    renderer.write(_line("failed") + "\n")
    renderer.finish(
        elapsed=1.0,
        success=False,
        failure_details=[_failure(error="bad input", ignored=True)],
        pages=[CliRenderedPage(task_label="survey", html_path=html)],
    )
    text = output.getvalue()
    assert "Rendered pages (1)" in text and "survey" in text
    assert "Run directory" in text

    renderer, output = _renderer(tmp_path)
    renderer.write(_line("failed") + "\n")
    renderer.finish(elapsed=1.0, success=False)
    assert "Run directory" not in output.getvalue()


def _page(tmp_path: Path, replayed_from_run_id: str | None = None) -> CliRenderedPage:
    return CliRenderedPage(
        task_label="render_overview_notebook",
        html_path=tmp_path / "notebooks" / "task_0014.html",
        replayed_from_run_id=replayed_from_run_id,
    )


@pytest.mark.parametrize(
    ("replayed", "phrases"),
    [
        ((None,), ["Rendered pages (1)", "render_overview_notebook", "!↺"]),
        (("run_1",), ["Rendered pages (1)  ↺ all reused from an earlier run", "↺ from run run_1"]),
        ((None, "run_1"), ["Rendered pages (2)  ↺ 1 reused from an earlier run"]),
    ],
    ids=["rendered", "reused", "mixed"],
)
def test_rendered_pages_are_counted_and_reuse_is_marked(tmp_path, replayed, phrases) -> None:
    """A cached rerun's page is marked as reused, so it does not read as this run's (#202)."""
    renderer, _ = _renderer(tmp_path, width=200)

    text = renderer._layout.render_pages([_page(tmp_path, run_id) for run_id in replayed]).plain

    for phrase in phrases:
        assert (phrase[1:] not in text) if phrase.startswith("!") else (phrase in text)


def test_the_resource_footer_says_its_rss_is_a_process_tree_total(tmp_path: Path) -> None:
    renderer, _ = _renderer(tmp_path)

    footer = renderer._layout.render_resource_footer(
        {
            "average": {"cpu_percent": 50.0, "rss_bytes": 1024},
            "peak": {"cpu_percent": 90.0, "rss_bytes": 2048, "process_count": 5},
        }
    )

    assert footer is not None
    assert "(process-tree total, up to 5 processes)" in footer.plain


@pytest.mark.parametrize(
    ("count", "phrases"),
    [
        (3, ["3 inputs are tracked by path only", "ginkgo cache explain"]),
        (1, ["1 input is tracked by path only"]),
        (0, ["!tracked by path"]),
    ],
)
def test_path_tracked_inputs_print_one_summary_line(tmp_path, count, phrases) -> None:
    """Issue #307: name the count once, and say nothing about zero."""
    renderer, output = _renderer(tmp_path)

    renderer.finish(elapsed=1.0, success=True, path_tracked_inputs=count)

    text = output.getvalue()
    for phrase in phrases:
        assert (phrase[1:] not in text) if phrase.startswith("!") else (phrase in text)


@pytest.mark.parametrize(
    ("summary", "phrases"),
    [
        ({}, ["Running locally on 4 Cores"]),
        ({"executor_label": "gpu-k8s (Kubernetes)"}, ["Running on gpu-k8s (Kubernetes)"]),
        # A local run still dispatches executor= tasks remotely; the header must say so.
        (
            {"pinned_executors": ("cheap-batch", "gpu-k8s")},
            ["Running locally on 4 Cores", "→ cheap-batch, gpu-k8s"],
        ),
        (
            {"executor_label": "Kubernetes", "pinned_executors": ("cheap-batch",)},
            ["Running on Kubernetes", "→ cheap-batch"],
        ),
    ],
    ids=["local", "default-executor", "pinned-on-local", "pinned-with-default"],
)
def test_the_header_names_where_tasks_run(tmp_path, summary, phrases) -> None:
    renderer, _ = _renderer(tmp_path, width=200, cores=4, **summary)

    header = renderer._layout.render_resource_info_line().plain

    for phrase in phrases:
        assert phrase in header


# Labels seeded from the graph --------------------------------------------------

_SITES = ("forest", "meadow", "wetland", "urban")


@task()
def fit_site_trend(*, site: str) -> str:
    return f"trend:{site}"


@task()
def audit_site(*, site: str, trend: str) -> str:
    return f"{site}:{trend}"


def _evaluator(expr: object) -> ConcurrentEvaluator:
    evaluator = ConcurrentEvaluator()
    evaluator.build_graph(expr)
    evaluator.validate_graph()
    return evaluator


def _seeded_labels(evaluator: ConcurrentEvaluator) -> dict[int, str]:
    """Return the run table's labels for a graph, before any event arrives."""
    state = _RunEventState()
    state.seed(planned_tasks=planned_task_rows(evaluator))
    assert all(row.status == "waiting" for row in state.rows.values())
    return {node_id: row.label for node_id, row in state.rows.items()}


def test_seeded_fanout_rows_use_the_dry_run_s_graph_labels() -> None:
    """An undispatched branch reads as it does under ``--dry-run`` (#204)."""
    with record_constructed_calls():
        trends = fit_site_trend().map(site=list(_SITES))
        evaluator = _evaluator(audit_site().map(site=list(_SITES), trend=trends))

    plan = build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")
    dry_run_labels = {task.node_id: task.label for wave in plan.waves for task in wave.tasks}

    assert _seeded_labels(evaluator) == dry_run_labels
    assert set(dry_run_labels.values()) == {
        f"{name}[{site}]" for name in ("fit_site_trend", "audit_site") for site in _SITES
    }


@pytest.mark.parametrize(
    ("build", "labels"),
    [
        # A ``per_branch()`` value derives from a branch, so it does not name it.
        (
            lambda: audit_site().product_map(
                site=["forest", "meadow"], trend=per_branch("{site}.trend")
            ),
            ["audit_site[site=forest]", "audit_site[site=meadow]"],
        ),
        # Two listed calls of one task are told apart by the argument that differs.
        (
            lambda: (
                audit_site(site="forest", trend="forest.trend"),
                audit_site(site="meadow", trend="meadow.trend"),
            ),
            ["audit_site[forest]", "audit_site[meadow]"],
        ),
    ],
    ids=["per-branch-argument", "repeated-calls"],
)
def test_seeded_labels_name_branches_by_what_differs(build, labels) -> None:
    with record_constructed_calls():
        evaluator = _evaluator(build())

    assert sorted(_seeded_labels(evaluator).values()) == labels
