"""The Rich run renderer: task labels and environment-preparation progress."""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from rich.console import Console

from ginkgo import per_branch, shell, task
from ginkgo.cli.commands.run import planned_task_rows
from ginkgo.cli.renderers.models import CliRunSummary
from ginkgo.envs.pixi import PixiEnvPrepareError, PixiRegistry
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
    TaskStarted,
)


_TESTS_DIR = Path(__file__).resolve().parents[1]
_TEST_ENV_NAME = "test_env"


@task(env=_TEST_ENV_NAME, kind="shell")
def needs_env() -> str:
    """A task whose environment must be prepared before it can run."""
    return shell(cmd="echo hello")


class _FailingPixiRegistry(PixiRegistry):
    """A registry whose environment installs always fail."""

    def prepare(self, *, env: str) -> Path:
        raise PixiEnvPrepareError(manifest=Path(env) / "pixi.toml", output="install failed")


class _RecordingRenderer:
    """Capture the JSON event lines a ``RichEventRenderer`` would write."""

    def __init__(self) -> None:
        self.lines: list[dict[str, object]] = []

    def write(self, text: str) -> int:
        self.lines.append(json.loads(text))
        return len(text)


def _event_line(status: str, *, env: str | None = None) -> str:
    payload: dict[str, object] = {"task": "mod.task_a", "status": status, "node_id": 0}
    if env is not None:
        payload["env"] = env
    return json.dumps(payload)


def _seeded_state(*, env_label: str = "pixi:analysis") -> _RunEventState:
    state = _RunEventState()
    state.seed(planned_tasks=[(0, "mod.task_a", "task_a", env_label)])
    return state


def _renderer(
    tmp_path: Path, *, env_label: str = "pixi:analysis"
) -> tuple[CliRunRenderer, StringIO]:
    output = StringIO()
    console = Console(file=output, width=120, force_terminal=False)
    summary = CliRunSummary(run_id="r1", mode="default", run_dir=tmp_path, cores=1)
    renderer = CliRunRenderer(console=console, summary=summary)
    renderer.start(planned_tasks=[(0, "mod.task_a", "task_a", env_label)])
    return renderer, output


def test_env_prepare_events_map_to_status_lines() -> None:
    sink = _RecordingRenderer()
    adapter = RichEventRenderer(renderer=sink)

    adapter(
        EnvPrepareStarted(run_id="r1", task_id="task_0", task_name="mod.task_a", env="analysis")
    )
    adapter(
        EnvPrepareCompleted(run_id="r1", task_id="task_0", task_name="mod.task_a", env="analysis")
    )

    assert [line["status"] for line in sink.lines] == ["preparing env", "waiting"]
    assert all(line["env"] == "analysis" for line in sink.lines)


def test_failed_preparation_maps_to_failed_status() -> None:
    sink = _RecordingRenderer()
    adapter = RichEventRenderer(renderer=sink)

    adapter(
        EnvPrepareFailed(
            run_id="r1",
            task_id="task_0",
            task_name="mod.task_a",
            env="analysis",
            error="pixi install failed",
        )
    )

    assert sink.lines[0]["status"] == "failed"
    assert sink.lines[0]["env"] == "analysis"


def test_task_started_still_maps_to_running() -> None:
    sink = _RecordingRenderer()
    adapter = RichEventRenderer(renderer=sink)

    adapter(TaskStarted(run_id="r1", task_id="task_0", task_name="mod.task_a"))

    assert sink.lines[0]["status"] == "running"


def test_preparing_env_is_an_active_status() -> None:
    state = _seeded_state()

    refresh_status = state.handle_event_line(_event_line("preparing env", env="analysis"))

    assert state.rows[0].status == "preparing env"
    assert refresh_status == "preparing env"


def test_preparation_does_not_start_the_task_clock() -> None:
    state = _seeded_state()

    state.handle_event_line(_event_line("preparing env", env="analysis"))
    assert state.rows[0].started_at is None

    state.handle_event_line(_event_line("waiting", env="analysis"))
    assert state.rows[0].started_at is None

    state.handle_event_line(_event_line("running"))
    assert state.rows[0].started_at is not None


def test_leaving_preparation_triggers_a_live_refresh() -> None:
    state = _seeded_state()

    state.handle_event_line(_event_line("preparing env", env="analysis"))
    refresh_status = state.handle_event_line(_event_line("waiting", env="analysis"))

    assert refresh_status == "waiting"
    assert state.rows[0].status == "waiting"
    assert state.prepared_envs == ["analysis"]


def test_unrelated_waiting_event_does_not_trigger_a_refresh() -> None:
    state = _seeded_state()

    assert state.handle_event_line(_event_line("waiting")) == ""


def test_env_prepare_time_accumulates_on_transition_out() -> None:
    state = _seeded_state()

    state.handle_event_line(_event_line("preparing env", env="analysis"))
    state._env_prepare_started[0] -= 5.0
    state.handle_event_line(_event_line("running"))

    assert state.env_prepare_seconds == pytest.approx(5.0, abs=0.05)


def test_failed_preparation_still_reports_its_time() -> None:
    state = _seeded_state()

    state.handle_event_line(_event_line("preparing env", env="analysis"))
    state._env_prepare_started[0] -= 25.0
    state.handle_event_line(_event_line("failed"))

    assert state.rows[0].status == "failed"
    assert state.env_prepare_seconds == pytest.approx(25.0, abs=0.05)


def test_summary_explains_slow_environment_preparation(tmp_path: Path) -> None:
    renderer, output = _renderer(tmp_path)

    renderer.write(_event_line("preparing env", env="analysis") + "\n")
    renderer._state._env_prepare_started[0] -= 25.0
    renderer.write(_event_line("running") + "\n")
    renderer.write(_event_line("succeeded") + "\n")
    renderer.finish(elapsed=26.0, success=True)

    text = output.getvalue()
    assert "Environment preparation took" in text
    assert "analysis" in text


def test_summary_explains_preparation_that_failed(tmp_path: Path) -> None:
    renderer, output = _renderer(tmp_path)

    renderer.write(_event_line("preparing env", env="analysis") + "\n")
    renderer._state._env_prepare_started[0] -= 25.0
    renderer.write(_event_line("failed") + "\n")
    renderer.finish(elapsed=25.0, success=False)

    assert "Environment preparation took" in output.getvalue()


def test_evaluator_reports_a_failed_environment_preparation() -> None:
    """A prepare failure must close the window it opened on the event stream."""
    from ginkgo.envs.pixi import PixiEnvPrepareError
    from ginkgo.runtime.backend import LocalEnvironment
    from ginkgo.runtime.evaluator import ConcurrentEvaluator
    from ginkgo.runtime.events import EventBus

    events: list[object] = []
    bus = EventBus()
    bus.subscribe(events.append)

    evaluator = ConcurrentEvaluator(
        backend=LocalEnvironment(pixi_registry=_FailingPixiRegistry(project_root=_TESTS_DIR)),
        event_bus=bus,
    )
    with pytest.raises(PixiEnvPrepareError):
        evaluator.evaluate(needs_env())

    started = [event for event in events if isinstance(event, EnvPrepareStarted)]
    failed = [event for event in events if isinstance(event, EnvPrepareFailed)]
    assert len(started) == 1
    assert len(failed) == 1
    assert failed[0].env == _TEST_ENV_NAME
    assert "install failed" in (failed[0].error or "")
    assert not [event for event in events if isinstance(event, EnvPrepareCompleted)]


def test_summary_omits_explanation_for_fast_preparation(tmp_path: Path) -> None:
    renderer, output = _renderer(tmp_path)

    renderer.write(_event_line("preparing env", env="analysis") + "\n")
    renderer._state._env_prepare_started[0] -= _ENV_PREPARE_REPORT_THRESHOLD_SECONDS / 2
    renderer.write(_event_line("running") + "\n")
    renderer.write(_event_line("succeeded") + "\n")
    renderer.finish(elapsed=1.0, success=True)

    assert renderer._state.env_prepare_seconds < _ENV_PREPARE_REPORT_THRESHOLD_SECONDS
    assert "Environment preparation took" not in output.getvalue()


_SITES = ("forest", "meadow", "wetland", "urban")


@task()
def fit_site_trend(*, site: str) -> str:
    """Fan-out leaf, one branch per site."""
    return f"trend:{site}"


@task()
def audit_site(*, site: str, trend: str) -> str:
    """Downstream fan-out branch, zipped against its upstream trend."""
    return f"{site}:{trend}"


def _validated_evaluator(expr: object, calls: tuple[object, ...]) -> ConcurrentEvaluator:
    """Return an evaluator with the graph behind ``expr`` built and validated."""
    evaluator = ConcurrentEvaluator(constructed_calls=calls)
    evaluator.build_and_validate(expr)
    return evaluator


def _seeded_labels(evaluator: ConcurrentEvaluator) -> dict[int, str]:
    """Return the run table's labels for a graph, before any event arrives."""
    state = _RunEventState()
    state.seed(planned_tasks=planned_task_rows(evaluator))
    assert all(row.status == "waiting" for row in state.rows.values())
    return {node_id: row.label for node_id, row in state.rows.items()}


def test_seeded_fanout_rows_use_graph_labels() -> None:
    """An undispatched branch reads as it does under ``--dry-run`` (#204).

    Both views label a node from the graph, so nothing still waiting to be
    dispatched falls back to a bare ordinal.
    """
    with record_constructed_calls() as calls:
        trends = fit_site_trend().map(site=list(_SITES))
        expr = audit_site().map(site=list(_SITES), trend=trends)
    evaluator = _validated_evaluator(expr, tuple(calls))

    plan = build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")
    dry_run_labels = {task.node_id: task.label for wave in plan.waves for task in wave.tasks}

    assert _seeded_labels(evaluator) == dry_run_labels
    assert set(dry_run_labels.values()) == {f"fit_site_trend[{site}]" for site in _SITES} | {
        f"audit_site[{site}]" for site in _SITES
    }


def test_per_branch_arguments_stay_out_of_seeded_labels() -> None:
    """A ``per_branch()`` value derives from a branch, so it does not name it."""
    with record_constructed_calls() as calls:
        expr = audit_site().product_map(
            site=["forest", "meadow"],
            trend=per_branch("{site}.trend"),
        )
    evaluator = _validated_evaluator(expr, tuple(calls))

    assert set(_seeded_labels(evaluator).values()) == {
        "audit_site[site=forest]",
        "audit_site[site=meadow]",
    }


def test_repeated_calls_without_fanout_values_keep_distinct_labels() -> None:
    """Two calls of one task have nothing to tell them apart but an ordinal."""
    with record_constructed_calls() as calls:
        first = audit_site(site="forest", trend="forest.trend")
        second = audit_site(site="meadow", trend="meadow.trend")
    evaluator = _validated_evaluator((first, second), tuple(calls))

    assert sorted(_seeded_labels(evaluator).values()) == ["audit_site", "audit_site[2]"]
