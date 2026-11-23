"""Agent tasks, run end to end with a stub agent CLI in place of a model."""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import pytest

from ginkgo import file, folder, task
from ginkgo.core.directive import DirectiveError
from ginkgo.runtime.events import EventBus, TaskAnnotated, TaskNotice
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.rundir import RunDir, make_run_id
from ginkgo_agent import AgentExitError, AgentTaskError, AgentTimeoutError, agent
from ginkgo_agent.clis import cli_version
from ginkgo_agent.runner import _fingerprint

STUB = Path(__file__).with_name("stub_agent.py")

_SCHEMA = {
    "type": "object",
    "properties": {"verdict": {"type": "string"}},
    "required": ["verdict"],
}


@task("agent")
def interpret(packet: folder, brief: file, schema: file) -> folder:
    return agent(brief=brief, reads=[packet], output_schema=schema)


@task("agent")
def summarise(brief: file) -> folder:
    return agent(brief=brief)


@task("agent")
def interpret_quickly(packet: folder, brief: file, schema: file) -> folder:
    return agent(brief=brief, reads=[packet], output_schema=schema, timeout_minutes=0.02)


@task("agent", retries=1, retry_on=AgentTaskError, retry_backoff=0)
def interpret_twice(packet: folder, brief: file, schema: file) -> folder:
    return agent(brief=brief, reads=[packet], output_schema=schema)


@task("agent")
def read_beside(brief: file) -> folder:
    return agent(brief=brief, reads=[Path(brief).parent / "notes.txt"])


@task("agent")
def read_by_name(brief: file, notes: str) -> folder:
    return agent(brief=brief, reads=[notes])


@pytest.fixture(autouse=True)
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A project whose [agents] runner is a copy of the stub, and its inputs."""
    monkeypatch.chdir(tmp_path)
    cli_version.cache_clear()
    stub = tmp_path / "stub_agent.py"
    shutil.copy2(STUB, stub)
    (tmp_path / "ginkgo.toml").write_text(
        f'[agents]\nrunner = "stub"\ncommand = "{stub}"\n', encoding="utf-8"
    )
    (tmp_path / "packet").mkdir()
    (tmp_path / "packet" / "system.json").write_text('{"system": "kpn-amk"}', encoding="utf-8")
    (tmp_path / "brief.md").write_text("Interpret the packet.\n", encoding="utf-8")
    (tmp_path / "schema.json").write_text(json.dumps(_SCHEMA), encoding="utf-8")
    (tmp_path / "notes.txt").write_text("notes\n", encoding="utf-8")
    calls = tmp_path / "calls.txt"
    monkeypatch.setenv("STUB_CALLS", str(calls))
    return tmp_path


def _inputs(root: Path) -> dict[str, str]:
    return {
        "packet": str(root / "packet"),
        "brief": str(root / "brief.md"),
        "schema": str(root / "schema.json"),
    }


def _evaluate(
    expr: object, *, root: Path, runs: Path | None = None
) -> tuple[object, RunDir, dict, list[str]]:
    """Run *expr* once; return its result, run directory, annotations and notices."""
    runs = root / "runs" if runs is None else runs
    run_dir = RunDir.create(run_id=make_run_id(workflow_path=root / "flow.py"), root=runs)
    bus = EventBus()
    fields: dict[str, object] = {}
    notices: list[str] = []
    bus.subscribe(lambda event: isinstance(event, TaskAnnotated) and fields.update(event.fields))
    bus.subscribe(lambda event: isinstance(event, TaskNotice) and notices.append(event.message))
    result = ConcurrentEvaluator(run_dir=run_dir, event_bus=bus, jobs=1, cores=1).evaluate(expr)
    return result, run_dir, fields, notices


def _calls(root: Path) -> int:
    calls = root / "calls.txt"
    return len(calls.read_text().splitlines()) if calls.exists() else 0


def test_an_agent_works_in_a_fresh_workspace_which_is_the_tasks_value(project: Path) -> None:
    result, run_dir, fields, _ = _evaluate(interpret(**_inputs(project)), root=project)

    workspace = Path(str(result))
    attempt = run_dir.path / "agents" / "task_0000_attempt_1"
    assert workspace == attempt / "workspace"
    # The agent ran in its workspace, and was told where that is and what to read.
    assert (attempt / "transcript.jsonl").is_file()
    prompt = (workspace / "report.md").read_text()
    assert prompt.startswith("Interpret the packet.")
    assert f"Your working directory is {workspace}." in prompt
    assert f"- {project / 'packet'}" in prompt
    assert json.loads((workspace / "final_message.json").read_text()) == {"verdict": "pass"}

    assert fields["agent_runner"] == "stub"
    assert str(fields["agent_cli_version"]).startswith("stub ")
    assert fields["agent_usage"] == {"input_tokens": 11, "output_tokens": 7}
    assert fields["agent_attempt_dir"] == "agents/task_0000_attempt_1"


def test_an_agent_runs_from_a_run_directory_given_relative_to_the_project(
    project: Path,
) -> None:
    # As `ginkgo run` gives it: the command changes into the workspace first.
    result, run_dir, fields, _ = _evaluate(
        interpret(**_inputs(project)), root=project, runs=Path(".ginkgo/runs")
    )

    assert not run_dir.path.is_absolute()
    workspace = Path(str(result))
    assert workspace == (run_dir.path / "agents" / "task_0000_attempt_1" / "workspace").resolve()
    assert json.loads((workspace / "final_message.json").read_text()) == {"verdict": "pass"}
    assert fields["agent_attempt_dir"] == "agents/task_0000_attempt_1"


def test_without_a_schema_the_final_message_is_kept_as_text(project: Path) -> None:
    result, *_ = _evaluate(summarise(brief=str(project / "brief.md")), root=project)

    assert json.loads((Path(str(result)) / "final_message.txt").read_text()) == {"verdict": "pass"}


def test_the_same_attempt_is_not_run_twice_and_any_change_to_it_reruns(project: Path) -> None:
    _evaluate(interpret(**_inputs(project)), root=project)
    _evaluate(interpret(**_inputs(project)), root=project)
    assert _calls(project) == 1

    # A different brief, the agent CLI's version, or the model each run it again.
    (project / "brief.md").write_text("Interpret the packet carefully.\n", encoding="utf-8")
    _evaluate(interpret(**_inputs(project)), root=project)
    assert _calls(project) == 2

    stub = project / "stub_agent.py"
    stub.write_text(stub.read_text() + "\n# edited\n", encoding="utf-8")
    cli_version.cache_clear()
    _evaluate(interpret(**_inputs(project)), root=project)
    assert _calls(project) == 3

    config = project / "ginkgo.toml"
    config.write_text(config.read_text() + 'model = "bigger"\n', encoding="utf-8")
    _evaluate(interpret(**_inputs(project)), root=project)
    assert _calls(project) == 4


@pytest.mark.parametrize(
    ("mode", "error", "match"),
    [
        ("fail", AgentExitError, "(?s)exited with code 3.*told to fail"),
        ("schema_mismatch", AgentTaskError, "does not match .*schema.json: 3 is not of type"),
        ("bad_json", AgentTaskError, "is not JSON"),
        ("no_message", AgentTaskError, "without a final message"),
        ("write_read", AgentTaskError, "changed a path it was given to read: .*packet"),
    ],
)
def test_a_failed_attempt_fails_the_task(
    project: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    error: type[Exception],
    match: str,
) -> None:
    monkeypatch.setenv("STUB_MODE", mode)
    with pytest.raises(error, match=match):
        _evaluate(interpret(**_inputs(project)), root=project)


def test_an_agent_past_its_timeout_is_stopped(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_MODE", "sleep")
    started = time.monotonic()
    with pytest.raises(AgentTimeoutError, match="stopped after 0.02 minutes"):
        _evaluate(interpret_quickly(**_inputs(project)), root=project)
    # Stopped at the timeout, not left to sleep its 60 seconds.
    assert time.monotonic() - started < 20


def test_a_retry_is_a_new_attempt_beside_the_failed_one(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_MODE", "fail_first")
    result, run_dir, fields, _ = _evaluate(interpret_twice(**_inputs(project)), root=project)

    agents = run_dir.path / "agents"
    assert sorted(path.name for path in agents.iterdir()) == [
        "task_0000_attempt_1",
        "task_0000_attempt_2",
    ]
    assert Path(str(result)) == agents / "task_0000_attempt_2" / "workspace"
    assert fields["agent_attempt_dir"] == "agents/task_0000_attempt_2"


def test_a_read_that_is_not_an_argument_is_named(project: Path) -> None:
    _, _, _, notices = _evaluate(read_beside(brief=str(project / "brief.md")), root=project)

    assert any("notes.txt, which is not one of its arguments" in notice for notice in notices)


def test_a_read_passed_as_a_plain_string_is_named(project: Path) -> None:
    expr = read_by_name(brief=str(project / "brief.md"), notes=str(project / "notes.txt"))
    _, _, _, notices = _evaluate(expr, root=project)

    assert any("notes.txt, which is not one of its arguments" in notice for notice in notices)


def test_a_stub_given_relative_to_the_project_runs(project: Path) -> None:
    (project / "ginkgo.toml").write_text(
        '[agents]\nrunner = "stub"\ncommand = "stub_agent.py"\n', encoding="utf-8"
    )
    result, _, _, _ = _evaluate(summarise(brief=str(project / "brief.md")), root=project)

    assert Path(str(result)).is_dir()
    assert _calls(project) == 1


def test_the_read_check_sees_new_directories_and_changes_through_links(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "data.txt").write_text("a", encoding="utf-8")
    read = tmp_path / "read"
    read.mkdir()
    (read / "linked").symlink_to(target)

    before = _fingerprint(read)
    (read / "empty").mkdir()
    assert _fingerprint(read) != before

    before = _fingerprint(read)
    (target / "data.txt").write_text("ab", encoding="utf-8")
    assert _fingerprint(read) != before


def test_an_invalid_schema_is_refused_before_the_agent_runs(project: Path) -> None:
    (project / "schema.json").write_text('{"type": "not-a-type"}', encoding="utf-8")
    with pytest.raises(DirectiveError, match="not a valid JSON Schema"):
        _evaluate(interpret(**_inputs(project)), root=project)
    assert _calls(project) == 0


@pytest.mark.parametrize(
    ("table", "match"),
    [
        ('runner = "gemini"', "not a known runner"),
        ('runner = "stub"', "needs command"),
        ('runner = "codex"\ncommand = "/no/such/codex"', "Cannot run the codex CLI"),
        ('runner = "codex"\nisolation = "bwrap"', "not available yet"),
    ],
)
def test_a_wrong_agents_table_is_refused(project: Path, table: str, match: str) -> None:
    (project / "ginkgo.toml").write_text(f"[agents]\n{table}\n", encoding="utf-8")
    with pytest.raises(DirectiveError, match=match):
        _evaluate(summarise(brief=str(project / "brief.md")), root=project)
