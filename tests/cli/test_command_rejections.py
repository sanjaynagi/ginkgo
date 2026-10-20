"""How commands reject what they were given.

A rejected command line exits 2 with its message on stderr, whether argparse
or the command's own check refused it; a run that never started names no run.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ginkgo.cli.app import main


@pytest.fixture(autouse=True)
def _workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["cache", "clear"], "Provide a cache key"),
        (["cache", "prune"], "Provide at least one of --older-than"),
        (["db", "prune"], "Provide --events-older-than"),
    ],
)
def test_a_command_level_rejection_exits_2_on_stderr(capsys, argv, message) -> None:
    assert main(argv) == 2

    output = capsys.readouterr()
    assert output.out == ""
    assert message in output.err


@pytest.mark.parametrize(
    "argv",
    [
        ["cache", "prune", "--older-than", "0d"],
        ["db", "prune", "--events-older-than", "0d"],
        ["cache", "prune", "--max-entries", "-1"],
        ["cache", "prune", "--max-size", "lots"],
        ["runs", "ls", "--status", "bogus"],
    ],
)
def test_an_invalid_option_value_is_refused_by_the_parser(capsys, argv) -> None:
    with pytest.raises(SystemExit) as exited:
        main(argv)

    assert exited.value.code == 2
    assert capsys.readouterr().out == ""


def test_a_run_that_never_started_prints_no_run_id(capsys) -> None:
    Path("flow.py").write_text("raise RuntimeError('broken at import')\n", encoding="utf-8")

    assert main(["run", "flow.py"]) == 1

    assert "ginkgo run" not in capsys.readouterr().out
    assert not Path(".ginkgo/runs").exists() or not any(Path(".ginkgo/runs").iterdir())


def test_agent_output_reports_a_run_that_never_started(capsys) -> None:
    assert main(["run", "missing.py", "--agent-output"]) == 1

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event"] == "run_rejected"
    assert event["run_id"] is None
    assert "missing.py" in event["error"]


def test_a_workflow_is_named_by_its_path_from_the_working_directory(capsys) -> None:
    Path("workflow").mkdir()
    Path("workflow/flow.py").write_text(
        "from ginkgo import flow, task\n\n\n"
        "@task()\ndef one() -> int:\n    return 1\n\n\n"
        "@flow\ndef main():\n    return one()\n",
        encoding="utf-8",
    )

    assert main(["run", "workflow/flow.py"]) == 0
    assert "ginkgo run workflow/flow.py (" in capsys.readouterr().out

    assert main(["runs", "ls"]) == 0
    assert "workflow/flow.py" in capsys.readouterr().out
