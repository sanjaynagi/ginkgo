"""One user-facing task name.

A single-file workflow loads under a synthetic module name
(``ginkgo_user_<stem>_<hash of its path>``). Output names a task by its
function name or its branch label instead, and the qualified name the ledger
records is location-independent (``<workflow>.<function>``), so ``history``
and ``cache`` filters accept the names output shows.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from rich.console import Console

from ginkgo.cli.app import main

_WORKFLOW = """
from ginkgo import file, flow, shell, task


@task()
def make(item: str) -> str:
    return item


@task("shell")
def runit(item: str, output_path: str) -> file:
    return shell(cmd="exit 3", output=output_path)


@flow
def main():
    return runit(item=make(item="a"), output_path="out/a.txt")
"""


@pytest.fixture
def failed_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> str:
    """Run a single-file workflow whose shell task fails; return its stderr."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "flow.py").write_text(_WORKFLOW, encoding="utf-8")

    assert main(["run", "flow.py"]) == 1
    output = capsys.readouterr()
    assert "ginkgo_user_" not in output.out
    return output.err


def _json(capsys, *argv: str) -> object:
    assert main(list(argv)) == 0
    return json.loads(capsys.readouterr().out)


def test_the_failure_names_the_task_by_its_function(failed_run: str) -> None:
    assert "Shell task runit failed with exit code 3" in failed_run
    assert "ginkgo_user_" not in failed_run


@pytest.mark.parametrize(
    "argv",
    [
        ("runs", "show", "--json"),
        ("debug", "--json"),
        ("history", "runit", "--json"),
        ("cache", "explain", "--json"),
        ("cache", "stats", "--json"),
        ("cache", "ls", "--json"),
    ],
)
def test_no_json_surface_shows_the_synthetic_module(failed_run: str, capsys, argv) -> None:
    assert "ginkgo_user_" not in json.dumps(_json(capsys, *argv))


def test_the_event_stream_records_the_location_independent_name(failed_run: str, capsys) -> None:
    assert main(["export", "events"]) == 0
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    names = {event["task_name"] for event in events if "task_name" in event}
    assert names == {"<workflow>.make", "<workflow>.runit"}


@pytest.mark.parametrize("name", ["runit", "<workflow>.runit"])
def test_history_accepts_the_names_output_shows(failed_run: str, capsys, name: str) -> None:
    rows = _json(capsys, "history", name, "--json")

    assert [(row["task_name"], row["qualified_name"]) for row in rows] == [
        ("runit", "<workflow>.runit")
    ]


def test_cache_ls_filters_by_task_name(failed_run: str, capsys) -> None:
    entries = _json(capsys, "cache", "ls", "--task", "make", "--json")

    assert [(entry["task_name"], entry["qualified_name"]) for entry in entries] == [
        ("make", "<workflow>.make")
    ]
    assert _json(capsys, "cache", "ls", "--task", "runit", "--json") == []


def test_runs_show_json_carries_the_display_label(failed_run: str, capsys) -> None:
    tasks = _json(capsys, "runs", "show", "--json")["tasks"]

    assert {task["task_name"] for task in tasks} == {"make", "runit"}
    assert all("display_label" in task for task in tasks)


def test_cache_ls_prints_each_key_whole_in_a_narrow_terminal(
    failed_run: str, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A key folded over two lines does not paste into `cache clear`."""
    import ginkgo.cli.commands.cache as cache_command

    (key,) = [entry["cache_key"] for entry in _json(capsys, "cache", "ls", "--json")]
    monkeypatch.setattr(
        cache_command, "console", lambda stream: Console(file=stream, width=80, highlight=False)
    )

    assert main(["cache", "ls"]) == 0
    assert key in capsys.readouterr().out
