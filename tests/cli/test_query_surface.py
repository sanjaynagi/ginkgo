"""``ginkgo runs``, ``history``, ``query`` and ``export`` over a real workspace."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from rich.console import Console

import ginkgo.query as query
from ginkgo.cli.commands.history import _table
from ginkgo.workspace_layout import WorkspaceLayout
from tests.cli.conftest import write
from tests.conftest import run_cli

WORKFLOW = """
from pathlib import Path
from ginkgo import flow, task, untracked

@task()
def greet(name: str, output_path: untracked) -> str:
    Path(output_path).write_text(f"hello {name}", encoding="utf-8")
    return output_path

@flow
def main():
    return greet(name="world", output_path="out.txt")
"""


def _ok(*args: str, cwd: Path) -> str:
    result = run_cli(*args, cwd=cwd)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _json(*args: str, cwd: Path):
    return json.loads(_ok(*args, "--json", cwd=cwd))


def _workspace(root: Path, source: str, *args: str, code: int = 0, runs: int = 1) -> Path:
    write(root / "workflow.py", source)
    for _ in range(runs):
        assert run_cli("run", "workflow.py", *args, cwd=root).returncode == code
    return root


@pytest.fixture(scope="module")
def workspace(tmp_path_factory) -> Path:
    """A workspace that ran one workflow twice, so the second run is cached."""
    return _workspace(tmp_path_factory.mktemp("workspace"), WORKFLOW, runs=2)


@pytest.fixture(scope="module")
def run_ids(workspace: Path) -> list[str]:
    """The workspace's run ids, newest first."""
    return [row["run_id"] for row in _json("runs", "ls", cwd=workspace)]


class TestRunsAndHistory:
    def test_runs_ls(self, workspace: Path, run_ids) -> None:
        payload = _json("runs", "ls", cwd=workspace)
        assert len(payload) == 2
        assert [row["status"] for row in payload] == ["succeeded", "succeeded"]
        assert all(row["workflow"].endswith("workflow.py") for row in payload)
        assert all(row["parent_run_id"] is None for row in payload)
        assert payload[0]["started_at"] > payload[1]["started_at"]
        assert set(payload[0]) == {
            "run_id",
            "workflow",
            "status",
            "started_at",
            "finished_at",
            "duration_s",
            "parent_run_id",
        }

        def ids(*args: str) -> list[str]:
            return [row["run_id"] for row in _json("runs", "ls", *args, cwd=workspace)]

        assert ids("--limit", "1") == run_ids[:1]
        assert ids("--status", "failed") == []
        assert ids("--workflow", "workflow.py") == run_ids
        assert ids("--workflow", "%") == []  # matched literally, not as a LIKE wildcard
        assert ids("--since", "2000-01-01") == run_ids

        table = _ok("runs", "ls", cwd=workspace)
        assert "🌿 ginkgo runs" in table and "workflow.py" in table
        bad_since = run_cli("runs", "ls", "--since", "last tuesday", cwd=workspace)
        assert bad_since.returncode == 1
        assert "not an ISO-8601 timestamp" in bad_since.stderr

    def test_runs_show_json_is_the_run_manifest(self, workspace: Path, run_ids) -> None:
        payload = _json("runs", "show", run_ids[0], cwd=workspace)
        assert (payload["run_id"], payload["status"]) == (run_ids[0], "succeeded")
        assert [task["status"] for task in payload["tasks"]] == ["cached"]
        manifest = workspace / ".ginkgo" / "runs" / run_ids[0] / "manifest.yaml"
        assert payload == json.loads(json.dumps(yaml.safe_load(manifest.read_text())))
        # Without a run id, the latest run.
        assert _json("runs", "show", cwd=workspace)["run_id"] == run_ids[0]

        inspect = run_cli("inspect", "run", run_ids[0], cwd=workspace)
        assert inspect.returncode == 2
        assert "invalid choice: 'run'" in inspect.stderr

    def test_history(self, workspace: Path, run_ids) -> None:
        payload = _json("history", "greet", cwd=workspace)
        assert [row["run_id"] for row in payload] == run_ids
        assert [(row["cached"], row["status"]) for row in payload] == [
            (True, "cached"),
            (False, "succeeded"),
        ]
        # The second run hit the entry the first one wrote.
        assert payload[0]["cache_key"] == payload[1]["cache_key"]
        assert {(row["task_name"], row["qualified_name"]) for row in payload} == {
            ("greet", "<workflow>.greet")
        }
        assert all(row["attempts"] == 1 for row in payload)
        assert payload[1]["duration_s"] >= 0
        # The cached run's row is present, and null where it measured nothing.
        assert payload[0]["peak_rss_bytes"] is None and payload[0]["cpu_seconds"] is None
        assert payload[1]["peak_rss_bytes"] is not None

        limited = _json("history", "greet", "--limit", "1", cwd=workspace)
        assert [row["run_id"] for row in limited] == run_ids[:1]
        # `history "%"` asks for a task called `%`, not for every task.
        assert _json("history", "%", cwd=workspace) == []

        table = _ok("history", "greet", cwd=workspace)
        assert "Cache Key" in table and "Attempts" in table and "Task" in table
        assert "No run has a task named absent." in _ok("history", "absent", cwd=workspace)

    def test_history_table_keeps_a_fan_out_label(self) -> None:
        """`[central]` is a label, not Rich markup; the table must print it."""
        row = query.TaskRow(
            run_id="r1",
            task_id="task_0001",
            name="workflow.flow.report",
            display_label="report[central]",
            status="succeeded",
            cached=False,
            cache_key="abc123",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:00:01+00:00",
            duration_s=1.0,
            attempts=1,
        )
        console = Console(record=True, width=200)
        console.print(_table([row]))
        assert "report[central]" in console.export_text()

    def test_history_resources_summarises_across_runs(self, workspace: Path, run_ids) -> None:
        """The workspace ran twice: once for real, once from the cache."""
        text = _ok("history", "greet", "--resources", cwd=workspace)
        assert "Peak RSS over 1 execution" in text and "(2 runs, 1 cached)" in text

        payload = _json("history", "greet", "--resources", cwd=workspace)
        assert payload["task"] == "greet"
        assert [row["run_id"] for row in payload["runs"]] == run_ids
        summary = payload["resources"][0]
        assert (summary["n"], summary["cached"], summary["runs"]) == (1, 1, 2)
        assert summary["peak_rss_bytes"]["max"] >= 0

        # A p95 that moved with --limit would be a p95 of nothing in particular.
        limited = _json("history", "greet", "--resources", "--limit", "1", cwd=workspace)
        assert len(limited["runs"]) == 1 and limited["resources"][0]["runs"] == 2


class TestQueryVerb:
    def test_rows_csv_and_truncation(self, workspace: Path, run_ids) -> None:
        statement = "SELECT run_id, status FROM runs ORDER BY started_at DESC"
        assert _json("query", statement, cwd=workspace) == {
            "columns": ["run_id", "status"],
            "rows": [{"run_id": run_id, "status": "succeeded"} for run_id in run_ids],
            "truncated": False,
            "limit": 1000,
        }
        assert _ok("query", "SELECT status FROM runs", "--csv", cwd=workspace).splitlines() == [
            "status",
            "succeeded",
            "succeeded",
        ]
        assert len(_json("query", "VALUES(1),(2)", cwd=workspace)["rows"]) == 2

        limited = ("query", "SELECT run_id FROM runs", "--limit", "1")
        assert "Stopped at 1 rows" in _ok(*limited, cwd=workspace)
        payload = _json(*limited, cwd=workspace)
        assert (payload["truncated"], payload["limit"], len(payload["rows"])) == (True, 1, 1)
        # CSV stdout stays something a spreadsheet can open; the warning goes elsewhere.
        csv = run_cli(*limited, "--csv", cwd=workspace)
        assert len(csv.stdout.splitlines()) == 2 and "Stopped at" not in csv.stdout
        assert "Stopped at 1 rows" in csv.stderr

    @pytest.mark.parametrize(
        ("statement", "phrases"),
        [
            ("DELETE FROM runs", ["is not a read"]),
            ("UPDATE runs SET status = 'x'", ["is not a read"]),
            ("PRAGMA table_list", ["is not a read"]),
            ("WITH t AS (SELECT 1) DELETE FROM runs", ["is not a read"]),
            ("SELECT 1; DELETE FROM runs", ["more than one statement"]),
            ("SELECT nope FROM runs", ["no such column: nope"]),
            (
                "SELECT task_name FROM tasks",
                ["tasks has run_id, task_id", "`ginkgo query --schema` lists every table."],
            ),
            ("SELECT name FROM task_runs", ["no such table: task_runs", "The tables are"]),
        ],
    )
    def test_a_rejected_statement_says_why(
        self, workspace: Path, statement: str, phrases: list[str]
    ) -> None:
        result = run_cli("query", statement, cwd=workspace)
        assert result.returncode == 1
        message = " ".join(result.stderr.split())
        assert all(phrase in message for phrase in phrases)
        # The old error sent people to docs that are not published.
        assert "documentation" not in message
        assert _json("query", "SELECT count(*) AS n FROM runs", cwd=workspace)["rows"] == [
            {"n": 2}
        ]

    def test_schema(self, workspace: Path) -> None:
        printed = " ".join(_ok("query", "--schema", cwd=workspace).split())
        assert "tasks" in printed and "display_label" in printed and "parent_run_id" in printed
        schema = _json("query", "--schema", cwd=workspace)
        assert schema["runs"][0] == "run_id" and "name" in schema["tasks"]

        missing = run_cli("query", cwd=workspace)
        assert missing.returncode == 2 and "--schema" in missing.stderr


class TestExport:
    def test_events_round_trip_and_match_the_ledger(
        self, workspace: Path, run_ids, tmp_path: Path
    ) -> None:
        piped = _ok("export", "events", run_ids[0], cwd=workspace)
        payloads = [json.loads(line) for line in piped.splitlines()]
        with query.open(WorkspaceLayout(root=workspace / ".ginkgo")) as reader:
            assert payloads == [event.payload for event in reader.events(run_ids[0])]
        assert payloads[0]["event"] == "run_started"
        assert all(payload["run_id"] == run_ids[0] for payload in payloads)

        destination = tmp_path / "events.jsonl"
        _ok("export", "events", run_ids[0], "--out", str(destination), cwd=workspace)
        assert destination.read_text(encoding="utf-8") == piped

    def test_manifest_equals_the_file_the_run_wrote(
        self, workspace: Path, run_ids, tmp_path: Path
    ) -> None:
        written = (workspace / ".ginkgo" / "runs" / run_ids[0] / "manifest.yaml").read_text()
        assert _ok("export", "manifest", run_ids[0], cwd=workspace) == written
        destination = tmp_path / "nested" / "manifest.yaml"
        _ok("export", "manifest", run_ids[0], "--out", str(destination), cwd=workspace)
        assert destination.read_text(encoding="utf-8") == written

    @pytest.mark.parametrize("subcommand", ["events", "manifest"])
    def test_out_writes_atomically_into_a_new_directory(
        self, workspace: Path, run_ids, tmp_path: Path, subcommand: str
    ) -> None:
        """Both exports land the same way: a temporary file, renamed over."""
        destination = tmp_path / subcommand / "export.out"
        _ok("export", subcommand, run_ids[0], "--out", str(destination), cwd=workspace)
        assert list(destination.parent.iterdir()) == [destination]

    def test_an_unknown_run_is_reported(self, workspace: Path) -> None:
        result = run_cli("export", "events", "no-such-run", cwd=workspace)
        assert result.returncode == 1
        assert "Run not found: no-such-run" in result.stderr


class TestEmptyWorkspace:
    """No database, no runs — every read verb answers, and none creates a file."""

    @pytest.fixture
    def empty(self, tmp_path: Path) -> Path:
        write(tmp_path / "ginkgo.toml")
        return tmp_path

    def test_every_read_verb_answers_without_creating_a_database(self, empty: Path) -> None:
        assert _json("runs", "ls", cwd=empty) == []
        assert _json("history", "greet", cwd=empty) == []
        selected = _json("query", "SELECT run_id FROM runs", cwd=empty)
        assert (selected["rows"], selected["columns"]) == ([], ["run_id"])
        assert "name" in _json("query", "--schema", cwd=empty)["tasks"]
        listed = _ok("runs", "ls", cwd=empty)
        assert "No runs recorded in this workspace." in listed and "no database" not in listed

        refused = run_cli("query", "WITH t AS (SELECT 1) DELETE FROM runs", cwd=empty)
        assert refused.returncode == 1 and "is not a read" in refused.stderr
        exported = run_cli("export", "events", cwd=empty)
        assert exported.returncode == 1 and "No runs recorded" in exported.stderr
        for args in (("runs", "show"), ("history", "greet"), ("export", "manifest")):
            run_cli(*args, cwd=empty)
        assert not (empty / ".ginkgo").exists()

    def test_runs_ls_warns_when_run_directories_outlived_the_ledger(self, empty: Path) -> None:
        """An empty listing must not affirm an empty workspace (issue #282)."""
        (empty / ".ginkgo" / "runs" / "20260101_000000_000000_deadbeef").mkdir(parents=True)
        assert "1 run directory under .ginkgo/runs but no database" in _ok("runs", "ls", cwd=empty)
        assert not (empty / ".ginkgo" / "ginkgo.db").exists()


def test_the_agent_stream_and_the_export_share_a_wire_shape(tmp_path: Path) -> None:
    """``export events`` replays what ``run --agent-output`` printed live."""
    write(tmp_path / "workflow.py", WORKFLOW)
    streamed = [
        json.loads(line)
        for line in _ok("run", "workflow.py", "--agent-output", cwd=tmp_path).splitlines()
        if line.strip()
    ]
    exported = _ok("export", "events", streamed[0]["run_id"], cwd=tmp_path)
    replayed = {(e["event"], e["ts"]): e for e in map(json.loads, exported.splitlines())}

    # The stream carries events the ledger does not store, and the ledger holds
    # the run's completion, printed after this point. What both hold is identical.
    shared = [event for event in streamed if (event["event"], event["ts"]) in replayed]
    assert shared, "no event reached both the live stream and the export"
    assert all(replayed[(event["event"], event["ts"])] == event for event in shared)


def test_history_of_a_task_that_never_started(tmp_path: Path) -> None:
    root = _workspace(
        tmp_path,
        """
        from ginkgo import flow, task

        @task()
        def bad() -> int:
            raise RuntimeError("boom")

        @task()
        def after(x: int) -> int:
            return x

        @flow
        def main():
            return after(x=bad())
        """,
        code=1,
    )
    assert _json("history", "after", cwd=root) == []
    unstarted = _json("history", "after", "--include-unstarted", cwd=root)
    assert [row["status"] for row in unstarted] == ["skipped"]
    assert _json("history", "after", "--resources", cwd=root)["resources"][0]["runs"] == 0


def test_a_failed_fan_out_run_names_the_failed_branch(tmp_path: Path) -> None:
    # 3: the run continued past the failure, then lost its result to it.
    root = _workspace(
        tmp_path,
        """
        from ginkgo import flow, task

        @task()
        def check(item: str) -> str:
            if item == "omega":
                raise ValueError("bad seed")
            return item

        @task()
        def gather(items: list[str]) -> int:
            return len(items)

        @flow
        def main():
            return gather(items=check().map(item=["alpha", "omega"]))
        """,
        "--keep-going",
        code=3,
    )
    recorded = _json("runs", "show", cwd=root)
    assert "check[omega]" in recorded["error"]
    assert recorded["error"] in " ".join(_ok("runs", "show", cwd=root).split())

    explain = run_cli("cache", "explain", "gather", cwd=root)
    assert explain.returncode == 1
    assert "'gather' is a task, not a run" in " ".join(explain.stderr.split())


def test_the_sizing_ratio_is_of_the_declaration_not_the_rounded_budget(tmp_path: Path) -> None:
    root = _workspace(
        tmp_path,
        """
        from ginkgo import flow, task

        @task(memory="512Mi")
        def hold() -> int:
            return len(bytearray(1))

        @flow
        def main():
            return hold()
        """,
    )
    text = _ok("history", "hold", "--resources", cwd=root)
    assert "declared 0.5 GiB" in text and "after escalation" not in text
    payload = _json("history", "hold", "--resources", cwd=root)
    assert payload["resources"][0]["declared_memory_gb"] == 0.5


def test_a_flag_of_another_command_is_refused_with_the_commands_own_usage() -> None:
    # A core command's flag, so the test holds without the plugins installed.
    result = run_cli("run", "--include-unstarted")
    assert result.returncode == 2
    assert "ginkgo run: error: unrecognized arguments: --include-unstarted" in result.stderr
