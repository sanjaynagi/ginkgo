"""``ginkgo runs``, ``history``, ``query`` and ``export`` over a real workspace."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest
import yaml

import ginkgo.query as query
from ginkgo.workspace_layout import WorkspaceLayout

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / ".pixi" / "envs" / "default" / "bin" / "python"

WORKFLOW = """
from pathlib import Path
from ginkgo import flow, task

@task()
def greet(name: str, output_path: str) -> str:
    Path(output_path).write_text(f"hello {name}", encoding="utf-8")
    return output_path

@flow
def main():
    return greet(name="world", output_path="out.txt")
"""


def _run_cli(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", *args],
        cwd=cwd or Path.cwd(),
        check=False,
        text=True,
        capture_output=True,
    )


@pytest.fixture(scope="class")
def workspace(tmp_path_factory) -> Path:
    """Build a workspace by running one workflow twice, so the second run is cached."""
    root = tmp_path_factory.mktemp("workspace")
    (root / "workflow.py").write_text(WORKFLOW.strip() + "\n", encoding="utf-8")
    for _ in range(2):
        result = _run_cli("run", "workflow.py", cwd=root)
        assert result.returncode == 0, result.stderr
    return root


@pytest.fixture(scope="class")
def run_ids(workspace: Path) -> list[str]:
    """Return the workspace's run ids, newest first."""
    result = _run_cli("runs", "ls", "--json", cwd=workspace)
    assert result.returncode == 0, result.stderr
    return [row["run_id"] for row in json.loads(result.stdout)]


class TestRunsAndHistory:
    def test_runs_ls_json_lists_both_runs_newest_first(self, workspace: Path) -> None:
        result = _run_cli("runs", "ls", "--json", cwd=workspace)

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
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

    def test_runs_ls_filters_narrow_the_index(self, workspace: Path, run_ids) -> None:
        limited = _run_cli("runs", "ls", "--limit", "1", "--json", cwd=workspace)
        assert [row["run_id"] for row in json.loads(limited.stdout)] == run_ids[:1]

        missed = _run_cli("runs", "ls", "--status", "failed", "--json", cwd=workspace)
        assert json.loads(missed.stdout) == []

        matched = _run_cli("runs", "ls", "--workflow", "workflow.py", "--json", cwd=workspace)
        assert len(json.loads(matched.stdout)) == 2

    def test_runs_ls_table_names_the_workflow_file(self, workspace: Path) -> None:
        result = _run_cli("runs", "ls", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert "🌿 ginkgo runs" in result.stdout
        assert "workflow.py" in result.stdout

    def test_runs_show_json_is_the_run_manifest(self, workspace: Path, run_ids) -> None:
        result = _run_cli("runs", "show", run_ids[0], "--json", cwd=workspace)

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["run_id"] == run_ids[0]
        assert payload["status"] == "succeeded"
        assert [task["status"] for task in payload["tasks"]] == ["cached"]

        written = yaml.safe_load(
            (workspace / ".ginkgo" / "runs" / run_ids[0] / "manifest.yaml").read_text(
                encoding="utf-8"
            )
        )
        assert payload == json.loads(json.dumps(written))

    def test_runs_show_without_a_run_id_takes_the_latest(self, workspace: Path, run_ids) -> None:
        result = _run_cli("runs", "show", "--json", cwd=workspace)

        assert json.loads(result.stdout)["run_id"] == run_ids[0]

    def test_inspect_no_longer_answers_for_a_run(self, workspace: Path, run_ids) -> None:
        result = _run_cli("inspect", "run", run_ids[0], cwd=workspace)

        assert result.returncode == 2
        assert "invalid choice: 'run'" in result.stderr

    def test_history_json_has_one_row_per_run(self, workspace: Path, run_ids) -> None:
        result = _run_cli("history", "greet", "--json", cwd=workspace)

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert [row["run_id"] for row in payload] == run_ids
        assert [row["cached"] for row in payload] == [True, False]
        assert [row["status"] for row in payload] == ["cached", "succeeded"]
        # The second run hit the entry the first one wrote.
        assert payload[0]["cache_key"] == payload[1]["cache_key"]
        assert all(row["name"].endswith(".greet") for row in payload)
        assert all(row["attempts"] == 1 for row in payload)
        assert payload[1]["duration_s"] >= 0

    def test_history_limit_is_honoured(self, workspace: Path, run_ids) -> None:
        result = _run_cli("history", "greet", "--limit", "1", "--json", cwd=workspace)

        assert [row["run_id"] for row in json.loads(result.stdout)] == run_ids[:1]

    def test_history_table_shows_a_cache_key_prefix(self, workspace: Path) -> None:
        result = _run_cli("history", "greet", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert "Cache Key" in result.stdout
        assert "Attempts" in result.stdout

    def test_history_names_the_task_in_its_own_column(self, workspace: Path) -> None:
        result = _run_cli("history", "greet", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert "Task" in result.stdout
        assert "greet" in result.stdout

    def test_a_like_wildcard_is_matched_literally(self, workspace: Path) -> None:
        """`history "%"` asks for a task called `%`, not for every task."""
        result = _run_cli("history", "%", "--json", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == []

    def test_runs_ls_workflow_filter_matches_literally(self, workspace: Path) -> None:
        result = _run_cli("runs", "ls", "--workflow", "%", "--json", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == []

    def test_runs_ls_rejects_a_since_that_is_not_a_timestamp(self, workspace: Path) -> None:
        result = _run_cli("runs", "ls", "--since", "last tuesday", cwd=workspace)

        assert result.returncode == 1
        assert "not an ISO-8601 timestamp" in result.stderr

    def test_runs_ls_since_accepts_a_bare_date(self, workspace: Path) -> None:
        result = _run_cli("runs", "ls", "--since", "2000-01-01", "--json", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert len(json.loads(result.stdout)) == 2

    def test_history_of_an_unknown_task_is_an_answer(self, workspace: Path) -> None:
        result = _run_cli("history", "absent", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert "No run has a task named absent." in result.stdout


class TestQueryVerb:
    def test_json_rows_carry_column_names(self, workspace: Path, run_ids) -> None:
        result = _run_cli(
            "query",
            "SELECT run_id, status FROM runs ORDER BY started_at DESC",
            "--json",
            cwd=workspace,
        )

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {
            "columns": ["run_id", "status"],
            "rows": [{"run_id": run_id, "status": "succeeded"} for run_id in run_ids],
            "truncated": False,
            "limit": 1000,
        }

    def test_csv_leads_with_a_header(self, workspace: Path) -> None:
        result = _run_cli("query", "SELECT status FROM runs", "--csv", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == ["status", "succeeded", "succeeded"]

    def test_the_limit_truncates_and_says_so(self, workspace: Path) -> None:
        result = _run_cli("query", "SELECT run_id FROM runs", "--limit", "1", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert "Stopped at 1 rows" in result.stdout

    def test_json_carries_the_truncation_signal(self, workspace: Path) -> None:
        result = _run_cli(
            "query", "SELECT run_id FROM runs", "--limit", "1", "--json", cwd=workspace
        )

        payload = json.loads(result.stdout)
        assert payload["truncated"] is True
        assert payload["limit"] == 1
        assert len(payload["rows"]) == 1

    def test_csv_keeps_stdout_clean_and_warns_on_stderr(self, workspace: Path) -> None:
        result = _run_cli(
            "query", "SELECT run_id FROM runs", "--limit", "1", "--csv", cwd=workspace
        )

        assert result.returncode == 0
        # stdout stays CSV a spreadsheet can open; the warning goes elsewhere.
        assert len(result.stdout.splitlines()) == 2
        assert "Stopped at 1 rows" in result.stderr
        assert "Stopped at" not in result.stdout

    @pytest.mark.parametrize(
        "statement",
        [
            "DELETE FROM runs",
            "UPDATE runs SET status = 'x'",
            "PRAGMA table_list",
            "WITH t AS (SELECT 1) DELETE FROM runs",
        ],
    )
    def test_a_write_is_refused_by_name(self, workspace: Path, statement: str) -> None:
        result = _run_cli("query", statement, cwd=workspace)

        assert result.returncode == 1
        assert "is not a read" in result.stderr

    def test_a_second_statement_is_refused(self, workspace: Path) -> None:
        result = _run_cli("query", "SELECT 1; DELETE FROM runs", cwd=workspace)

        assert result.returncode == 1
        assert "more than one statement" in result.stderr

    def test_an_unknown_column_is_reported(self, workspace: Path) -> None:
        result = _run_cli("query", "SELECT nope FROM runs", cwd=workspace)

        assert result.returncode == 1
        assert "no such column: nope" in result.stderr

    def test_values_without_a_space_is_a_read(self, workspace: Path) -> None:
        result = _run_cli("query", "VALUES(1),(2)", "--json", cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert len(json.loads(result.stdout)["rows"]) == 2

    def test_a_refused_write_left_the_ledger_alone(self, workspace: Path) -> None:
        _run_cli("query", "DELETE FROM runs", cwd=workspace)

        counted = _run_cli("query", "SELECT count(*) AS n FROM runs", "--json", cwd=workspace)

        assert json.loads(counted.stdout)["rows"] == [{"n": 2}]


class TestExport:
    def test_events_round_trip_and_match_the_ledger(self, workspace: Path, run_ids) -> None:
        result = _run_cli("export", "events", run_ids[0], cwd=workspace)

        assert result.returncode == 0, result.stderr
        lines = result.stdout.splitlines()
        payloads = [json.loads(line) for line in lines]

        with query.open(WorkspaceLayout(root=workspace / ".ginkgo")) as reader:
            expected = [event.payload for event in reader.events(run_ids[0])]
        assert payloads == expected
        assert payloads[0]["event"] == "run_started"
        assert all(payload["run_id"] == run_ids[0] for payload in payloads)

    def test_events_out_writes_the_same_bytes(self, workspace: Path, run_ids, tmp_path) -> None:
        destination = tmp_path / "events.jsonl"

        written = _run_cli(
            "export", "events", run_ids[0], "--out", str(destination), cwd=workspace
        )
        piped = _run_cli("export", "events", run_ids[0], cwd=workspace)

        assert written.returncode == 0, written.stderr
        assert destination.read_text(encoding="utf-8") == piped.stdout

    def test_manifest_equals_the_file_the_run_wrote(self, workspace: Path, run_ids) -> None:
        result = _run_cli("export", "manifest", run_ids[0], cwd=workspace)

        assert result.returncode == 0, result.stderr
        assert result.stdout == (
            workspace / ".ginkgo" / "runs" / run_ids[0] / "manifest.yaml"
        ).read_text(encoding="utf-8")

    def test_manifest_out_writes_the_same_document(
        self, workspace: Path, run_ids, tmp_path
    ) -> None:
        destination = tmp_path / "nested" / "manifest.yaml"

        result = _run_cli(
            "export", "manifest", run_ids[0], "--out", str(destination), cwd=workspace
        )

        assert result.returncode == 0, result.stderr
        assert destination.read_text(encoding="utf-8") == (
            workspace / ".ginkgo" / "runs" / run_ids[0] / "manifest.yaml"
        ).read_text(encoding="utf-8")

    @pytest.mark.parametrize("subcommand", ["events", "manifest"])
    def test_out_writes_atomically_into_a_new_directory(
        self, workspace: Path, run_ids, tmp_path, subcommand: str
    ) -> None:
        """Both exports land the same way: a temporary file, renamed over."""
        destination = tmp_path / subcommand / "export.out"

        result = _run_cli(
            "export", subcommand, run_ids[0], "--out", str(destination), cwd=workspace
        )

        assert result.returncode == 0, result.stderr
        assert destination.is_file()
        assert list(destination.parent.iterdir()) == [destination]

    def test_an_unknown_run_is_reported(self, workspace: Path) -> None:
        result = _run_cli("export", "events", "no-such-run", cwd=workspace)

        assert result.returncode == 1
        assert "Run not found: no-such-run" in result.stderr


class TestEmptyWorkspace:
    """No database, no runs — every read verb answers, and none creates a file."""

    @pytest.fixture
    def empty(self, tmp_path: Path) -> Path:
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        return tmp_path

    def test_runs_ls_is_an_empty_list(self, empty: Path) -> None:
        result = _run_cli("runs", "ls", "--json", cwd=empty)

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == []

    def test_history_is_an_empty_list(self, empty: Path) -> None:
        result = _run_cli("history", "greet", "--json", cwd=empty)

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == []

    def test_query_selects_no_rows(self, empty: Path) -> None:
        result = _run_cli("query", "SELECT run_id FROM runs", "--json", cwd=empty)

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["rows"] == []
        assert payload["columns"] == ["run_id"]

    def test_a_write_is_refused_the_same_way_as_on_a_real_ledger(self, empty: Path) -> None:
        result = _run_cli("query", "WITH t AS (SELECT 1) DELETE FROM runs", cwd=empty)

        assert result.returncode == 1
        assert "is not a read" in result.stderr

    def test_export_names_the_missing_database(self, empty: Path) -> None:
        result = _run_cli("export", "events", cwd=empty)

        assert result.returncode == 1
        assert "No runs recorded" in result.stderr

    def test_no_read_verb_created_a_database(self, empty: Path) -> None:
        for args in (
            ("runs", "ls"),
            ("runs", "show"),
            ("history", "greet"),
            ("query", "SELECT 1"),
            ("export", "events"),
            ("export", "manifest"),
        ):
            _run_cli(*args, cwd=empty)

        assert not (empty / ".ginkgo").exists()


def test_the_agent_stream_and_the_export_share_a_wire_shape(tmp_path: Path) -> None:
    """``export events`` replays what ``run --agent-output`` printed live."""
    (tmp_path / "workflow.py").write_text(WORKFLOW.strip() + "\n", encoding="utf-8")

    live = _run_cli("run", "workflow.py", "--agent-output", cwd=tmp_path)
    assert live.returncode == 0, live.stderr
    streamed = [json.loads(line) for line in live.stdout.splitlines() if line.strip()]
    run_id = streamed[0]["run_id"]

    exported = _run_cli("export", "events", run_id, cwd=tmp_path)
    assert exported.returncode == 0, exported.stderr
    replayed = [json.loads(line) for line in exported.stdout.splitlines()]

    # The stream carries events the ledger does not store, and the ledger holds
    # the run's completion, which the stream printed after this comparison
    # point. What both hold is identical.
    by_key = {(event["event"], event["ts"]): event for event in replayed}
    shared = [event for event in streamed if (event["event"], event["ts"]) in by_key]
    assert shared, "no event reached both the live stream and the export"
    for event in shared:
        assert by_key[(event["event"], event["ts"])] == event
