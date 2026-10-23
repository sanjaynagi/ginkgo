"""``Query.sql`` — what it will run, and what it refuses."""

from __future__ import annotations

from pathlib import Path

import pytest

import ginkgo.query as query
from ginkgo.store.errors import StoreError
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


@pytest.fixture
def reader(ledger, tmp_path: Path):
    """Yield a read-only ``Query`` over a workspace holding one finished run."""
    ledger.finish()
    with query.open(WorkspaceLayout(root=tmp_path / ".ginkgo")) as opened:
        yield opened


@pytest.fixture
def empty(tmp_path: Path):
    """Yield a ``Query`` over a workspace nothing has run in yet."""
    with query.open(WorkspaceLayout(root=tmp_path / "empty" / ".ginkgo"), missing_ok=True) as q:
        yield q


def test_a_select_returns_columns_and_rows(reader) -> None:
    run_id = reader.runs()[0].run_id

    result = reader.sql("SELECT run_id, status FROM runs WHERE run_id = ?", (run_id,))

    assert result.columns == ("run_id", "status")
    assert result.to_payload() == {
        "columns": ["run_id", "status"],
        "rows": [{"run_id": run_id, "status": "succeeded"}],
        "truncated": False,
        "limit": 1000,
    }
    absent = reader.sql("SELECT run_id, workflow FROM runs WHERE run_id = 'absent'")
    assert (absent.rows, absent.columns) == ([], ("run_id", "workflow"))


WRITES = [
    "INSERT INTO runs (run_id, workflow, status, started_at) VALUES ('x', 'y', 'z', 'now')",
    "UPDATE runs SET status = 'failed'",
    "DELETE FROM runs",
    "DROP TABLE runs",
    "PRAGMA table_list",
    "VACUUM",
    "ATTACH DATABASE 'other.db' AS other",
    # A read verb in front of a write: the WITH clause is only as read-only as
    # the statement it introduces.
    "WITH t AS (SELECT 1) INSERT INTO runs (run_id, workflow, status, started_at) "
    "SELECT 'x', 'y', 'z', 'now'",
    "WITH t AS (SELECT 1) DELETE FROM runs",
    "WITH t AS (SELECT 1) UPDATE runs SET status = 'failed'",
]


@pytest.mark.parametrize("workspace", ["reader", "empty"])
@pytest.mark.parametrize("statement", WRITES)
def test_a_statement_that_is_not_a_read_is_refused(request, workspace, statement: str) -> None:
    """The in-memory ledger of an empty workspace answers like a real one."""
    with pytest.raises(StoreError, match="is not a read"):
        request.getfixturevalue(workspace).sql(statement)


@pytest.mark.parametrize("workspace", ["reader", "empty"])
def test_the_engine_refuses_a_write_too(request, workspace) -> None:
    """The keyword check is not the only guard: ``query_only`` is the other."""
    with pytest.raises(Exception, match="readonly database"):
        request.getfixturevalue(workspace).store.query("DELETE FROM runs")


@pytest.mark.parametrize(
    "statement",
    [
        "VALUES(1),(2)",
        "WITH t AS (SELECT 1 AS a) SELECT * FROM t",
        "WITH t(a) AS (VALUES(1),(2)) SELECT * FROM t",
        "-- a leading comment\nSELECT 1 AS n",
        "/* a leading comment */ SELECT 1 AS n",
        # 'delete' is a value here, not a verb.
        "SELECT 1 AS n WHERE 'delete' = 'delete'",
        # A trailing semicolon is not a second statement.
        "SELECT 1 AS n;",
    ],
)
def test_a_read_is_run_however_it_is_spelled(reader, statement: str) -> None:
    assert reader.sql(statement).rows


@pytest.mark.parametrize(
    ("statement", "message"),
    [("SELECT 1; DELETE FROM runs", "more than one statement"), ("   ", "No SQL to run")],
)
def test_malformed_sql_is_refused(reader, statement: str, message: str) -> None:
    with pytest.raises(StoreError, match=message):
        reader.sql(statement)


def test_an_unknown_column_is_answered_with_the_table_s_columns(reader) -> None:
    """The answer comes to the terminal the user is standing at."""
    with pytest.raises(StoreError, match="no such column: task_name") as raised:
        reader.sql("SELECT task_name FROM tasks")

    message = str(raised.value)
    assert "tasks has run_id, task_id" in message
    assert "display_label" in message
    assert "`ginkgo query --schema`" in message
    assert "documentation" not in message


def test_an_unknown_table_is_answered_with_the_tables_there_are(reader) -> None:
    """``task_runs`` is not a mention of ``tasks``, so the table list is the answer."""
    with pytest.raises(StoreError, match="no such table: task_runs") as raised:
        reader.sql("SELECT name FROM task_runs")

    message = str(raised.value)
    assert "The tables are" in message and "tasks" in message
    assert "tasks has run_id" not in message
    assert "documentation" not in message


def test_schema_names_every_table_and_its_columns_even_when_empty(reader, empty) -> None:
    schema = reader.schema()

    assert schema["runs"][:3] == ("run_id", "workflow", "status")
    assert "name" in schema["tasks"]
    assert {"events", "cache_entries", "artifacts", "edges"} <= set(schema)
    assert not [table for table in schema if table.startswith("sqlite_")]
    assert empty.schema() == schema


def test_duplicate_column_names_are_suffixed_in_the_payload(reader) -> None:
    result = reader.sql("SELECT a.run_id, b.run_id FROM runs a JOIN runs b USING (run_id)")

    payload = result.to_payload()
    assert payload["columns"] == ["run_id", "run_id_2"]
    assert payload["rows"][0]["run_id"] == payload["rows"][0]["run_id_2"]
    # The raw columns are untouched: CSV can carry a repeated header.
    assert result.columns == ("run_id", "run_id")


def test_the_row_limit_truncates_and_says_so(reader) -> None:
    many = "SELECT seq FROM events ORDER BY seq"
    assert len(reader.sql(many).rows) == 2

    payload = reader.sql(many, limit=1).to_payload()
    assert (len(payload["rows"]), payload["truncated"], payload["limit"]) == (1, True, 1)
    assert reader.sql("SELECT run_id FROM runs", limit=2).truncated is False


def test_like_wildcards_in_filters_match_literally(reader) -> None:
    """`history "%"` asks for a task called `%`, not for every task there is."""
    assert reader.task_history("%") == []
    assert reader.task_history("_") == []
    assert reader.runs(workflow="%") == []
    assert reader.runs(workflow="flow.py")


def test_since_must_be_a_timestamp(reader) -> None:
    with pytest.raises(ValueError, match="not an ISO-8601 timestamp"):
        reader.runs(since="last tuesday")

    assert reader.runs(since="2000-01-01")


def test_an_empty_workspace_answers_without_creating_a_database(tmp_path: Path, empty) -> None:
    result = empty.sql("SELECT run_id FROM runs")

    assert (result.rows, result.columns) == ([], ("run_id",))
    assert not (tmp_path / "empty" / ".ginkgo" / "ginkgo.db").exists()


def test_task_history_limits_runs_not_rows(tmp_path: Path) -> None:
    """A fan-out puts several rows in one run; ``limit`` never splits a run."""
    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    with open_store(layout.db) as store, store.transaction():
        for run_id, started_at in (("run-1", "2026-08-01"), ("run-2", "2026-08-02")):
            store.apply(
                [
                    ProjectionOp(
                        sql="INSERT INTO runs (run_id, workflow, status, started_at) "
                        "VALUES (?, 'flow.py', 'succeeded', ?)",
                        params=(run_id, started_at),
                    ),
                    *(
                        ProjectionOp(
                            sql="INSERT INTO tasks (run_id, task_id, node_id, name, "
                            "display_label, kind, execution_mode, status, attempts) "
                            "VALUES (?, ?, ?, '<workflow>.fit', ?, 'task', 'worker', "
                            "'succeeded', 1)",
                            params=(run_id, f"task_000{n}", n, f"fit[{n}]"),
                        )
                        for n in range(3)
                    ),
                ]
            )

    with query.open(layout) as reader:
        assert [row.run_id for row in reader.task_history("fit", limit=1)] == ["run-2"] * 3
        assert len(reader.task_history("fit", limit=None)) == 6
