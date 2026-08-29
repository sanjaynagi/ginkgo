"""``Query.sql`` — what it will run, and what it refuses."""

from __future__ import annotations

from pathlib import Path

import pytest

import ginkgo.query as query
from ginkgo.store.errors import StoreError
from ginkgo.workspace_layout import WorkspaceLayout


@pytest.fixture
def reader(ledger, tmp_path: Path):
    """Yield a read-only ``Query`` over a workspace holding one finished run."""
    ledger.finish()
    with query.open(WorkspaceLayout(root=tmp_path / ".ginkgo")) as opened:
        yield opened


def test_a_select_returns_columns_and_rows(reader) -> None:
    result = reader.sql("SELECT run_id, status FROM runs")

    assert result.columns == ("run_id", "status")
    assert [row["status"] for row in result.rows] == ["succeeded"]
    assert result.truncated is False
    assert result.to_payload() == [{"run_id": reader.runs()[0].run_id, "status": "succeeded"}]


def test_placeholders_carry_values(reader) -> None:
    run_id = reader.runs()[0].run_id

    result = reader.sql("SELECT status FROM runs WHERE run_id = ?", (run_id,))

    assert [row["status"] for row in result.rows] == ["succeeded"]


def test_an_empty_result_still_names_its_columns(reader) -> None:
    result = reader.sql("SELECT run_id, workflow FROM runs WHERE run_id = 'absent'")

    assert result.rows == []
    assert result.columns == ("run_id", "workflow")


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO runs (run_id, workflow, status, started_at) VALUES ('x', 'y', 'z', 'now')",
        "UPDATE runs SET status = 'failed'",
        "DELETE FROM runs",
        "DROP TABLE runs",
        "PRAGMA table_list",
        "VACUUM",
    ],
)
def test_a_statement_that_is_not_a_read_is_refused(reader, statement: str) -> None:
    with pytest.raises(StoreError, match="is not a read"):
        reader.sql(statement)


def test_a_second_statement_is_refused(reader) -> None:
    with pytest.raises(StoreError, match="more than one statement"):
        reader.sql("SELECT 1; DELETE FROM runs")


def test_a_trailing_semicolon_is_not_a_second_statement(reader) -> None:
    assert reader.sql("SELECT 1 AS n;").rows[0]["n"] == 1


def test_an_unknown_column_is_reported_readably(reader) -> None:
    with pytest.raises(StoreError, match="no such column: nope"):
        reader.sql("SELECT nope FROM runs")


def test_empty_sql_is_refused(reader) -> None:
    with pytest.raises(StoreError, match="No SQL to run"):
        reader.sql("   ")


def test_the_row_limit_is_honoured(reader) -> None:
    many = "SELECT seq FROM events ORDER BY seq"
    assert len(reader.sql(many).rows) == 2

    limited = reader.sql(many, limit=1)

    assert len(limited.rows) == 1
    assert limited.truncated is True


def test_a_result_inside_the_limit_is_not_truncated(reader) -> None:
    result = reader.sql("SELECT run_id FROM runs", limit=2)

    assert len(result.rows) == 1
    assert result.truncated is False


def test_a_read_connection_refuses_writes_in_the_engine(reader) -> None:
    """The keyword check is not the only guard: ``query_only`` is the other."""
    with pytest.raises(Exception, match="readonly database"):
        reader.store.query("DELETE FROM runs")


def test_an_empty_workspace_answers_rather_than_failing(tmp_path: Path) -> None:
    layout = WorkspaceLayout(root=tmp_path / "empty" / ".ginkgo")

    with query.open(layout, missing_ok=True) as reader:
        result = reader.sql("SELECT run_id FROM runs")

    assert result.rows == []
    assert result.columns == ("run_id",)
    assert not Path(layout.db).exists()
