"""Tests for ``Query.why`` and the task-to-artifact provenance it reads."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, file, query, task
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

from tests.conftest import Ledger


@task()
def _write_report(label: str, output_path: Out[file]) -> file:
    Path(output_path).write_text(f"report {label}\n", encoding="utf-8")
    return file(output_path)


@task()
def _summarise(report: file, output_path: Out[file]) -> file:
    Path(output_path).write_text(Path(report).read_text(encoding="utf-8").upper())
    return file(output_path)


@pytest.fixture
def chained_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run report -> summary through the evaluator and return the database."""
    monkeypatch.chdir(tmp_path)
    ledger = Ledger.start(root=tmp_path)
    evaluator = ConcurrentEvaluator(run_dir=ledger.run_dir, event_bus=ledger.bus, jobs=1, cores=1)
    report = _write_report(label="a", output_path="results/report.txt")
    evaluator.evaluate(_summarise(report=report, output_path="results/summary.txt"))
    ledger.finish()
    return ledger.db


def _reader(db: Path) -> query.Query:
    return query.open(WorkspaceLayout(root=db.parent), readonly=True)


class TestOutputProvenance:
    def test_a_file_output_row_names_its_artifact(self, chained_run: Path) -> None:
        with open_store(chained_run, readonly=True) as store:
            rows = store.query("SELECT path, artifact_id FROM task_outputs ORDER BY path")

        assert [Path(row["path"]).name for row in rows] == ["report.txt", "summary.txt"]
        assert all(row["artifact_id"] for row in rows)

    def test_each_producer_has_a_produced_edge_to_its_artifact(self, chained_run: Path) -> None:
        with open_store(chained_run, readonly=True) as store:
            edges = store.query(
                "SELECT src_kind, dst_kind FROM edges WHERE edge = 'produced' ORDER BY src_id"
            )

        assert [(row["src_kind"], row["dst_kind"]) for row in edges] == [
            ("task", "artifact"),
            ("task", "artifact"),
        ]


class TestWhy:
    def test_a_materialized_path_names_its_producer(
        self, chained_run: Path, tmp_path: Path
    ) -> None:
        with _reader(chained_run) as reader:
            provenance = reader.why(str(tmp_path / "results" / "summary.txt"))

        assert provenance.path == str((tmp_path / "results" / "summary.txt").resolve())
        assert provenance.task_name is not None
        assert provenance.task_name.endswith("_summarise")
        assert provenance.cache_key is not None
        assert [entry["param"] for entry in provenance.inputs] == ["output_path", "report"]

    def test_an_artifact_id_resolves_like_its_path(
        self, chained_run: Path, tmp_path: Path
    ) -> None:
        with _reader(chained_run) as reader:
            by_path = reader.why(str(tmp_path / "results" / "report.txt"))
            by_id = reader.why(by_path.artifact_id)

        assert by_id.task_id == by_path.task_id
        assert by_id.path is None

    def test_an_unknown_target_is_not_found(self, chained_run: Path) -> None:
        with _reader(chained_run) as reader, pytest.raises(FileNotFoundError, match="Nothing"):
            reader.why("not-an-artifact")
