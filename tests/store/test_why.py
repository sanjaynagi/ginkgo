"""Tests for ``Query.why`` and the task-to-artifact provenance it reads."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from rich.console import Console

from ginkgo import Out, file, query, task
from ginkgo.cli.commands.why import _render_why
from ginkgo.query import Provenance
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


@task()
def _write_marker(name: str, output_path: Out[file]) -> file:
    Path(output_path).write_text("ok\n", encoding="utf-8")
    return file(output_path)


@task()
def _both_markers(first: file, second: file) -> list[str]:
    return [str(first), str(second)]


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

    def test_identical_bytes_at_two_paths_name_their_own_producers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ledger = Ledger.start(root=tmp_path)
        evaluator = ConcurrentEvaluator(
            run_dir=ledger.run_dir, event_bus=ledger.bus, jobs=1, cores=1
        )
        evaluator.evaluate(
            _both_markers(
                first=_write_marker(name="a", output_path="out/a.ok"),
                second=_write_marker(name="b", output_path="out/b.ok"),
            )
        )
        ledger.finish()

        with _reader(ledger.db) as reader:
            first = reader.why(str(tmp_path / "out" / "a.ok"))
            second = reader.why(str(tmp_path / "out" / "b.ok"))

        assert first.artifact_id == second.artifact_id
        assert first.task_id != second.task_id
        assert [entry["value_summary"] for entry in first.inputs if entry["param"] == "name"] == [
            '"a"'
        ]

    def test_an_unknown_target_is_not_found(self, chained_run: Path) -> None:
        with _reader(chained_run) as reader, pytest.raises(FileNotFoundError, match="Nothing"):
            reader.why("not-an-artifact")

    def test_an_untracked_path_is_told_how_to_track_it(self, chained_run: Path) -> None:
        """A path returned as a plain ``str`` leaves no record; say what would."""
        with (
            _reader(chained_run) as reader,
            pytest.raises(FileNotFoundError, match=r"plain `str` is not tracked.*-> file"),
        ):
            reader.why("results/returned_as_str.txt")

    def test_a_path_from_before_a_ledger_reset_names_the_reset(self, chained_run: Path) -> None:
        """EV-4: a lost ledger, not the task's annotation, is why nothing is found."""
        for path in chained_run.parent.glob("ginkgo.db*"):
            path.unlink()
        open_store(chained_run).close()

        with (
            _reader(chained_run) as reader,
            pytest.raises(FileNotFoundError, match="cannot be traced") as excinfo,
        ):
            reader.why("results/summary.txt")
        assert "plain `str`" not in str(excinfo.value)


class TestWhyRendering:
    @staticmethod
    def _render(provenance: Provenance) -> str:
        buffer = io.StringIO()
        _render_why(
            Console(file=buffer, width=400, force_terminal=False),
            provenance=provenance,
            as_json=False,
        )
        return buffer.getvalue()

    def test_a_recorded_run_shows_values_and_file_digests(
        self, chained_run: Path, tmp_path: Path
    ) -> None:
        with _reader(chained_run) as reader:
            summary = reader.why(str(tmp_path / "results" / "summary.txt"))
            report = reader.why(str(tmp_path / "results" / "report.txt"))
        file_digest = next(e["digest"] for e in summary.inputs if e["param"] == "report")

        summary_output = self._render(summary)
        report_output = self._render(report)

        # A file input names its path unquoted, plus the digest its key used.
        assert f"report = results/report.txt ({file_digest[:12]})" in summary_output
        assert "output_path = results/summary.txt\n" in summary_output
        # A string parameter shows its value, not its JSON encoding or digest.
        assert "label = a\n" in report_output

    def test_a_non_string_value_is_shown_as_json_and_not_read_as_markup(self) -> None:
        output = self._render(
            Provenance(
                artifact_id="artifact-1",
                inputs=(
                    {"param": "flags", "value_summary": "[true]", "tracking": "value"},
                    {"param": "scratch", "digest": "untracked-digest"},
                ),
            )
        )

        assert "flags = [true]" in output
        assert "scratch = (untracked-di)" in output
