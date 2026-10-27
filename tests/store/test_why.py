"""``Query.why`` and the task-to-artifact provenance it reads."""

from __future__ import annotations

import io
import os
import shutil
from pathlib import Path

import pytest
from rich.console import Console

from ginkgo import Out, file, folder, query, task
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
def _both(first: file, second: file) -> list[str]:
    return [str(first), str(second)]


@task()
def _write_folder(name: str, output_dir: Out[folder]) -> folder:
    Path(output_dir).mkdir(parents=True)
    (Path(output_dir) / "hit.txt").write_text(name, encoding="utf-8")
    return folder(output_dir)


@task()
def _fan_out(names: list[str], attempt: int = 0) -> list[folder]:
    return [_write_folder(name=name, output_dir=f"results/{name}") for name in names]


def _record(root: Path, expr: object) -> Path:
    """Run ``expr`` in ``root`` through a ledger, as ``ginkgo run`` would; return the db."""
    previous = Path.cwd()
    os.chdir(root)
    try:
        ledger = Ledger.start(root=root)
        ConcurrentEvaluator(
            run_dir=ledger.run_dir, event_bus=ledger.bus, jobs=1, cores=1
        ).evaluate(expr)
        ledger.finish()
        ledger.close()
    finally:
        os.chdir(previous)
    return ledger.db


@pytest.fixture(scope="module")
def recorded(tmp_path_factory) -> Path:
    """One recorded run of report -> summary, plus two tasks writing identical bytes."""
    root = tmp_path_factory.mktemp("why")
    report = _write_report(label="a", output_path="results/report.txt")
    _record(
        root,
        (
            _summarise(report=report, output_path="results/summary.txt"),
            _both(
                first=_write_marker(name="a", output_path="out/a.ok"),
                second=_write_marker(name="b", output_path="out/b.ok"),
            ),
            _fan_out(names=["tetracycline"]),
        ),
    )
    return root


@pytest.fixture
def workspace(recorded: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The recorded workspace, as the working directory ``ginkgo why`` runs in."""
    monkeypatch.chdir(recorded)
    return recorded


def _reader(root: Path) -> query.Query:
    return query.open(WorkspaceLayout(root=root / ".ginkgo"), readonly=True)


def test_each_file_output_is_a_row_with_a_produced_edge_to_its_artifact(workspace: Path) -> None:
    with open_store(workspace / ".ginkgo" / "ginkgo.db", readonly=True) as store:
        rows = store.query("SELECT path, artifact_id FROM task_outputs WHERE path LIKE '%.txt'")
        edges = store.query("SELECT src_kind, dst_kind FROM edges WHERE edge = 'produced'")

    assert sorted(Path(row["path"]).name for row in rows) == ["report.txt", "summary.txt"]
    assert all(row["artifact_id"] for row in rows)
    assert {(row["src_kind"], row["dst_kind"]) for row in edges} == {("task", "artifact")}


def test_a_path_or_its_artifact_id_names_its_producer(workspace: Path) -> None:
    summary = workspace / "results" / "summary.txt"
    with _reader(workspace) as reader:
        provenance = reader.why(str(summary))
        by_id = reader.why(provenance.artifact_id)

    assert provenance.path == str(summary.resolve())
    assert str(provenance.task_name).endswith("_summarise")
    assert provenance.cache_key is not None
    assert [entry["param"] for entry in provenance.inputs] == ["output_path", "report"]
    assert by_id.task_id == provenance.task_id
    assert by_id.path is None


def test_identical_bytes_at_two_paths_name_their_own_producers(workspace: Path) -> None:
    with _reader(workspace) as reader:
        first = reader.why(str(workspace / "out" / "a.ok"))
        second = reader.why(str(workspace / "out" / "b.ok"))

    assert first.artifact_id == second.artifact_id
    assert first.task_id != second.task_id
    assert [entry["value_summary"] for entry in first.inputs if entry["param"] == "name"] == [
        '"a"'
    ]


def test_a_path_names_its_own_producer_from_outside_the_workspace(
    recorded: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stored output paths are workspace-relative, not relative to the caller."""
    (tmp_path / "elsewhere").mkdir()
    monkeypatch.chdir(tmp_path / "elsewhere")
    with _reader(recorded) as reader:
        second = reader.why(str(recorded / "out" / "b.ok"))

    assert [entry["value_summary"] for entry in second.inputs if entry["param"] == "name"] == [
        '"b"'
    ]


def test_an_output_of_a_dynamically_returned_task_names_that_task(workspace: Path) -> None:
    """The parent's result carries the child's folder, but the child wrote it."""
    with _reader(workspace) as reader:
        provenance = reader.why(str(workspace / "results" / "tetracycline"))

    assert str(provenance.task_name).endswith("_write_folder")
    assert [entry["param"] for entry in provenance.inputs] == ["name", "output_dir"]


def test_a_rerun_parent_does_not_claim_its_cached_childs_output(tmp_path: Path) -> None:
    _record(tmp_path, _fan_out(names=["tetracycline"]))
    _record(tmp_path, _fan_out(names=["tetracycline"], attempt=1))

    with _reader(tmp_path) as reader:
        provenance = reader.why(str(tmp_path / "results" / "tetracycline"))

    assert str(provenance.task_name).endswith("_write_folder")


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("not-an-artifact", "Nothing"),
        # A path returned as a plain ``str`` leaves no record; say what would.
        ("results/returned_as_str.txt", r"plain `str` is not tracked.*-> file"),
    ],
)
def test_an_unrecorded_target_is_not_found(workspace: Path, target: str, message: str) -> None:
    with _reader(workspace) as reader, pytest.raises(FileNotFoundError, match=message):
        reader.why(target)


def test_a_path_from_before_a_ledger_reset_names_the_reset(recorded: Path, tmp_path) -> None:
    """EV-4: a lost ledger, not the task's annotation, is why nothing is found."""
    shutil.copytree(recorded / "results", tmp_path / "results")
    shutil.copytree(recorded / ".ginkgo" / "runs", tmp_path / ".ginkgo" / "runs")
    open_store(tmp_path / ".ginkgo" / "ginkgo.db").close()

    with (
        _reader(tmp_path) as reader,
        pytest.raises(FileNotFoundError, match="cannot be traced") as excinfo,
    ):
        reader.why("results/summary.txt")
    assert "plain `str`" not in str(excinfo.value)


def _render(provenance: Provenance) -> str:
    buffer = io.StringIO()
    _render_why(
        Console(file=buffer, width=400, force_terminal=False), provenance=provenance, as_json=False
    )
    return buffer.getvalue()


def test_a_recorded_run_shows_values_and_file_digests(workspace: Path) -> None:
    with _reader(workspace) as reader:
        summary = reader.why(str(workspace / "results" / "summary.txt"))
        report = reader.why(str(workspace / "results" / "report.txt"))
    file_digest = next(e["digest"] for e in summary.inputs if e["param"] == "report")

    # A file input names its path unquoted, plus the digest its key used.
    assert f"report = results/report.txt ({file_digest[:12]})" in _render(summary)
    assert "output_path = results/summary.txt\n" in _render(summary)
    # A string parameter shows its value, not its JSON encoding or digest.
    assert "label = a\n" in _render(report)


def test_a_non_string_value_is_shown_as_json_and_not_read_as_markup() -> None:
    output = _render(
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
