"""Two ``ginkgo run`` processes must be able to share one workspace.

Nothing stops a user starting a second run while the first is going, so the
ledger has to cope: WAL, one writer thread per process and a busy timeout are
the whole of the answer, and this checks it holds. Separate processes are the
point, so these runs are real subprocesses.
"""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

_WORKFLOW = """
from pathlib import Path

from ginkgo import flow, task


@task()
def step(index: int) -> int:
    Path(f"step-{index}.txt").write_text(str(index), encoding="utf-8")
    return index


@flow
def main():
    return [step(index=index) for index in range(4)]
"""


@pytest.mark.integration
def test_two_simultaneous_runs_of_one_workflow_both_record_and_agree(tmp_path: Path) -> None:
    """Content-addressed keys collide by design: ``INSERT OR IGNORE`` keeps one row."""
    for name in ("first", "second"):
        (tmp_path / f"{name}.py").write_text(_WORKFLOW, encoding="utf-8")

    def run(name: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "ginkgo.cli", "run", f"{name}.py"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        for result in pool.map(run, ("first", "second")):
            assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"

    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    with open_store(layout.db, readonly=True) as store:
        runs = store.query("SELECT status, workflow FROM runs")
        seqs = [row["seq"] for row in store.query("SELECT seq FROM events ORDER BY rowid")]
        entries = [row["cache_key"] for row in store.query("SELECT cache_key FROM cache_entries")]
        task_keys = store.query("SELECT DISTINCT cache_key FROM tasks WHERE cache_key IS NOT NULL")
        integrity = [row[0] for row in store.query("PRAGMA integrity_check")]

    assert {row["status"] for row in runs} == {"succeeded"}
    assert sorted(Path(row["workflow"]).stem for row in runs) == ["first", "second"]
    assert seqs == sorted(set(seqs))
    assert len(entries) == len(set(entries)) == 4
    assert set(entries) == {row["cache_key"] for row in task_keys}
    assert all((layout.cache / key / "output.json").is_file() for key in entries)
    assert integrity == ["ok"]
