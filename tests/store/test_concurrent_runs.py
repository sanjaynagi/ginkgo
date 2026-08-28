"""Two ``ginkgo run`` processes must be able to share one workspace.

Nothing in ginkgo stops a user starting a second run while the first is going,
so the ledger has to cope: WAL plus one writer thread per process plus a busy
timeout is the whole of the answer, and this is what checks it holds.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

_WORKFLOW = """
from pathlib import Path

from ginkgo import flow, task


@task()
def step(marker: str, index: int) -> str:
    Path(f"{{marker}}-{{index}}.txt").write_text(str(index), encoding="utf-8")
    return marker


@flow
def main():
    return [step(marker={marker!r}, index=index) for index in range(4)]
"""


def _write_workflow(path: Path, *, marker: str) -> None:
    path.write_text(textwrap.dedent(_WORKFLOW).format(marker=marker), encoding="utf-8")


@pytest.mark.integration
def test_two_simultaneous_runs_in_one_workspace_both_record(tmp_path: Path) -> None:
    for marker in ("alpha", "beta"):
        _write_workflow(tmp_path / f"{marker}.py", marker=marker)

    def _run(marker: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "ginkgo.cli", "run", str(tmp_path / f"{marker}.py")],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(_run, ("alpha", "beta")))

    for result in results:
        assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"

    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    with open_store(layout.db, readonly=True) as store:
        runs = store.query("SELECT run_id, status, workflow FROM runs")
        seqs = [row["seq"] for row in store.query("SELECT seq FROM events ORDER BY seq")]
        integrity = [row[0] for row in store.query("PRAGMA integrity_check")]

    assert len(runs) == 2
    assert {row["status"] for row in runs} == {"succeeded"}
    assert {Path(row["workflow"]).stem for row in runs} == {"alpha", "beta"}
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)
    assert integrity == ["ok"]


@pytest.mark.integration
def test_two_runs_saving_the_same_cache_keys_agree(tmp_path: Path) -> None:
    """The same workflow twice at once: identical keys, one row each.

    Cache entries are content-addressed, so two runs computing the same task
    write the same key. ``INSERT OR IGNORE`` is the contract that makes the
    second write a no-op rather than a conflict, and both runs then serve from
    an entry either of them could have written.
    """
    for name in ("first", "second"):
        _write_workflow(tmp_path / f"{name}.py", marker="shared")

    def _run(name: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "ginkgo.cli", "run", str(tmp_path / f"{name}.py")],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(_run, ("first", "second")))

    for result in results:
        assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"

    layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
    with open_store(layout.db, readonly=True) as store:
        entries = store.query("SELECT cache_key, function FROM cache_entries")
        task_keys = store.query("SELECT DISTINCT cache_key FROM tasks WHERE cache_key IS NOT NULL")
        integrity = [row[0] for row in store.query("PRAGMA integrity_check")]

    assert {row["cache_key"] for row in entries} == {row["cache_key"] for row in task_keys}
    assert len(entries) == len({row["cache_key"] for row in entries})
    for row in entries:
        assert (layout.cache / row["cache_key"] / "output.json").is_file()
    assert integrity == ["ok"]
