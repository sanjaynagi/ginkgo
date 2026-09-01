"""A cache hit re-establishes the catalog rows it replays (issue #263).

Catalog rows are written where a task executes, but the asset identities a
consumer sees are replayed from the producer's cache entry on disk. The two
have independent lifetimes: a catalog rebuilt or restored behind an intact
cache leaves every replayed ref naming a version with no row. Lineage through
those versions is then dropped, and the artifact collector — which protects
``cache_artifacts ∪ asset_versions`` — stops seeing their bytes as live.

The producer repairs that on its own cache hit, so the invariant the
registrar's warning asserts holds again by the time a consumer resolves.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from ginkgo import table, task
from ginkgo.query import Query
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.rundir import make_run_id
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

from tests.conftest import Ledger


@task()
def _produce_table() -> object:
    return table(pd.DataFrame({"a": [1, 2, 3]}), name="a")


@task()
def _summarise(upstream: pd.DataFrame) -> object:
    return table(pd.DataFrame({"rows": [len(upstream)]}), name="summary")


@task()
def _summarise_edited(upstream: pd.DataFrame) -> object:
    """The consumer after an edit: a different body, so a different cache key."""
    return table(pd.DataFrame({"row_count": [len(upstream)]}), name="summary")


def _run(root: Path, expr: object, *, suffix: str) -> Ledger:
    """Evaluate one expression as its own run against the workspace at *root*."""
    ledger = Ledger.start(
        root=root, run_id=make_run_id(workflow_path=root / "workflow.py") + suffix
    )
    ConcurrentEvaluator(run_dir=ledger.run_dir, event_bus=ledger.bus, jobs=1, cores=1).evaluate(
        expr
    )
    ledger.finish()
    return ledger


def _versions(db: Path) -> list[dict]:
    with open_store(db, readonly=True) as store:
        return [dict(row) for row in store.query("SELECT * FROM asset_versions")]


def _forget_the_catalog(db: Path) -> None:
    """Drop every catalog row, leaving the cache on disk untouched.

    What the phase-3 migration left behind: a cache full of refs whose version
    ids the database has never seen, and which re-execution cannot re-mint
    because a version id hashes over the run that made it.
    """
    with open_store(db) as store, store.transaction():
        store.apply([ProjectionOp(sql="DELETE FROM asset_versions")])


@pytest.fixture
def wiped_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A workspace one run old whose catalog rows have been deleted."""
    monkeypatch.chdir(tmp_path)
    _run(tmp_path, _summarise(upstream=_produce_table()), suffix="-cold")
    _forget_the_catalog(tmp_path / ".ginkgo" / "ginkgo.db")
    return tmp_path


def _warm(root: Path) -> Ledger:
    """Re-run with the consumer edited, so it executes while its producer hits."""
    return _run(root, _summarise_edited(upstream=_produce_table()), suffix="-warm")


class TestCatalogLostBehindAnIntactCache:
    """The reproduction: only the ``asset_versions`` rows are gone."""

    def test_the_replayed_version_gets_its_row_back(self, wiped_workspace: Path) -> None:
        warm = _warm(wiped_workspace)

        rows = {str(row["asset_key"]) for row in _versions(warm.db)}
        assert "table:a" in rows, "the cache-hit producer's version was not re-asserted"

    def test_the_consumer_does_not_warn_about_a_missing_row(
        self, wiped_workspace: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            _warm(wiped_workspace)

        assert "catalog has no row" not in caplog.text

    def test_lineage_reaches_the_replayed_parent(self, wiped_workspace: Path) -> None:
        warm = _warm(wiped_workspace)

        with Query(
            open_store(warm.db, readonly=True), layout=WorkspaceLayout.relative()
        ) as reader:
            graph = reader.lineage("table:summary")

        assert "table:a" in {str(version.key) for version in graph.versions.values()}

    def test_a_re_asserted_row_names_the_run_and_task_that_made_it(
        self, wiped_workspace: Path
    ) -> None:
        """A partial row, but not an anonymous one: the entry says who wrote it."""
        warm = _warm(wiped_workspace)

        row = next(row for row in _versions(warm.db) if row["asset_key"] == "table:a")
        assert row["producer_task"].endswith("_produce_table")
        assert str(row["run_id"]).endswith("-cold")
        assert row["cache_key"]
        # What only the producing execution knew is left null rather than guessed.
        assert row["code_version"] is None
        assert row["data_version"] is None

    def test_the_artifact_is_protected_again(self, wiped_workspace: Path) -> None:
        """The GC's reference set is the union of the two halves (issue #263)."""
        warm = _warm(wiped_workspace)

        row = next(row for row in _versions(warm.db) if row["asset_key"] == "table:a")
        with CacheIndex.open(path=warm.db, readonly=True) as index:
            assert str(row["artifact_id"]) in index.referenced_artifact_ids()


class TestAHealthyWarmRun:
    """Where the catalog is intact, a hit must not trade a row down."""

    def test_the_original_row_survives_untouched(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        monkeypatch.chdir(tmp_path)
        cold = _run(tmp_path, _summarise(upstream=_produce_table()), suffix="-cold")
        before = next(row for row in _versions(cold.db) if row["asset_key"] == "table:a")

        with caplog.at_level(logging.WARNING):
            warm = _run(tmp_path, _summarise_edited(upstream=_produce_table()), suffix="-warm")
        after = next(row for row in _versions(warm.db) if row["asset_key"] == "table:a")

        assert after == before
        assert "catalog has no row" not in caplog.text
