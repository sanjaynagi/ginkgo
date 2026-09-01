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
from ginkgo.core.asset import AssetKey, AssetRef, asset_ref_from_version, make_asset_version
from ginkgo.query import Query
from ginkgo.runtime.artifacts.asset_registration import AssetRegistrar
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.value_codec import encoded_asset_refs
from ginkgo.runtime.caching.cache import CacheStore
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

    def test_a_repair_that_fails_does_not_fail_the_cache_hit(
        self,
        wiped_workspace: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A repair is best-effort: failing it must degrade to the old warning."""

        def _explode(self, **_):
            raise RuntimeError("catalog is unwritable")

        monkeypatch.setattr(AssetStore, "reassert_version", _explode)

        with caplog.at_level(logging.WARNING):
            warm = _warm(wiped_workspace)

        assert any(row.get("cached") for row in warm.tasks().values()), (
            "the producer's cache hit did not complete"
        )
        assert "Could not re-assert the catalog rows" in caplog.text
        # And the workspace is left in exactly the state the repair exists to
        # fix, which the consumer still warns about.
        assert "catalog has no row" in caplog.text


def _replayed_ref(*, minted_in: str) -> AssetRef:
    """Return the ref a cache entry replays for a version minted in one run."""
    return asset_ref_from_version(
        version=make_asset_version(
            key=AssetKey(namespace="table", name="a"),
            kind="table",
            artifact_id="artifact-a",
            content_hash="hash-a",
            run_id=minted_in,
            producer_task="pkg.workflow.produce",
        ),
        artifact_path="/artifacts/artifact-a",
    )


class TestWhatARepairedRowMayClaim:
    """The entry describes whoever wrote it, which is not always the producer."""

    @pytest.fixture
    def registrar(self, tmp_path: Path) -> AssetRegistrar:
        """A registrar over a workspace holding one cache entry, written by ``run-mint``."""
        layout = WorkspaceLayout(root=tmp_path / ".ginkgo")
        index = CacheIndex.open(path=layout.db)
        index.record_entry(
            cache_key="key-1",
            meta={"function": "pkg.workflow.produce", "created_at": "2026-08-28T10:00:00+00:00"},
            artifact_ids={},
            size_bytes=1,
            run_id="run-mint",
        )
        return AssetRegistrar(
            cache_store=CacheStore(index=index, root=layout.cache),
            asset_store=AssetStore.attached_to(index),
            run_id_provider=lambda: "run-warm",
        )

    @pytest.fixture
    def db(self, tmp_path: Path) -> Path:
        return WorkspaceLayout(root=tmp_path / ".ginkgo").db

    def _claims(self, db: Path, version_id: str) -> dict:
        """Return what the row itself says, where a null is still a null.

        Read back through ``version_by_id`` an unattributed row is
        indistinguishable from one attributed to the empty string, and the
        distinction is the whole point of withholding.
        """
        with open_store(db, readonly=True) as store:
            rows = store.query(
                "SELECT run_id, producer_task FROM asset_versions WHERE version_id = ?",
                (version_id,),
            )
        assert rows, "the replayed version was not re-asserted"
        return dict(rows[0])

    def test_a_provable_producer_is_recorded(self, registrar: AssetRegistrar, db: Path) -> None:
        """The entry's run minted this version, so the entry can be believed."""
        ref = _replayed_ref(minted_in="run-mint")

        assert registrar.reassert_cached_versions(value=ref, cache_key="key-1") == [ref]
        assert self._claims(db, ref.version_id) == {
            "run_id": "run-mint",
            "producer_task": "pkg.workflow.produce",
        }

    def test_an_unprovable_producer_is_withheld(self, registrar: AssetRegistrar, db: Path) -> None:
        """A version some other run minted: the entry only replayed it.

        This is the pass-through shape — a task handing an upstream ref back
        out — where believing the entry would name the wrong producer.
        """
        ref = _replayed_ref(minted_in="run-elsewhere")

        assert registrar.reassert_cached_versions(value=ref, cache_key="key-1") == [ref]
        assert self._claims(db, ref.version_id) == {"run_id": None, "producer_task": None}

    def test_a_ref_nested_in_the_replayed_value_is_repaired(
        self, registrar: AssetRegistrar
    ) -> None:
        """Cached results are rarely a bare ref; the walk has to reach inside."""
        ref = _replayed_ref(minted_in="run-mint")

        written = registrar.reassert_cached_versions(
            value={"tables": [ref], "count": 1}, cache_key="key-1"
        )

        assert written == [ref]
        assert registrar.asset_store.version_by_id(ref.version_id) is not None

    def test_a_missing_entry_leaves_the_row_unattributed(
        self, registrar: AssetRegistrar, db: Path
    ) -> None:
        """A cache directory the index never knew still gets its bytes protected."""
        ref = _replayed_ref(minted_in="run-mint")

        assert registrar.reassert_cached_versions(value=ref, cache_key="key-absent") == [ref]
        assert self._claims(db, ref.version_id) == {"run_id": None, "producer_task": None}


class TestEncodedAssetRefs:
    """What the integrity check can see without decoding an entry."""

    def test_it_finds_refs_nested_in_containers(self) -> None:
        from ginkgo.runtime.artifacts.value_codec import encode_value

        ref = _replayed_ref(minted_in="run-mint")
        payload = encode_value({"tables": [ref, None], "n": 2}, base_dir=Path("/tmp"))

        assert [found.version_id for found in encoded_asset_refs(payload)] == [ref.version_id]

    def test_it_finds_nothing_in_a_value_that_names_no_asset(self) -> None:
        assert encoded_asset_refs({"__ginkgo_type__": "file", "value": "/out/a.txt"}) == []


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
