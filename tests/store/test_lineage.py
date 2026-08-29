"""Lineage across runs: what an asset was built from, and what came of it."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ginkgo import query, table, task
from ginkgo.core.asset import AssetKey, AssetRef, AssetVersion, make_asset_version
from ginkgo.query import Query
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

from tests.conftest import Ledger


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "ginkgo.db"


def _ref(version: AssetVersion) -> AssetRef:
    return AssetRef(
        key=version.key,
        version_id=version.version_id,
        kind=version.kind,
        artifact_id=version.artifact_id,
        content_hash=version.content_hash,
        artifact_path=f"/artifacts/{version.artifact_id}",
        metadata=dict(version.metadata),
    )


def _register(
    catalog: AssetStore,
    *,
    namespace: str,
    name: str,
    run_id: str,
    task_id: str,
    parents: list[AssetRef] | None = None,
) -> AssetVersion:
    """Register one version the way the registrar does, and return it."""
    version = make_asset_version(
        key=AssetKey(namespace=namespace, name=name),
        kind=namespace,
        artifact_id=f"artifact-{namespace}-{name}",
        content_hash=f"hash-{namespace}-{name}",
        run_id=run_id,
        producer_task=f"pkg.workflow.build_{name}",
    )
    catalog.register_version(
        version=version,
        parents=parents or [],
        code_version=f"source-{name}",
        task_id=task_id,
    )
    return version


def _chain(db_path: Path) -> tuple[AssetVersion, AssetVersion, AssetVersion]:
    """Build a three-stage chain across two runs.

    Run 1 produces ``table:a``. Run 2 consumes it to produce ``table:b`` and
    then ``fig:c`` — so tracing ``fig:c`` upstream has to leave run 2 and
    arrive at a version another run wrote.
    """
    with CacheIndex.open(path=db_path) as index:
        catalog = AssetStore.attached_to(index)
        a = _register(catalog, namespace="table", name="a", run_id="run-1", task_id="task_0001")
        b = _register(
            catalog,
            namespace="table",
            name="b",
            run_id="run-2",
            task_id="task_0001",
            parents=[_ref(a)],
        )
        c = _register(
            catalog,
            namespace="fig",
            name="c",
            run_id="run-2",
            task_id="task_0002",
            parents=[_ref(b)],
        )
    return a, b, c


def _reader(db_path: Path) -> Query:
    return Query(open_store(db_path, readonly=True), layout=WorkspaceLayout.relative())


class TestLineageAcrossRuns:
    def test_upstream_reaches_the_run_that_produced_the_root(self, db_path: Path) -> None:
        a, b, c = _chain(db_path)

        with _reader(db_path) as reader:
            graph = reader.lineage("fig:c")

        assert graph.direction == "upstream"
        assert graph.root.version_id == c.version_id
        assert set(graph.versions) == {a.version_id, b.version_id, c.version_id}
        assert graph.edges == tuple(
            sorted([(a.version_id, b.version_id), (b.version_id, c.version_id)])
        )
        assert graph.versions[a.version_id].run_id == "run-1"

    def test_downstream_walks_the_other_way(self, db_path: Path) -> None:
        a, b, c = _chain(db_path)

        with _reader(db_path) as reader:
            graph = reader.lineage("table:a", direction="downstream")

        assert set(graph.versions) == {a.version_id, b.version_id, c.version_id}
        assert graph.neighbours(a.version_id) == [b.version_id]

    def test_depth_stops_the_walk(self, db_path: Path) -> None:
        a, b, c = _chain(db_path)

        with _reader(db_path) as reader:
            graph = reader.lineage("fig:c", depth=1)

        assert set(graph.versions) == {b.version_id, c.version_id}
        assert a.version_id not in graph.versions

    def test_a_version_selector_pins_the_root(self, db_path: Path) -> None:
        _, b, _ = _chain(db_path)

        with _reader(db_path) as reader:
            graph = reader.lineage("table:b", b.version_id)

        assert graph.root.version_id == b.version_id

    def test_an_unknown_asset_is_not_found(self, db_path: Path) -> None:
        _chain(db_path)

        with _reader(db_path) as reader, pytest.raises(FileNotFoundError):
            reader.lineage("table:absent")

    def test_direction_must_be_upstream_or_downstream(self, db_path: Path) -> None:
        _chain(db_path)

        with _reader(db_path) as reader, pytest.raises(ValueError, match="direction"):
            reader.lineage("fig:c", direction="sideways")

    def test_payload_is_json_ready(self, db_path: Path) -> None:
        _, _, c = _chain(db_path)

        with _reader(db_path) as reader:
            payload = reader.lineage("fig:c").to_payload()

        assert payload["root"]["version_id"] == c.version_id
        assert payload["direction"] == "upstream"
        assert len(payload["edges"]) == 2
        assert all({"parent", "child"} == set(edge) for edge in payload["edges"])


class TestWhy:
    def test_an_asset_artifact_names_its_producer(self, db_path: Path) -> None:
        _, b, c = _chain(db_path)

        with open_store(db_path, readonly=False) as store, store.transaction():
            store.apply(
                [
                    ProjectionOp(
                        sql="INSERT INTO tasks (run_id, task_id, node_id, name, kind, "
                        "execution_mode, status, display_label) VALUES "
                        "('run-2', 'task_0002', 2, 'build_c', 'python', 'local', "
                        "'succeeded', 'build_c')",
                    ),
                    ProjectionOp(
                        sql="INSERT INTO task_inputs (run_id, task_id, param, position, "
                        "value_type, asset_key, asset_version_id) VALUES "
                        "('run-2', 'task_0002', 'frame', 0, 'AssetRef', 'table:b', ?)",
                        params=(b.version_id,),
                    ),
                    ProjectionOp(
                        sql="INSERT INTO artifacts (artifact_id, kind, digest_algorithm, "
                        "digest_hex, created_at) VALUES (?, 'file', 'blake3', 'x', 'now')",
                        params=(c.artifact_id,),
                    ),
                ]
            )

        with _reader(db_path) as reader:
            provenance = reader.why(c.artifact_id)

        assert provenance.artifact_id == c.artifact_id
        assert provenance.run_id == "run-2"
        assert provenance.task_id == "task_0002"
        assert provenance.task_name == "build_c"
        assert provenance.asset_key == "fig:c"
        assert [entry["param"] for entry in provenance.inputs] == ["frame"]
        assert provenance.to_payload()["artifact_id"] == c.artifact_id

    def test_a_materialized_path_resolves_to_its_artifact(self, db_path: Path) -> None:
        _, _, c = _chain(db_path)
        materialized = db_path.parent / "outputs" / "figure.png"
        materialized.parent.mkdir(parents=True)
        materialized.write_bytes(b"png")

        with CacheIndex.open(path=db_path) as index:
            index.record_materialization(path=materialized, artifact_id=c.artifact_id)

        with _reader(db_path) as reader:
            provenance = reader.why(str(materialized))

        assert provenance.artifact_id == c.artifact_id
        assert provenance.path == str(materialized.resolve())

    def test_an_unknown_target_is_not_found(self, db_path: Path) -> None:
        _chain(db_path)

        with _reader(db_path) as reader, pytest.raises(FileNotFoundError, match="Nothing"):
            reader.why("not-an-artifact")


# ---------------------------------------------------------------------------
# Consumption through a semantically typed parameter (#253)
# ---------------------------------------------------------------------------


@task()
def _produce_table_a() -> object:
    return table(pd.DataFrame({"a": [1, 2]}), name="a")


@task()
def _consume_dataframe(upstream: pd.DataFrame) -> object:
    """Consume an asset as its payload, the way a user normally would."""
    assert isinstance(upstream, pd.DataFrame)
    return table(pd.DataFrame({"b": [len(upstream)]}), name="b")


class TestPlainValueConsumption:
    """An asset consumed as its payload is still an asset the ledger can see.

    The evaluator rehydrates the ref into a DataFrame before the cache key is
    built, so neither the hash entry nor the rendered argument remembers where
    the value came from. The identity has to be captured at resolution time.
    """

    @pytest.fixture
    def consumed_run(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        monkeypatch.chdir(tmp_path)
        ledger = Ledger.start(root=tmp_path)
        evaluator = ConcurrentEvaluator(
            run_dir=ledger.run_dir, event_bus=ledger.bus, jobs=1, cores=1
        )
        evaluator.evaluate(_consume_dataframe(upstream=_produce_table_a()))
        ledger.finish()
        return ledger.db

    def test_the_input_row_carries_the_asset_identity(self, consumed_run: Path) -> None:
        with open_store(consumed_run, readonly=True) as store:
            rows = store.query(
                "SELECT asset_key, asset_version_id, artifact_id FROM task_inputs "
                "WHERE param = 'upstream'"
            )

        assert [row["asset_key"] for row in rows] == ["table:a"]
        assert all(row["asset_version_id"] and row["artifact_id"] for row in rows)

    def test_a_consumed_edge_is_written(self, consumed_run: Path) -> None:
        with open_store(consumed_run, readonly=True) as store:
            edges = store.query("SELECT src_kind, dst_kind FROM edges WHERE edge = 'consumed'")

        assert [(row["src_kind"], row["dst_kind"]) for row in edges] == [("asset_version", "task")]

    def test_why_names_the_asset_the_consumer_read(self, consumed_run: Path) -> None:
        with query.open(WorkspaceLayout(root=consumed_run.parent)) as reader:
            produced = reader.store.query(
                "SELECT artifact_id FROM task_outputs WHERE asset_key = 'table:b'"
            )
            provenance = reader.why(str(produced[0]["artifact_id"]))

        assert "table:a" in str(provenance.inputs)

    def test_lineage_downstream_reaches_the_consumer(self, consumed_run: Path) -> None:
        with query.open(WorkspaceLayout(root=consumed_run.parent)) as reader:
            graph = reader.lineage("table:a", direction="downstream")

        assert "table:b" in {str(version.key) for version in graph.versions.values()}
