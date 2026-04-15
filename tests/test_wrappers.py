"""Unit and integration tests for the special asset wrappers."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

import ginkgo
from ginkgo import array, fig, table, task, text
from ginkgo.core.asset import AssetRef
from ginkgo.core.wrappers import (
    ArrayResult,
    FigureResult,
    TableResult,
    TextResult,
)
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.wrapper_serialization import (
    WrapperSerializationError,
    serialize_wrapper,
)


# ---------------------------------------------------------------------------
# Factory-level tests
# ---------------------------------------------------------------------------


class TestFactories:
    def test_package_exports(self) -> None:
        # Import via the top-level package to verify export wiring.
        assert ginkgo.table is table
        assert ginkgo.array is array
        assert ginkgo.fig is fig
        assert ginkgo.text is text

    def test_table_pandas_detection(self) -> None:
        wrapper = table(pd.DataFrame({"a": [1, 2]}))
        assert isinstance(wrapper, TableResult)
        assert wrapper.sub_kind == "pandas"
        assert wrapper.name is None

    def test_table_csv_path_detection(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
        wrapper = table(csv_path, name="raw")
        assert wrapper.sub_kind == "csv"
        assert wrapper.name == "raw"

    def test_table_rejects_unsupported_type(self) -> None:
        with pytest.raises(TypeError):
            table(42)

    def test_array_numpy_detection(self) -> None:
        wrapper = array(np.zeros((2, 3)), name="emb")
        assert isinstance(wrapper, ArrayResult)
        assert wrapper.sub_kind == "numpy"

    def test_fig_path_detection(self, tmp_path: Path) -> None:
        png_path = tmp_path / "plot.png"
        png_path.write_bytes(b"\x89PNG\r\n\x1a\nfake")
        wrapper = fig(png_path)
        assert isinstance(wrapper, FigureResult)
        assert wrapper.sub_kind == "png"

    def test_text_dict_becomes_json(self) -> None:
        wrapper = text({"a": 1})
        assert isinstance(wrapper, TextResult)
        assert wrapper.sub_kind == "json"
        assert wrapper.text_format == "json"
        assert '"a"' in wrapper.payload

    def test_text_string_is_inline_plain(self) -> None:
        wrapper = text("hello world")
        assert wrapper.text_format == "plain"
        # Crucially: a plain string that happens to resemble a path must
        # never be probed against the filesystem at construction time.
        wrapper_path_like = text("this/path/should-not-be-resolved")
        assert wrapper_path_like.text_format == "plain"
        assert wrapper_path_like.payload == "this/path/should-not-be-resolved"

    def test_text_path_suffix_infers_format(self, tmp_path: Path) -> None:
        md_path = tmp_path / "notes.md"
        md_path.write_text("# header", encoding="utf-8")
        wrapper = text(md_path)
        assert wrapper.text_format == "markdown"

    def test_text_explicit_format_override(self) -> None:
        wrapper = text("raw", format="markdown")
        assert wrapper.text_format == "markdown"

    def test_text_dict_rejects_non_json_format(self) -> None:
        with pytest.raises(ValueError):
            text({"a": 1}, format="markdown")


# ---------------------------------------------------------------------------
# Serialiser-level tests
# ---------------------------------------------------------------------------


class TestSerializers:
    def test_serialize_pandas_table(self) -> None:
        frame = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        result = serialize_wrapper(wrapper=table(frame, name="t"), wrapper_index=0)
        assert result.extension == "parquet"
        assert result.metadata["sub_kind"] == "pandas"
        assert result.metadata["row_count"] == 3
        assert result.metadata["byte_size"] == len(result.data)
        columns = [entry["name"] for entry in result.metadata["schema"]]
        assert columns == ["a", "b"]

        # Round-trip through parquet to confirm bytes are a real Parquet file.
        restored = pd.read_parquet(io.BytesIO(result.data))
        assert list(restored.columns) == ["a", "b"]

    def test_serialize_csv_path_table(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("x,y\n1,2\n3,4\n", encoding="utf-8")

        result = serialize_wrapper(wrapper=table(csv_path), wrapper_index=0)
        restored = pd.read_parquet(io.BytesIO(result.data))
        assert list(restored.columns) == ["x", "y"]
        assert len(restored) == 2

    def test_serialize_polars_lazy_frame(self) -> None:
        pl = pytest.importorskip("polars")
        lazy = pl.LazyFrame({"a": [1, 2, 3]}).filter(pl.col("a") > 1)

        result = serialize_wrapper(wrapper=table(lazy), wrapper_index=0)
        assert result.metadata["sub_kind"] == "polars"
        restored = pd.read_parquet(io.BytesIO(result.data))
        assert list(restored["a"]) == [2, 3]

    def test_serialize_pyarrow_table(self) -> None:
        pa = pytest.importorskip("pyarrow")
        tbl = pa.table({"a": [1, 2, 3]})
        result = serialize_wrapper(wrapper=table(tbl), wrapper_index=0)
        assert result.metadata["sub_kind"] == "pyarrow"
        assert result.metadata["row_count"] == 3

    def test_serialize_numpy_array(self) -> None:
        # numpy path: either zarr store or npy fallback depending on env.
        arr = np.arange(12).reshape(3, 4).astype("float32")
        result = serialize_wrapper(wrapper=array(arr), wrapper_index=0)
        assert result.metadata["shape"] == [3, 4]
        assert result.metadata["dtype"] == "float32"
        assert result.metadata["byte_size"] == len(result.data)
        assert result.extension in {"npy", "zarr.zip"}

    def test_serialize_dask_array_triggers_compute(self) -> None:
        pytest.importorskip("zarr")
        da = pytest.importorskip("dask.array")
        arr = da.ones((4, 4), chunks=(2, 2))

        result = serialize_wrapper(wrapper=array(arr), wrapper_index=0)
        assert result.metadata["sub_kind"] == "dask"
        assert result.metadata["shape"] == [4, 4]

    def test_serialize_matplotlib_fig(self) -> None:
        plt = pytest.importorskip("matplotlib.pyplot")
        figure = plt.figure()
        ax = figure.add_subplot()
        ax.plot([0, 1], [0, 1])

        result = serialize_wrapper(wrapper=fig(figure), wrapper_index=0)
        assert result.extension == "png"
        assert result.metadata["source_format"] == "png"
        assert result.metadata["dimensions"] is not None
        plt.close(figure)

    def test_serialize_plotly_fig(self) -> None:
        go = pytest.importorskip("plotly.graph_objects")
        figure = go.Figure(data=[go.Scatter(x=[1, 2], y=[1, 2])])

        result = serialize_wrapper(wrapper=fig(figure), wrapper_index=0)
        assert result.extension == "html"
        assert result.data.startswith(b"<")

    def test_serialize_text_string(self) -> None:
        wrapper = text("hello\nworld")
        result = serialize_wrapper(wrapper=wrapper, wrapper_index=0)
        assert result.extension == "txt"
        assert result.metadata["format"] == "plain"
        assert result.metadata["line_count"] == 2
        assert result.metadata["byte_size"] == len(result.data)

    def test_serialize_text_dict_as_json(self) -> None:
        wrapper = text({"a": 1, "b": [1, 2]})
        result = serialize_wrapper(wrapper=wrapper, wrapper_index=0)
        assert result.extension == "json"
        assert result.metadata["format"] == "json"
        # Body must be valid JSON.
        import json as _json

        assert _json.loads(result.data.decode("utf-8")) == {"a": 1, "b": [1, 2]}

    def test_serialization_error_wraps_underlying_failure(self) -> None:
        class Exploding:
            def savefig(self, *args: Any, **kwargs: Any) -> None:
                raise RuntimeError("boom")

        # Build a fig wrapper manually so sub-kind detection does not run.
        wrapper = FigureResult(
            payload=Exploding(),
            name="bad",
            sub_kind="matplotlib",
            metadata={},
        )
        with pytest.raises(WrapperSerializationError) as excinfo:
            serialize_wrapper(wrapper=wrapper, wrapper_index=2)
        assert "name='bad'" in str(excinfo.value)
        assert excinfo.value.wrapper_kind == "fig"


# ---------------------------------------------------------------------------
# Evaluator integration tests
# ---------------------------------------------------------------------------


@task()
def make_table_task() -> object:
    return table(
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
        name="features",
    )


@task()
def make_positional_tables_task() -> object:
    return [
        table(pd.DataFrame({"x": [1]})),
        table(pd.DataFrame({"x": [2]})),
    ]


@task()
def make_mixed_task() -> object:
    return [
        table(pd.DataFrame({"a": [1]}), name="features"),
        array(np.arange(4)),
        text({"scalar": 7}, name="summary"),
        42,
    ]


@task()
def make_duplicate_names_task() -> object:
    return [
        table(pd.DataFrame({"a": [1]}), name="dup"),
        table(pd.DataFrame({"b": [2]}), name="dup"),
    ]


@task()
def make_exploding_table_task() -> object:
    return table(
        pd.DataFrame({"a": [1]}),
        name="bad",
        metadata={"force_failure": True},
    )


@task()
def consumer_task(upstream: object) -> int:
    assert isinstance(upstream, AssetRef)
    return 1


class TestEvaluatorIntegration:
    def test_named_table_asset_has_derived_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(make_table_task())
        assert isinstance(result, AssetRef)
        assert result.key.namespace == "table"
        assert result.key.name == "make_table_task.features"
        assert result.metadata["row_count"] == 2

    def test_positional_tables_index_per_kind(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(make_positional_tables_task())
        assert isinstance(result, list)
        names = [ref.key.name for ref in result]
        assert names == [
            "make_positional_tables_task.table[0]",
            "make_positional_tables_task.table[1]",
        ]

    def test_mixed_return_materialises_each_wrapper(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(make_mixed_task())
        assert isinstance(result, list)
        assert len(result) == 4

        table_ref, array_ref, text_ref, scalar = result
        assert isinstance(table_ref, AssetRef)
        assert isinstance(array_ref, AssetRef)
        assert isinstance(text_ref, AssetRef)
        assert scalar == 42

        assert table_ref.key.namespace == "table"
        assert table_ref.key.name == "make_mixed_task.features"
        assert array_ref.key.namespace == "array"
        assert array_ref.key.name == "make_mixed_task.array[0]"
        assert text_ref.key.namespace == "text"
        assert text_ref.key.name == "make_mixed_task.summary"

        # Each asset carries kind-specific metadata.
        assert table_ref.metadata["sub_kind"] == "pandas"
        assert table_ref.metadata["row_count"] == 1
        assert array_ref.metadata["shape"] == [4]
        assert array_ref.metadata["dtype"] == "int64"
        assert text_ref.metadata["format"] == "json"
        assert text_ref.metadata["line_count"] >= 1

    def test_duplicate_names_raise_before_registration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        with pytest.raises(Exception) as excinfo:
            ginkgo.evaluate(make_duplicate_names_task())
        assert "duplicate wrapped asset name" in str(excinfo.value)

        # No asset version should have been registered.
        asset_dir = tmp_path / ".ginkgo" / "assets"
        if asset_dir.is_dir():
            store = AssetStore(root=asset_dir)
            keys = store.list_asset_keys()
            for key in keys:
                assert key.namespace != "table" or "dup" not in key.name

    def test_cache_hit_reuses_artifact_id(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)

        first_result = ginkgo.evaluate(make_table_task())
        assert isinstance(first_result, AssetRef)

        # Re-run the same task; cache should hit and return the same artifact.
        second_result = ginkgo.evaluate(make_table_task())
        assert isinstance(second_result, AssetRef)
        assert second_result.artifact_id == first_result.artifact_id

    def test_consumer_downstream_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)

        expr = consumer_task(upstream=make_table_task())
        assert ginkgo.evaluate(expr) == 1
        # Second call must also succeed, keyed on artifact id rather than payload.
        assert ginkgo.evaluate(expr) == 1


# ---------------------------------------------------------------------------
# CLI asset show
# ---------------------------------------------------------------------------


class TestAssetShow:
    def test_show_table_renders_metadata_only(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(make_table_task())

        # Patch ArtifactStore.read_bytes to raise, proving the show path does
        # not rehydrate the main artifact.
        from ginkgo.runtime.artifacts import artifact_store as artifact_store_mod

        original_read = artifact_store_mod.LocalArtifactStore.read_bytes

        def _forbidden(self: Any, *, artifact_id: str) -> bytes:
            raise AssertionError("asset show must not read artifact bytes")

        monkeypatch.setattr(artifact_store_mod.LocalArtifactStore, "read_bytes", _forbidden)
        try:
            from ginkgo.cli.app import main

            rc = main(
                [
                    "asset",
                    "show",
                    "table:make_table_task.features",
                ]
            )
        finally:
            monkeypatch.setattr(artifact_store_mod.LocalArtifactStore, "read_bytes", original_read)

        assert rc == 0
        output = capsys.readouterr().out
        assert "make_table_task.features" in output
        assert "Row count" in output
        assert "Column" in output  # schema table header
