"""Unit and integration tests for the semantic asset kinds."""

from __future__ import annotations

import contextlib
import io
import subprocess
import sys
import textwrap
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

import ginkgo
from ginkgo import array, asset, fig, file, model, table, task, text
from ginkgo.core.asset import AssetKey, AssetRef, AssetResult
from ginkgo.runtime.artifacts.asset_serialization import (
    AssetSerializationError,
    serialize_asset,
)
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.asset_registration import AssetCheckError
from ginkgo.runtime.artifacts.remote_arg_transfer import (
    hydrate_result_from_remote,
    stage_result_for_remote,
)
from ginkgo.runtime.artifacts.value_codec import CodecError, decode_value, encode_value


FLOW_MODULE_NAME = "ginkgo_user_wf_9f0f3043f1"


class FlowEstimator:
    """Stand-in for a class a user defines in their flow script."""

    def __init__(self, coef: float = 1.5) -> None:
        self.coef = coef


@contextlib.contextmanager
def flow_defined_class() -> Iterator[type]:
    """Yield a class that lives only in a synthetic single-file-flow module.

    Ginkgo loads a single-file flow under a ``ginkgo_user_*`` module name
    unique to the process and the file path, so a class defined there pickles
    (the module is in ``sys.modules`` during the run) but can never be
    unpickled anywhere else. Reproducing that here needs a real module entry,
    not just a rewritten ``__module__``, or pickling would fail for the
    unrelated reason that the class cannot be looked up.
    """
    module = types.ModuleType(FLOW_MODULE_NAME)
    cls = type("FlowEstimator", (FlowEstimator,), {"__module__": FLOW_MODULE_NAME})
    module.FlowEstimator = cls  # type: ignore[attr-defined]
    sys.modules[FLOW_MODULE_NAME] = module
    try:
        yield cls
    finally:
        del sys.modules[FLOW_MODULE_NAME]


def has_rows(payload: Any) -> bool:
    """Return whether a tabular payload has at least one row."""
    return len(payload) > 0


def always_fails(payload: Any) -> bool:
    """Return a deterministic failed asset check outcome."""
    del payload
    return False


def returns_non_boolean(payload: Any) -> str:
    """Return an invalid asset check outcome for testing."""
    del payload
    return "yes"


def raises_from_check(payload: Any) -> bool:
    """Raise a deterministic exception from an asset check."""
    del payload
    raise ValueError("check exploded")


# ---------------------------------------------------------------------------
# Canonical kind list
# ---------------------------------------------------------------------------


def test_asset_kind_registry_matches_canonical_kinds() -> None:
    from typing import get_args

    from ginkgo.core.asset import ASSET_KIND_NAMES, _VALID_KINDS, AssetKind
    from ginkgo.runtime.artifacts.asset_kinds import ASSET_KINDS

    assert ASSET_KIND_NAMES == get_args(AssetKind)
    assert _VALID_KINDS == frozenset(ASSET_KIND_NAMES)
    assert frozenset(ASSET_KINDS) == frozenset(ASSET_KIND_NAMES)


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
        assert ginkgo.model is model

    def test_table_pandas_detection(self) -> None:
        wrapper = table(pd.DataFrame({"a": [1, 2]}))
        assert isinstance(wrapper, AssetResult)
        assert wrapper.kind == "table"
        assert wrapper.sub_kind == "pandas"
        assert wrapper.name is None

    def test_asset_table_equivalent_to_table_factory(self) -> None:
        frame = pd.DataFrame({"a": [1, 2]})
        via_asset = ginkgo.asset(
            frame,
            kind="table",
            name="features",
            group="QC metrics",
            caption="Variant counts after QC filtering",
        )
        via_shorthand = table(
            frame,
            name="features",
            group="QC metrics",
            caption="Variant counts after QC filtering",
        )
        assert via_asset.kind == via_shorthand.kind == "table"
        assert via_asset.sub_kind == via_shorthand.sub_kind == "pandas"
        assert via_asset.name == via_shorthand.name == "features"
        assert via_asset.group == via_shorthand.group == "QC metrics"
        assert via_asset.caption == via_shorthand.caption == "Variant counts after QC filtering"

    def test_checks_are_preserved_by_asset_factories(self) -> None:
        frame = pd.DataFrame({"a": [1]})
        via_asset = ginkgo.asset(frame, kind="table", checks=[has_rows])
        via_shorthand = table(frame, checks=[has_rows])

        assert via_asset.checks == (has_rows,)
        assert via_shorthand.checks == (has_rows,)
        assert table(frame).checks == ()

    def test_presentation_labels_are_normalized(self) -> None:
        grouped = table(
            pd.DataFrame({"a": [1]}),
            group="  QC metrics  ",
            caption="  Variant counts after QC filtering  ",
        )
        ungrouped = table(pd.DataFrame({"a": [1]}), group="  ", caption="  ")
        assert grouped.group == "QC metrics"
        assert grouped.caption == "Variant counts after QC filtering"
        assert ungrouped.group is None
        assert ungrouped.caption is None

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
        assert isinstance(wrapper, AssetResult)
        assert wrapper.kind == "array"
        assert wrapper.sub_kind == "numpy"

    def test_fig_path_detection(self, tmp_path: Path) -> None:
        png_path = tmp_path / "plot.png"
        png_path.write_bytes(b"\x89PNG\r\n\x1a\nfake")
        wrapper = fig(png_path)
        assert isinstance(wrapper, AssetResult)
        assert wrapper.kind == "fig"
        assert wrapper.sub_kind == "png"

    def test_text_dict_becomes_json(self) -> None:
        wrapper = text({"a": 1})
        assert isinstance(wrapper, AssetResult)
        assert wrapper.kind == "text"
        assert wrapper.sub_kind == "json"
        assert wrapper.kind_fields["format"] == "json"
        assert '"a"' in wrapper.payload

    def test_text_string_is_inline_plain(self) -> None:
        wrapper = text("hello world")
        assert wrapper.kind_fields["format"] == "plain"
        # Crucially: a plain string that happens to resemble a path must
        # never be probed against the filesystem at construction time.
        wrapper_path_like = text("this/path/should-not-be-resolved")
        assert wrapper_path_like.kind_fields["format"] == "plain"
        assert wrapper_path_like.payload == "this/path/should-not-be-resolved"

    def test_text_path_suffix_infers_format(self, tmp_path: Path) -> None:
        md_path = tmp_path / "notes.md"
        md_path.write_text("# header", encoding="utf-8")
        wrapper = text(md_path)
        assert wrapper.kind_fields["format"] == "markdown"

    def test_text_explicit_format_override(self) -> None:
        wrapper = text("raw", format="markdown")
        assert wrapper.kind_fields["format"] == "markdown"

    def test_text_dict_rejects_non_json_format(self) -> None:
        with pytest.raises(ValueError):
            text({"a": 1}, format="markdown")

    def test_model_sklearn_detection(self) -> None:
        sklearn = pytest.importorskip("sklearn.linear_model")
        clf = sklearn.LogisticRegression()
        wrapper = model(clf, name="classifier", metrics={"auc": 0.91})
        assert isinstance(wrapper, AssetResult)
        assert wrapper.kind == "model"
        assert wrapper.sub_kind == "sklearn"
        assert wrapper.kind_fields["metrics"] == {"auc": 0.91}
        assert wrapper.kind_fields["framework"] == "sklearn"
        assert wrapper.name == "classifier"

    def test_model_framework_override(self) -> None:
        sklearn = pytest.importorskip("sklearn.dummy")
        clf = sklearn.DummyClassifier()
        wrapper = model(clf, framework="sklearn")
        assert wrapper.sub_kind == "sklearn"

    def test_model_framework_override_rejects_unknown(self) -> None:
        sklearn = pytest.importorskip("sklearn.dummy")
        clf = sklearn.DummyClassifier()
        with pytest.raises(ValueError):
            model(clf, framework="onnx")

    def test_model_generic_framework_override(self) -> None:
        sklearn = pytest.importorskip("sklearn.dummy")
        clf = sklearn.DummyClassifier()
        wrapper = model(clf, framework="pickle")
        assert wrapper.sub_kind == "pickle"
        assert wrapper.kind_fields["framework"] == "pickle"

    def test_model_unrecognised_payload_falls_back_to_pickle(self) -> None:
        wrapper = model({"weights": [0.1, 0.2], "bias": 0.5}, metrics={"acc": 0.9})
        assert wrapper.kind == "model"
        assert wrapper.sub_kind == "pickle"
        assert wrapper.kind_fields["framework"] == "pickle"
        assert wrapper.kind_fields["metrics"] == {"acc": 0.9}

    def test_model_rejects_unpicklable_payload(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            model(lambda x: x)
        message = str(excinfo.value)
        assert "picklable" in message
        # The failing exception is named, so a broken __reduce__ is not
        # reported as if the payload were merely unpicklable.
        assert "Error" in message or "Exception" in message

    def test_model_rejects_path_string(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            model("outputs/model.pkl")
        message = str(excinfo.value)
        assert "does not accept a path" in message
        assert "file(path)" in message

    def test_model_rejects_path_object(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            model(Path("outputs/model.pkl"))
        assert "file(path)" in str(excinfo.value)

    def test_model_rejects_flow_defined_class(self) -> None:
        with flow_defined_class() as estimator_cls:
            with pytest.raises(TypeError) as excinfo:
                model(estimator_cls())
        message = str(excinfo.value)
        assert FLOW_MODULE_NAME in message
        assert "importable module" in message

    def test_model_rejects_flow_defined_class_inside_a_dict(self) -> None:
        with flow_defined_class() as estimator_cls:
            with pytest.raises(TypeError) as excinfo:
                model({"estimator": estimator_cls(), "bias": 0.5})
        assert FLOW_MODULE_NAME in str(excinfo.value)

    def test_model_accepts_plain_values_in_a_dict(self) -> None:
        wrapper = model({"weights": [0.1, 0.2], "notes": "linear"})
        assert wrapper.sub_kind == "pickle"


# ---------------------------------------------------------------------------
# Serialiser-level tests
# ---------------------------------------------------------------------------


class TestSerializers:
    def test_serialize_pandas_table(self) -> None:
        frame = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        result = serialize_asset(result=table(frame, name="t"), index=0)
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

        result = serialize_asset(result=table(csv_path), index=0)
        restored = pd.read_parquet(io.BytesIO(result.data))
        assert list(restored.columns) == ["x", "y"]
        assert len(restored) == 2

    def test_serialize_polars_lazy_frame(self) -> None:
        pl = pytest.importorskip("polars")
        lazy = pl.LazyFrame({"a": [1, 2, 3]}).filter(pl.col("a") > 1)

        result = serialize_asset(result=table(lazy), index=0)
        assert result.metadata["sub_kind"] == "polars"
        restored = pd.read_parquet(io.BytesIO(result.data))
        assert list(restored["a"]) == [2, 3]

    def test_serialize_pyarrow_table(self) -> None:
        pa = pytest.importorskip("pyarrow")
        tbl = pa.table({"a": [1, 2, 3]})
        result = serialize_asset(result=table(tbl), index=0)
        assert result.metadata["sub_kind"] == "pyarrow"
        assert result.metadata["row_count"] == 3

    def test_serialize_numpy_array(self) -> None:
        # numpy path: either zarr store or npy fallback depending on env.
        arr = np.arange(12).reshape(3, 4).astype("float32")
        result = serialize_asset(result=array(arr), index=0)
        assert result.metadata["shape"] == [3, 4]
        assert result.metadata["dtype"] == "float32"
        assert result.metadata["byte_size"] == len(result.data)
        assert result.extension in {"npy", "zarr.zip"}

    def test_serialize_dask_array_triggers_compute(self) -> None:
        pytest.importorskip("zarr")
        da = pytest.importorskip("dask.array")
        arr = da.ones((4, 4), chunks=(2, 2))

        result = serialize_asset(result=array(arr), index=0)
        assert result.metadata["sub_kind"] == "dask"
        assert result.metadata["shape"] == [4, 4]

    def test_serialize_matplotlib_fig(self) -> None:
        plt = pytest.importorskip("matplotlib.pyplot")
        figure = plt.figure()
        ax = figure.add_subplot()
        ax.plot([0, 1], [0, 1])

        result = serialize_asset(result=fig(figure), index=0)
        assert result.extension == "png"
        assert result.metadata["source_format"] == "png"
        assert result.metadata["dimensions"] is not None
        plt.close(figure)

    def test_serialize_plotly_fig(self) -> None:
        go = pytest.importorskip("plotly.graph_objects")
        figure = go.Figure(data=[go.Scatter(x=[1, 2], y=[1, 2])])

        result = serialize_asset(result=fig(figure), index=0)
        assert result.extension == "html"
        assert result.data.startswith(b"<")

    def test_serialize_text_string(self) -> None:
        wrapper = text("hello\nworld")
        result = serialize_asset(result=wrapper, index=0)
        assert result.extension == "txt"
        assert result.metadata["format"] == "plain"
        assert result.metadata["line_count"] == 2
        assert result.metadata["byte_size"] == len(result.data)

    def test_serialize_text_dict_as_json(self) -> None:
        wrapper = text({"a": 1, "b": [1, 2]})
        result = serialize_asset(result=wrapper, index=0)
        assert result.extension == "json"
        assert result.metadata["format"] == "json"
        # Body must be valid JSON.
        import json as _json

        assert _json.loads(result.data.decode("utf-8")) == {"a": 1, "b": [1, 2]}

    def test_serialize_sklearn_model_roundtrip(self) -> None:
        sklearn_lm = pytest.importorskip("sklearn.linear_model")
        pytest.importorskip("joblib")
        clf = sklearn_lm.LogisticRegression()
        clf.fit(np.array([[0.0], [1.0], [2.0], [3.0]]), np.array([0, 0, 1, 1]))

        wrapper = model(clf, name="clf", metrics={"score": 0.875})
        result = serialize_asset(result=wrapper, index=0)
        assert result.extension == "joblib"
        assert result.metadata["framework"] == "sklearn"
        assert result.metadata["sub_kind"] == "sklearn"
        assert result.metadata["metrics"] == {"score": 0.875}
        assert result.metadata["byte_size"] == len(result.data)

        # Round-trip via joblib so we know the bytes are a real joblib blob.
        import joblib  # type: ignore[import-not-found]

        restored = joblib.load(io.BytesIO(result.data))
        assert list(restored.predict([[0.5], [2.5]])) == [0, 1]

    def test_serialize_pickle_model_roundtrip(self) -> None:
        from ginkgo.runtime.artifacts.asset_loaders import load_model_bytes

        payload = {"weights": [0.1, -0.4], "bias": 0.25}
        wrapper = model(payload, name="hand_rolled", metrics={"accuracy": 1.0})
        result = serialize_asset(result=wrapper, index=0)
        assert result.extension == "pkl"
        assert result.metadata["sub_kind"] == "pickle"
        assert result.metadata["framework"] == "pickle"
        assert result.metadata["metrics"] == {"accuracy": 1.0}
        assert result.metadata["byte_size"] == len(result.data)

        class _Store:
            def read_bytes(self, *, artifact_id: str) -> bytes:  # noqa: ARG002
                return result.data

        restored = load_model_bytes(
            artifact_store=_Store(),  # type: ignore[arg-type]
            artifact_id="stub",
            metadata=dict(result.metadata),
        )
        assert restored == payload

    def test_pickle_model_blob_loads_in_a_fresh_process(self, tmp_path: Path) -> None:
        """The stored blob must survive the process that wrote it.

        A pickle whose classes come from an unimportable module unpickles
        happily in the writing process and fails everywhere else, so an
        in-process round trip cannot pin this. The blob is read back by a bare
        interpreter that never imported Ginkgo.
        """
        wrapper = model({"weights": [0.1, -0.4], "bias": 0.25})
        result = serialize_asset(result=wrapper, index=0)
        blob = tmp_path / "model.pkl"
        blob.write_bytes(result.data)

        script = textwrap.dedent(
            f"""
            import pickle
            with open({str(blob)!r}, "rb") as handle:
                print(pickle.load(handle))
            """
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )
        assert completed.returncode == 0, completed.stderr
        assert "'bias': 0.25" in completed.stdout

    def test_serialization_error_wraps_underlying_failure(self) -> None:
        class Exploding:
            def savefig(self, *args: Any, **kwargs: Any) -> None:
                raise RuntimeError("boom")

        # Build a fig asset manually so sub-kind detection does not run.
        bad = AssetResult(
            payload=Exploding(),
            kind="fig",
            sub_kind="matplotlib",
            name="bad",
        )
        with pytest.raises(AssetSerializationError) as excinfo:
            serialize_asset(result=bad, index=2)
        assert "name='bad'" in str(excinfo.value)
        assert excinfo.value.kind == "fig"


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
def make_grouped_table_task() -> object:
    return table(
        pd.DataFrame({"a": [1]}),
        name="features",
        group="QC metrics",
        caption="Variant counts after QC filtering",
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
def make_checked_table_task() -> object:
    return table(pd.DataFrame({"a": [1]}), name="checked", checks=[has_rows])


@task()
def make_failed_check_task() -> object:
    return table(pd.DataFrame({"a": [1]}), name="rejected", checks=[always_fails])


@task()
def make_invalid_check_task() -> object:
    return table(pd.DataFrame({"a": [1]}), name="invalid", checks=[returns_non_boolean])


@task()
def make_raising_check_task() -> object:
    return table(pd.DataFrame({"a": [1]}), name="raising", checks=[raises_from_check])


@task()
def consumer_task(upstream: object) -> int:
    # Wrapped ``AssetRef`` inputs are rehydrated to the live payload at
    # arg-binding time, so downstream tasks observe the canonical
    # deserialised object rather than the reference.
    assert isinstance(upstream, pd.DataFrame)
    assert list(upstream.columns) == ["a", "b"]
    return 1


class TestEvaluatorIntegration:
    def test_named_table_asset_has_derived_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(make_table_task())
        assert isinstance(result, AssetRef)
        assert result.key.namespace == "table"
        assert result.key.name == "features"
        assert result.metadata["row_count"] == 2

    def test_grouped_asset_persists_group_metadata(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(make_grouped_table_task())
        assert isinstance(result, AssetRef)
        assert result.key.namespace == "table"
        assert result.key.name == "features"
        assert result.metadata["ginkgo_group"] == "QC metrics"
        assert result.metadata["ginkgo_caption"] == "Variant counts after QC filtering"

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
        assert table_ref.key.name == "features"
        assert array_ref.key.namespace == "array"
        assert array_ref.key.name == "make_mixed_task.array[0]"
        assert text_ref.key.namespace == "text"
        assert text_ref.key.name == "summary"

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

        # The error is raised before registration, so no version for the
        # offending name may exist. Assert this unconditionally — an empty
        # store is the expected outcome, not a reason to skip the check.
        with AssetStore.for_reading(tmp_path / ".ginkgo" / "ginkgo.db") as catalog:
            keys = catalog.list_asset_keys()
        assert not any(key.namespace == "table" and key.name == "dup" for key in keys)

    def test_passing_check_is_persisted_with_asset_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)

        result = ginkgo.evaluate(make_checked_table_task())

        assert isinstance(result, AssetRef)
        assert result.metadata["ginkgo_checks"] == [{"name": "has_rows", "passed": True}]

    @pytest.mark.parametrize(
        ("expression", "message"),
        [
            (make_failed_check_task(), "Asset check 'always_fails' failed"),
            (make_invalid_check_task(), "must return bool, got str"),
            (make_raising_check_task(), "Asset check 'raises_from_check' raised an exception"),
        ],
    )
    def test_rejected_checks_do_not_register_versions(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        expression: object,
        message: str,
    ) -> None:
        monkeypatch.chdir(tmp_path)

        with pytest.raises(AssetCheckError, match=message):
            ginkgo.evaluate(expression)

        with AssetStore.for_reading(tmp_path / ".ginkgo" / "ginkgo.db") as catalog:
            keys = catalog.list_asset_keys()
        assert not keys

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
# Explicit names and key format
# ---------------------------------------------------------------------------


EXPLICIT_NAME = "sites/forest/observations"


@task()
def named_file_asset_task(output_path: str) -> file:
    out = Path(output_path)
    out.write_text("bytes\n", encoding="utf-8")
    return asset(out, name=EXPLICIT_NAME)


@task()
def named_table_asset_task() -> object:
    return table(pd.DataFrame({"a": [1]}), name=EXPLICIT_NAME)


@task()
def named_array_asset_task() -> object:
    return array(np.arange(3), name=EXPLICIT_NAME)


@task()
def named_fig_asset_task(output_path: str) -> object:
    out = Path(output_path)
    out.write_bytes(b"\x89PNG\r\n\x1a\nfake")
    return fig(out, name=EXPLICIT_NAME)


@task()
def named_text_asset_task() -> object:
    return text("one line\n", name=EXPLICIT_NAME)


@task()
def other_named_text_asset_task() -> object:
    """A second task claiming the same explicit name, with other content."""
    return text("another line\n", name=EXPLICIT_NAME)


@task()
def named_model_asset_task() -> object:
    from sklearn.linear_model import LogisticRegression

    clf = LogisticRegression()
    clf.fit(np.array([[0.0], [1.0]]), np.array([0, 1]))
    return model(clf, name=EXPLICIT_NAME)


class TestExplicitAssetNames:
    """An explicit ``name=`` is the asset name, for every kind (issue #203)."""

    @pytest.mark.parametrize(
        ("kind", "expression_factory", "required_modules"),
        [
            ("file", lambda: named_file_asset_task(output_path="note.txt"), ()),
            ("table", named_table_asset_task, ("pyarrow",)),
            ("array", named_array_asset_task, ()),
            ("fig", lambda: named_fig_asset_task(output_path="plot.png"), ()),
            ("text", named_text_asset_task, ()),
            ("model", named_model_asset_task, ("sklearn.linear_model", "joblib")),
        ],
    )
    def test_explicit_name_is_the_key_name_for_every_kind(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        kind: str,
        expression_factory: Any,
        required_modules: tuple[str, ...],
    ) -> None:
        for module in required_modules:
            pytest.importorskip(module)
        monkeypatch.chdir(tmp_path)

        result = ginkgo.evaluate(expression_factory())

        assert isinstance(result, AssetRef)
        assert result.key.namespace == kind
        # No task-name prefix: the key name is exactly what the task asked for.
        assert result.key.name == EXPLICIT_NAME
        # And the documented format is what gets rendered and parsed back.
        assert str(result.key) == f"{kind}:{EXPLICIT_NAME}"
        assert AssetKey.parse(str(result.key)) == result.key

        with AssetStore.for_reading(tmp_path / ".ginkgo" / "ginkgo.db") as catalog:
            assert result.key in catalog.list_asset_keys()

    @pytest.mark.parametrize(
        ("kind", "expression_factory", "required_modules"),
        [
            ("file", lambda: named_file_asset_task(output_path="note.txt"), ()),
            ("table", named_table_asset_task, ("pyarrow",)),
            ("array", named_array_asset_task, ()),
            ("fig", lambda: named_fig_asset_task(output_path="plot.png"), ()),
            ("text", named_text_asset_task, ()),
            ("model", named_model_asset_task, ("sklearn.linear_model", "joblib")),
        ],
    )
    def test_explicit_name_round_trips_through_asset_show(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        kind: str,
        expression_factory: Any,
        required_modules: tuple[str, ...],
    ) -> None:
        for module in required_modules:
            pytest.importorskip(module)
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(expression_factory())
        capsys.readouterr()

        from ginkgo.cli.app import main

        # The bare name the task passed resolves without naming the kind.
        assert main(["asset", "show", EXPLICIT_NAME]) == 0
        assert f"Asset Key: {kind}:{EXPLICIT_NAME}" in capsys.readouterr().out

        # The qualified key from ``asset ls`` resolves too.
        assert main(["asset", "show", f"{kind}:{EXPLICIT_NAME}"]) == 0
        assert f"Asset Key: {kind}:{EXPLICIT_NAME}" in capsys.readouterr().out

    def test_bare_name_shared_by_two_kinds_asks_for_qualification(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        pytest.importorskip("pyarrow")
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(named_table_asset_task())
        ginkgo.evaluate(named_text_asset_task())
        capsys.readouterr()

        from ginkgo.cli.app import main

        assert main(["asset", "show", EXPLICIT_NAME]) == 1
        error = capsys.readouterr().err
        assert "exists in several kinds" in error
        assert f"table:{EXPLICIT_NAME}" in error
        assert f"text:{EXPLICIT_NAME}" in error

    def test_unknown_name_suggests_catalogued_keys(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        pytest.importorskip("pyarrow")
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(named_table_asset_task())
        capsys.readouterr()

        from ginkgo.cli.app import main

        assert main(["asset", "show", "sites/forest/observation"]) == 1
        error = capsys.readouterr().err
        # No invented ``file:`` asset, and the real key is offered instead.
        assert "file:" not in error
        assert f"table:{EXPLICIT_NAME}" in error

    def test_two_tasks_naming_one_key_share_the_asset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dropping the task prefix makes an explicit name shared, by design."""
        monkeypatch.chdir(tmp_path)

        first = ginkgo.evaluate(named_text_asset_task())
        second = ginkgo.evaluate(other_named_text_asset_task())

        assert isinstance(first, AssetRef)
        assert isinstance(second, AssetRef)
        assert first.key == second.key
        with AssetStore.for_reading(tmp_path / ".ginkgo" / "ginkgo.db") as catalog:
            assert len(catalog.list_versions(key=first.key)) == 2


class TestAssetCheckTransport:
    def test_asset_checks_round_trip_through_value_codec(self, tmp_path: Path) -> None:
        encoded = encode_value(
            table(pd.DataFrame({"a": [1]}), checks=[has_rows]),
            base_dir=tmp_path,
        )

        decoded = decode_value(encoded, base_dir=tmp_path)

        assert isinstance(decoded, AssetResult)
        assert decoded.checks == (has_rows,)

    def test_unserializable_asset_check_raises_clear_error(self, tmp_path: Path) -> None:
        def nested_check(payload: Any) -> bool:
            return payload is not None

        with pytest.raises(CodecError, match="importable module-level functions"):
            encode_value(
                table(pd.DataFrame({"a": [1]}), checks=[nested_check]),
                base_dir=tmp_path,
            )

    def test_asset_checks_survive_remote_result_transport(self, tmp_path: Path) -> None:
        encoded = encode_value(
            table(pd.DataFrame({"a": [1]}), checks=[has_rows]),
            base_dir=tmp_path,
        )
        staged = stage_result_for_remote(result=encoded, remote_store=object())
        hydrated = hydrate_result_from_remote(
            result=staged,
            remote_store=object(),
            scratch_dir=tmp_path / "hydrated",
        )

        decoded = decode_value(hydrated, base_dir=tmp_path)

        assert isinstance(decoded, AssetResult)
        assert decoded.checks == (has_rows,)


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
                    "table:features",
                ]
            )
        finally:
            monkeypatch.setattr(artifact_store_mod.LocalArtifactStore, "read_bytes", original_read)

        assert rc == 0
        output = capsys.readouterr().out
        assert "table:features" in output
        assert "Row count" in output
        assert "Column" in output  # schema table header

    def test_show_renders_caption(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(make_grouped_table_task())

        from ginkgo.cli.app import main

        rc = main(
            [
                "asset",
                "show",
                "features",
            ]
        )

        assert rc == 0
        output = capsys.readouterr().out
        assert "Caption:" in output
        assert "Variant counts after QC filtering" in output

    def test_show_renders_checks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(make_checked_table_task())

        from ginkgo.cli.app import main

        rc = main(
            [
                "asset",
                "show",
                "checked",
            ]
        )

        assert rc == 0
        output = capsys.readouterr().out
        assert "Check: has_rows passed" in output


# ---------------------------------------------------------------------------
# ginkgo models CLI
# ---------------------------------------------------------------------------


class TestModelsCommand:
    def test_empty_state_when_no_runs(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.chdir(tmp_path)
        from ginkgo.cli.app import main

        rc = main(["models"])
        assert rc == 1
        output = capsys.readouterr().out
        # The run domain's own answer, not a missing-database error: an empty
        # workspace reads the same here as it does for inspect and notebooks.
        assert "No runs recorded in" in output

    def test_lists_models_from_latest_run(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        pytest.importorskip("sklearn.linear_model")
        pytest.importorskip("joblib")
        monkeypatch.chdir(tmp_path)

        _run_with_provenance(tmp_path, produce_sklearn_model())

        from ginkgo.cli.app import main

        rc = main(["models"])
        assert rc == 0
        output = capsys.readouterr().out
        assert "🌿 ginkgo models" in output
        assert "produce_sklearn_model" in output
        assert "sklearn" in output
        assert "classifier" in output
        assert "score=1" in output

    def test_empty_state_when_run_has_no_models(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _run_with_provenance(tmp_path, make_table_task())

        from ginkgo.cli.app import main

        rc = main(["models"])
        assert rc == 0
        output = capsys.readouterr().out
        assert "No model assets in this run." in output


def _run_with_provenance(tmp_path: Path, expr: object) -> None:
    """Evaluate an expression against a real ledger.

    Records the run in ``.ginkgo/ginkgo.db`` so the ``ginkgo models`` command
    path can discover it and its asset entries.
    """
    from tests.conftest import Ledger

    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text("# placeholder\n", encoding="utf-8")
    ledger = Ledger.start(root=tmp_path, workflow=str(workflow_path))
    try:
        ginkgo.evaluate(expr, run_dir=ledger.run_dir, event_bus=ledger.bus, jobs=1, cores=1)
        ledger.finish()
    except Exception:
        ledger.finish(status="failed")
        raise
    finally:
        ledger.close()


# ---------------------------------------------------------------------------
# Rehydration-on-receive
# ---------------------------------------------------------------------------


@task()
def produce_table() -> object:
    return table(
        pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]}),
        name="features",
    )


@task()
def produce_array() -> object:
    return array(np.arange(12, dtype=np.int64).reshape(3, 4), name="grid")


@task()
def produce_text() -> object:
    return text("line one\nline two\n", name="notes")


#: Longer than NAME_MAX, so probing it as a filesystem path raises ENAMETOOLONG.
LONG_MARKDOWN = "# Summary\n\n" + "- a line of report prose\n" * 40


@task()
def produce_long_text() -> object:
    return text(LONG_MARKDOWN, name="summary", format="markdown")


@task()
def consume_dataframe_sum(upstream: object) -> int:
    assert isinstance(upstream, pd.DataFrame)
    assert list(upstream.columns) == ["x", "y"]
    assert upstream.shape == (3, 2)
    return int(upstream["x"].sum())


@task()
def consume_array_sum(upstream: object) -> int:
    assert isinstance(upstream, np.ndarray)
    assert upstream.shape == (3, 4)
    return int(upstream.sum())


@task()
def consume_text_length(upstream: object) -> int:
    assert isinstance(upstream, str)
    return len(upstream)


@task()
def consume_scalar_expecting_int(upstream: int) -> int:
    return upstream + 1


@task()
def produce_sklearn_model() -> object:
    from sklearn.linear_model import LogisticRegression

    clf = LogisticRegression()
    clf.fit(np.array([[0.0], [1.0], [2.0], [3.0]]), np.array([0, 0, 1, 1]))
    return model(clf, name="classifier", metrics={"score": 1.0})


@task()
def produce_table_from_csv(output_path: str) -> object:
    """Return a table asset whose payload is a CSV path, not a live frame.

    The contents differ from :func:`produce_table` so the two assets get
    distinct artifact ids — the live registry is keyed by artifact id, and
    identical bytes would otherwise let one producer serve the other.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"site": ["north", "south"], "count": [10, 20]}).to_csv(out, index=False)
    return table(out, name="summary")


def _ref_of_kind(kind: str) -> AssetRef:
    """Build a resolved ref of one kind, without running a workflow."""
    from ginkgo.core.asset import AssetKey

    return AssetRef(
        key=AssetKey(namespace=kind, name="producer.summary"),
        version_id="v1",
        kind=kind,
        artifact_id="abc123",
        content_hash="def456",
        artifact_path="/tmp/blobs/abc123",
    )


@task()
def produce_file_asset(output_path: str) -> file:
    """Return a ``file`` asset — the only kind whose artifact is a real file."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("site,count\nnorth,10\n", encoding="utf-8")
    return ginkgo.asset(out, name="summary")


@task()
def record_payload_type(upstream: object) -> str:
    return type(upstream).__name__


@task()
def consume_file_or_ref(summary: file | AssetRef, output_path: str) -> file:
    """Consume an upstream asset through the documented ``file | AssetRef`` idiom."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(f"{type(summary).__name__}\n", encoding="utf-8")
    return file(str(out))


@task()
def produce_text_asset() -> object:
    """A text asset — stored as raw UTF-8, so its artifact is a readable file."""
    return text("alpha\nbeta\n", name="notes")


@task()
def produce_fig_asset(output_path: str) -> object:
    """A fig asset from a PNG path — stored as the native image bytes."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(b"\x89PNG\r\n\x1a\n")
    return fig(out, name="plot")


@task()
def read_ref_bytes(summary: file | AssetRef) -> str:
    """Read the bound path's first bytes, as a downstream tool would."""
    path = Path(summary.artifact_path) if isinstance(summary, AssetRef) else Path(str(summary))
    return path.read_bytes()[:8].decode("utf-8", errors="replace")


@task()
def consume_file_ref_list(summaries: list[file | AssetRef]) -> list[str]:
    """A path-shaped annotation over a container — refs must survive nesting."""
    return [type(item).__name__ for item in summaries]


@task()
def return_table_from_file_annotation() -> file:
    """Declare ``-> file`` but return a table asset — always invalid."""
    return table(pd.DataFrame({"x": [1, 2, 3]}), name="tbl")


@task()
def consume_table_as_file(summary: file) -> str:
    """Declare a bare ``file`` parameter fed by a table asset — always invalid."""
    return str(summary)


@task()
def consume_model_predict(trained: object) -> list[int]:
    # Rehydration should hand us back the trained sklearn estimator.
    predictions = trained.predict(np.array([[0.5], [2.5]]))
    return [int(value) for value in predictions]


class TestRehydration:
    def test_table_ref_rehydrated_to_dataframe(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Consumer observes a live pandas DataFrame, not the AssetRef."""
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(consume_dataframe_sum(upstream=produce_table()))
        assert result == 6  # 1 + 2 + 3

    def test_array_ref_rehydrated_to_ndarray(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(consume_array_sum(upstream=produce_array()))
        assert result == 66  # sum(range(12))

    def test_text_ref_rehydrated_to_str(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(consume_text_length(upstream=produce_text()))
        assert result == len("line one\nline two\n")

    def test_long_text_ref_reaches_its_consumer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rehydrated contents are scanned as a possible path, and must not raise."""
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(consume_text_length(upstream=produce_long_text()))
        assert result == len(LONG_MARKDOWN)

    def test_live_payload_hit_skips_disk_loader(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the live registry has the payload, the loader is not called."""
        monkeypatch.chdir(tmp_path)

        from ginkgo.runtime import evaluator as evaluator_mod

        loader_call_count = {"n": 0}
        original_loader = evaluator_mod.load_wrapped_ref

        def _counting_loader(**kwargs: Any) -> Any:
            loader_call_count["n"] += 1
            return original_loader(**kwargs)

        monkeypatch.setattr(evaluator_mod, "load_wrapped_ref", _counting_loader)

        result = ginkgo.evaluate(consume_dataframe_sum(upstream=produce_table()))
        assert result == 6
        assert loader_call_count["n"] == 0  # live cache hit

    def test_loader_fallback_when_live_cache_misses(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the live cache is empty, rehydration falls back to disk."""
        monkeypatch.chdir(tmp_path)

        from ginkgo.runtime.artifacts import live_payloads as live_payloads_mod
        from ginkgo.runtime import evaluator as evaluator_mod

        # Force every lookup to miss.
        monkeypatch.setattr(
            live_payloads_mod.LivePayloadRegistry,
            "get",
            lambda self, *, artifact_id: None,
        )

        loader_call_count = {"n": 0}
        original_loader = evaluator_mod.load_wrapped_ref

        def _counting_loader(**kwargs: Any) -> Any:
            loader_call_count["n"] += 1
            return original_loader(**kwargs)

        monkeypatch.setattr(evaluator_mod, "load_wrapped_ref", _counting_loader)

        result = ginkgo.evaluate(consume_dataframe_sum(upstream=produce_table()))
        assert result == 6
        assert loader_call_count["n"] == 1  # exactly one disk load

    def test_type_mismatch_still_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rehydration does not swallow genuine type errors."""
        monkeypatch.chdir(tmp_path)

        with pytest.raises(Exception):  # noqa: PT011 — any runtime failure is fine
            ginkgo.evaluate(
                consume_scalar_expecting_int(upstream=produce_table()),
            )

    def test_model_ref_rehydrated_to_estimator(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A downstream consumer observes the live sklearn estimator."""
        pytest.importorskip("sklearn.linear_model")
        pytest.importorskip("joblib")
        monkeypatch.chdir(tmp_path)
        result = ginkgo.evaluate(consume_model_predict(trained=produce_sklearn_model()))
        assert result == [0, 1]

    def test_live_payload_and_loader_branches_agree(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Both rehydration branches yield the same type within one run.

        The DataFrame-backed table is served from the live registry; the
        CSV-backed one is not cached live, so it takes the disk loader. The
        loader call count proves each arm took the branch it claims.
        """
        monkeypatch.chdir(tmp_path)

        from ginkgo.runtime import evaluator as evaluator_mod

        loader_call_count = {"n": 0}
        original_loader = evaluator_mod.load_wrapped_ref

        def _counting_loader(**kwargs: Any) -> Any:
            loader_call_count["n"] += 1
            return original_loader(**kwargs)

        monkeypatch.setattr(evaluator_mod, "load_wrapped_ref", _counting_loader)

        result = ginkgo.evaluate(
            {
                "from_frame": record_payload_type(upstream=produce_table()),
                "from_csv": record_payload_type(
                    upstream=produce_table_from_csv(output_path="out/table.csv")
                ),
            }
        )

        # Exactly one disk load: the CSV-backed ref. A live-cached raw path
        # would make this zero and hand the consumer a PosixPath.
        assert loader_call_count["n"] == 1
        assert result == {"from_frame": "DataFrame", "from_csv": "DataFrame"}

    def test_string_text_payload_is_live_cached(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An inline ``text()`` string is a live payload, not a path."""
        monkeypatch.chdir(tmp_path)

        from ginkgo.runtime import evaluator as evaluator_mod

        loader_call_count = {"n": 0}
        original_loader = evaluator_mod.load_wrapped_ref

        def _counting_loader(**kwargs: Any) -> Any:
            loader_call_count["n"] += 1
            return original_loader(**kwargs)

        monkeypatch.setattr(evaluator_mod, "load_wrapped_ref", _counting_loader)

        result = ginkgo.evaluate(consume_text_length(upstream=produce_text()))
        assert result == len("line one\nline two\n")
        assert loader_call_count["n"] == 0  # served from the live registry

    def test_live_registry_capacity_eviction(self) -> None:
        """The registry is bounded — oldest entries evict when full."""
        from ginkgo.runtime.artifacts.live_payloads import LivePayloadRegistry

        registry = LivePayloadRegistry(capacity=3)
        registry.put(artifact_id="a", payload="first")
        registry.put(artifact_id="b", payload="second")
        registry.put(artifact_id="c", payload="third")
        registry.put(artifact_id="d", payload="fourth")

        assert registry.get(artifact_id="a") is None
        assert registry.get(artifact_id="b") == "second"
        assert registry.get(artifact_id="d") == "fourth"


# ---------------------------------------------------------------------------
# Asset kind vs path-shaped annotation
# ---------------------------------------------------------------------------


def _payload_probe(result: AssetResult) -> dict[str, Any]:
    """Unpack the fields ``is_path_backed_payload`` dispatches on."""
    return {"kind": result.kind, "sub_kind": result.sub_kind, "payload": result.payload}


class TestKindVersusPathAnnotation:
    """Behaviour when an asset kind meets a ``file`` / ``folder`` annotation."""

    def test_file_union_consumer_survives_rerun(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``file | AssetRef`` consumer of a file asset works cold and warm."""
        monkeypatch.chdir(tmp_path)

        def build() -> Any:
            return consume_file_or_ref(
                summary=produce_file_asset(output_path="results/summary.csv"),
                output_path="results/report.txt",
            )

        cold = ginkgo.evaluate(build())
        warm = ginkgo.evaluate(build())

        assert Path(cold).read_text(encoding="utf-8").strip() == "AssetRef"
        assert warm == cold

    def test_file_union_consumer_rejects_table_asset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``file | AssetRef`` fed a table asset is refused, not silently bound.

        The union arm is not an escape hatch from the kind rule: the artifact
        behind a table ref is Parquet, so a task that treats the path as the
        file it asked for reads a serialized blob and succeeds on garbage.
        """
        monkeypatch.chdir(tmp_path)

        with pytest.raises(TypeError) as excinfo:
            ginkgo.evaluate(
                consume_file_or_ref(
                    summary=produce_table_from_csv(output_path="results/summary.csv"),
                    output_path="results/report.txt",
                )
            )

        message = str(excinfo.value)
        assert "consume_file_or_ref.summary" in message
        assert "annotated `file` but is a `table` asset" in message
        assert not Path("results/report.txt").exists()

    def test_text_and_fig_assets_bind_a_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Native-byte kinds keep the union binding: the path reads as the file.

        ``serialize_text`` writes raw UTF-8 and ``serialize_fig`` writes native
        image bytes, so refusing these would deny a real capability and would
        have to claim an encoding that is not there.
        """
        monkeypatch.chdir(tmp_path)

        assert ginkgo.evaluate(read_ref_bytes(summary=produce_text_asset())) == "alpha\nbe"

        observed = ginkgo.evaluate(
            read_ref_bytes(summary=produce_fig_asset(output_path="results/plot.png"))
        )
        assert observed.startswith("�PNG")

    def test_nested_refs_survive_path_shaped_annotation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``list[file | AssetRef]`` keeps refs at every depth, not just the top."""
        monkeypatch.chdir(tmp_path)

        observed = ginkgo.evaluate(
            consume_file_ref_list(
                summaries=[
                    produce_file_asset(output_path="results/one.csv"),
                    produce_file_asset(output_path="results/two.csv"),
                ]
            )
        )
        assert observed == ["AssetRef", "AssetRef"]

    def test_nested_table_ref_rejected_under_path_shaped_annotation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The kind rule reaches nested elements, naming the offending index."""
        monkeypatch.chdir(tmp_path)

        with pytest.raises(TypeError, match=r"summaries\[1\] is annotated `file`"):
            ginkgo.evaluate(
                consume_file_ref_list(
                    summaries=[
                        produce_file_asset(output_path="results/one.csv"),
                        produce_table_from_csv(output_path="results/two.csv"),
                    ]
                )
            )

    def test_encoding_flag_matches_the_serializers(self) -> None:
        """Native-byte kinds are exactly the ones whose serialiser writes bytes as-is.

        ``file`` is copied verbatim, ``serialize_fig`` writes native PNG / SVG /
        HTML, and ``serialize_text`` writes raw UTF-8. The rest render a Python
        object into Ginkgo's own encoding, and each names it.
        """
        from ginkgo.runtime.artifacts.asset_kinds import (
            ASSET_KINDS,
            NATIVE_ARTIFACT_KINDS,
            artifact_encoding_for,
        )

        assert NATIVE_ARTIFACT_KINDS == {"file", "fig", "text"}
        assert artifact_encoding_for("table") == "Parquet"
        assert artifact_encoding_for("fig") is None
        # An unregistered kind counts as encoded: nothing vouches for its bytes.
        assert artifact_encoding_for("mystery") is not None
        for kind, spec in ASSET_KINDS.items():
            assert (spec.artifact_encoding is None) == (kind in NATIVE_ARTIFACT_KINDS)

    def test_as_file_follows_the_per_kind_rule(self) -> None:
        """``as_file()`` serves native-byte kinds and refuses encoded ones."""
        assert _ref_of_kind("file").as_file() == "/tmp/blobs/abc123"
        # A fig artifact is a real PNG and a text artifact is raw UTF-8.
        assert _ref_of_kind("fig").as_file() == "/tmp/blobs/abc123"
        assert _ref_of_kind("text").as_file() == "/tmp/blobs/abc123"

        for kind, encoding in (("table", "Parquet"), ("array", "zarr"), ("model", "model")):
            with pytest.raises(TypeError) as excinfo:
                _ref_of_kind(kind).as_file()
            message = str(excinfo.value)
            assert f"is a `{kind}` asset, so it has no readable file path" in message
            assert encoding in message

    def test_refusal_offers_remedies_the_consuming_task_can_use(self) -> None:
        """A driver task is not told to annotate ``object``; a Python task is."""
        from ginkgo.core.types import require_path_value

        with pytest.raises(TypeError) as driver:
            require_path_value(
                value=_ref_of_kind("table"),
                annotation_label="file",
                label="awk_the_table.scores",
                execution_mode="driver",
            )
        driver_message = str(driver.value)
        # `object` would hand a shell command a DataFrame repr.
        assert "`object`" not in driver_message
        assert "asset(path)" in driver_message
        assert "the format the command expects" in driver_message

        with pytest.raises(TypeError) as worker:
            require_path_value(
                value=_ref_of_kind("table"),
                annotation_label="file",
                label="summarise.scores",
                execution_mode="worker",
            )
        assert "Annotate it `object`" in str(worker.value)

    def test_path_backed_payload_detection(self, tmp_path: Path) -> None:
        """Only genuine path payloads count as path-backed, per kind."""
        from ginkgo.runtime.artifacts.asset_kinds import is_path_backed_payload

        csv_path = tmp_path / "frame.csv"
        csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

        # A table given a path — by Path or by str — reads from disk.
        assert is_path_backed_payload(**_payload_probe(table(csv_path)))
        assert is_path_backed_payload(**_payload_probe(table(str(csv_path))))

        # In-memory payloads are not path-backed, including an inline string
        # for text() — which shares its sub-kinds with the Path form.
        assert not is_path_backed_payload(**_payload_probe(table(pd.DataFrame({"a": [1]}))))
        assert not is_path_backed_payload(**_payload_probe(text("some notes")))
        assert not is_path_backed_payload(**_payload_probe(text("# heading", format="markdown")))
        assert not is_path_backed_payload(**_payload_probe(array(np.zeros(3))))

        # A text() payload given as a Path is path-backed despite the shared
        # sub-kind, which is why sub_kind alone cannot decide this.
        md_path = tmp_path / "notes.md"
        assert is_path_backed_payload(**_payload_probe(text(md_path)))

    def test_file_annotation_rejects_table_asset_input(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A bare ``file`` parameter fed a table asset names the kind."""
        monkeypatch.chdir(tmp_path)

        with pytest.raises(TypeError, match=r"annotated `file` but is a `table` asset"):
            ginkgo.evaluate(
                consume_table_as_file(
                    summary=produce_table_from_csv(output_path="results/summary.csv")
                )
            )

    def test_file_return_annotation_rejects_table_asset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``-> file`` returning ``table(...)`` names the kind, not path syntax."""
        monkeypatch.chdir(tmp_path)

        with pytest.raises(TypeError) as excinfo:
            ginkgo.evaluate(return_table_from_file_annotation())

        message = str(excinfo.value)
        assert "annotated `file` but is a `table` asset" in message
        assert "must not contain spaces" not in message

    def test_file_annotation_rejects_non_path_value(self) -> None:
        """A non-path, non-asset value bound to ``file`` names the type."""
        from ginkgo.runtime.task_validation import TaskValidator

        validator = TaskValidator()
        with pytest.raises(TypeError, match=r"received a pandas\.DataFrame"):
            validator.validate_annotated_value(
                annotation=file,
                value=pd.DataFrame({"x": [1]}),
                label="demo.summary",
            )


# ---------------------------------------------------------------------------
# Path-based wrapper outputs (shell / notebook / script)
# ---------------------------------------------------------------------------


@task(kind="shell")
def shell_write_fig_task(*, output_path: str) -> object:
    from ginkgo import shell

    cmd = (
        f'python -c "import struct, zlib; open({output_path!r}, \\"wb\\").write('
        'b\\"\\\\x89PNG\\\\r\\\\n\\\\x1a\\\\n\\")"'
    )
    return shell(cmd=cmd, output=fig(output_path, name="plot"))


@task(kind="shell")
def shell_write_table_task(*, output_path: str) -> object:
    from ginkgo import shell

    return shell(
        cmd=f'python -c "open({output_path!r}, \\"w\\").write(\\"a,b\\\\n1,2\\\\n\\")"',
        output=table(output_path, name="data"),
    )


@task(kind="shell")
def shell_wrapper_bad_payload_task(*, output_path: str) -> object:
    from ginkgo import shell

    # Construct a fig AssetResult with an in-memory payload, bypassing
    # the factory's sub-kind detection. Simulates a workflow author
    # mistakenly putting a non-path wrapper into shell/notebook outputs.
    bad_wrapper = AssetResult(payload=object(), kind="fig", sub_kind="matplotlib")
    return shell(
        cmd=f"touch {output_path}",
        output=bad_wrapper,
    )


class TestPathWrappedOutputs:
    """Shell / notebook / script tasks can declare outputs via ``fig(path)`` etc."""

    def test_shell_fig_path_produces_fig_asset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "plot.png"

        result = ginkgo.evaluate(shell_write_fig_task(output_path=str(output)))

        assert isinstance(result, AssetRef)
        assert result.key.namespace == "fig"
        assert result.key.name == "plot"
        assert output.is_file()

    def test_shell_table_path_produces_table_asset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "data.csv"

        result = ginkgo.evaluate(shell_write_table_task(output_path=str(output)))

        assert isinstance(result, AssetRef)
        assert result.key.namespace == "table"
        assert result.key.name == "data"

    def test_wrapper_with_in_memory_payload_in_outputs_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``fig(dataframe)`` declared in outputs must raise a clear error."""
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "irrelevant.png"

        with pytest.raises(TypeError, match="wrap a declared file path"):
            ginkgo.evaluate(shell_wrapper_bad_payload_task(output_path=str(output)))

    def test_iter_output_values_handles_wrappers(self, tmp_path: Path) -> None:
        """Unit check: iter_output_values extracts paths from path-wrapped results."""
        from ginkgo.runtime.task_runners.shell import iter_output_values

        png = tmp_path / "figure.png"
        png.write_bytes(b"\x89PNG\r\n\x1a\nfake")
        csv = tmp_path / "frame.csv"
        csv.write_text("a\n1\n", encoding="utf-8")

        paths = iter_output_values([fig(png), table(csv, name="raw")])
        assert paths == [png, csv]
