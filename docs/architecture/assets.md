# Assets

Ginkgo includes a file-backed asset catalog layered over the cache and
artifact store. Assets add stable logical identity and lineage to managed
outputs without changing the run-centric execution model.

The asset layer is implemented by:

- `ginkgo/core/asset.py` for the public asset types and builders
- `ginkgo/runtime/artifacts/asset_store.py` for the local catalog metadata store
- evaluator integration in `ginkgo/runtime/evaluator.py`

The current asset model supports:

- `asset(path, name=..., metadata=...)` as a task return wrapper for file
  outputs
- immutable `AssetVersion` records keyed by logical `AssetKey`
- resolved `AssetRef` values passed to downstream tasks
- alias pointers and version history in `.ginkgo/assets/`
- upstream lineage edges recorded from consumed `AssetRef` inputs
- provenance records that include asset metadata alongside cache keys and
  artifact identifiers

The catalog is metadata-only. Asset bytes are never stored in the asset store
itself; every asset version points to an immutable `artifact_id` in the
artifact store. This keeps three identities distinct:

- logical asset identity (`AssetKey`)
- physical materialization (`artifact_id`)
- cache entry identity (`cache_key`)

`AssetRef` values participate directly in cache and transport semantics. The
value codec can serialize and deserialize them, cache metadata summarizes them
recursively, and downstream cache invalidation follows `AssetRef.version_id`
rather than re-hashing artifact bytes.

The current implementation is intentionally narrow:

- file assets and the wrapped asset kinds (`table`, `array`, `fig`, `text`)
  are supported
- assets do not drive scheduling
- the asset store is local and file-backed

Model assets, staleness reporting, and asset-aware lifecycle policy remain
future work.

## Wrapped Asset Sentinels

Task bodies can tag selected return values as immutable assets with
kind-specific metadata by wrapping them with one of four sentinel factories:

- `table(df, name=..., metadata=...)` — pandas, polars (eager or lazy),
  pyarrow Table/Dataset, DuckDB relation, or CSV/TSV path. Stored as Parquet.
- `array(arr, name=..., metadata=...)` — numpy, xarray, zarr, or dask. Stored
  as a zipped zarr store when the `zarr` package is installed, or as a `.npy`
  blob otherwise for numpy-only inputs. Non-numpy backends require `zarr`.
- `fig(figure, name=..., metadata=...)` — matplotlib (PNG), plotly (HTML),
  bokeh (HTML), or a path to an existing PNG/SVG/HTML file.
- `text(body, name=..., format=..., metadata=...)` — string, dict (stored as
  JSON), or a `Path` to a text document. Format is `{plain, markdown, json}`.
- `model(clf, name=..., framework=..., metrics=..., metadata=...)` — trained
  ML models. Supports scikit-learn estimators, xgboost/lightgbm sklearn
  wrappers (all via `joblib`), PyTorch `nn.Module` (via `torch.save`), and
  Keras/TensorFlow (via the native `.keras` archive). `metrics` is a
  first-class `dict[str, float]` field stored on the asset version so the
  UI and `ginkgo models` can render training metrics without walking
  free-form metadata. All ML backends (`joblib`/`scikit-learn`,
  `torch`, `keras`, `xgboost`, `lightgbm`) are user-managed
  dependencies, lazy-imported at serialisation/load time.

Wrappers follow the same pattern as `shell()`/`ShellExpr`: the user calls a
factory inside the task body, returns the sentinel, and the evaluator
replaces it with a resolved `AssetRef` after registering the serialised
payload with the artifact store.

Implementation is split between:

- `ginkgo/core/wrappers.py` — sentinel dataclasses and factories, with
  sub-kind detection via MRO walks (no optional backend imports at
  construction).
- `ginkgo/runtime/artifacts/wrapper_serialization.py` — per-kind serializers
  producing Parquet/zarr/PNG/HTML/text bytes plus kind-specific metadata.
- `ginkgo/runtime/artifacts/wrapper_loaders.py` — a small loader registry
  used by the CLI read path and any future programmatic consumers.
- `ginkgo/runtime/artifacts/asset_registration.py` — extended to unwrap
  sentinels at task completion, serialising each payload, storing the bytes
  through `ArtifactStore.store_bytes`, and registering an `AssetVersion` in
  the wrapper-specific namespace (`table` / `array` / `fig` / `text` /
  `model`). Dict return values are walked recursively so sentinels nested
  inside task-level result dicts register alongside list/tuple outputs.

Named outputs use the asset key `<task_fn>.<name>`. Unnamed outputs are
indexed per kind as `<task_fn>.<kind>[<index>]`. Duplicate explicit names
within a single task raise a `ValueError` before any artifact is written.
Serialisation errors surface as `WrapperSerializationError` identifying the
offending wrapper by name and index.

Kind-specific metadata stored on each `AssetVersion`:

- `table`: `sub_kind`, `schema`, `row_count`, `byte_size`
- `array`: `sub_kind`, `shape`, `dtype`, `chunks`, `coordinates`, `byte_size`
- `fig`: `sub_kind`, `source_format`, `byte_size`, `dimensions`
- `text`: `sub_kind`, `format`, `byte_size`, `line_count`
- `model`: `sub_kind`, `framework`, `metrics`, `byte_size`

`ginkgo asset show <key>` renders this metadata through the CLI without
re-reading the stored bytes. The UI asset payload surfaces the same fields
under a `kind_metadata` key for future frontend consumers.

### Rehydration on receive

Downstream tasks that declare `pd.DataFrame`, `np.ndarray`, `str`, or a
trained-model parameter receive the live Python object rather than the
`AssetRef` produced by an upstream wrapper. The evaluator rehydrates wrapped refs
in `_resolve_task_args` via `_rehydrate_wrapped_refs`, which consults a
per-run `LivePayloadRegistry` before falling back to the on-disk
`wrapper_loaders.load_from_ref` path.

The live registry
(`ginkgo/runtime/artifacts/live_payloads.py`) is a capped-LRU cache
keyed by `artifact_id`. When `AssetRegistrar` serialises a wrapped
payload it stores the producer's Python object in the registry, so a
subsequent consumer in the same evaluator process is served from memory
and avoids a Parquet/zarr round-trip. The on-disk loader path is the
fallback for subprocess workers, cache resumes, and cross-run
consumers. `fig` refs are left as `AssetRef` since binary image
payloads are rarely consumed as live Python objects. `file` refs are
untouched — the existing `file` coercion path handles them.

Rehydration is transparent to task authors: a task annotated
`compounds: pd.DataFrame` continues to work unchanged when its upstream
switches from returning a raw DataFrame to `table(df, name="...")`.
The examples in `examples/chem/.../inputs.py::annotate_compounds` and
`examples/retail/.../inputs.py::enrich_orders` demonstrate this pattern
in a real multi-stage workflow.
