# Assets

Ginkgo includes a file-backed asset catalog layered over the cache and
artifact store. Assets add stable logical identity and lineage to managed
outputs without changing the run-centric execution model.

The asset layer is implemented by:

- `ginkgo/core/asset.py` for the public asset types and the `asset()`
  constructor plus shorthand factories
- `ginkgo/runtime/artifacts/asset_kinds.py` for the kind registry that
  drives dispatch across detection, serialisation, and loading
- `ginkgo/runtime/artifacts/asset_serialization.py` for the per-kind
  byte encoders invoked at registration time
- `ginkgo/runtime/artifacts/asset_loaders.py` for the per-kind
  rehydration path used by the CLI and the evaluator
- `ginkgo/runtime/artifacts/asset_store.py` for the local catalog
  metadata store
- `ginkgo/runtime/artifacts/asset_registration.py` for the evaluator
  integration that turns sentinels into resolved references

The current asset model supports:

- a single `AssetResult` sentinel with a `kind` discriminator (one of
  `file`, `table`, `array`, `fig`, `text`, `model`)
- `asset(payload, kind=..., name=..., metadata=..., **kind_fields)` as
  the canonical constructor, with `table`, `array`, `fig`, `text`, and
  `model` as kind-preset shorthand factories
- immutable `AssetVersion` records keyed by logical `AssetKey`
- resolved `AssetRef` values passed to downstream tasks
- alias pointers and version history in `.ginkgo/assets/`
- upstream lineage edges recorded from consumed `AssetRef` inputs
- provenance records that include asset metadata alongside cache keys
  and artifact identifiers

The catalog is metadata-only. Asset bytes are never stored in the asset
store itself; every asset version points to an immutable `artifact_id`
in the artifact store. This keeps three identities distinct:

- logical asset identity (`AssetKey`)
- physical materialization (`artifact_id`)
- cache entry identity (`cache_key`)

`AssetRef` values participate directly in cache and transport semantics.
The value codec can serialize and deserialize them, cache metadata
summarizes them recursively, and downstream cache invalidation follows
`AssetRef.version_id` rather than re-hashing artifact bytes.

The current implementation is intentionally narrow:

- assets do not drive scheduling
- the asset store is local and file-backed

Staleness reporting and asset-aware lifecycle policy remain future work.

## Asset Sentinels

Task bodies tag return values as immutable assets by wrapping them with
`asset()` or one of the kind-preset shorthand factories. Every factory
returns the same `AssetResult` dataclass; the `kind` field determines
downstream behaviour.

### Kinds

- **`file`** is the fallback kind for managed bytes whose semantic shape
  Ginkgo does not track. The payload must be a path; the registrar
  copies the bytes into the artifact store verbatim. Constructed with
  `asset(path, ...)`.
- **`table`** — pandas, polars (eager or lazy), pyarrow Table/Dataset,
  DuckDB relation, or CSV/TSV path. Stored as Parquet. Shorthand:
  `table(payload, name=..., metadata=...)`.
- **`array`** — numpy, xarray, zarr, or dask. Stored as a zipped zarr
  store when the `zarr` package is installed, or as a `.npy` blob
  otherwise for numpy-only inputs. Non-numpy backends require `zarr`.
  Shorthand: `array(payload, name=..., metadata=...)`.
- **`fig`** — matplotlib (PNG), plotly (HTML), bokeh (HTML), or a path
  to an existing PNG/SVG/HTML file. Shorthand:
  `fig(payload, name=..., metadata=...)`.
- **`text`** — string, dict (stored as JSON), or a `Path` to a text
  document. Format is `{plain, markdown, json}`. Shorthand:
  `text(payload, name=..., format=..., metadata=...)`.
- **`model`** — trained ML models. Supports scikit-learn estimators,
  xgboost/lightgbm sklearn wrappers (via `joblib`), PyTorch
  `nn.Module` (via `torch.save`), and Keras/TensorFlow (via the native
  `.keras` archive). `metrics` is a first-class `dict[str, float]`
  field stored on the asset version so the UI and `ginkgo models` can
  render training metrics without walking free-form metadata. All ML
  backends are user-managed dependencies, lazy-imported at
  serialisation/load time. Shorthand:
  `model(payload, name=..., framework=..., metrics=..., metadata=...)`.

`file` is not a peer of the semantic kinds — it is the fallback kind
for typed-unknown bytes. The semantic kinds can all be file-backed at
construction (a CSV is a valid `table`, a PNG is a valid `fig`) but the
kind tag always determines serialization format, preview renderer,
loader, and rehydration behaviour downstream.

### Equivalence of `asset()` and shorthand factories

`asset(df, kind="table")` and `table(df)` produce identical
`AssetResult` values. `asset()` is the canonical constructor that
dispatches through the kind registry; the shorthands are one-line
wrappers that pre-fill `kind=` and forward their kind-specific keyword
arguments (`format=` for `text`, `framework=` / `metrics=` for
`model`).

### `AssetResult` shape

`AssetResult` is kind-agnostic:

- `payload` — the user-provided value (path or live object).
- `kind` — one of the registered kinds.
- `sub_kind` — the detected backend (`"pandas"`, `"matplotlib"`,
  `"sklearn"`, …); `None` for `file`.
- `name` — optional explicit local asset name.
- `metadata` — free-form user metadata persisted on the version.
- `kind_fields` — a `dict[str, Any]` bag carrying kind-specific
  construction-time fields (`format` for `text`, `framework` and
  `metrics` for `model`). The keys are scoped to the kind's
  serializer/loader and do not leak into user code.

Path-backed payloads also work as asset shorthand in declared outputs
of `shell` / `notebook` / `script` tasks. `fig("results/fig_pca.png",
name="pca")` and `table("data/frame.csv", name="frame")` may appear in
the `outputs=` list alongside plain strings; the runner validates that
the declared path exists after execution and the registrar stores the
bytes under the kind's namespace so the report and `ginkgo inspect run`
render rich previews. In-memory payloads (e.g. `fig(matplotlib_figure)`)
are invalid in declared outputs and raise a clear error — they remain
valid as Python-task return values where the evaluator serialises the
object directly.

## Kind Registry

Dispatch for every registered kind is centralised in a single
module-level table:

```python
# ginkgo/runtime/artifacts/asset_kinds.py

@dataclass(frozen=True, kw_only=True)
class AssetKindSpec:
    kind: str
    detect: Callable[..., tuple[Any, str | None, dict[str, Any]]]
    serializer: Callable[..., SerializedAsset] | None
    loader: Callable[..., Any] | None
    rehydrate_on_receive: bool
    default_name_strategy: str

ASSET_KINDS: dict[str, AssetKindSpec] = { ... }
```

- `detect(payload, **kind_fields) -> (payload, sub_kind, kind_fields)`
  is called from `asset()` to probe sub-kind and normalise the payload
  (e.g. dict → JSON for `text`).
- `serializer(result)` is called from `AssetRegistrar` for non-`file`
  kinds; `file` assets skip this path since the registrar copies the
  source path's bytes directly.
- `loader(artifact_store=..., artifact_id=..., metadata=...)` rehydrates
  stored bytes into a live Python value for the CLI and evaluator.
- `rehydrate_on_receive` flags kinds that the evaluator should
  auto-rehydrate when an `AssetRef` is passed as a task argument
  (everything except `file` and `fig`).
- `default_name_strategy` is `"task_name"` for `file` (the task
  function's name is the default when no explicit `name` is supplied)
  and `"kind_index"` for every other kind (per-kind counter producing
  `<task>.<kind>[<index>]`).

Adding a new asset kind is a pure-registry change: register one entry
in `ASSET_KINDS` with the four callables and an optional shorthand
factory in `core/asset.py`. No changes are needed in the constructor,
registrar, value codec, or task runners.

## Registration

`AssetRegistrar.materialize_results` walks a task's return value, finds
every nested `AssetResult`, and replaces it with a resolved `AssetRef`:

1. A validation pre-walk checks that all explicitly-named non-file
   assets have unique `(kind, name)` within the task, so a duplicate
   leaves no partial catalog state.
2. For each `AssetResult`, the registrar looks up the kind's spec:
   - For `file` kinds it copies the source path into the artifact
     store.
   - For semantic kinds it invokes the registered serializer, stores
     the bytes via `ArtifactStore.store_bytes`, and extracts the
     per-kind metadata returned by the serializer.
3. An immutable `AssetVersion` is registered under the kind-scoped
   namespace (`file` / `table` / `array` / `fig` / `text` / `model`),
   and the sentinel is replaced with an `AssetRef` pointing at the
   stored artifact.
4. Upstream lineage is recorded for any consumed `AssetRef` inputs.
5. For kinds flagged as `rehydrate_on_receive` (all except `file` and
   `fig`), the producer's live Python object is stashed in the
   per-evaluator `LivePayloadRegistry` keyed by `artifact_id`, so a
   downstream task in the same process can consume it without a disk
   round-trip.

Named outputs use the asset key `<task_fn>.<name>`. Unnamed non-file
outputs are indexed per kind as `<task_fn>.<kind>[<index>]`. Unnamed
file outputs fall back to `<task_fn>` as their name. Duplicate
explicit names within a single task raise a `ValueError` before any
artifact is written. Serialisation errors surface as
`AssetSerializationError` identifying the offending result by name and
index.

## Kind-specific metadata

Each `AssetVersion` records kind-specific metadata alongside user
metadata:

- `table`: `sub_kind`, `schema`, `row_count`, `byte_size`
- `array`: `sub_kind`, `shape`, `dtype`, `chunks`, `coordinates`,
  `byte_size`
- `fig`: `sub_kind`, `source_format`, `byte_size`, `dimensions`
- `text`: `sub_kind`, `format`, `byte_size`, `line_count`
- `model`: `sub_kind`, `framework`, `metrics`, `byte_size`

`ginkgo asset show <key>` renders this metadata through the CLI without
re-reading the stored bytes. The UI asset payload surfaces the same
fields under a `kind_metadata` key for future frontend consumers.

## Rehydration on receive

Downstream tasks that declare `pd.DataFrame`, `np.ndarray`, `str`, or a
trained-model parameter receive the live Python object rather than the
`AssetRef` produced by an upstream semantic asset. The evaluator
rehydrates wrapped refs in `_resolve_task_args` via
`_rehydrate_wrapped_refs`, which consults a per-run
`LivePayloadRegistry` before falling back to the on-disk loader path
(`asset_loaders.load_from_ref`). The set of rehydratable kinds is
derived from the registry (`REHYDRATABLE_KINDS`) rather than hard-coded.

The live registry (`ginkgo/runtime/artifacts/live_payloads.py`) is a
capped-LRU cache keyed by `artifact_id`. When `AssetRegistrar`
serialises a wrapped payload it also stores the producer's Python
object in the registry, so a subsequent consumer in the same evaluator
process is served from memory and avoids a Parquet/zarr round-trip.
The on-disk loader path is the fallback for subprocess workers, cache
resumes, and cross-run consumers. `fig` refs are left as `AssetRef`
since binary image payloads are rarely consumed as live Python objects.
`file` refs flow through the existing `file` coercion path.

Rehydration is transparent to task authors: a task annotated
`compounds: pd.DataFrame` continues to work unchanged when its upstream
switches from returning a raw DataFrame to `table(df, name="...")`.
The examples in `examples/chem/.../inputs.py::annotate_compounds` and
`examples/retail/.../inputs.py::enrich_orders` demonstrate this pattern
in a real multi-stage workflow.

## Wire transport

Non-file `AssetResult` payloads (DataFrames, arrays, figures, text
bodies, trained models) need byte-exact round-trip so the driver-side
registrar sees the producer's original Python object. The generic
Parquet encoder in `_encode_bytes` silently turns a `RangeIndex` into
a plain `Index`, which would perturb downstream cache-key hashing and
break cache reuse. The value codec therefore pickles non-file payloads
wholesale under a `pickled_payload` wire tag when encoding an
`AssetResult`, keeping type and metadata identity intact across the
worker→driver hop. File payloads retain the existing path-based
encoding so `remote_arg_transfer` can stage them to a remote artifact
store.
