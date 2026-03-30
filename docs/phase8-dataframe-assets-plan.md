# Phase 8 — Versioned DataFrame and Figure Assets

## Problem Definition

Data science workflows frequently produce two categories of output that
deserve richer treatment than opaque file blobs:

1. **DataFrames** — tabular data that benefits from schema capture, row count
   tracking, parent-child version chains, multi-library support, and cheap
   cache invalidation via snapshot identity rather than in-memory hashing.
2. **Figures** — plots and visualizations (often interactive) that are
   critical for experiment analysis but currently buried in run output
   directories with no versioning or browsable UI.

Phase 7 introduces the `asset()` wrapper and asset catalog with file-based
serialization. This phase extends Phase 7's kind serializer registry with
`"dataframe"` and `"figure"` backends that provide smart serialization,
metadata capture, and UI preview for both types.

---

## Proposed Solution

Register `"dataframe"` and `"figure"` kind serializers and loaders in Phase 7's
asset infrastructure. The evaluator auto-detects known types in `asset()`
return values and routes them to the appropriate serializer. No new task kinds
or decorators are needed — the DSL surface is just `asset(df)` and
`asset(fig)`.

The design has seven parts:

1. **DataFrame serializer** — pandas/polars/pyarrow -> Parquet with metadata
2. **DataFrame loader** — multi-library `.load()` with type preservation
3. **Snapshot manifest** — tabular-specific metadata in `AssetVersion`
4. **Figure serializer** — matplotlib/plotly/bokeh/altair -> PNG/HTML/JSON
5. **Figure UI preview** — inline rendering with interactive figure support
6. **UI data preview** — inline tabular preview for DataFrames
7. **CLI formatting** — kind-specific columns in asset commands

---

## Target DSL

```python
from ginkgo import task, flow, file, asset

@task()
def prepare_features(*, raw_data: file):
    import pandas as pd
    df = pd.read_csv(str(raw_data))
    df["feature_1"] = df["value"] * 2
    return asset(df)

@task()
def enrich_features(*, features: asset, lookup: file):
    df = features.load()
    lookup_df = pd.read_csv(str(lookup))
    merged = df.merge(lookup_df, on="id")
    return asset(merged, name="enriched_features")

# Polars works the same way
@task()
def prepare_with_polars(*, raw_data: file):
    import polars as pl
    df = pl.read_csv(str(raw_data))
    return asset(df)

# Regular task consuming a DataFrame asset
@task()
def train_model(*, features: asset) -> file:
    df = features.load()            # returns pandas (original library)
    df = features.load(as_polars=True)  # or force polars
    # ... training logic ...

# Per-sample fan-out
@task()
def process_sample(*, raw: file, sample_id: str):
    df = pd.read_csv(str(raw))
    return asset(df, name=f"features/{sample_id}")

# --- Figures ---

# Static figure (matplotlib)
@task()
def plot_distribution(*, features: asset):
    import matplotlib.pyplot as plt
    df = features.load()
    fig, ax = plt.subplots()
    ax.hist(df["value"], bins=50)
    ax.set_title("Value Distribution")
    return asset(fig, name="value_distribution")

# Interactive figure (plotly)
@task()
def plot_interactive(*, features: asset):
    import plotly.express as px
    df = features.load()
    fig = px.scatter(df, x="feature_1", y="value", color="category")
    return asset(fig, name="feature_scatter")

# Multiple outputs — one asset figure, one non-asset report
@task()
def analyze(*, features: asset) -> file:
    fig = make_plot(features.load())
    report = write_report(features.load())
    return asset(fig, name="analysis_plot"), report

@flow
def main():
    raw = prepare_features(raw_data=file("data/raw.csv"))
    enriched = enrich_features(features=raw, lookup=file("data/lookup.csv"))
    dist = plot_distribution(features=enriched)
    scatter = plot_interactive(features=enriched)
    return enriched
```

After execution:

```
$ ginkgo asset versions dataframe/prepare_features
VERSION     CREATED              ROWS    COLS   SIZE      PARENT
v-a1b2c3    2026-03-28 14:22     10000   12     2.1 MB    -
v-d4e5f6    2026-03-29 10:15     12500   12     2.6 MB    v-a1b2c3

$ ginkgo asset inspect dataframe/prepare_features@v-d4e5f6
Asset:        dataframe/prepare_features
Version:      v-d4e5f6
Parent:       v-a1b2c3
Created:      2026-03-29T10:15:00Z
Run:          run-20260329-101500
Artifact:     abc123def.parquet
Rows:         12500
Columns:      12
Library:      pandas
Schema:
  id            int64
  value         float64
  feature_1     float64
  ...
```

---

## Part 1 — DataFrame Serializer

**File:** `ginkgo/runtime/asset_serializers.py`

Registered in Phase 7's kind serializer registry as `"dataframe"`.

### Multi-library support

The serializer accepts pandas, polars, and pyarrow table types and normalizes
through PyArrow as the common serialization path:

| Input type | Serialization path |
|---|---|
| `pandas.DataFrame` | `pyarrow.Table.from_pandas(df)` -> Parquet |
| `polars.DataFrame` | `df.to_arrow()` -> Parquet |
| `pyarrow.Table` | direct Parquet write |

The original library type is recorded in metadata as
`source_library: "pandas" | "polars" | "pyarrow"`.

### Kind auto-detection

Phase 7's kind auto-detection is extended to recognize DataFrame types:

```python
def _detect_kind(value: Any) -> str | None:
    # Phase 7 detections (str/Path -> "file")
    ...
    # Phase 8 additions
    if _is_pandas_dataframe(value):
        return "dataframe"
    if _is_polars_dataframe(value):
        return "dataframe"
    if _is_pyarrow_table(value):
        return "dataframe"
    return None
```

Detection uses `type(value).__module__` and `type(value).__name__` checks to
avoid importing pandas/polars/pyarrow when they aren't used.

### Serialization

```python
class DataFrameSerializer:
    """Serialize DataFrame-like objects to Parquet via PyArrow."""

    def serialize(
        self, value: Any, *, artifact_store: ArtifactStore
    ) -> tuple[str, str, dict]:
        """Serialize a DataFrame and return (artifact_id, content_hash, metadata).

        Parameters
        ----------
        value
            pandas.DataFrame, polars.DataFrame, or pyarrow.Table.
        artifact_store
            The workspace artifact store for immutable storage.

        Returns
        -------
        tuple[str, str, dict]
            artifact_id, content_hash, and DataFrame-specific metadata.
        """
        # 1. Detect source library
        # 2. Convert to PyArrow Table
        # 3. Write to temp Parquet file
        # 4. Store via artifact_store.store()
        # 5. Capture schema, row count, column count, byte size
        # 6. Return (artifact_id, content_hash, metadata)
```

Metadata captured during serialization:

```python
{
    "source_library": "pandas",
    "row_count": 12500,
    "column_count": 12,
    "byte_size": 2621440,
    "schema": {
        "id": "int64",
        "value": "float64",
        "feature_1": "float64",
        "category": "object",
    },
    "parent_version_id": "v-a1b2c3",   # from asset_store.get_latest()
}
```

---

## Part 2 — DataFrame Loader

Registered in Phase 7's loader registry as `"dataframe"`.

```python
class DataFrameLoader:
    """Load Parquet artifacts as DataFrames."""

    def load(self, artifact_path: Path, **kwargs: Any) -> Any:
        """Load a Parquet artifact as a DataFrame.

        By default returns the same library type that produced the
        snapshot (recorded in metadata as source_library). Use keyword
        flags to force a specific return type.

        Parameters
        ----------
        artifact_path
            Local path to the Parquet file.
        as_pandas
            Force return as pandas.DataFrame.
        as_polars
            Force return as polars.DataFrame.
        as_arrow
            Force return as pyarrow.Table.

        Returns
        -------
        pandas.DataFrame | polars.DataFrame | pyarrow.Table
        """
```

Return type logic:

| `source_library` | No flag | `as_pandas` | `as_polars` | `as_arrow` |
|---|---|---|---|---|
| `"pandas"` | pandas | pandas | polars | pyarrow |
| `"polars"` | polars | pandas | polars | pyarrow |
| `"pyarrow"` | pyarrow | pandas | polars | pyarrow |

Libraries are lazy-imported. Only the library the user actually uses needs to
be installed. Requesting a library that isn't installed raises a clear error.

Parquet is the canonical on-disk format regardless of source library. This
means all three can read each other's snapshots — a pandas-producing task's
output can be consumed as a polars DataFrame downstream.

---

## Part 3 — Snapshot Manifest

Each `AssetVersion` for a DataFrame asset carries tabular-specific fields in
its `metadata` dict:

```yaml
# .ginkgo/assets/dataframe/features/versions/v-d4e5f6/meta.yaml
version_id: "v-d4e5f6"
asset_key:
  namespace: "dataframe"
  name: "features"
run_id: "run-20260329-101500"
task_id: "task_0002"
created_at: "2026-03-29T10:15:00Z"
content_hash: "sha256:abc123..."
artifact_id: "abc123def.parquet"
metadata:
  source_library: "pandas"
  parent_version_id: "v-a1b2c3"
  row_count: 12500
  column_count: 12
  byte_size: 2621440
  schema:
    id: "int64"
    value: "float64"
    feature_1: "float64"
    category: "object"
```

Design notes:

- The snapshot ID **is** the `version_id` from Phase 7. No separate identity
  scheme.
- `parent_version_id` creates a linked list of snapshots per asset key. The
  parent is the most recent existing version at materialization time.
- Schema is a column-name-to-dtype mapping — enough for inspection and drift
  detection, not a full Iceberg schema evolution system.

---

## Part 4 — Figure Serializer

**File:** `ginkgo/runtime/asset_serializers.py`

Registered in Phase 7's kind serializer registry as `"figure"`.

### Supported libraries

| Library | Detection | Storage format | UI rendering |
|---|---|---|---|
| matplotlib (`Figure`) | `matplotlib.figure.Figure` | PNG (default) or SVG | `<img>` tag |
| plotly (`Figure`) | `plotly.graph_objs.Figure` | Self-contained HTML | `<iframe>` |
| bokeh (`Model`, `Document`) | `bokeh.model.Model` | Self-contained HTML | `<iframe>` |
| altair (`Chart`, `LayerChart`, etc.) | `altair.TopLevelMixin` | Vega-Lite JSON spec | vega-embed JS |

Detection uses `type(value).__module__` checks to avoid importing libraries
that aren't used.

### Serialization

```python
class FigureSerializer:
    """Serialize figure objects to their canonical display format."""

    def serialize(
        self, value: Any, *, artifact_store: ArtifactStore
    ) -> tuple[str, str, dict]:
        """Serialize a figure and return (artifact_id, content_hash, metadata)."""
```

Serialization paths:

- **matplotlib**: `fig.savefig(path, format="png", dpi=150, bbox_inches="tight")`.
  Users can request SVG via `asset(fig, metadata={"format": "svg"})`.
- **plotly**: `fig.to_html(full_html=True, include_plotlyjs="cdn")` for a
  self-contained HTML file. Also stores a static PNG thumbnail for list views.
- **bokeh**: `bokeh.embed.file_html(model, resources=CDN)` for self-contained
  HTML. Also stores a PNG thumbnail via `bokeh.io.export_png` if selenium is
  available (falls back to no thumbnail).
- **altair**: `chart.to_dict()` serialized as JSON. The UI renders it via
  vega-embed. Also stores a PNG thumbnail via `chart.save(format="png")` if
  altair_saver is available.

### Metadata

```python
{
    "source_library": "plotly",
    "format": "html",              # "png", "svg", "html", "vega-lite"
    "interactive": True,           # True for plotly/bokeh/altair
    "width": 800,                  # pixels (when available)
    "height": 600,
    "thumbnail_artifact_id": "...",  # PNG thumbnail for list views (optional)
}
```

### Kind auto-detection

Phase 7's kind auto-detection is extended:

```python
def _detect_kind(value: Any) -> str | None:
    # Phase 7: str/Path -> "file"
    # Phase 8 DataFrame detections...
    if _is_matplotlib_figure(value):
        return "figure"
    if _is_plotly_figure(value):
        return "figure"
    if _is_bokeh_model(value):
        return "figure"
    if _is_altair_chart(value):
        return "figure"
    return None
```

### Figure loader

```python
class FigureLoader:
    """Load figure artifacts."""

    def load(self, artifact_path: Path, **kwargs: Any) -> Any:
        """Load a figure artifact.

        Returns the raw file path by default. For interactive figures
        (HTML format), the path can be opened in a browser. For
        matplotlib PNGs, returns a PIL Image if pillow is available,
        otherwise the Path.
        """
```

Figures are primarily consumed through the UI rather than programmatically.
The `.load()` method returns the artifact path, which is sufficient for
programmatic access (open in browser, embed in notebook, etc.).

---

## Part 5 — Figure UI Preview

The UI is where figure assets really shine. The Assets sidebar renders figures
inline:

### List view

- Figure assets show a small thumbnail (64x64) in the asset list alongside
  name, version, and alias badges.
- Thumbnails come from the stored PNG thumbnail artifact. For static figures
  (matplotlib), the main artifact IS the thumbnail. For interactive figures
  (plotly/bokeh/altair), a separate thumbnail is stored during serialization.

### Version detail — figure rendering

- **Static figures** (matplotlib PNG/SVG): rendered as a full-size `<img>` tag,
  zoomable.
- **Interactive figures** (plotly HTML, bokeh HTML): rendered in a sandboxed
  `<iframe>` that preserves full interactivity — hover tooltips, zoom, pan,
  selection, etc.
- **Altair figures** (Vega-Lite JSON): rendered using the vega-embed JavaScript
  library loaded in the UI frontend. Full interactivity preserved.

### API routes

```
GET /workspaces/{id}/assets/figure/{name}/{ver}/render
```

Returns the stored artifact directly with the appropriate `Content-Type`:
- `image/png` for matplotlib PNG
- `image/svg+xml` for matplotlib SVG
- `text/html` for plotly/bokeh HTML
- `application/json` for altair Vega-Lite specs

The UI frontend handles rendering based on the content type and the
`interactive` flag in the asset metadata.

### Version comparison

When viewing a figure asset's version list, the UI can show thumbnails side by
side for quick visual comparison across versions — useful for tracking how a
plot changes as upstream data evolves.

---

## Part 6 — DataFrame UI Data Preview

Extend Phase 7's Assets sidebar with DataFrame-specific features:

- **Version list**: add rows, columns, and size columns.
- **Version detail**: schema table, parent link, byte size, source library.
- **Data preview**: inline tabular preview showing the first N rows as a
  sortable, scrollable table. For large assets, a banner shows "Showing 50 of
  125,000 rows" with summary statistics alongside.
- **Schema diff** (stretch goal): when viewing a version with a parent,
  highlight schema changes (added/removed/changed columns).

### Data preview API

```
GET /workspaces/{id}/assets/dataframe/{name}/{ver}/preview?rows=50&offset=0
```

Response:

```json
{
  "columns": ["id", "value", "feature_1"],
  "dtypes": {"id": "int64", "value": "float64", "feature_1": "float64"},
  "rows": [[1, 3.14, 6.28], [2, 2.71, 5.42]],
  "total_rows": 125000,
  "returned_rows": 50,
  "summary": {
    "id": {"min": 1, "max": 125000, "nulls": 0},
    "value": {"min": 0.01, "max": 99.99, "mean": 42.3, "nulls": 12},
    "feature_1": {"min": 0.02, "max": 199.98, "mean": 84.6, "nulls": 12}
  }
}
```

The preview reads directly from the Parquet artifact using PyArrow's
row-group-aware slicing, so it never loads the full dataset into memory.
Summary statistics are computed lazily on first request and cached in a
`preview_cache.json` alongside the version metadata.

---

## Part 7 — CLI Formatting

Phase 7's `ginkgo asset` commands already work for DataFrame assets. This
phase adds DataFrame-specific formatting:

- `ginkgo asset versions dataframe/<name>` — shows rows, columns, and size
  alongside standard version fields.
- `ginkgo asset inspect dataframe/<name>@<ver>` — shows schema table, parent
  chain, byte size, and source library.

New command:

```
ginkgo asset schema dataframe/<name>[@<ver>]    # show column schema
```

Output:

```
COLUMN          DTYPE
id              int64
value           float64
feature_1       float64
category        object
timestamp       datetime64
```

---

## Implementation Sequence

| Step | Scope | Dependencies |
|------|-------|-------------|
| 1 | `DataFrameSerializer` — multi-library detection and Parquet serialization | Phase 7 serializer registry |
| 2 | `DataFrameLoader` — multi-library `.load()` with type preservation | Phase 7 loader registry |
| 3 | Register `"dataframe"` in Phase 7's kind auto-detection | Step 1 |
| 4 | Snapshot metadata capture (schema, row count, parent version) | Step 1 |
| 5 | `FigureSerializer` — matplotlib/plotly/bokeh/altair serialization | Phase 7 serializer registry |
| 6 | `FigureLoader` + register `"figure"` in kind auto-detection | Step 5 |
| 7 | CLI — DataFrame-specific formatting and schema command | Step 4 |
| 8 | CLI — Figure-specific formatting in asset commands | Step 5 |
| 9 | UI — DataFrame data preview with PyArrow row-group slicing | Step 1 |
| 10 | UI — Figure rendering (static `<img>`, interactive `<iframe>`, vega-embed) | Step 5 |
| 11 | UI — DataFrame version detail, schema display, summary stats | Step 4 |

Steps 1-2 and 5-6 can be developed in parallel (DataFrame and figure tracks
are independent). Steps 7-11 can be developed in parallel once their
respective serializers land.

---

## Risks and Tradeoffs

| Risk | Mitigation |
|------|-----------|
| Three DataFrame libraries to support | All serialize through PyArrow -> Parquet. Detection is type-based. Libraries are lazy-imported. |
| Four figure libraries to support | Each has a single clear serialization path. Detection is type-based. Libraries are lazy-imported. |
| Schema summary as flat dict loses nested/complex dtype info | Sufficient for inspection and drift detection. Full schema evolution is Iceberg territory. |
| Large DataFrames may be slow to serialize | Parquet compression is efficient. Very large DataFrames should use external storage (Iceberg/Delta) as remote inputs. |
| Parent version chain grows for frequently re-materialized assets | Informational only. No performance impact — versions are looked up by ID. |
| Interactive figure HTML may be large (plotly.js bundled) | Use CDN references (`include_plotlyjs="cdn"`) to keep HTML small. Offline mode is opt-in via metadata. |
| PNG thumbnail generation for interactive figures needs optional deps | Thumbnails are optional — list views degrade to a generic icon when unavailable. |

---

## Relationship to Other Phases

### Phase 7 (prerequisite)

This phase registers into Phase 7's infrastructure:

- `"dataframe"` serializer in the kind serializer registry
- `"dataframe"` loader in the loader registry
- DataFrame type detection in the kind auto-detection function
- All `AssetStore` operations (register, lineage, alias) are inherited

No Phase 7 code changes are needed — Phase 8 is purely additive.

### Phase 9 (downstream consumer)

Phase 9's model and eval tasks benefit from Phase 8 in two ways:

1. **Upstream dataset lineage**: when a model training task consumes a DataFrame
   asset via `features: asset`, the snapshot's `version_id` participates in
   cache identity. Retraining automatically invalidates when upstream data
   changes, and provenance records exactly which data snapshot each model saw.

2. **Serializer pattern**: Phase 8's `DataFrameSerializer` and
   `FigureSerializer` establish the pattern that Phase 9's `ModelSerializer`
   and `EvalSerializer` follow.

Figure assets are particularly useful alongside Phase 9: eval tasks can produce
versioned confusion matrices, ROC curves, and training loss plots that are
automatically linked to the model version and browsable in the UI.

### Phase 10 (downstream — data quality)

Phase 10 hooks into the DataFrame serialization path to add statistical
profiling, quality gates, and drift detection. The schema and row count
metadata captured here is the foundation for Phase 10's richer profiling.

### Future: Iceberg/Delta integration

The snapshot manifest is intentionally minimal — it is not Iceberg. The
`"dataframe"` serializer is an implementation detail behind Phase 7's asset
interface, so a future phase could substitute an Iceberg or Delta Lake backend
while preserving the DSL surface (`asset(df)`) and cache semantics.

---

## Validation

### DataFrame

1. A task returning `asset(pandas_df)` auto-detects `kind="dataframe"`,
   serializes to Parquet, and registers a version with correct row count,
   column count, and schema.
2. A task returning `asset(polars_df)` and `asset(pyarrow_table)` each
   round-trip correctly through Parquet.
3. `.load()` returns the original library type by default;
   `.load(as_polars=True)` forces polars from a pandas-produced snapshot.
4. Re-run with different input data creates a new version with the correct
   `parent_version_id` linking to the first version.
5. Re-run with unchanged inputs hits cache and does not register a duplicate.
6. A downstream task with `features: asset` annotation receives an `AssetRef`
   whose cache key uses the `version_id`, not the DataFrame hash.
7. `asset.ref("features@v-a1b2c3")` resolves to the historical snapshot and
   `.load()` returns the correct data.
8. Schema summary and row count appear in both the snapshot manifest and run
   provenance.
9. With remote-backed artifact storage, time-travel reads are served through
   the remote store and local staging cache correctly.
10. The UI data preview returns the first 50 rows and summary statistics
    without loading the full dataset into memory.

### Figure

11. A task returning `asset(matplotlib_fig)` auto-detects `kind="figure"`,
    stores a PNG artifact, and registers a version with correct metadata
    (source library, format, dimensions).
12. A task returning `asset(plotly_fig)` stores a self-contained HTML artifact
    with `interactive: true` in metadata.
13. A task returning `asset(altair_chart)` stores a Vega-Lite JSON spec.
14. The UI renders a matplotlib figure as a static image and a plotly figure
    as an interactive iframe with working hover/zoom/pan.
15. The UI renders an altair chart via vega-embed with full interactivity.
16. Figure version list shows thumbnails for visual comparison across versions.
17. Re-run with unchanged inputs hits cache and does not register a duplicate
    figure version.

---

## Success Criteria

- `asset(df)` works transparently for pandas, polars, and pyarrow with correct
  type round-tripping.
- `asset(fig)` works transparently for matplotlib, plotly, bokeh, and altair.
- DataFrame assets have snapshot history with parent-child relationships and
  schema tracking.
- Cache invalidation is driven by snapshot identity (`version_id`), not
  in-memory hashing.
- Both serializers are registered through Phase 7's plugin registry with no
  changes to Phase 7 code.
- CLI and UI surface kind-specific metadata alongside standard asset version
  information.
- The UI data preview enables interactive exploration of DataFrame assets
  directly in the browser.
- Interactive figures (plotly, bokeh, altair) render with full interactivity
  in the UI — hover, zoom, pan, and selection all work.
