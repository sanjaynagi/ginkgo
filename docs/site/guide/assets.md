# Assets And Reports

Tasks can return ordinary files, but Ginkgo also has a richer output type — the
**asset** — and a command that renders a whole run as a browsable HTML report.

## Assets

An asset is a typed, named, versioned task output. Where a `file` return is just
bytes at a path, an asset also carries a *kind*, a stable *key*, a content hash,
and metadata, and is tracked across runs.

Return an asset from a task with `asset()` or one of the typed helpers:

```python
from pathlib import Path

from ginkgo import asset, file, task


@task()
def write_seed_card(item: str, output_path: str) -> file:
    output = Path(output_path)
    output.write_text(f"item={item}\n", encoding="utf-8")
    return asset(output, name=f"starter/seed_cards/{item}")
```

The typed helpers each map to an asset **kind**:

| Helper | Kind | Payload | Return annotation |
|---|---|---|---|
| `asset(payload, kind=...)` | any | explicit kind | `file` only for `kind="file"`, else `object` |
| `table(payload)` | `table` | a dataframe, or a path to a CSV/TSV file | `object` |
| `array(payload)` | `array` | a NumPy / array payload | `object` |
| `fig(payload)` | `fig` | a matplotlib figure, or a path to an image | `object` |
| `text(payload)` | `text` | plain, markdown, or JSON text | `object` |
| `model(payload)` | `model` | a trained model object | `object` |

The return annotation follows the kind, not the payload. Only `asset(path)` —
the `file` kind — may be returned from a task annotated `-> file`. Every other
kind is a semantic asset rather than a path, so its producing task returns
`object` (or the payload's own type), even when the payload you passed in was a
file path: `table("data/frame.csv")` stores the rows as Parquet and yields a
`table` asset, not the CSV you handed it. Declaring `-> file` and returning
`table(...)` fails with an error naming the kind.

The annotation decides what a *consuming* task receives too, covered in
[Consuming Assets Downstream](#consuming-assets-downstream) below.

Each helper accepts a `name` (the asset key, written `namespace/name`), a
`group` label for report sections, a `caption` shown beneath the asset name,
and a `metadata` dict. Each also accepts `checks`: small data-quality
assertions that run before Ginkgo registers the asset version. `model()` also
takes `framework` and `metrics`.

```python
import pandas as pd

from ginkgo import table, task


def has_rows(frame: pd.DataFrame) -> bool:
    return not frame.empty


@task()
def prepare_observations() -> object:
    frame = load_observations()
    return table(frame, name="observations", checks=[has_rows])
```

A check receives the wrapped payload and must return `True` or `False`.
`False`, a raised exception, or any other return value fails the producing
task and prevents the asset version from being registered. Define checks as
top-level functions in an importable workflow module: lambdas, nested
functions, and closures cannot be transported to worker or remote execution.
Passing outcomes are stored with the asset version and displayed on HTML report
cards. Checks are not rerun for cached assets.

`model()` also takes `framework` and `metrics`:

```python
from ginkgo import AssetRef, file, model, task


@task()
def train_classifier(features: file | AssetRef) -> object:
    clf = fit_model(features)
    return model(
        clf,
        name="models/classifier",
        group="Model outputs",
        caption="Classifier trained on the filtered feature matrix.",
        metrics={"auc": 0.93},
    )
```

Assets with the same `group` are rendered together under a named heading in
HTML reports. Assets without a group appear under "Ungrouped assets". Captions
are rendered as short subtitles on each asset card and are also shown by
`ginkgo asset show`.

Assets are content-addressed and stored under `.ginkgo/assets/`. Re-running a
task that produces the same content adds a new *version* pointing at the same
bytes, so an asset key gives you a stable handle with full version history.

### Consuming Assets Downstream

A task that depends on an asset-producing task does not receive a plain path.
What arrives is decided by the **consuming parameter's annotation**:

- Annotated `file`, `folder`, or a union including one of them (`file |
  AssetRef`) — the parameter binds a filesystem path, so the value passes
  through as an `AssetRef`: a record carrying the asset `key`, `version_id`,
  `kind`, `content_hash`, `metadata`, and `artifact_path` (the path to the
  immutable stored bytes). This holds on cache hits as well as cold runs.
- Annotated `object` or the payload's own type (`pd.DataFrame`) — a `table`,
  `array`, `text`, or `model` ref is rehydrated into the live Python payload
  before the task body runs, so the task takes the DataFrame, array, or model
  object directly. `file` and `fig` refs stay as an `AssetRef`, since they
  carry paths and binary blobs rather than objects worth loading.

So a consumer of a file asset receives an `AssetRef`, not the path its `file`
annotation suggests. Widen the annotation and branch on the type:

```python
from pathlib import Path

from ginkgo import AssetRef, file, task


@task()
def normalize_seed_card(seed_card: file | AssetRef, output_path: str) -> file:
    input_path = (
        Path(seed_card.artifact_path)
        if isinstance(seed_card, AssetRef)
        else Path(str(seed_card))
    )
    ...
```

Both branches are kept because the same task also works when called with a plain
`file` path, from a producer that returns `file(...)` rather than `asset(...)`.

`AssetRef` also offers two accessors instead of reading `artifact_path`
directly: `load()` returns the artifact path as a string, and `as_file()`
returns it wrapped as a `ginkgo.file` marker.

### Inspecting Assets

```bash
ginkgo asset ls                 # all asset keys
ginkgo asset versions <key>     # version history for one key
ginkgo asset show <ref>         # kind-specific metadata stats (schema, shape, dimensions, etc.)
ginkgo asset inspect <ref>      # raw AssetVersion record (artifact_id, content_hash, run_id, path)
ginkgo models [run_id]          # model assets with their recorded metrics
```

## HTML Reports

`ginkgo report` renders a **completed** run (status `succeeded` or `failed`) as
a self-contained HTML report. Running or pending runs are rejected with an
error.

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The report includes the run summary, the task graph, per-task status and
timing, failure detail with log tails, asset previews (tables, figures, model
metrics), and links to rendered notebooks. By default it is written to
`.ginkgo/reports/<run-id>/`.

The task ledger's **Peak RSS** column shows each task's measured peak memory
against what it declared (`3.2 GiB / 16 GiB`), or the measured figure alone
when the task declared no `memory`. It reads an em dash for tasks that never
ran or were served from cache. See [Measured Usage](resources.md#measured-usage).

Useful flags:

- `--single-file` — emit one HTML file with CSS, fonts, figures, and log files
  inlined as data URIs; easy to share or attach. Notebook iframes are not
  inlined and remain as relative references. A small `.ginkgo-report.json`
  marker is written beside it so the directory can be re-rendered; the HTML
  itself is self-contained and can be shared on its own.
- `--out <dir>` — write the report bundle somewhere other than the default. The
  directory must be empty, missing, or hold an earlier ginkgo report; otherwise
  the command stops and changes nothing. The default destination always
  re-renders.
- `--force` — replace the contents of an `--out` directory that holds files
  ginkgo did not write.
- `--open` / `--no-open` — open (or do not open) the report in a browser when
  the build finishes.
- `--embed-full-assets` — copy artifact bytes into the bundle alongside the
  rendered previews. Only applies to assets stored as single files; directory-
  backed artifacts (e.g. zarr stores) are excluded.
- `--max-log-lines N` — control how many log lines are shown per failed task
  (default 80).

To list just the rendered notebook artifacts produced by runs, use
`ginkgo notebooks`.

## See Also

- [Tasks and Flows](tasks-and-flows.md) — notebook tasks render to HTML and
  appear in reports.
- [Caching and Provenance](caching-and-provenance.md) — how run outputs are
  stored and reused.
