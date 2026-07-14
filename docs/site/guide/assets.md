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

| Helper | Kind | Payload |
|---|---|---|
| `asset(payload, kind=...)` | any | explicit kind |
| `table(payload)` | `table` | a dataframe or tabular file |
| `array(payload)` | `array` | a NumPy / array payload |
| `fig(payload)` | `fig` | a matplotlib figure or image |
| `text(payload)` | `text` | plain, markdown, or JSON text |
| `model(payload)` | `model` | a trained model object |

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
from ginkgo import model, task


@task()
def train_classifier(features: file) -> file:
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

Useful flags:

- `--single-file` — emit one HTML file with CSS, fonts, figures, and log files
  inlined as data URIs; easy to share or attach. Notebook iframes are not
  inlined and remain as relative references.
- `--out <dir>` — write the report bundle somewhere other than the default.
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
