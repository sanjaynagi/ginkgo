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

Each helper accepts a `name` (the asset key, written `namespace/name`) and a
`metadata` dict. `model()` also takes `framework` and `metrics`:

```python
from ginkgo import model, task


@task()
def train_classifier(features: file) -> file:
    clf = fit_model(features)
    return model(clf, name="models/classifier", metrics={"auc": 0.93})
```

Assets are content-addressed and stored under `.ginkgo/assets/`. Re-running a
task that produces the same content adds a new *version* pointing at the same
bytes, so an asset key gives you a stable handle with full version history.

### Inspecting Assets

```bash
ginkgo asset ls                 # all asset keys
ginkgo asset versions <key>     # version history for one key
ginkgo asset inspect <ref>      # metadata for one asset version
ginkgo asset show <ref>         # render the asset payload
ginkgo models [run_id]          # model assets with their recorded metrics
```

## HTML Reports

`ginkgo report` renders a finished run as a self-contained HTML report:

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The report includes the run summary, the task graph, per-task status and
timing, failure detail with log tails, asset previews (tables, figures, model
metrics), and links to rendered notebooks. By default it is written to
`.ginkgo/reports/<run-id>/`.

Useful flags:

- `--single-file` — emit one HTML file with CSS, fonts, and figures inlined as
  data URIs; easy to share or attach.
- `--out <dir>` — write the report bundle somewhere other than the default.
- `--open` / `--no-open` — open (or do not open) the report in a browser when
  the build finishes.
- `--embed-full-assets` — copy full artifact bytes into the bundle alongside the
  rendered previews.

To list just the rendered notebook artifacts produced by runs, use
`ginkgo notebooks`.

## See Also

- [Tasks and Flows](tasks-and-flows.md) — notebook tasks render to HTML and
  appear in reports.
- [Caching and Provenance](caching-and-provenance.md) — how run outputs are
  stored and reused.
