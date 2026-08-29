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

Each helper accepts a `name`, a `group` label for report sections, a `caption`
shown beneath the asset name, and a `metadata` dict. Each also accepts
`checks`: small data-quality assertions that run before Ginkgo registers the
asset version. `model()` also takes `framework` and `metrics`.

The `name` you pass is the asset's name verbatim, for every kind. The full key
is `<kind>:<name>` — a colon, with the kind coming from the helper you called,
not from you. So `table(frame, name="sites/forest/trend")` is keyed
`table:sites/forest/trend`, and slashes inside the name are just part of the
name. Omit `name` and Ginkgo generates one: `<task>` for a `file` asset,
`<task>.<kind>[<n>]` for the others.

Two tasks that pass the same `name` to the same helper write two versions of
one asset key. That is the point of naming an asset — the key is yours — but
give distinct outputs distinct names.

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

Asset bytes are content-addressed and stored under `.ginkgo/artifacts/`; the
catalog that names and versions them lives in `.ginkgo/ginkgo.db`. Re-running a
task that produces the same content adds a new *version* pointing at the same
bytes, so an asset key gives you a stable handle with full version history.

### Consuming Assets Downstream

A task that depends on an asset-producing task does not receive a plain path.
What arrives is decided by the **consuming parameter's annotation**:

- Annotated `file`, `folder`, or a union including one of them (`file |
  AssetRef`) — the parameter binds a filesystem path, so a **`file`, `fig`, or
  `text` asset** passes through as an `AssetRef`: a record carrying the asset
  `key`, `version_id`, `kind`, `content_hash`, `metadata`, and `artifact_path`
  (the path to the immutable stored bytes). This holds on cache hits as well as
  cold runs. A `table`, `array`, or `model` asset bound to such a parameter is
  an error, named at the consuming task before it runs — see [Which assets have
  a path](#which-assets-have-a-path).
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

Instead of reading `artifact_path` directly you can call `as_file()`, which
returns the same path wrapped as a `ginkgo.file` marker.

### Which assets have a path

It depends on what the kind's artifact holds:

| Kind | Artifact holds | Binds a path |
|---|---|---|
| `file` | the bytes the task wrote, copied verbatim | yes |
| `fig` | native PNG, SVG, or HTML | yes |
| `text` | raw UTF-8 | yes |
| `table` | Parquet | no |
| `array` | a zipped zarr store or `.npy` blob | no |
| `model` | a framework-specific serialized model | no |

The first three are the file they appear to be, so a command can read them. The
last three are Ginkgo's *encoding* of a Python object: `table("data.csv")`
stores Parquet, not the CSV you handed it. Passing that path to code expecting
readable text gives it a serialized blob — and a shell command such as
`awk -F,` will consume Parquet bytes and exit 0, which is silent wrong data
rather than a failure.

So a `table`, `array`, or `model` asset does not bind a path:

- Binding one to a `file` / `folder` parameter — bare or in a union — fails with
  an error naming the task, the parameter, and the kind. It fails when the
  consuming task's inputs are resolved, before its command runs.
- `as_file()` on such a ref fails the same way, rather than wrapping the encoded
  blob in a `file` marker.
- Passing one to a `script` or `notebook` task fails by kind too, since those
  forward arguments to another process as text.

To feed one of those kinds to a shell, script, or notebook task, take the
payload in Python and write the format the command expects:

```python
import pandas as pd

from ginkgo import file, shell, task


@task(kind="shell")
def count_rows(scores: object, csv_path: str, output_path: str) -> file:
    # `scores` arrives as the live DataFrame, not a path.
    pd.DataFrame(scores).to_csv(csv_path, index=False)
    return shell(cmd=f"wc -l < {csv_path} > {output_path}", output=output_path)
```

Alternatively, have the producer return a `file` asset — `asset(csv_path)` —
when the bytes on disk, rather than the typed payload, are what downstream
tasks need.

One caveat for the kinds that do bind a path: stored artifacts are
content-addressed blobs with no file extension, so a command that switches
behaviour on the suffix (`.png` versus `.svg`, say) may still need the bytes
copied to a named path first.

### Inspecting Assets

```bash
ginkgo asset ls                 # all asset keys
ginkgo asset versions <key>     # version history for one key
ginkgo asset show <ref>         # kind-specific metadata stats (schema, shape, dimensions, etc.)
ginkgo asset inspect <ref>      # raw AssetVersion record (artifact_id, content_hash, run_id, path)
ginkgo models [run_id]          # model assets with their recorded metrics
```

`<key>` is either the full `<kind>:<name>` printed by `ginkgo asset ls`, or the
bare `<name>` you passed to the helper — a bare name is searched across kinds,
and Ginkgo asks you to qualify it only when the same name exists under more
than one kind. A name it does not recognise is reported with the nearest keys
in the catalog. `<ref>` additionally accepts `@<version-or-alias>`, as in
`ginkgo asset show table:sites/forest/trend@<version-id>`; without it you get
the latest version.

## HTML Reports

`ginkgo report` renders a **completed** run (status `succeeded` or `failed`) as
an HTML report. By default this produces a directory bundle (`index.html`
plus an `assets/` folder); pass `--single-file` to emit a single
self-contained HTML file instead. Running or pending runs are rejected with an
error.

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The report includes the run summary, the task graph, per-task status and
timing, failure detail with log tails, asset previews (tables, figures, model
metrics), and links to rendered notebooks. By default it is written to
`.ginkgo/reports/<run-id>/`.

Every asset card carries a fragment id built from its key, so a single figure
or table can be linked directly: `table:sales/by-region` renders at
`#asset-table-sales-by-region`. A `#` beside the asset name navigates to that
fragment, leaving the URL to share in the address bar. It appears when you
hover the card, or stays visible where there is no pointer to hover with.

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
