# Reporting

Phase 10 adds a static HTML report export for completed runs. The report is
a self-contained document — no server, no framework runtime, no network at
view time — that a user can share, archive, or print alongside a finished
run directory.

The implementation lives in `ginkgo/reporting/` and is wired into the CLI as
`ginkgo report <run-id>`.

## Goals

- **Shareable.** Opens in any modern browser directly from `file://`; can be
  zipped or mailed without losing fidelity.
- **Read-only.** Consumes manifest, params, logs, and the asset catalog.
  Writes nothing back to workspace state.
- **Intentional aesthetic.** A document, not a control panel; typography
  and palette are fixed tokens with no runtime theming.
- **Provenance-bound content.** Every rendered string is either a fixed
  label or a value derived from provenance. No generated prose, no
  narrative filler.
- **Explicit truncation.** Every cap surfaces as a visible marker in the
  rendered document.

## Scope

Reports are for **terminal runs** only. `build_report_data` raises
`ValueError` when the manifest status is not `succeeded` or `failed`. The
exporter does not render running / pending runs.

Live streaming, multi-run comparison, upload to remote storage, and
report-side interactivity beyond sort/collapse/scrollspy are explicitly
out of scope.

## Package layout

```
ginkgo/reporting/
├── __init__.py          # build_report_data, export_report, SizingPolicy
├── model.py             # ReportData dataclass + builder
├── sizing.py            # per-kind caps and preview builders
├── render.py            # Jinja env, bundle writer, single-file writer
├── templates/
│   ├── index.html.j2    # master document shell
│   ├── _sidebar.html.j2
│   ├── _masthead.html.j2
│   ├── _summary.html.j2
│   ├── _parameters.html.j2
│   ├── _graph.html.j2
│   ├── _tasks.html.j2
│   ├── _failure.html.j2
│   ├── _assets.html.j2
│   ├── _notebooks.html.j2
│   ├── _environment.html.j2
│   └── _footer.html.j2
└── static/
    ├── report.css       # tokens + component styles
    ├── islands.js       # scrollspy + sortable task table
    └── fonts/           # Commissioner, JetBrains Mono (woff2)
```

## Data flow

```
run_dir ──► RunSummary.load ─┐
           AssetStore        ├─► build_report_data ──► ReportData
           LocalArtifactStore┘                              │
                                                            ▼
                                 render.export_report ──► bundle/ or index.html
```

`ReportData` is the single intermediate. Templates never read the manifest
directly — everything they render lives as a typed field on the report.
This constraint is what keeps the document provenance-bound: if a field
isn't present on `ReportData`, it can't show up on the page.

`ReportData` consumes `RunSummary` (the existing shared data model for the
UI server and CLI renderers) plus `AssetStore` and `LocalArtifactStore`
for asset resolution. The UI server continues to build its own payloads
from the same `RunSummary`; the two presentation layers diverge cleanly
without duplicating parsing logic.

`RunSummary` is the single source of truth for post-run derivation. The
semantic transforms every read-only presenter needs — `failure_kind`,
`kind_label`, `cache_label`, `attempts_label` — live as read-only
properties on `TaskSummary` rather than being re-derived per consumer.
Value-to-string formatting (`format_duration`, `format_bytes`,
`format_timestamp`, `format_int`) is shared from `ginkgo/formatting.py`, so
the CLI run-finish output and this report format identical values
identically. Presentation *vocabulary* stays with each presenter: the CLI
uses Rich styles and icons, the report uses tone tokens (`ok` / `fail` /
`warn` / …); these legitimately differ and are not merged.

Asset cards are grouped into `AssetSection` objects before rendering. The
section title comes from the asset version metadata key `ginkgo_group`,
which is populated by `asset(..., group=...)` and the shorthand factories.
Assets without a non-empty group are rendered under "Ungrouped assets".
Grouping is presentation-only and does not affect `AssetKey.namespace`,
asset names, cache keys, or artifact identity.

Asset captions are read from the asset version metadata key
`ginkgo_caption`, populated by `asset(..., caption=...)` and the shorthand
factories. Captions become `AssetCard.caption` and render beneath the asset
name in the card header; they are also surfaced by `ginkgo asset show`.
Like groups, captions are presentation-only and do not affect identity or
cache behaviour.

Asset check outcomes are read from the reserved `_checks` version-metadata
field into typed `CheckOutcome` values on `AssetCard`. Each asset card renders
these outcomes as pass/fail badges beside its kind and name. Current strict
asset checks register only all-passing versions, but the report model accepts
either persisted outcome so it remains forward-compatible with future soft
checks. Assets created before checks have an empty check collection.

## Bundle layout

```
<out>/
├── index.html                     # entry point, references relative paths
├── assets/
│   ├── report.css
│   ├── islands.js
│   └── fonts/
│       ├── commissioner.woff2
│       ├── fraunces-roman.woff2
│       ├── fraunces-italic.woff2
│       └── jetbrains-mono.woff2
├── figures/<artifact_id>.{png,jpg,svg,html}   # if figs were inlined
├── notebooks/<task>.html                       # copied rendered_html sidecars
└── logs/<log_filename>                         # failed-task logs
```

Figures, notebooks, and logs are copied rather than inlined. The
`--single-file` mode switches to data URIs for everything so the bundle
collapses to one HTML document.

## Size caps (`SizingPolicy`)

| Kind     | Default cap                            | Rendered marker                           |
| -------- | -------------------------------------- | ----------------------------------------- |
| table    | 50 rows                                | ``showing N of M rows``                   |
| text     | 4 096 bytes                            | ``N of M bytes shown · K bytes truncated``|
| log tail | last 80 lines                          | ``last N of M lines``                     |
| array    | metadata + stats only (never inlined)  | ``raw tensor not inlined``                |
| model    | metadata + metrics only                | ``model weights not inlined``             |
| fig      | always inlined (image or HTML iframe)  | —                                         |

Nothing is silently clipped. Every cap surfaces as either a
`trunc-note` banner or a `message` field on the preview.

`--embed-full-assets` copies the raw artifact bytes for any asset whose
artifact is stored as a single file (`path.is_file()`) into
`artifacts/<artifact_id>...` in addition to the rendered preview.
Directory-backed artifacts (e.g. zarr stores) are excluded. Previews
themselves still obey the row/byte caps — this flag is about archival
completeness, not in-document rendering.

## Aesthetic

Committed visual direction (issue #48): a plain, sleek document — warm
off-white neutrals, all-sans headings and data text, hairline borders,
and a single dusk-blue accent. No background washes, ornaments, or boxed
section chips.

**Typography** — all bundled locally; no CDN references at view time:

- Commissioner (humanist sans) — headings, body text, table cells,
  key/value panels, chips, badges, navigation.
- JetBrains Mono — genuinely code-like content only: the sidebar run id,
  params and code blocks, log tails, text previews, task-graph labels,
  and the footer line.

**Palette** — defined as CSS custom properties on `:root`; templates
reference only tokens.

| Token            | Hex        | Use                                              |
| ---------------- | ---------- | ------------------------------------------------ |
| `--paper`        | `#FDFCF8`  | background                                       |
| `--panel`        | `#FFFFFE`  | cards, tables, code blocks                       |
| `--panel-deep`   | `#F0EDE4`  | inline code, log fills, hover fills              |
| `--sidebar-bg`   | `#F7F5EF`  | sidebar background                               |
| `--ink`          | `#26231D`  | primary text                                     |
| `--accent`       | `#45658A`  | dusk-blue accent — links, run tag, section nums  |
| `--ok`           | `#5C7A4C`  | success status                                   |
| `--fail`         | `#B04E48`  | failure status                                   |
| `--warn`         | `#A1722E`  | warning status, table kind badge                 |
| `--cool`         | `#45658A`  | terminal node, array/model kind badges           |

Status tokens map to `ok` / `fail` / `warn` / `cool` / `neutral` and are
used consistently across stat cards, pills, graph node strokes, and the
sidebar status line. Colour is reserved for status and the accent; all
structure is drawn with hairline `--line*` borders on neutral panels.

## Layout

- Fixed 248 px left sidebar (warm-tinted, sticky): brand, run_id,
  status, three TOC groups (Execution / Results / Appendix), optional
  workspace link.
- Main column (max 1040 px): breadcrumb → H1 + run tag → chip strip →
  full KV grid → numbered sections.
- Mobile (< 960 px) collapses to a single column.

Section indices (`01..08`) are stable regardless of which optional
sections render. `05 Failure` is only rendered — and only linked from the
sidebar — when the manifest reports at least one failure.

## Interactivity (islands)

`static/islands.js` is a tiny ES-module file that adds:

- **Scrollspy** — highlights the sidebar TOC link matching the current
  section via `IntersectionObserver`.
- **Sortable task table** — click any column header to sort. Duration
  columns are parsed as `Hh Mm Ss` for correct ordering.

The report renders fully without JS; the islands are pure progressive
enhancement. There is no framework, no bundler, no build step.

## `--single-file` mode

`export_report(..., single_file=True)` writes one HTML document with:

- Stylesheet inlined in a `<style>` block.
- Fonts rewritten to `data:font/woff2;base64,…` URIs inside `@font-face`.
- Figure images rewritten to `data:image/*;base64,…` URIs.
- Islands script inlined in a `<script type="module">` block.

Notebook iframes remain as relative references (their content is too
large to reasonably base64 into a single document); `--single-file` is
intended for short, self-contained report sharing. Logs copied into the
bundle default mode are also referenced by relative path.

## Determinism

Given the same inputs, the exporter produces byte-identical output for
everything except a single `<span data-generated>` element in the footer.
This element is excluded from determinism assertions in tests.

Contributors extending the report must preserve this property. Stable
ordering hooks worth knowing about:

- Tasks are ordered by `node_id` ascending (already true for
  `RunSummary.tasks`).
- Asset sections are ordered by first referenced asset version in the run
  manifest; cards inside each section are sorted by `(namespace, name)`.
- Graph columns are keyed by longest-path level, with nodes inside each
  column ordered by `node_id`.

## CLI

```
ginkgo report <run-id>
    [--out DIR]                 # default: <workspace>/.ginkgo/reports/<run-id>/
    [--single-file]             # inline CSS, fonts, figures as data URIs
    [--embed-full-assets]       # copy raw artifact bytes into the bundle
    [--max-log-lines N]         # default 80
    [--open]                    # open the result in the browser
```

## Testing

`tests/test_reporting.py` covers:

- Formatters (`format_duration`, `format_bytes`) across s/m/h ranges.
- Sizing helpers (`build_log_tail`, `build_table_preview`) on synthetic
  inputs with explicit truncation assertions.
- `build_report_data` against fixture runs produced via
  `RunProvenanceRecorder` — success and failure paths, graph layout,
  asset resolution, running-run rejection.
- `export_report` — bundle and single-file modes, presence of key
  strings, absence of external URLs, conditional failure section,
  overwrite guard, deterministic re-render.

The fixture helper builds a two-task run with an optional failure and a
registered file asset, mirroring the real provenance flow end-to-end.
