# Reporting

Phase 10 adds a static HTML report export for completed runs. The report is
a self-contained document — no server, no framework runtime, no network at
view time — that a user can share, archive, or print alongside a finished
run directory.

The implementation is the `ginkgo-report` plugin (`plugins/ginkgo-report`,
package `ginkgo_report`), which contributes `ginkgo report <run-id>` as a
`CliCommand` and depends on `jinja2`; core has neither.

## Goals

- **Shareable.** Opens in any modern browser directly from `file://`; can be
  zipped or mailed without losing fidelity.
- **Read-only.** Consumes the run summary, params, logs, and rendered
  notebooks. Writes nothing back to workspace state.
- **Intentional aesthetic.** A document, not a control panel; typography
  and palette are fixed tokens with no runtime theming.
- **Provenance-bound content.** Every rendered string is either a fixed
  label or a value derived from provenance. No generated prose, no
  narrative filler.
- **Explicit truncation.** Every cap surfaces as a visible marker in the
  rendered document.

## Scope

Reports are for **terminal runs** only. `build_report_data` raises
`ValueError` when the manifest status is not `succeeded`, `failed` or
`cancelled`. The exporter does not render running / pending runs.

Live streaming, multi-run comparison, upload to remote storage, and
report-side interactivity beyond sort/collapse/scrollspy are explicitly
out of scope.

## Package layout

```
ginkgo_report/
├── __init__.py          # PLUGIN: the `report` CliCommand
├── command.py           # argument parsing and command_report
├── model.py             # ReportData dataclass + builder
├── sizing.py            # log-tail cap and builder
├── render.py            # Jinja env, bundle writer, single-file writer
├── templates/
│   ├── index.html.j2    # master document shell
│   ├── _section.html.j2 # numbered <h2> heading macro
│   ├── _sidebar.html.j2
│   ├── _masthead.html.j2
│   ├── _summary.html.j2
│   ├── _parameters.html.j2
│   ├── _graph.html.j2
│   ├── _tasks.html.j2
│   ├── _failure.html.j2
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
RunSummary.load ──► build_report_data ──► ReportData
                                             │
                                             ▼
                    render.export_report ──► bundle/ or index.html
```

`ReportData` is the single intermediate. Templates never read the manifest
directly — everything they render lives as a typed field on the report.
This constraint is what keeps the document provenance-bound: if a field
isn't present on `ReportData`, it can't show up on the page.

`ReportData` consumes only `RunSummary` (the shared post-run data model used
by the CLI renderers). The report and the CLI renderers build their own payloads from
the same `RunSummary`, so the two presentation layers diverge cleanly
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

## Bundle layout

```
<out>/
├── .ginkgo-report.json            # ownership marker: run id, timestamp, version
├── index.html                     # entry point, references relative paths
├── assets/
│   ├── report.css
│   ├── islands.js
│   └── fonts/
│       ├── commissioner.woff2
│       ├── fraunces-roman.woff2
│       ├── fraunces-italic.woff2
│       └── jetbrains-mono.woff2
├── notebooks/<task>.html                       # copied rendered_html sidecars
└── logs/<log_filename>                         # failed-task logs
```

Notebooks and logs are copied rather than inlined. The `--single-file` mode
inlines the stylesheet, fonts and islands script so the document stands on
its own.

## Destination safety

Writing a bundle replaces the contents of `<out>`, so `export_report` first
decides whether the directory is ginkgo's to replace. Two signals say it is,
and `_prepare_out_dir` accepts either:

- `managed_destination=True` — the caller derived the path rather than taking
  it from a user. `command_report` sets it whenever `--out` was omitted, since
  `.ginkgo/reports/<run-id>/` is ginkgo's by construction. Ownership is known
  here, so it is stated, not inferred.
- `.ginkgo-report.json` at the bundle root, stamped by every export. This is
  what lets a user's own `--out` directory be re-rendered once ginkgo has
  written a report into it.

| `out_dir` state                       | Behaviour                             |
| ------------------------------------- | ------------------------------------- |
| missing or empty                      | used as is                            |
| `managed_destination`                 | emptied and rewritten                 |
| holds `.ginkgo-report.json`           | emptied and rewritten                 |
| non-empty, neither signal             | `FileExistsError`, nothing is touched |
| non-empty, neither signal, `force`    | emptied and rewritten                 |

The marker alone would not do: a report directory written before the marker
existed carries none, so inferring ownership from it would break re-rendering
the default destination and push users towards `--force` on the one path where
it should never be needed. Sniffing for `index.html` instead would re-open the
original hole for any `--out` target that happens to hold one.

`force=False` and `managed_destination=False` are the library defaults, so the
Python API cannot lose a caller's files by omission either.

In `--single-file` mode the marker sits beside `index.html` as the directory's
only companion file. The HTML stays self-contained, so sharing it alone still
works; the dotfile is what allows a second `--single-file` export over the same
directory.

## Size caps (`SizingPolicy`)

`SizingPolicy.log_lines` (default 80, `--max-log-lines`) caps the log tail on
each failure card, which renders `last N of M lines` when it truncates.
Nothing is silently clipped.

## Aesthetic

Committed visual direction (issue #48): a plain, sleek document — warm
off-white neutrals, all-sans headings and data text, hairline borders,
and a single dusk-blue accent. No background washes, ornaments, or boxed
section chips.

**Typography** — all bundled locally; no CDN references at view time:

- Commissioner (humanist sans) — headings, body text, table cells,
  key/value panels, chips, badges, navigation.
- JetBrains Mono — genuinely code-like content only: the sidebar run id,
  params and code blocks, log tails, task-graph labels,
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
| `--warn`         | `#A1722E`  | warning status                                   |
| `--cool`         | `#45658A`  | terminal node                                    |

Status tokens map to `ok` / `fail` / `warn` / `cool` / `neutral` and are
used consistently across stat cards, pills, graph node strokes, and the
sidebar status line. Colour is reserved for status and the accent; all
structure is drawn with hairline `--line*` borders on neutral panels.

## Layout

- Fixed 248 px left sidebar (warm-tinted, sticky): brand, run_id,
  status, TOC groups (Execution, Results when the run produced notebooks,
  Appendix), optional
  workspace link.
- Main column (max 1040 px): breadcrumb → H1 + run tag → chip strip →
  full KV grid → numbered sections.
- Mobile (< 960 px) collapses to a single column.

Sections are numbered over the ones a given run actually renders, so the
numerals are always contiguous: a clean run with no notebooks reads
`01..05`, and the same run with a failure reads `01..06` with `05 Failure`
inserted. `_SECTION_LAYOUT` in `model.py` is the single home for section
order, titles, anchors, and sidebar grouping; `build_report_data` turns it
into the `ReportSectionGroup` tree on `ReportData`, which both the sidebar
and the `section_heading` macro read. Numbering a section in its own
partial is what previously left holes (`01 02 03 04 06 08`) and read as
sections having silently vanished.

`Failure` renders — and is linked from the sidebar — only when the manifest
reports at least one failure; `Notebooks` only when the run produced one.
Both conditions live in `index.html.j2` beside the include.

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
- Graph columns are keyed by longest-path level, with nodes inside each
  column ordered by `node_id`.

## CLI

```
ginkgo report <run-id>
    [--out DIR]                 # default: <workspace>/.ginkgo/reports/<run-id>/
    [--force]                   # replace an --out dir ginkgo did not write
    [--single-file]             # inline CSS, fonts and script
    [--max-log-lines N]         # default 80
    [--open]                    # open the result in the browser
```

## Testing

`plugins/ginkgo-report/tests/test_reporting.py` covers:

- Formatters (`format_duration`, `format_bytes`) across s/m/h ranges.
- `build_log_tail` on synthetic inputs with explicit truncation assertions.
- `build_report_data` against fixture runs recorded through the event bus —
  success, failure, and all-cached paths, graph
  layout, section numbering, running-run rejection. The all-cached run must
  yield the same sections as the executed one; only the cache labels differ.
- `export_report` — bundle and single-file modes, presence of key
  strings, absence of external URLs, contiguous section numerals,
  conditional failure section,
  the destination guard (foreign directory refused, `force=True`
  replaces it, an own report directory re-renders, a
  `managed_destination` replaces an unmarked bundle), deterministic
  re-render.
- `ginkgo report` end to end — `--out` at a directory holding unrelated
  files leaves them intact and exits 1, `--force` replaces it, and the
  managed report directory re-renders with no flag whether or not it
  carries the marker.

The fixture helper builds a two-task run with an optional failure, mirroring
the real provenance flow end-to-end.
