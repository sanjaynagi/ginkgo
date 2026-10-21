# Reports

`ginkgo report` renders a **completed** run (status `succeeded`, `failed` or
`cancelled`) as an HTML report. By default this produces a directory bundle
(`index.html` plus an `assets/` folder for the stylesheet and fonts); pass
`--single-file` to emit a single self-contained HTML file instead. Running or
pending runs are rejected with an error.

`ginkgo report` comes from the `ginkgo-report` plugin; see
[Plugins](../getting-started/installation.md#plugins) to install it.

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The report includes the run summary, the run parameters, the task graph,
per-task status and timing, failure detail with log tails, and links to
rendered pages such as notebooks. By default it is written to `.ginkgo/reports/<run-id>/`.

The task ledger's **Peak RSS** column shows each task's measured peak memory
against what it declared (`3.2 GiB / 16 GiB`), or the measured figure alone
when the task declared no `memory`. It reads an em dash for tasks that never
ran or were served from cache. See [Measured Usage](resources.md#measured-usage).

Useful flags:

- `--single-file` — emit one HTML file with the CSS, fonts and script inlined;
  easy to share or attach. It is written to `.ginkgo/reports/<run-id>.html`,
  beside the bundle directory, so exporting both keeps both. Rendered pages
  and logs are not included: their links stay relative and will not open from
  the single file, and the command lists them when it writes the report. Use the
  bundle when those pages matter.
- `--out <path>` — write the report somewhere other than the default. For a
  bundle, `<path>` is a directory that must be empty, missing, or hold an
  earlier ginkgo report; otherwise the command stops and changes nothing. With
  `--single-file`, a path ending in `.html` is the file itself
  (`--out report.html`), and any other path is a directory that `index.html` is
  written into; only that file is written, and an existing file is replaced
  only when it is an earlier ginkgo report. The default destinations always
  re-render.
- `--force` — replace an `--out` destination that holds files ginkgo did not
  write.
- `--open` / `--no-open` — open (or do not open) the report in a browser when
  the build finishes.
- `--max-log-lines N` — control how many log lines are shown per failed task
  (default 80).

Any task that renders a page for people to read — a notebook task's HTML, for
example — gets a card in the report's **Rendered pages** section, titled by the
task's label. A page reused from a cache hit says which run rendered it.

## See Also

- [Tasks and Flows](tasks-and-flows.md) — notebook tasks render marimo
  notebooks to HTML, which appear in reports.
- [Caching and Provenance](caching-and-provenance.md) — how run outputs are
  stored and reused.
