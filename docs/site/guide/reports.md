# Reports

`ginkgo report` renders a **completed** run (status `succeeded`, `failed` or
`cancelled`) as an HTML report. By default this produces a directory bundle
(`index.html` plus an `assets/` folder for the stylesheet and fonts); pass
`--single-file` to emit one HTML file that opens offline instead. Running or
pending runs are rejected with an error.

`ginkgo report` comes from the `ginkgo-report` plugin; see
[Plugins](../getting-started/installation.md#plugins) to install it.

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The report includes the run summary, the run parameters, the task graph,
per-task status and timing, failure detail with log tails, the error of each
attempt a retry recovered from, the files and folders each task produced, and
rendered pages such as notebooks. By default it is written to
`.ginkgo/reports/<run-id>/`.

The summary's **Cache hits** card counts the tasks served from the cache and
says what share of the run that is, from "every task ran" to "every task reused
from the cache".

The task ledger's **Peak memory** column shows the memory each task used
against what it declared, as written (`48.0 KiB / 64Mi`), or the measured figure
alone when the task declared no `memory`. A Python task's figure is how far its
body raised its worker's memory, so the worker's own interpreter and imports are
left out and a task that allocates nothing reads close to `0 B`. When a retry
escalated the memory, the figure is shown against the budget the attempt ran
with (`30.1 GiB / 32 GiB`). A task served from the cache measured nothing in
this run, so it shows the peak of the earlier run that produced its result,
named after that run (`48.0 KiB (run 9e1f3e6c)`). The column reads an em dash
for a task that never ran, and for a shell or script task too short to sample.
See [Measured Usage](resources.md#measured-usage).

A run that did not succeed says so first: the summary opens with the run's
error, such as which failure left the workflow without a result, and the
**Tasks** card counts the tasks that succeeded, failed and were skipped. The
**Failure** section lists each skipped task with why it never ran
(`blocked by analyze[omega]`), or counts them per cause when there are many.
Paths under the project in a failure's error and log tail are shown relative to
the project, since the report is read on other machines.

The **Attempts** column reads `1 of 2 allowed` for a task whose `retries` allow
two attempts and that needed one.

Useful flags:

- `--single-file` — emit one HTML file with the CSS, fonts, script and rendered
  pages inlined, so a collaborator can open it offline. It is written to
  `.ginkgo/reports/<run-id>.html`, beside the bundle directory, so exporting
  both keeps both. A marimo notebook page is embedded as a static copy of its
  outputs (see below). Logs are not included: their links stay relative and
  will not open from the single file, and the command lists them when it writes
  the report. It warns when the file is over 25 MB.
- `--out <path>` — write the report somewhere other than the default. For a
  bundle, `<path>` is a directory that must be empty, missing, or hold an
  earlier ginkgo report; otherwise the command stops and changes nothing.
  Re-rendering into an earlier report replaces only the report's own files
  (`index.html`, `assets/`, `pages/`, `logs/`), so anything else kept there
  stays. With `--single-file`, a path ending in `.html` is the file itself
  (`--out report.html`), a path with no extension is a directory that
  `index.html` is written into, and any other extension (`--out report.txt`) is
  refused; only that file is written, and an existing file is replaced only
  when it is an earlier ginkgo report. The default destinations always
  re-render.
- `--force` — write into an `--out` destination that holds files ginkgo did not
  write. Only the files the report itself writes are replaced; nothing else in
  the directory is removed.
- `--hide-code` — leave each cell's code out of the static copies of notebook
  pages, for readers who want the results only. By default the code is shown,
  folded under a "code" toggle above each cell's output; a cell the notebook
  itself hides keeps its code hidden either way. The interactive marimo page is
  the notebook's own export and is not changed.
- `--open` / `--no-open` — open (or do not open) the report in a browser when
  the build finishes.
- `--max-log-lines N` — control how many log lines are shown per failed task
  (default 80).

Any task that renders a page for people to read — a notebook task's HTML, for
example — gets a card in the report's **Rendered pages** section, titled by the
task's label. A page reused from a cache hit says which run rendered it.

### Notebook pages and the network

The page `marimo export html` writes is drawn by marimo's interface, which the
page loads from cdn.jsdelivr.net. Opened without a network connection, it is
blank. So the report also makes a **static copy** of each marimo page from the
code and outputs the page carries: markdown, HTML, figures and console output
as the notebook produced them, errors as text, and tables as plain tables (the
first 200 rows). Other interactive elements, such as sliders, are named and left
out. A figure the notebook links by a relative path, rather than embedding,
does not travel with the copy.

- The bundle links both: **open** for the interactive marimo page, which needs
  the network, and **static copy** for the offline one.
- The single file embeds the static copy, so it needs no network at all. For
  the interactive pages, share the bundle.

## See Also

- [Tasks and Flows](tasks-and-flows.md) — notebook tasks render marimo
  notebooks to HTML, which appear in reports.
- [Caching and Provenance](caching-and-provenance.md) — how run outputs are
  stored and reused.
