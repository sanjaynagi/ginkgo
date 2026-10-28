# Reports

`ginkgo report` turns a finished run into an HTML report. It comes from the
`ginkgo-report` [plugin](concepts.md#glossary). See [Plugins](../getting-started/installation.md#plugins)
to install it.

## Create A Report

```bash
ginkgo report                   # the most recent run
ginkgo report <run_id> --open   # a specific run, opened in the browser
```

The run must be finished, with a status of `succeeded`, `failed` or
`cancelled`. A run that is still running or pending is an error.

By default, the report is a folder, `.ginkgo/reports/<run_id>/`. It holds
`index.html`, and an `assets/` folder for the stylesheet and fonts. Pass
`--single-file` to write one HTML file that opens offline instead.

## What The Report Contains

The report includes:

- the run summary and the run parameters
- the task graph
- each task's status and timing
- failure detail, with the end of each failed task's log
- the error of each failed attempt that a retry then recovered from
- the files and folders each task produced
- rendered pages, such as notebooks

### Summary

The **Cache hits** card shows how many tasks came from the cache, and what
share of the run that is.

A run that did not succeed says so first. The summary opens with the run's
error, such as the failure that left the workflow without a result. The
**Tasks** card counts the tasks that succeeded, failed and were skipped.

### Failures

The **Failure** section lists each skipped task, with why it never ran, such as
`blocked by analyze[omega]`. If many tasks were skipped, it groups them by
cause and shows a count for each.

A path inside the project is shown relative to the project root, in each error
and log. The report is often read on another machine, where the full path
means nothing.

### Tasks

The **Peak memory** column in the **Tasks** table shows how much memory each
task used, next to the amount it declared. For example, `48.0 KiB / 64Mi`. The
declared amount is shown as the task wrote it. These cases differ:

- A task that declared no `memory` shows the measured figure only.
- A Python task counts only the memory its body added, not the worker
  process's own start-up. So a task that allocates nothing shows about `0 B`.
- When a retry raised the memory, the figure is shown against the memory that
  attempt ran with, such as `30.1 GiB / 32 GiB`.
- A task that came from the cache measured nothing in this run. It shows the
  peak from the run that produced its result, and names that run, as in
  `48.0 KiB (run 9e1f3e6c)`.
- A task that never ran shows a dash. So does a shell or script task that
  finished too quickly to measure.

See [Measured Usage](resources.md#measured-usage) for how memory is measured.

The **Attempts** column shows how many attempts a task used, out of the number
its `retries` allow. A task with `retries=1` that succeeded on the first
attempt shows `1 of 2 allowed`.

### Rendered Pages

A task that renders a page for people to read, such as a notebook task's HTML,
gets a card in the **Rendered pages** section. The card's title is the task's
label. A page reused from a cache hit says which run rendered it. See
[Notebook Tasks](tasks-and-flows.md#notebook-tasks).

## Notebook Pages Offline

Notebook tasks produce their pages with marimo, the notebook tool Ginkgo uses.
A marimo page loads its display code from the internet, from cdn.jsdelivr.net,
so it is blank without a connection. The report therefore also makes a static
copy of each notebook page, which works offline.

The static copy keeps markdown, HTML, figures and console output as the
notebook produced them. It keeps errors as text, and tables as plain tables of
the first 200 rows. It names interactive elements, such as sliders, and leaves
them out. A figure that the notebook links by a relative path, rather than
embedding, is not copied.

- The folder report links both: **open** for the interactive marimo page,
  which needs the internet, and **static copy** for the offline one.
- The single-file report embeds the static copy, so it needs no internet at
  all. To share the interactive pages, share the folder report.

## Options

- `--single-file` writes one HTML file, with the stylesheet, fonts, script and
  rendered pages inside it, so it opens offline. It goes to
  `.ginkgo/reports/<run_id>.html`, beside the folder report, so you can keep
  both. Notebook pages are embedded as static copies. Logs are not included,
  and the command lists them when it writes the report. It warns when the file
  is over 25 MB.
- `--out <path>` writes the report somewhere other than the default.
- `--force` writes into an `--out` destination that holds other files.
- `--hide-code` leaves each cell's code out of the static copies of notebook
  pages, for readers who want only the results. By default, the code is folded
  under a "code" toggle above each cell's output. A cell the notebook itself
  hides keeps its code hidden either way. The interactive marimo page is not
  changed.
- `--open` opens the report in a browser when it is built. `--no-open` does
  not.
- `--max-log-lines N` limits the log lines shown for each failed task. The
  default is 80.

### Writing The Report Elsewhere

For a folder report, `--out <path>` names a directory. The directory must be
empty, missing, or hold an earlier Ginkgo report. Otherwise the command stops
and writes nothing. Writing over an earlier report replaces only the report's
own files (`index.html`, `assets/`, `pages/` and `logs/`), so anything else in
the directory stays.

With `--single-file`, a path ending in `.html` is the file itself, as in
`--out report.html`. A path with no extension is a directory to write
`index.html` into. Any other extension, as in `--out report.txt`, is an error.
An existing file is replaced only when it is an earlier Ginkgo report.

Pass `--force` to write into a destination that holds files Ginkgo did not
write. It replaces only the files the report writes, and removes nothing else.
The default destinations under `.ginkgo/reports/` are always written over,
with no `--force` needed.

## See Also

- [Tasks And Flows](tasks-and-flows.md): notebook tasks render marimo
  notebooks to HTML, which appear in reports.
- [Caching And Provenance](caching-and-provenance.md): how run outputs are
  stored and reused.
