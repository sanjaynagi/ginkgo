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

Errors and logs show a path inside the project relative to the project root.

### Tasks

The **Peak memory** column in the **Tasks** table shows how much memory each
task used, next to the amount it declared. For example, `48.0 KiB / 64Mi`. A
task that came from the cache shows the peak from the run that produced its
result, and names that run, as in `48.0 KiB (run 9e1f3e6c)`. See
[Measured Usage](resources.md#measured-usage) for how memory is measured.

The **Attempts** column shows how many attempts a task used, out of the number
its `retries` allow. A task with `retries=1` that succeeded on the first
attempt shows `1 of 2 allowed`.

### Rendered Pages

A task that renders a page for people to read, such as a notebook task's HTML,
gets a card in the **Rendered pages** section. The card's title is the task's
label. A page reused from a cache hit says which run rendered it. See
[Notebook Tasks](tasks-and-flows.md#notebook-tasks).

## Notebook Pages Offline

A marimo notebook page loads its display code from the internet, so it is
blank without a connection. The report therefore also makes a static copy of
each notebook page, which works offline. It keeps the notebook's text, figures
and output, shows tables as their first 200 rows, and leaves out interactive
elements such as sliders.

- The folder report links both: **open** for the interactive marimo page,
  and **static copy** for the offline one.
- The single-file report embeds only the static copy. To share the
  interactive pages, share the folder report.

## Options

- `--single-file` writes one HTML file, `.ginkgo/reports/<run_id>.html`, that
  opens offline. It embeds notebook pages as static copies, and leaves out
  logs.
- `--hide-code` leaves each cell's code out of the static copies of notebook
  pages. By default, the code is folded under a "code" toggle.
- `--open` opens the report in a browser when it is built.
- `--max-log-lines N` limits the log lines shown for each failed task. The
  default is 80.

### Writing The Report Elsewhere

`--out <path>` writes the report somewhere other than `.ginkgo/reports/`. For
a folder report it names a directory. With `--single-file`, a path ending in
`.html` is the file itself, as in `--out report.html`.

Ginkgo writes over an earlier Ginkgo report, but stops if the destination
holds other files. Pass `--force` to write anyway. It replaces only the files
the report writes, and removes nothing else.

## See Also

- [Tasks And Flows](tasks-and-flows.md): notebook tasks render marimo
  notebooks to HTML, which appear in reports.
- [Caching And Provenance](caching-and-provenance.md): how run outputs are
  stored and reused.
