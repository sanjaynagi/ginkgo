# Notebook Task Details

This page covers how a notebook task runs, and what to do when one goes wrong.
[Notebook Tasks](tasks-and-flows.md#notebook-tasks) covers the basics: how to
write a notebook task, and how it receives its arguments.

## How A Notebook Task Runs

Each notebook task runs one `marimo export html` process. That process runs
the notebook with the task's arguments and renders the page in the same pass.

- **Environment.** A notebook task can declare an `env`. It then runs
  `python -m marimo` inside that environment, so the environment must include
  marimo. Without `env=`, the notebook runs under the same Python as the
  `ginkgo` command.
- **Disk space.** An environment with marimo, pandas and a plotting library
  takes several hundred MB on disk. The first run that needs it installs it,
  and reports how long that took. See
  [Where An Environment Is Installed](environments.md#where-an-environment-is-installed).
- **The notebook file.** marimo rewrites the file it exports, to stamp
  `__generated_with` into it. Ginkgo puts the notebook's original bytes back
  afterwards.
- **Session files.** As with any marimo export, marimo writes a session file
  under `__marimo__/` beside the notebook. You can leave it out of version
  control.
- **The rendered page.** Ginkgo records the page against the task, lists it
  under "Rendered pages" at the end of the run, and links it from the
  [run report](reports.md).

## Check A Notebook Before You Run It

`ginkgo doctor` and `ginkgo run --dry-run` check notebook tasks before anything
runs. They check tasks whose body is a plain `return notebook(...)`, with a
path that doesn't depend on another task's result. They report:

- an error for a path that isn't a marimo `.py` notebook
- a warning for each argument the notebook reads that the task doesn't pass

A run gives the same warning, once per task. The check looks for literal keys
in `args.get("k")`, `args.get_all("k")`, `args["k"]` and `"k" in args`. A key
the task doesn't pass reads `None`. The usual cause is a key written with
underscores, such as `summary_path`, where the notebook should read
`summary-path`.

## When A Notebook Fails

If a cell fails, the task fails. marimo still writes the page, and it shows
the failing cell. The error message points to that cell.

Some mistakes stop marimo before any cell runs. Two examples are two cells
that define the same name (`MultipleDefinitionError`), and a cycle between
cells. The task then fails, with marimo's message in its log.

Run `marimo check` after you edit a notebook by hand. It reports these
mistakes in about a second, without running the workflow:

```bash
marimo check notebooks/overview.py
```

## Images In The Page

The rendered page is a single HTML file. So an image that a markdown cell
links by a relative path, such as `![plot](results/plot.png)`, doesn't load
from where Ginkgo stores the page.

Embed the image instead, with `mo.image(...)` or a `data:` URI. Both put the
image's bytes in the page. Pass the image's path to the task as an argument,
rather than writing it into the notebook.
[Figures From Another Task](tasks-and-flows.md#figures-from-another-task)
shows how.

## Viewing The Page Offline

marimo's exported page loads its frontend from the `cdn.jsdelivr.net` content
delivery network. So the page shows blank without a network connection.
