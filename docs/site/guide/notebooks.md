# Notebook Task Details

This page covers how a notebook task runs, and what to do when one goes wrong.
[Notebook Tasks](tasks-and-flows.md#notebook-tasks) covers the basics: how to
write a notebook task, and how it receives its arguments.

## How A Notebook Task Runs

Each notebook task runs `marimo export html`, which runs the notebook with the
task's arguments and renders the page in one pass.

- **Environment.** With `env=`, the task runs `python -m marimo` inside that
  environment, so it must include marimo. Without `env=`, the notebook runs
  under the same Python as the `ginkgo` command. An environment with marimo,
  pandas and a plotting library takes several hundred MB. See
  [Where An Environment Is Installed](environments.md#where-an-environment-is-installed).
- **Session files.** marimo writes a session file under `__marimo__/` beside
  the notebook. You can leave it out of version control.
- **The rendered page.** Ginkgo lists the page under "Rendered pages" at the
  end of the run, and links it from the [run report](reports.md).

## Check A Notebook Before You Run It

`ginkgo doctor` and `ginkgo run --dry-run` check notebook tasks before anything
runs. They report an error for a path that isn't a marimo `.py` notebook, and
a warning for each argument the notebook reads that the task doesn't pass. That
argument reads `None`. The usual cause is a key written with underscores, such
as `summary_path`, where the notebook should read `summary-path`.

## When A Notebook Fails

If a cell fails, the task fails. marimo still writes the page, which shows the
failing cell. Some mistakes, such as two cells that define the same name, stop
marimo before any cell runs, and the task's log has marimo's message.

Run `marimo check` after you edit a notebook by hand. It catches these mistakes
without running the workflow:

```bash
marimo check notebooks/overview.py
```

## Images In The Page

An image linked by a relative path, such as `![plot](results/plot.png)`,
doesn't load in the rendered page. Embed it with `mo.image(...)` or a `data:`
URI instead, and pass its path to the task as an argument.
[Figures From Another Task](tasks-and-flows.md#figures-from-another-task)
shows how.

## Viewing The Page Offline

marimo's exported page loads its frontend from the `cdn.jsdelivr.net` content
delivery network. So the page shows blank without a network connection.
