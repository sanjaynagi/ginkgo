import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo

    mo.md(
        """
        # Starter Overview

        This marimo notebook is rendered by the default `ginkgo init` workflow.
        Ginkgo passes the notebook task's arguments as command-line arguments,
        read here with `mo.cli_args()`. The defaults below are used when you
        open the notebook by hand with `marimo edit`; during a run the task
        signature in `workflow/modules/reporting.py` wins.
        """
    )
    return (mo,)


@app.cell
def _(mo):
    args = mo.cli_args()
    summary_path = args.get("summary-path") or "results/summary.json"
    run_label = args.get("run-label") or "Starter demo"
    return run_label, summary_path


@app.cell
def _(run_label, summary_path):
    import json
    from pathlib import Path

    import pandas as pd

    summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    print(f"run_label={run_label}")
    print(f"row_count={summary['row_count']}")

    rows = pd.DataFrame(summary["rows"])
    rows
    return


if __name__ == "__main__":
    app.run()
