import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo

    args = mo.cli_args()
    channel_metrics = args.get("channel-metrics") or "results/channel_metrics.csv"
    report_title = args.get("report-title") or "Retail Channel Performance Notebook"
    return channel_metrics, report_title


@app.cell
def _(channel_metrics, report_title):
    from pathlib import Path

    import pandas as pd

    frame = pd.read_csv(Path(channel_metrics))
    summary = frame.groupby("channel", as_index=False).agg(
        revenue=("revenue", "sum"),
        margin=("margin", "sum"),
        orders=("orders", "sum"),
    )
    summary["avg_revenue_per_order"] = (summary["revenue"] / summary["orders"]).round(2)
    print(report_title)
    print(summary.to_string(index=False))
    summary
    return


if __name__ == "__main__":
    app.run()
