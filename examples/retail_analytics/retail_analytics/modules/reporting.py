"""Reporting tasks for the retail analytics example."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ginkgo import file, task


def _write_dataframe(*, frame: pd.DataFrame, output_path: str) -> file:
    """Write a DataFrame and return it as a Ginkgo ``file`` marker."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return file(str(output))


@task()
def write_channel_metrics(enriched_orders: pd.DataFrame) -> file:
    """Aggregate month and channel performance metrics.

    Parameters
    ----------
    enriched_orders : pandas.DataFrame
        Enriched order table.

    Returns
    -------
    file
        CSV with channel-level KPIs.
    """
    summary = (
        enriched_orders.groupby(["order_month", "channel"], as_index=False)
        .agg(
            orders=("order_id", "count"),
            revenue=("gross_revenue", "sum"),
            margin=("gross_margin", "sum"),
            priority_revenue=("priority_revenue", "sum"),
        )
        .sort_values(["order_month", "channel"])
    )
    return _write_dataframe(frame=summary, output_path="results/channel_metrics.csv")


@task()
def write_region_margin_report(region: str, enriched_orders: pd.DataFrame) -> file:
    """Build a region-specific category margin report.

    Parameters
    ----------
    region : str
        Region name to summarize.
    enriched_orders : pandas.DataFrame
        Enriched order table.

    Returns
    -------
    file
        CSV containing region-level category metrics.
    """
    region_frame = enriched_orders.loc[enriched_orders["region"] == region].copy()
    summary = (
        region_frame.groupby(["region", "category"], as_index=False)
        .agg(
            orders=("order_id", "count"),
            units=("units", "sum"),
            revenue=("gross_revenue", "sum"),
            margin=("gross_margin", "sum"),
        )
        .sort_values(["region", "margin"], ascending=[True, False])
    )
    output = f"results/regions/{region}_margin.csv"
    return _write_dataframe(frame=summary, output_path=output)


@task()
def write_inventory_hotspots(
    enriched_orders: pd.DataFrame,
    region_reports: list[file],
    hotspot_threshold: float,
) -> file:
    """Identify high-value regions and categories that deserve follow-up.

    Parameters
    ----------
    enriched_orders : pandas.DataFrame
        Enriched order table.
    region_reports : list[file]
        Region-level category reports.
    hotspot_threshold : float
        Margin threshold for escalating a hotspot.

    Returns
    -------
    file
        JSON document describing hotspot recommendations.
    """
    hotspot_candidates: list[dict[str, object]] = []
    for report_path in region_reports:
        region_frame = pd.read_csv(report_path)
        elevated = region_frame.loc[region_frame["margin"] >= hotspot_threshold]
        for row in elevated.to_dict(orient="records"):
            hotspot_candidates.append(
                {
                    "region": row["region"],
                    "category": row["category"],
                    "margin": round(float(row["margin"]), 2),
                    "units": int(row["units"]),
                }
            )

    top_customers = (
        enriched_orders.groupby(["region", "customer_name"], as_index=False)["gross_revenue"]
        .sum()
        .sort_values(["region", "gross_revenue"], ascending=[True, False])
        .groupby("region", as_index=False)
        .head(1)
    )

    payload = {
        "hotspots": hotspot_candidates,
        "top_customers": top_customers.to_dict(orient="records"),
    }
    output = Path("results/inventory_hotspots.json")
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return file(str(output))


@task()
def build_executive_report(
    channel_metrics: file,
    region_reports: list[file],
    hotspot_report: file,
) -> file:
    """Assemble a markdown executive summary for commercial review.

    Parameters
    ----------
    channel_metrics : file
        Channel KPI summary.
    region_reports : list[file]
        Region-level category reports.
    hotspot_report : file
        JSON hotspot recommendations.

    Returns
    -------
    file
        Markdown report for leadership review.
    """
    channel_frame = pd.read_csv(channel_metrics)
    hotspot_payload = json.loads(Path(hotspot_report).read_text(encoding="utf-8"))

    region_rank = []
    for report_path in region_reports:
        frame = pd.read_csv(report_path)
        region_rank.append(
            (
                frame.loc[0, "region"],
                round(float(frame["margin"].sum()), 2),
                frame.loc[0, "category"],
            )
        )
    region_rank.sort(key=lambda item: item[1], reverse=True)
    top_region, top_margin, top_category = region_rank[0]

    lines = [
        "# Retail Weekly Executive Brief",
        "",
        "## Channel Performance",
        channel_frame.to_string(index=False),
        "",
        "## Regional Highlight",
        (
            f"Top region: **{top_region}** with total margin **{top_margin:.2f}** "
            f"driven by **{top_category}**."
        ),
        "",
        "## Hotspots",
        f"Escalated categories: {len(hotspot_payload['hotspots'])}",
    ]
    for item in hotspot_payload["hotspots"]:
        lines.append(
            f"- {item['region']} / {item['category']}: margin={item['margin']} units={item['units']}"
        )

    output = Path("results/executive_report.md")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))
