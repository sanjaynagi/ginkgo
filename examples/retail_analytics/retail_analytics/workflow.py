"""Canonical workflow entrypoint for the retail analytics example."""

from __future__ import annotations

from pathlib import Path

import ginkgo
import pandas as pd
from ginkgo import file, flow

from retail_analytics.modules.delivery import write_delivery_manifest
from retail_analytics.modules.inputs import (
    clean_orders,
    enrich_orders,
    read_customers,
    read_orders,
    read_products,
)
from retail_analytics.modules.reporting import (
    build_executive_report,
    write_channel_metrics,
    write_inventory_hotspots,
    write_region_margin_report,
)


cfg = ginkgo.config("ginkgo.toml")
regions = sorted(pd.read_csv(cfg["paths"]["customers_csv"])["region"].unique().tolist())

for relative_path in (
    "logs",
    "results",
    "results/regions",
):
    Path(relative_path).mkdir(parents=True, exist_ok=True)


@flow
def main() -> file:
    """Build a retail analytics delivery bundle.

    Parameters
    ----------
    None

    Returns
    -------
    file
        Artifact manifest for the generated report bundle.
    """
    orders = read_orders(orders_path=cfg["paths"]["orders_csv"])
    customers = read_customers(customers_path=cfg["paths"]["customers_csv"])
    products = read_products(products_path=cfg["paths"]["products_csv"])
    cleaned_orders = clean_orders(orders=orders)
    enriched_orders = enrich_orders(
        orders=cleaned_orders,
        customers=customers,
        products=products,
    )
    channel_metrics = write_channel_metrics(enriched_orders=enriched_orders)
    region_reports = write_region_margin_report(enriched_orders=enriched_orders).map(
        region=regions
    )
    hotspots = write_inventory_hotspots(
        enriched_orders=enriched_orders,
        region_reports=region_reports,
        hotspot_threshold=float(cfg["reporting"]["hotspot_threshold"]),
    )
    executive_report = build_executive_report(
        channel_metrics=channel_metrics,
        region_reports=region_reports,
        hotspot_report=hotspots,
    )
    return write_delivery_manifest(
        executive_report=executive_report,
        channel_metrics=channel_metrics,
        hotspot_report=hotspots,
        region_reports=region_reports,
    )
