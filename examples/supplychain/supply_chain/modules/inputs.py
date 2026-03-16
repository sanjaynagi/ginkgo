"""Input loading and baseline-planning tasks for the supply chain example."""

from __future__ import annotations

import pandas as pd

from ginkgo import file, task


@task()
def load_demand_forecast(demand_path: file) -> pd.DataFrame:
    """Load forecasted demand by distribution center."""
    return pd.read_csv(demand_path)


@task()
def load_supplier_capacity(suppliers_path: file) -> pd.DataFrame:
    """Load supplier capacity and reliability data."""
    return pd.read_csv(suppliers_path)


@task()
def load_shipping_lanes(lanes_path: file) -> pd.DataFrame:
    """Load shipping lane options by supplier and destination."""
    return pd.read_csv(lanes_path)


@task()
def normalize_demand(forecast: pd.DataFrame) -> pd.DataFrame:
    """Derive replenishment demand from the raw forecast."""
    normalized = forecast.copy()
    normalized["required_units"] = normalized["weekly_demand"] * normalized["target_weeks_cover"]
    return normalized


@task()
def build_replenishment_plan(
    forecast: pd.DataFrame,
    suppliers: pd.DataFrame,
    lanes: pd.DataFrame,
) -> pd.DataFrame:
    """Select a baseline replenishment plan across suppliers and lanes."""
    candidates = forecast.merge(suppliers, on="sku", how="left").merge(
        lanes, on=["supplier_id", "distribution_center"], how="left"
    )
    candidates = candidates.dropna(subset=["transit_days"]).copy()
    candidates["planned_units"] = candidates[["required_units", "max_units"]].min(axis=1)
    candidates["landed_unit_cost"] = candidates["unit_cost"] + candidates["lane_cost"]
    candidates["risk_penalty"] = (
        (2.0 - candidates["supplier_reliability"] - candidates["lane_reliability"]) * 10.0
    )
    candidates["selection_score"] = candidates["landed_unit_cost"] + candidates["risk_penalty"]

    selected = (
        candidates.sort_values(["sku", "distribution_center", "selection_score"])
        .groupby(["sku", "distribution_center"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    return selected[
        [
            "sku",
            "distribution_center",
            "supplier_id",
            "required_units",
            "planned_units",
            "transit_days",
            "landed_unit_cost",
            "supplier_reliability",
            "lane_reliability",
        ]
    ]
