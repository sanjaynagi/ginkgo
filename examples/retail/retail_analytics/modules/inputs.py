"""Input loading and normalization tasks for the retail analytics example."""

from __future__ import annotations

import pandas as pd

from ginkgo import file, table, task
from ginkgo.core.asset import AssetResult


@task()
def read_orders(orders_path: file) -> pd.DataFrame:
    """Load the raw orders table.

    Parameters
    ----------
    orders_path : file
        CSV file containing order-level events.

    Returns
    -------
    pandas.DataFrame
        Loaded order records.
    """
    return pd.read_csv(orders_path)


@task()
def read_customers(customers_path: file) -> pd.DataFrame:
    """Load the customer dimension table.

    Parameters
    ----------
    customers_path : file
        CSV file containing customer metadata.

    Returns
    -------
    pandas.DataFrame
        Loaded customer records.
    """
    return pd.read_csv(customers_path)


@task()
def read_products(products_path: file) -> pd.DataFrame:
    """Load the product dimension table.

    Parameters
    ----------
    products_path : file
        CSV file containing product attributes.

    Returns
    -------
    pandas.DataFrame
        Loaded product records.
    """
    return pd.read_csv(products_path)


@task()
def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Standardize order fields and derive revenue metrics.

    Parameters
    ----------
    orders : pandas.DataFrame
        Raw order records.

    Returns
    -------
    pandas.DataFrame
        Normalized orders with derived metrics.
    """
    cleaned = orders.copy()

    # Normalize timestamps and derive commercial metrics once near the source.
    cleaned["ordered_at"] = pd.to_datetime(cleaned["ordered_at"])
    cleaned["order_month"] = cleaned["ordered_at"].dt.to_period("M").astype(str)
    cleaned["gross_revenue"] = cleaned["units"] * cleaned["unit_price"]
    cleaned["priority_flag"] = cleaned["priority"].eq("yes")

    return cleaned


@task()
def enrich_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> AssetResult:
    """Join dimensions onto cleaned orders and compute margin.

    Parameters
    ----------
    orders : pandas.DataFrame
        Normalized orders.
    customers : pandas.DataFrame
        Customer attributes.
    products : pandas.DataFrame
        Product attributes.

    Returns
    -------
    AssetResult
        Wrapped enriched order table registered as
        ``enrich_orders.enriched_orders`` in the asset catalog;
        downstream tasks receive the rehydrated DataFrame transparently.
    """
    enriched = (
        orders.merge(customers, on="customer_id", how="left")
        .merge(products, on="sku", how="left")
        .sort_values(["ordered_at", "order_id"])
        .reset_index(drop=True)
    )
    enriched["cost_basis"] = enriched["units"] * enriched["unit_cost"]
    enriched["gross_margin"] = enriched["gross_revenue"] - enriched["cost_basis"]
    enriched["priority_revenue"] = enriched["gross_revenue"].where(enriched["priority_flag"], 0.0)
    return table(enriched, name="enriched_orders")
