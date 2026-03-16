"""Canonical workflow entrypoint for the supply chain example."""

from __future__ import annotations

from pathlib import Path

import ginkgo
from ginkgo import file, flow

from supply_chain.modules.inputs import (
    build_replenishment_plan,
    load_demand_forecast,
    load_shipping_lanes,
    load_supplier_capacity,
    normalize_demand,
)
from supply_chain.modules.reporting import build_operations_brief, write_artifact_manifest
from supply_chain.modules.scenarios import (
    identify_expedite_candidates,
    simulate_scenario,
    summarize_resilience,
)


cfg = ginkgo.config("ginkgo.toml")
scenarios = cfg["scenarios"]

for relative_path in (
    "logs",
    "results",
    "results/scenarios",
):
    Path(relative_path).mkdir(parents=True, exist_ok=True)


@flow
def main() -> file:
    """Build a supply-chain scenario analysis bundle.

    Parameters
    ----------
    None

    Returns
    -------
    file
        Artifact manifest for the scenario analysis outputs.
    """
    demand = load_demand_forecast(demand_path=cfg["paths"]["demand_csv"])
    suppliers = load_supplier_capacity(suppliers_path=cfg["paths"]["suppliers_csv"])
    lanes = load_shipping_lanes(lanes_path=cfg["paths"]["lanes_csv"])
    normalized_demand = normalize_demand(forecast=demand)
    replenishment_plan = build_replenishment_plan(
        forecast=normalized_demand,
        suppliers=suppliers,
        lanes=lanes,
    )
    scenario_reports = simulate_scenario(plan=replenishment_plan).map(
        scenario_id=[item["scenario_id"] for item in scenarios],
        delay_days=[item["delay_days"] for item in scenarios],
        capacity_multiplier=[item["capacity_multiplier"] for item in scenarios],
    )
    resilience_scorecard = summarize_resilience(
        plan=replenishment_plan,
        scenario_reports=scenario_reports,
    )
    expedite_candidates = identify_expedite_candidates(
        plan=replenishment_plan,
        scenario_reports=scenario_reports,
    )
    operations_brief = build_operations_brief(
        resilience_scorecard=resilience_scorecard,
        expedite_candidates=expedite_candidates,
        scenario_reports=scenario_reports,
    )
    return write_artifact_manifest(
        operations_brief=operations_brief,
        resilience_scorecard=resilience_scorecard,
        expedite_candidates=expedite_candidates,
        scenario_reports=scenario_reports,
    )
