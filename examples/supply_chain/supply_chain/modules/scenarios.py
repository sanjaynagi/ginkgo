"""Scenario simulation tasks for the supply chain example."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ginkgo import file, task


@task()
def simulate_scenario(
    scenario_id: str,
    delay_days: int,
    capacity_multiplier: float,
    plan: pd.DataFrame,
) -> file:
    """Evaluate a disruption scenario against the baseline plan."""
    simulated = plan.copy()
    simulated["delivered_units"] = (simulated["planned_units"] * capacity_multiplier).round(0)
    simulated["effective_transit_days"] = simulated["transit_days"] + delay_days
    simulated["fill_rate"] = (simulated["delivered_units"] / simulated["required_units"]).round(3)

    payload = {
        "scenario_id": scenario_id,
        "delay_days": delay_days,
        "capacity_multiplier": capacity_multiplier,
        "average_fill_rate": round(float(simulated["fill_rate"].mean()), 3),
        "worst_fill_rate": round(float(simulated["fill_rate"].min()), 3),
        "impacted_skus": simulated.loc[simulated["fill_rate"] < 0.9, "sku"].tolist(),
    }
    output = Path(f"results/scenarios/{scenario_id}.json")
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return file(str(output))


@task()
def summarize_resilience(plan: pd.DataFrame, scenario_reports: list[file]) -> file:
    """Compare disruption scenarios and write a resilience scorecard."""
    rows = []
    for report_path in scenario_reports:
        payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
        rows.append(
            {
                "scenario_id": payload["scenario_id"],
                "average_fill_rate": payload["average_fill_rate"],
                "worst_fill_rate": payload["worst_fill_rate"],
                "impacted_sku_count": len(payload["impacted_skus"]),
            }
        )

    scorecard = pd.DataFrame(rows).sort_values("worst_fill_rate")
    scorecard["baseline_plan_rows"] = len(plan)
    output = Path("results/resilience_scorecard.csv")
    scorecard.to_csv(output, index=False)
    return file(str(output))


@task()
def identify_expedite_candidates(plan: pd.DataFrame, scenario_reports: list[file]) -> file:
    """Flag supply lines most exposed to disruption."""
    impacted_skus = set()
    for report_path in scenario_reports:
        payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
        impacted_skus.update(payload["impacted_skus"])

    flagged = plan.loc[plan["sku"].isin(impacted_skus)].copy()
    flagged["expedite_priority"] = (
        flagged["required_units"]
        * (1.0 - flagged["supplier_reliability"] * flagged["lane_reliability"])
    ).round(2)
    flagged = flagged.sort_values("expedite_priority", ascending=False)
    output = Path("results/expedite_candidates.csv")
    flagged.to_csv(output, index=False)
    return file(str(output))
