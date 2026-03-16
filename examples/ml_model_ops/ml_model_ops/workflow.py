"""Canonical workflow entrypoint for the ML model ops example."""

from __future__ import annotations

from pathlib import Path

import ginkgo
from ginkgo import file, flow

from ml_model_ops.modules.delivery import write_delivery_manifest
from ml_model_ops.modules.inputs import (
    build_feature_matrix,
    load_training_events,
    write_feature_profile,
    write_holdout_summary,
)
from ml_model_ops.modules.modeling import (
    evaluate_candidate,
    select_champion,
    write_candidate_scorecard,
)
from ml_model_ops.modules.reporting import write_model_card, write_serving_checklist


cfg = ginkgo.config("ginkgo.toml")
candidates = cfg["candidates"]

for relative_path in (
    "logs",
    "results",
    "results/candidates",
):
    Path(relative_path).mkdir(parents=True, exist_ok=True)


@flow
def main() -> file:
    """Build an ML model evaluation and promotion bundle.

    Parameters
    ----------
    None

    Returns
    -------
    file
        Artifact manifest for model review and promotion.
    """
    events = load_training_events(events_path=cfg["paths"]["training_events_csv"])
    feature_matrix = build_feature_matrix(events=events)
    feature_profile = write_feature_profile(feature_matrix=feature_matrix)
    holdout_summary = write_holdout_summary(feature_matrix=feature_matrix)
    candidate_reports = evaluate_candidate(feature_matrix=feature_matrix).map(
        model_name=[item["model_name"] for item in candidates],
        weight_scale=[float(item["weight_scale"]) for item in candidates],
        ticket_penalty=[float(item["ticket_penalty"]) for item in candidates],
        decision_threshold=[float(item["decision_threshold"]) for item in candidates],
    )
    candidate_scorecard = write_candidate_scorecard(
        candidate_reports=candidate_reports,
        feature_profile=feature_profile,
    )
    champion_report = select_champion(candidate_reports=candidate_reports)
    model_card = write_model_card(
        champion_report=champion_report,
        candidate_scorecard=candidate_scorecard,
        feature_profile=feature_profile,
        holdout_summary=holdout_summary,
    )
    serving_checklist = write_serving_checklist(
        champion_report=champion_report,
        holdout_summary=holdout_summary,
    )
    return write_delivery_manifest(
        model_card=model_card,
        candidate_scorecard=candidate_scorecard,
        champion_report=champion_report,
        serving_checklist=serving_checklist,
        candidate_reports=candidate_reports,
    )
