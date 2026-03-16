"""Candidate evaluation tasks for the ML model ops example."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from ginkgo import file, task


def _safe_slug(value: str) -> str:
    """Return a file-safe slug for candidate names."""
    return "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_")


@task()
def evaluate_candidate(
    model_name: str,
    weight_scale: float,
    ticket_penalty: float,
    decision_threshold: float,
    feature_matrix: pd.DataFrame,
) -> file:
    """Evaluate one model candidate against the feature matrix.

    Parameters
    ----------
    model_name : str
        Candidate model identifier.
    weight_scale : float
        Weight applied to expansion readiness.
    ticket_penalty : float
        Penalty weight applied to ticket burden.
    decision_threshold : float
        Probability threshold for positive predictions.
    feature_matrix : pandas.DataFrame
        Derived feature matrix used for candidate scoring.

    Returns
    -------
    file
        JSON report with candidate metrics and decision metadata.
    """
    scored = feature_matrix.copy()

    # Keep candidate evaluation deterministic and lightweight for CI execution.
    logit = (
        scored["segment_weight"] * 0.8
        + scored["stickiness_score"] * 0.11
        + scored["expansion_readiness"] * weight_scale * 1.4
        + scored["tenure_months"] * 0.03
        - scored["ticket_burden"] * ticket_penalty * 2.5
    )
    centered_logit = logit - float(logit.mean())
    scored["predicted_probability"] = 1.0 / (1.0 + np.exp(-centered_logit))
    scored["predicted_renewal"] = scored["predicted_probability"] >= decision_threshold

    truth = scored["renewed"]
    predicted = scored["predicted_renewal"]
    true_positive = int((predicted & truth).sum())
    true_negative = int((~predicted & ~truth).sum())
    false_positive = int((predicted & ~truth).sum())
    false_negative = int((~predicted & truth).sum())

    accuracy = round(float((predicted == truth).mean()), 3)
    precision = round(true_positive / max(true_positive + false_positive, 1), 3)
    recall = round(true_positive / max(true_positive + false_negative, 1), 3)
    business_score = round(
        accuracy * 45.0
        + recall * 35.0
        + float(scored["predicted_probability"].mean()) * 10.0
        - false_positive * 1.5,
        3,
    )

    payload = {
        "model_name": model_name,
        "weight_scale": weight_scale,
        "ticket_penalty": ticket_penalty,
        "decision_threshold": decision_threshold,
        "accounts": len(scored),
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "business_score": business_score,
        "confusion_matrix": {
            "true_positive": true_positive,
            "true_negative": true_negative,
            "false_positive": false_positive,
            "false_negative": false_negative,
        },
        "top_segments": (
            scored.groupby("segment", as_index=False)["predicted_probability"]
            .mean()
            .sort_values("predicted_probability", ascending=False)
            .round({"predicted_probability": 3})
            .to_dict(orient="records")
        ),
    }

    output = Path(f"results/candidates/{_safe_slug(model_name)}.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return file(str(output))


@task()
def write_candidate_scorecard(candidate_reports: list[file], feature_profile: file) -> file:
    """Aggregate candidate metrics into a scorecard.

    Parameters
    ----------
    candidate_reports : list[file]
        Candidate JSON evaluation reports.
    feature_profile : file
        Dataset-level feature summary.

    Returns
    -------
    file
        CSV scorecard across all model candidates.
    """
    feature_rows = int(pd.read_csv(feature_profile).loc[0, "accounts"])
    rows = []
    for report_path in candidate_reports:
        payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
        rows.append(
            {
                "model_name": payload["model_name"],
                "accuracy": payload["accuracy"],
                "precision": payload["precision"],
                "recall": payload["recall"],
                "business_score": payload["business_score"],
                "decision_threshold": payload["decision_threshold"],
                "dataset_accounts": feature_rows,
            }
        )

    scorecard = pd.DataFrame(rows).sort_values(
        ["business_score", "model_name"], ascending=[False, True]
    )
    output = Path("results/candidate_scorecard.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    scorecard.to_csv(output, index=False)
    return file(str(output))


@task()
def select_champion(candidate_reports: list[file]) -> file:
    """Select the highest-scoring model candidate.

    Parameters
    ----------
    candidate_reports : list[file]
        Candidate JSON evaluation reports.

    Returns
    -------
    file
        JSON report for the promoted champion candidate.
    """
    payloads = [
        json.loads(Path(report_path).read_text(encoding="utf-8")) for report_path in candidate_reports
    ]
    champion = max(payloads, key=lambda item: (float(item["business_score"]), str(item["model_name"])))

    output = Path("results/champion_model.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(champion, indent=2), encoding="utf-8")
    return file(str(output))
