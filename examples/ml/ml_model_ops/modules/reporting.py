"""Reporting tasks for the ML model ops example."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ginkgo import file, task


@task()
def write_model_card(
    champion_report: file,
    candidate_scorecard: file,
    feature_profile: file,
    holdout_summary: file,
) -> file:
    """Assemble a markdown model card for the promoted candidate.

    Parameters
    ----------
    champion_report : file
        JSON report for the promoted champion.
    candidate_scorecard : file
        CSV scorecard across all candidates.
    feature_profile : file
        Dataset-level feature summary.
    holdout_summary : file
        Segment-level holdout summary.

    Returns
    -------
    file
        Markdown model card for review and deployment planning.
    """
    champion = json.loads(Path(champion_report).read_text(encoding="utf-8"))
    scorecard = pd.read_csv(candidate_scorecard)
    profile = pd.read_csv(feature_profile)
    holdout = pd.read_csv(holdout_summary)
    runner_up = scorecard.iloc[1]
    champion_metrics = champion["metrics"]

    lines = [
        "# Champion Model Card",
        "",
        f"Champion: **{champion['model_name']}**",
        f"Business score: **{champion_metrics['business_score']}**",
        f"Decision threshold: **{champion['decision_threshold']}**",
        "",
        "## Dataset Profile",
        profile.to_string(index=False),
        "",
        "## Holdout Summary",
        holdout.to_string(index=False),
        "",
        "## Runner Up",
        (
            f"{runner_up['model_name']} with business_score={runner_up['business_score']} "
            f"and recall={runner_up['recall']}"
        ),
    ]

    output = Path("results/model_card.md")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))


@task()
def write_serving_checklist(champion_report: file, holdout_summary: file) -> file:
    """Write a deployment checklist for the promoted model.

    Parameters
    ----------
    champion_report : file
        JSON report for the promoted champion.
    holdout_summary : file
        Segment-level holdout summary.

    Returns
    -------
    file
        CSV checklist for production readiness review.
    """
    champion = json.loads(Path(champion_report).read_text(encoding="utf-8"))
    holdout = pd.read_csv(holdout_summary)
    checklist = pd.DataFrame(
        [
            {
                "check": "threshold_locked",
                "status": "ready",
                "detail": f"threshold={champion['decision_threshold']}",
            },
            {
                "check": "segment_coverage",
                "status": "ready" if len(holdout) >= 3 else "review",
                "detail": f"segments={len(holdout)}",
            },
            {
                "check": "false_positive_review",
                "status": "review",
                "detail": (
                    "false_positive="
                    f"{champion['confusion_matrix']['false_positive']}"
                ),
            },
        ]
    )

    output = Path("results/serving_checklist.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    checklist.to_csv(output, index=False)
    return file(str(output))
