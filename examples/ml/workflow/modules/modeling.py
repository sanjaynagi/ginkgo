"""Candidate training and evaluation tasks for the ML model ops example.

Each candidate fits a real scikit-learn pipeline on the engineered
feature matrix, wraps the fitted estimator with :func:`ginkgo.model`,
and returns a dict that carries both the model asset and the scalar
metrics downstream reporting tasks need. The model assets are
automatically registered in the local asset catalog and listable via
``ginkgo models``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ginkgo import file, model, task


_FEATURE_COLUMNS = [
    "tenure_months",
    "segment_weight",
    "stickiness_score",
    "ticket_burden",
    "expansion_readiness",
]


@task()
def evaluate_candidate(
    model_name: str,
    weight_scale: float,
    ticket_penalty: float,
    decision_threshold: float,
    feature_matrix: pd.DataFrame,
) -> dict[str, Any]:
    """Fit and score one logistic-regression candidate.

    Parameters
    ----------
    model_name : str
        Candidate identifier used as the model asset name.
    weight_scale : float
        Inverse regularisation strength forwarded as scikit-learn's
        ``C`` parameter. Higher values mean less regularisation.
    ticket_penalty : float
        Additional positive-class weight, scaled so candidates that
        tolerate more false positives train more aggressively on
        renewals.
    decision_threshold : float
        Probability threshold applied to ``predict_proba`` when
        converting scores to renewal decisions.
    feature_matrix : pandas.DataFrame
        Derived feature matrix produced by
        :func:`build_feature_matrix`.

    Returns
    -------
    dict
        A report carrying the wrapped model asset, the scalar metrics,
        and confusion-matrix details used by the reporting tasks. The
        model is rehydrated into a live pipeline when a downstream
        task consumes this report.
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
    )
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    features = feature_matrix[_FEATURE_COLUMNS].to_numpy(dtype=float)
    labels = feature_matrix["renewed"].to_numpy().astype(bool)

    pipeline = Pipeline(
        [
            ("scale", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    C=weight_scale,
                    class_weight={False: 1.0, True: 1.0 + ticket_penalty},
                    solver="liblinear",
                    random_state=0,
                ),
            ),
        ]
    )
    pipeline.fit(features, labels)

    # Resubstitution metrics keep the example self-contained for CI;
    # real pipelines should evaluate on a held-out fold.
    positive_proba = pipeline.predict_proba(features)[:, 1]
    predictions = positive_proba >= decision_threshold

    true_positive = int(np.sum(predictions & labels))
    true_negative = int(np.sum(~predictions & ~labels))
    false_positive = int(np.sum(predictions & ~labels))
    false_negative = int(np.sum(~predictions & labels))

    metrics = {
        "accuracy": round(float(accuracy_score(labels, predictions)), 3),
        "precision": round(
            float(precision_score(labels, predictions, zero_division=0)), 3
        ),
        "recall": round(float(recall_score(labels, predictions, zero_division=0)), 3),
        "f1": round(float(f1_score(labels, predictions, zero_division=0)), 3),
        "decision_threshold": float(decision_threshold),
    }
    business_score = round(
        metrics["accuracy"] * 45.0
        + metrics["recall"] * 35.0
        + float(positive_proba.mean()) * 10.0
        - false_positive * 1.5,
        3,
    )
    metrics["business_score"] = business_score

    top_segments = (
        feature_matrix.assign(predicted_probability=positive_proba)
        .groupby("segment", as_index=False)["predicted_probability"]
        .mean()
        .sort_values("predicted_probability", ascending=False)
        .round({"predicted_probability": 3})
        .to_dict(orient="records")
    )

    return {
        "model": model(
            pipeline,
            name=model_name,
            metrics=metrics,
            metadata={"feature_columns": _FEATURE_COLUMNS},
        ),
        "model_name": model_name,
        "weight_scale": float(weight_scale),
        "ticket_penalty": float(ticket_penalty),
        "decision_threshold": float(decision_threshold),
        "accounts": int(len(feature_matrix)),
        "metrics": metrics,
        "confusion_matrix": {
            "true_positive": true_positive,
            "true_negative": true_negative,
            "false_positive": false_positive,
            "false_negative": false_negative,
        },
        "top_segments": top_segments,
    }


@task()
def write_candidate_scorecard(
    candidate_reports: list[dict[str, Any]],
    feature_profile: file,
) -> file:
    """Aggregate candidate metrics into a single scorecard CSV.

    Parameters
    ----------
    candidate_reports : list[dict]
        One report per trained candidate, as produced by
        :func:`evaluate_candidate`.
    feature_profile : file
        Dataset-level feature summary from the inputs stage.

    Returns
    -------
    file
        CSV scorecard across all trained candidates, ranked by
        business score.
    """
    feature_rows = int(pd.read_csv(feature_profile).loc[0, "accounts"])
    rows = []
    for report in candidate_reports:
        metrics = report["metrics"]
        rows.append(
            {
                "model_name": report["model_name"],
                "accuracy": metrics["accuracy"],
                "precision": metrics["precision"],
                "recall": metrics["recall"],
                "f1": metrics["f1"],
                "business_score": metrics["business_score"],
                "decision_threshold": metrics["decision_threshold"],
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
def select_champion(candidate_reports: list[dict[str, Any]]) -> file:
    """Select the highest-scoring candidate and freeze a JSON summary.

    Parameters
    ----------
    candidate_reports : list[dict]
        One report per trained candidate.

    Returns
    -------
    file
        JSON report for the promoted champion candidate.
    """
    champion = max(
        candidate_reports,
        key=lambda item: (
            float(item["metrics"]["business_score"]),
            str(item["model_name"]),
        ),
    )
    summary = {
        "model_name": champion["model_name"],
        "decision_threshold": champion["decision_threshold"],
        "metrics": champion["metrics"],
        "confusion_matrix": champion["confusion_matrix"],
        "top_segments": champion["top_segments"],
        "accounts": champion["accounts"],
    }

    output = Path("results/champion_model.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return file(str(output))
