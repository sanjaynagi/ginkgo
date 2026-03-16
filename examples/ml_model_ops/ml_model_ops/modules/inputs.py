"""Input preparation tasks for the ML model ops example."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ginkgo import file, task


@task()
def load_training_events(events_path: file) -> pd.DataFrame:
    """Load model training events.

    Parameters
    ----------
    events_path : file
        CSV file containing account-level training records.

    Returns
    -------
    pandas.DataFrame
        Loaded training-event records.
    """
    return pd.read_csv(events_path)


@task()
def build_feature_matrix(events: pd.DataFrame) -> pd.DataFrame:
    """Derive reusable model features from raw account events.

    Parameters
    ----------
    events : pandas.DataFrame
        Raw training-event records.

    Returns
    -------
    pandas.DataFrame
        Feature matrix used by candidate evaluation tasks.
    """
    matrix = events.copy()

    # Keep feature derivation centralized so candidate tasks only consume stable inputs.
    matrix["renewed"] = matrix["renewed"].eq("yes")
    matrix["stickiness_score"] = (
        matrix["monthly_active_days"] * 0.4 + matrix["product_adoption"] * 1.6
    ).round(3)
    matrix["ticket_burden"] = (
        matrix["support_tickets"] / matrix["monthly_active_days"].clip(lower=1)
    ).round(3)
    matrix["expansion_readiness"] = (
        matrix["expansion_signal"] * 0.7 + matrix["product_adoption"] / 10.0
    ).round(3)
    matrix["segment_weight"] = matrix["segment"].map(
        {"enterprise": 1.2, "mid_market": 1.0, "smb": 0.85}
    )
    return matrix.sort_values("account_id").reset_index(drop=True)


@task()
def write_feature_profile(feature_matrix: pd.DataFrame) -> file:
    """Write a feature-profile summary for the training dataset.

    Parameters
    ----------
    feature_matrix : pandas.DataFrame
        Derived feature matrix.

    Returns
    -------
    file
        CSV summary of dataset-level feature statistics.
    """
    profile = pd.DataFrame(
        [
            {
                "accounts": len(feature_matrix),
                "renewal_rate": round(float(feature_matrix["renewed"].mean()), 3),
                "avg_stickiness_score": round(float(feature_matrix["stickiness_score"].mean()), 3),
                "avg_ticket_burden": round(float(feature_matrix["ticket_burden"].mean()), 3),
                "avg_expansion_readiness": round(
                    float(feature_matrix["expansion_readiness"].mean()), 3
                ),
            }
        ]
    )
    output = Path("results/feature_profile.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    profile.to_csv(output, index=False)
    return file(str(output))


@task()
def write_holdout_summary(feature_matrix: pd.DataFrame) -> file:
    """Write a segment-level holdout-style evaluation summary.

    Parameters
    ----------
    feature_matrix : pandas.DataFrame
        Derived feature matrix.

    Returns
    -------
    file
        CSV summary of segment renewal behavior.
    """
    summary = (
        feature_matrix.groupby("segment", as_index=False)
        .agg(
            accounts=("account_id", "count"),
            renewal_rate=("renewed", "mean"),
            avg_expansion_readiness=("expansion_readiness", "mean"),
        )
        .sort_values("segment")
    )
    summary["renewal_rate"] = summary["renewal_rate"].round(3)
    summary["avg_expansion_readiness"] = summary["avg_expansion_readiness"].round(3)

    output = Path("results/holdout_summary.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output, index=False)
    return file(str(output))
