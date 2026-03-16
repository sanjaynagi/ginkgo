"""Scoring and summary tasks for the newsroom example."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ginkgo import file, task


@task()
def load_story_backlog(backlog_path: file) -> pd.DataFrame:
    """Load the editorial backlog.

    Parameters
    ----------
    backlog_path : file
        CSV file containing editorial planning records.

    Returns
    -------
    pandas.DataFrame
        Loaded backlog records.
    """
    return pd.read_csv(backlog_path)


@task()
def enrich_story_scores(backlog: pd.DataFrame) -> pd.DataFrame:
    """Compute planning scores and review workload signals.

    Parameters
    ----------
    backlog : pandas.DataFrame
        Raw editorial backlog.

    Returns
    -------
    pandas.DataFrame
        Enriched backlog with priority and workload estimates.
    """
    scored = backlog.copy()

    # Centralize score derivation so downstream packet generation stays thin.
    scored["legal_review_required"] = scored["legal_review_required"].eq("yes")
    scored["breaking_news"] = scored["breaking_news"].eq("yes")
    scored["priority_score"] = (
        scored["audience_signal"]
        + scored["sources"]
        + scored["breaking_news"].astype(int) * 4
        + scored["legal_review_required"].astype(int) * 3
    )
    scored["editing_hours"] = (
        scored["word_count"] / 500.0
        + scored["legal_review_required"].astype(int) * 1.5
        + scored["breaking_news"].astype(int) * 0.75
    ).round(2)
    scored["publish_band"] = scored["priority_score"].map(
        lambda value: "lead" if value >= 14 else ("watch" if value >= 10 else "standard")
    )
    return scored.sort_values(["priority_score", "story_id"], ascending=[False, True]).reset_index(
        drop=True
    )


@task()
def write_publication_schedule(stories: pd.DataFrame) -> file:
    """Write the publication queue ordered by editorial priority."""
    schedule = stories[
        ["story_id", "desk", "headline", "priority_score", "publish_band", "draft_status"]
    ].copy()
    output = Path("results/publication_schedule.csv")
    schedule.to_csv(output, index=False)
    return file(str(output))


@task()
def write_flagged_story_report(stories: pd.DataFrame, flagged_priority: int) -> file:
    """Export stories that need senior editorial review."""
    flagged = stories.loc[
        stories["legal_review_required"] | (stories["priority_score"] >= flagged_priority)
    ][["story_id", "desk", "headline", "priority_score", "legal_review_required", "draft_status"]]
    output = Path("results/flagged_stories.csv")
    flagged.to_csv(output, index=False)
    return file(str(output))


@task()
def write_budget_summary(stories: pd.DataFrame) -> file:
    """Summarize editorial workload by desk."""
    summary = (
        stories.groupby("desk", as_index=False)
        .agg(
            stories=("story_id", "count"),
            lead_stories=("publish_band", lambda values: int((values == "lead").sum())),
            editing_hours=("editing_hours", "sum"),
        )
        .sort_values("desk")
    )
    payload = {"desks": summary.to_dict(orient="records")}
    output = Path("results/desk_budget.json")
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return file(str(output))
