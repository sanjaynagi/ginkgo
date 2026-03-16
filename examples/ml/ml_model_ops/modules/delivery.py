"""Delivery tasks for the ML model ops example."""

from __future__ import annotations

import shlex

from ginkgo import file, shell_task, task


@task()
def write_delivery_manifest(
    model_card: file,
    candidate_scorecard: file,
    champion_report: file,
    serving_checklist: file,
    candidate_reports: list[file],
) -> file:
    """Write a shell-generated artifact manifest for model review.

    Parameters
    ----------
    model_card : file
        Markdown model card for the promoted candidate.
    candidate_scorecard : file
        CSV scorecard across all evaluated candidates.
    champion_report : file
        JSON report for the promoted champion.
    serving_checklist : file
        CSV deployment checklist.
    candidate_reports : list[file]
        JSON evaluation reports for all candidates.

    Returns
    -------
    file
        Plain-text artifact manifest.
    """
    output = "results/delivery_manifest.txt"
    manifest_items = [
        model_card,
        candidate_scorecard,
        champion_report,
        serving_checklist,
        *candidate_reports,
    ]
    quoted_items = " ".join(shlex.quote(str(item)) for item in manifest_items)
    return shell_task(
        cmd=f"printf '%s\\n' {quoted_items} > {shlex.quote(output)}",
        output=output,
        log="logs/write_delivery_manifest.log",
    )
