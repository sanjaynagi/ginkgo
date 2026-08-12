"""Reporting and delivery tasks for the supply chain example."""

from __future__ import annotations

from pathlib import Path
import shlex

import pandas as pd

from ginkgo import file, shell, task


@task()
def build_operations_brief(
    resilience_scorecard: file,
    expedite_candidates: file,
    scenario_reports: list[file],
) -> file:
    """Assemble a markdown operations brief."""
    scorecard = pd.read_csv(resilience_scorecard)
    expedite = pd.read_csv(expedite_candidates)
    lines = [
        "# Supply-Chain Resilience Brief",
        "",
        "## Scenario Scorecard",
        scorecard.to_string(index=False),
        "",
        f"Expedite candidates: {len(expedite)}",
        "",
        "## Scenario Artifacts",
    ]
    for report_path in scenario_reports:
        lines.append(f"- {report_path}")

    output = Path("results/operations_brief.md")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))


@task(kind="shell")
def write_artifact_manifest(
    operations_brief: file,
    resilience_scorecard: file,
    expedite_candidates: file,
    scenario_reports: list[file],
) -> file:
    """Write a shell-generated artifact manifest for operations delivery."""
    output = "results/artifact_manifest.txt"
    manifest_items = [
        operations_brief,
        resilience_scorecard,
        expedite_candidates,
        *scenario_reports,
    ]
    quoted_items = " ".join(shlex.quote(str(item)) for item in manifest_items)
    return shell(
        cmd=f"printf '%s\\n' {quoted_items} > {shlex.quote(output)}",
        output=output,
        log="logs/write_artifact_manifest.log",
    )
