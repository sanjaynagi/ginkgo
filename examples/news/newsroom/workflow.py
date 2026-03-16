"""Canonical workflow entrypoint for the newsroom example."""

from __future__ import annotations

import ginkgo
from ginkgo import file, flow

from newsroom.modules.delivery import write_delivery_manifest
from newsroom.modules.packets import compile_newsroom_digest, plan_desk_packets
from newsroom.modules.scoring import (
    enrich_story_scores,
    load_story_backlog,
    write_budget_summary,
    write_flagged_story_report,
    write_publication_schedule,
)


cfg = ginkgo.config("ginkgo.toml")


@flow
def main() -> file:
    """Build a newsroom planning bundle.

    Parameters
    ----------
    None

    Returns
    -------
    file
        Artifact manifest for editorial delivery.
    """
    backlog = load_story_backlog(backlog_path=cfg["paths"]["story_backlog_csv"])
    scored_stories = enrich_story_scores(backlog=backlog)
    publication_schedule = write_publication_schedule(stories=scored_stories)
    flagged_report = write_flagged_story_report(
        stories=scored_stories,
        flagged_priority=int(cfg["editorial"]["flagged_priority"]),
    )
    budget_summary = write_budget_summary(stories=scored_stories)
    desk_packets = plan_desk_packets(stories=scored_stories)
    digest = compile_newsroom_digest(
        publication_schedule=publication_schedule,
        flagged_report=flagged_report,
        budget_summary=budget_summary,
        desk_packets=desk_packets,
    )
    return write_delivery_manifest(
        digest=digest,
        publication_schedule=publication_schedule,
        flagged_report=flagged_report,
        desk_packets=desk_packets,
    )
