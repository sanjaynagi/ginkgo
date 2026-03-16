"""Desk-packet generation tasks for the newsroom example."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ginkgo import ExprList, file, task


def _safe_slug(value: str) -> str:
    """Return a file-safe slug for a desk or artifact name."""
    return "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_")


@task()
def write_desk_packet(desk: str, stories: list[dict[str, object]]) -> file:
    """Render a desk-specific planning packet."""
    ordered = sorted(
        stories,
        key=lambda item: (float(item["priority_score"]), str(item["story_id"])),
        reverse=True,
    )
    lines = [f"# {desk.title()} Desk Packet", ""]
    for item in ordered:
        lines.append(
            (
                f"- {item['story_id']}: {item['headline']} "
                f"(priority={item['priority_score']}, band={item['publish_band']})"
            )
        )
    output = Path(f"results/desk_packets/{_safe_slug(desk)}_packet.md")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))


@task()
def plan_desk_packets(stories: pd.DataFrame) -> list[file]:
    """Expand to one packet task per desk discovered at runtime."""
    exprs = []

    # Desk count depends on resolved input data, so the graph expands at runtime.
    for desk, group in sorted(stories.groupby("desk"), key=lambda item: item[0]):
        records = (
            group[["story_id", "headline", "priority_score", "publish_band"]]
            .sort_values(["priority_score", "story_id"], ascending=[False, True])
            .to_dict(orient="records")
        )
        exprs.append(write_desk_packet(desk=desk, stories=records))

    return ExprList(exprs=exprs)


@task()
def compile_newsroom_digest(
    publication_schedule: file,
    flagged_report: file,
    budget_summary: file,
    desk_packets: list[file],
) -> file:
    """Assemble a newsroom-level digest across all desks."""
    schedule = pd.read_csv(publication_schedule)
    flagged = pd.read_csv(flagged_report)
    budget = json.loads(Path(budget_summary).read_text(encoding="utf-8"))

    lines = [
        "# Newsroom Digest",
        "",
        f"Stories in queue: {len(schedule)}",
        f"Flagged stories: {len(flagged)}",
        "",
        "## Desk Workload",
    ]
    for item in budget["desks"]:
        lines.append(
            (
                f"- {item['desk']}: stories={item['stories']} "
                f"lead={item['lead_stories']} editing_hours={item['editing_hours']}"
            )
        )

    lines.extend(["", "## Desk Packets"])
    for packet_path in desk_packets:
        lines.append(f"- {packet_path}")

    output = Path("results/newsroom_digest.md")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return file(str(output))
