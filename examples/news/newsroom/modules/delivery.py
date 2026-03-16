"""Delivery tasks for the newsroom example."""

from __future__ import annotations

import shlex

from ginkgo import file, shell, task


@task(kind="shell")
def write_delivery_manifest(
    digest: file,
    publication_schedule: file,
    flagged_report: file,
    desk_packets: list[file],
) -> file:
    """Write a shell-generated newsroom artifact manifest."""
    output = "results/delivery_manifest.txt"
    manifest_items = [digest, publication_schedule, flagged_report, *desk_packets]
    quoted_items = " ".join(shlex.quote(str(item)) for item in manifest_items)
    return shell(
        cmd=f"printf '%s\\n' {quoted_items} > {shlex.quote(output)}",
        output=output,
        log="logs/write_delivery_manifest.log",
    )
