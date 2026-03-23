"""Delivery tasks for the retail analytics example."""

from __future__ import annotations

import shlex

from ginkgo import file, shell, task


@task(kind="shell")
def write_delivery_manifest(
    executive_report: file,
    channel_metrics: file,
    channel_notebook: file,
    hotspot_report: file,
    region_reports: list[file],
) -> file:
    """Write a shell-generated artifact manifest for downstream delivery.

    Parameters
    ----------
    executive_report : file
        Markdown executive brief.
    channel_metrics : file
        Channel KPI summary.
    channel_notebook : file
        Rendered notebook HTML summary.
    hotspot_report : file
        Hotspot JSON output.
    region_reports : list[file]
        Region-level category reports.

    Returns
    -------
    file
        Plain-text artifact manifest.
    """
    output = "results/delivery_manifest.txt"
    manifest_items = [executive_report, channel_metrics, channel_notebook, hotspot_report, *region_reports]
    quoted_items = " ".join(shlex.quote(str(item)) for item in manifest_items)
    return shell(
        cmd=f"printf '%s\\n' {quoted_items} > {shlex.quote(output)}",
        output=output,
        log="logs/write_delivery_manifest.log",
    )
