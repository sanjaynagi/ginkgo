"""Delivery tasks for the chemistry screening example."""

from __future__ import annotations

import shlex

from ginkgo import file, shell_task, task


@task()
def write_delivery_manifest(
    portfolio_summary: file,
    developability_matrix: file,
    candidate_register: file,
    series_packets: list[file],
) -> file:
    """Write a shell-generated manifest for chemistry review artifacts.

    Parameters
    ----------
    portfolio_summary : file
        Portfolio-level markdown summary.
    developability_matrix : file
        Compound-level screening matrix.
    candidate_register : file
        Compound advancement register.
    series_packets : list[file]
        Series-level review packets.

    Returns
    -------
    file
        Plain-text artifact manifest.
    """
    output = "results/delivery_manifest.txt"
    manifest_items = [portfolio_summary, developability_matrix, candidate_register, *series_packets]
    quoted_items = " ".join(shlex.quote(str(item)) for item in manifest_items)
    return shell_task(
        cmd=f"printf '%s\\n' {quoted_items} > {shlex.quote(output)}",
        output=output,
        log="logs/write_delivery_manifest.log",
    )
