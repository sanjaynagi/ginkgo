"""Mini bioinformatics workflow for local Ginkgo testing.

Demonstrates mixed execution environments: Pixi-based shell tasks for
bioinformatics tools, a Docker container shell task for basic Unix
processing, and local Python tasks for data aggregation.
"""

from __future__ import annotations

import shlex
from pathlib import Path

import ginkgo
import pandas as pd
from ginkgo import file, flow, shell, task


cfg = ginkgo.config("ginkgo.toml")
samples = pd.read_csv(cfg["paths"]["samples_csv"])


@task(env="bioinfo_tools", kind="shell")
def filter_fastq(sample_id: str, fastq: file, min_length: int) -> file:
    """Filter reads shorter than ``min_length`` with seqkit."""
    output = f"results/filtered/{sample_id}.filtered.fastq"
    return shell(
        cmd=f"seqkit seq -m {min_length} {fastq} > {output}",
        output=output,
        log=f"logs/filter_{sample_id}.log",
    )


@task(env="bioinfo_tools", kind="shell")
def fastq_stats(sample_id: str, fastq: file) -> file:
    """Compute per-sample FASTQ QC metrics with seqkit."""
    output = f"results/qc/{sample_id}.stats.tsv"
    return shell(
        cmd=f"seqkit stats -T {fastq} > {output}",
        output=output,
        log=f"logs/stats_{sample_id}.log",
    )


@task(kind="shell", env="docker://ubuntu:24.04")
def count_reads(sample_id: str, fastq: file) -> file:
    """Count reads in a FASTQ using grep inside a Docker container.

    Parameters
    ----------
    sample_id : str
        Unique sample identifier.
    fastq : file
        Input FASTQ file (each read occupies four lines).

    Returns
    -------
    file
        Tab-separated file with ``sample_id`` and ``read_count`` columns.
    """
    output = f"results/read_counts/{sample_id}.counts.tsv"
    cmd = (
        f"mkdir -p results/read_counts && "
        f"printf 'sample_id\\tread_count\\n' > {shlex.quote(output)} && "
        f"printf '%s\\t%s\\n' {shlex.quote(sample_id)} "
        f"$(grep -c '^@' {shlex.quote(str(fastq))}) >> {shlex.quote(output)}"
    )
    return shell(cmd=cmd, output=output)


@task()
def build_summary(
    sample_ids: list[str],
    stats_tables: list[file],
    count_tables: list[file],
) -> file:
    """Merge per-sample QC tables and read counts into a single CSV summary.

    Parameters
    ----------
    sample_ids : list[str]
        Sample identifiers, parallel to *stats_tables*.
    stats_tables : list[file]
        Per-sample seqkit statistics TSVs.
    count_tables : list[file]
        Per-sample read count TSVs from the container task.

    Returns
    -------
    file
        Combined summary CSV.
    """
    # Merge QC stats.
    frames: list[pd.DataFrame] = []
    for sample_id, stats_path in zip(sample_ids, stats_tables, strict=True):
        frame = pd.read_csv(stats_path, sep="\t")
        frame.insert(0, "sample_id", sample_id)
        frames.append(frame)

    summary = pd.concat(frames, ignore_index=True)

    # Merge container-produced read counts.
    count_frames = [pd.read_csv(str(p), sep="\t") for p in count_tables]
    counts = pd.concat(count_frames, ignore_index=True)
    summary = summary.merge(counts, on="sample_id", how="left")

    output = Path("results/summary.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output, index=False)
    return file(str(output))


@flow
def main():
    """Filter FASTQs (Pixi), compute stats (Pixi), count reads (Docker), merge (local)."""
    filtered_fastqs = filter_fastq(min_length=int(cfg["qc"]["min_length"])).map(
        sample_id=samples["sample_id"],
        fastq=samples["fastq"],
    )
    qc_tables = fastq_stats().map(
        sample_id=samples["sample_id"],
        fastq=filtered_fastqs,
    )
    read_counts = count_reads().map(
        sample_id=samples["sample_id"],
        fastq=filtered_fastqs,
    )
    return build_summary(
        sample_ids=samples["sample_id"].tolist(),
        stats_tables=qc_tables,
        count_tables=read_counts,
    )
