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
from ginkgo import AssetRef, asset, file, flow, shell, table, task
from ginkgo.core.asset import AssetResult


cfg = ginkgo.config("ginkgo.toml")
samples = pd.read_csv(cfg["paths"]["samples_csv"])


@task(env="bioinfo_tools", kind="shell")
def filter_fastq(sample_id: str, fastq_1: file, fastq_2: file, min_length: int) -> list[file]:
    """Filter paired-end reads shorter than ``min_length`` with seqkit."""
    out_1 = f"results/filtered/{sample_id}_1.filtered.fastq.gz"
    out_2 = f"results/filtered/{sample_id}_2.filtered.fastq.gz"
    return shell(
        cmd=(
            f"seqkit seq -m {min_length} {fastq_1} -o {out_1} && "
            f"seqkit seq -m {min_length} {fastq_2} -o {out_2}"
        ),
        output=[
            asset(
                out_1,
                name=f"bioinfo/filtered_fastq/{sample_id}_r1",
                metadata={"sample_id": sample_id, "read": "R1", "stage": "filter"},
            ),
            asset(
                out_2,
                name=f"bioinfo/filtered_fastq/{sample_id}_r2",
                metadata={"sample_id": sample_id, "read": "R2", "stage": "filter"},
            ),
        ],
        log=f"logs/filter_{sample_id}.log",
    )


@task(env="bioinfo_tools", kind="shell")
def fastq_stats(sample_id: str, fastq_1: file | AssetRef, fastq_2: file | AssetRef) -> file:
    """Compute per-sample paired-end FASTQ QC metrics with seqkit."""
    output = f"results/qc/{sample_id}.stats.tsv"
    fastq_1_path = fastq_1.artifact_path if isinstance(fastq_1, AssetRef) else str(fastq_1)
    fastq_2_path = fastq_2.artifact_path if isinstance(fastq_2, AssetRef) else str(fastq_2)
    return shell(
        cmd=f"seqkit stats -T {fastq_1_path} {fastq_2_path} > {output}",
        output=output,
        log=f"logs/stats_{sample_id}.log",
    )


@task(kind="shell", env="docker://ubuntu:24.04")
def count_reads(sample_id: str, fastq_1: file | AssetRef, fastq_2: file | AssetRef) -> file:
    """Count reads in paired-end FASTQs using grep inside a Docker container.

    Parameters
    ----------
    sample_id : str
        Unique sample identifier.
    fastq_1 : file
        Forward reads FASTQ file (each read occupies four lines).
    fastq_2 : file
        Reverse reads FASTQ file (each read occupies four lines).

    Returns
    -------
    file
        Tab-separated file with ``sample_id``, ``read_count_r1``, and
        ``read_count_r2`` columns.
    """
    output = f"results/read_counts/{sample_id}.counts.tsv"
    fastq_1_path = fastq_1.artifact_path if isinstance(fastq_1, AssetRef) else str(fastq_1)
    fastq_2_path = fastq_2.artifact_path if isinstance(fastq_2, AssetRef) else str(fastq_2)
    cmd = (
        f"printf 'sample_id\\tread_count_r1\\tread_count_r2\\n' > {shlex.quote(output)} && "
        f"printf '%s\\t%s\\t%s\\n' {shlex.quote(sample_id)} "
        f"$(zgrep -c '^@' {shlex.quote(fastq_1_path)}) "
        f"$(zgrep -c '^@' {shlex.quote(fastq_2_path)}) >> {shlex.quote(output)}"
    )
    return shell(cmd=cmd, output=output)


@task()
def build_summary(
    sample_ids: list[str],
    stats_tables: list[file | AssetRef],
    count_tables: list[file | AssetRef],
) -> AssetResult:
    """Merge per-sample QC tables and read counts into a single summary table.

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
    AssetResult
        Wrapped tabular asset registered as ``build_summary.qc_summary`` in
        the catalog, stored as Parquet with schema and row-count metadata.
    """
    # Merge QC stats.
    frames: list[pd.DataFrame] = []
    for sample_id, stats_path in zip(sample_ids, stats_tables, strict=True):
        stats_table_path = (
            stats_path.artifact_path if isinstance(stats_path, AssetRef) else str(stats_path)
        )
        frame = pd.read_csv(stats_table_path, sep="\t")
        frame.insert(0, "sample_id", sample_id)
        frames.append(frame)

    summary = pd.concat(frames, ignore_index=True)

    # Merge container-produced read counts.
    count_frames = [
        pd.read_csv(
            p.artifact_path if isinstance(p, AssetRef) else str(p),
            sep="\t",
        )
        for p in count_tables
    ]
    counts = pd.concat(count_frames, ignore_index=True)
    summary = summary.merge(counts, on="sample_id", how="left")

    # Keep the CSV on disk for external consumers that still read files
    # directly; the wrapped asset below is the canonical catalog entry.
    output = Path("results/summary.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output, index=False)

    return table(summary, name="qc_summary")


@flow
def main():
    """Filter FASTQs (Pixi), compute stats (Pixi), count reads (Docker), merge (local)."""
    filtered_pairs = filter_fastq(min_length=int(cfg["qc"]["min_length"])).map(
        sample_id=samples["sample_id"],
        fastq_1=samples["fastq_1"],
        fastq_2=samples["fastq_2"],
    )

    qc_tables = fastq_stats().map(
        sample_id=samples["sample_id"],
        fastq_1=filtered_pairs.output[0],
        fastq_2=filtered_pairs.output[1],
    )
    read_counts = count_reads().map(
        sample_id=samples["sample_id"],
        fastq_1=filtered_pairs.output[0],
        fastq_2=filtered_pairs.output[1],
    )
    return build_summary(
        sample_ids=samples["sample_id"].tolist(),
        stats_tables=qc_tables,
        count_tables=read_counts,
    )
