"""Mini bioinformatics workflow for local Ginkgo testing."""

from __future__ import annotations

from pathlib import Path

import ginkgo
import pandas as pd
from ginkgo import file, flow, shell, task


cfg = ginkgo.config("ginkgo.toml")
samples = pd.read_csv(cfg["paths"]["samples_csv"])

# Ensure output directories exist before task execution starts.
for relative_path in ("logs", "results", "results/filtered", "results/qc"):
    Path(relative_path).mkdir(parents=True, exist_ok=True)


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


@task()
def build_summary(sample_ids: list[str], stats_tables: list[file]) -> file:
    """Merge per-sample QC tables into a single CSV summary."""
    frames: list[pd.DataFrame] = []
    for sample_id, stats_path in zip(sample_ids, stats_tables, strict=True):
        frame = pd.read_csv(stats_path, sep="\t")
        frame.insert(0, "sample_id", sample_id)
        frames.append(frame)

    summary = pd.concat(frames, ignore_index=True)
    output = Path("results/summary.csv")
    summary.to_csv(output, index=False)
    return file(str(output))


@flow
def main():
    filtered_fastqs = filter_fastq(min_length=int(cfg["qc"]["min_length"])).map(
        sample_id=samples["sample_id"],
        fastq=samples["fastq"],
    )
    qc_tables = fastq_stats().map(
        sample_id=samples["sample_id"],
        fastq=filtered_fastqs,
    )
    return build_summary(
        sample_ids=samples["sample_id"].tolist(),
        stats_tables=qc_tables,
    )
