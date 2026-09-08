"""Mini bioinformatics workflow for local Ginkgo testing.

Demonstrates mixed execution environments: Pixi-based shell tasks for
bioinformatics tools, a Docker container shell task for basic Unix
processing, and local Python tasks for data aggregation.
"""

import shlex
from pathlib import Path

import ginkgo
import pandas as pd
from ginkgo import AssetRef, asset, file, flow, shell, table, task


cfg = ginkgo.config("ginkgo.toml")
samples = pd.read_csv(cfg["paths"]["samples_csv"])


def _summary_has_records(payload: object) -> bool:
    """Return whether a QC summary contains at least one sample record."""
    return isinstance(payload, pd.DataFrame) and not payload.empty


def _logical_names(*asset_lists: list) -> dict[str, str]:
    """Map each stored artifact path to the filename its producer declared.

    A tool reads an asset's bytes from the artifact store, so what it echoes
    into its own output is the content-addressed blob path
    (``.ginkgo/artifacts/blobs/<hash>.gz``). That is storage internals, not
    something to show a reader. ``AssetRef.filename`` is the name the
    producing task gave the file, which is what belongs in a results table.
    """
    return {
        ref.artifact_path: ref.filename or Path(ref.artifact_path).name
        for refs in asset_lists
        for ref in refs
        if isinstance(ref, AssetRef) and ref.filename is not None
    }


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
    filtered_r1: list[file | AssetRef],
    filtered_r2: list[file | AssetRef],
) -> pd.DataFrame:
    """Merge per-sample QC tables and read counts into a single summary table.

    Parameters
    ----------
    sample_ids : list[str]
        Sample identifiers, parallel to *stats_tables*.
    stats_tables : list[file]
        Per-sample seqkit statistics TSVs.
    count_tables : list[file]
        Per-sample read count TSVs from the container task.
    filtered_r1 : list[file]
        Filtered forward-read FASTQs, carried in so the summary can show the
        filename each producer declared rather than the artifact-store path
        seqkit echoed into its stats table.
    filtered_r2 : list[file]
        Filtered reverse-read FASTQs, for the same reason.

    Returns
    -------
    pandas.DataFrame
        QC summary table. Wrapping with ``table(..., name=...)`` registers
        it as an asset so the UI and `ginkgo asset show` render rich
        previews; downstream tasks still receive the plain DataFrame.
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

    # seqkit's "file" column repeats the path it was handed, which is the
    # artifact store's blob. Show the name the filtering task declared.
    logical_names = _logical_names(filtered_r1, filtered_r2)
    if "file" in summary.columns:
        summary["file"] = summary["file"].map(
            lambda path: logical_names.get(path, Path(path).name)
        )

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

    return table(summary, name="qc_summary", checks=[_summary_has_records])


@flow
def main():
    """Filter FASTQs (Pixi), compute stats (Pixi), count reads (Docker), merge (local)."""
    filtered_pairs = filter_fastq(min_length=cfg["qc"]["min_length"]).map(
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
        filtered_r1=filtered_pairs.output[0],
        filtered_r2=filtered_pairs.output[1],
    )
