"""Preparation of benchmark-only bioinformatics inputs."""

from __future__ import annotations

from csv import DictReader, DictWriter
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

from benchmarks.sources import BenchmarkSourceManifest


@dataclass(frozen=True, kw_only=True)
class PreparedBioinfoDataset:
    """Generated benchmark inputs for the bioinformatics example.

    Parameters
    ----------
    samples_csv : Path
        Generated sample sheet path.
    reads_dir : Path
        Directory containing downloaded benchmark FASTQs.
    sample_count : int
        Number of samples written to the generated sample sheet.
    """

    samples_csv: Path
    reads_dir: Path
    sample_count: int


def prepare_bioinfo_benchmark_dataset(
    *,
    example_dir: Path,
    manifest_path: Path,
) -> PreparedBioinfoDataset:
    """Generate benchmark inputs for the bioinformatics example.

    Parameters
    ----------
    example_dir : Path
        Copied benchmark workspace for the bioinformatics example.
    manifest_path : Path
        Pinned benchmark source manifest.

    Returns
    -------
    PreparedBioinfoDataset
        Paths to the generated sample sheet and downloaded FASTQ directory.
    """
    manifest = BenchmarkSourceManifest.from_toml(path=manifest_path)

    # Load and validate the upstream metadata rows.
    metadata_rows = _load_metadata_rows(manifest=manifest)

    # Download the pinned FASTQs into a benchmark-only input directory.
    reads_dir = example_dir / "data" / "benchmark_reads"
    reads_dir.mkdir(parents=True, exist_ok=True)
    samples_csv = example_dir / "data" / "samples.csv"
    _write_samples_csv(
        manifest=manifest,
        metadata_rows=metadata_rows,
        reads_dir=reads_dir,
        samples_csv=samples_csv,
    )
    return PreparedBioinfoDataset(
        samples_csv=samples_csv,
        reads_dir=reads_dir,
        sample_count=len(metadata_rows),
    )


def _load_metadata_rows(
    *,
    manifest: BenchmarkSourceManifest,
) -> list[dict[str, str]]:
    """Download and filter the pinned metadata table."""
    with urlopen(manifest.metadata_url) as response:
        raw_text = response.read().decode("utf-8")

    delimiter = "\t" if manifest.metadata_format == "tsv" else ","
    rows = list(DictReader(raw_text.splitlines(), delimiter=delimiter))
    if not rows:
        raise ValueError("Benchmark metadata file is empty.")
    if manifest.sample_id_column not in rows[0]:
        raise ValueError(
            f"Benchmark metadata is missing required column {manifest.sample_id_column!r}."
        )

    # Preserve the manifest's sample ordering to keep the benchmark case stable.
    rows_by_id = {row[manifest.sample_id_column]: row for row in rows}
    missing = [sample_id for sample_id in manifest.sample_ids if sample_id not in rows_by_id]
    if missing:
        raise ValueError(
            "Benchmark metadata is missing pinned sample IDs: " + ", ".join(sorted(missing))
        )
    return [rows_by_id[sample_id] for sample_id in manifest.sample_ids]


def _write_samples_csv(
    *,
    manifest: BenchmarkSourceManifest,
    metadata_rows: list[dict[str, str]],
    reads_dir: Path,
    samples_csv: Path,
) -> None:
    """Download benchmark reads and write the generated sample sheet."""
    fieldnames = [manifest.sample_id_column, manifest.read1_column, manifest.read2_column]
    with samples_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in metadata_rows:
            sample_id = row[manifest.sample_id_column]

            # Download both read pairs into deterministic local paths.
            read1_url = (
                f"{manifest.reads_base_url}/{manifest.read1_pattern.format(sample_id=sample_id)}"
            )
            read2_url = (
                f"{manifest.reads_base_url}/{manifest.read2_pattern.format(sample_id=sample_id)}"
            )
            read1_path = reads_dir / _filename_from_url(read1_url)
            read2_path = reads_dir / _filename_from_url(read2_url)
            _download_to_path(url=read1_url, destination=read1_path)
            _download_to_path(url=read2_url, destination=read2_path)

            writer.writerow(
                {
                    manifest.sample_id_column: sample_id,
                    manifest.read1_column: str(read1_path),
                    manifest.read2_column: str(read2_path),
                }
            )


def _download_to_path(*, url: str, destination: Path) -> None:
    """Download a URL into a local file path."""
    with urlopen(url) as response:
        destination.write_bytes(response.read())


def _filename_from_url(url: str) -> str:
    """Return the trailing path component from a raw URL."""
    return Path(urlsplit(url).path).name
