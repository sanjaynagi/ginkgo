"""Tests for the Phase 16 benchmark harness."""

from __future__ import annotations

import json
import io
from pathlib import Path

import pytest

from benchmarks.bioinfo import prepare_bioinfo_benchmark_dataset
from benchmarks.harness import (
    BenchmarkRecord,
    _print_benchmark_summary,
    compare_against_baseline,
)
from benchmarks.sources import BenchmarkSourceManifest


def test_source_manifest_loads_pinned_bioinfo_benchmark() -> None:
    manifest = BenchmarkSourceManifest.from_toml(
        path=Path(__file__).resolve().parents[1] / "benchmarks" / "sources" / "bioinfo_agam.toml"
    )

    assert manifest.name == "ampseeker-bioinfo"
    assert manifest.read1_column == "fastq_1"
    assert manifest.read2_column == "fastq_2"
    assert len(manifest.sample_ids) == 10
    assert manifest.sample_ids[0] == "ERR3058522"


def test_prepare_bioinfo_benchmark_dataset_generates_local_sample_sheet(tmp_path: Path) -> None:
    metadata_path = tmp_path / "metadata.tsv"
    metadata_path.write_text(
        "sample_id\tcountry\nERR3058522\tGhana\nERR3058532\tGhana\n",
        encoding="utf-8",
    )
    reads_dir = tmp_path / "reads"
    reads_dir.mkdir()
    for filename in (
        "ERR3058522_1.fastq.gz",
        "ERR3058522_2.fastq.gz",
        "ERR3058532_1.fastq.gz",
        "ERR3058532_2.fastq.gz",
    ):
        (reads_dir / filename).write_bytes(b"FASTQ")

    manifest_path = tmp_path / "source.toml"
    manifest_path.write_text(
        "\n".join(
            [
                "[source]",
                'name = "test-bioinfo"',
                'repo = "owner/repo"',
                'commit = "deadbeef"',
                f'metadata_url = "{metadata_path.as_uri()}"',
                f'reads_base_url = "{reads_dir.as_uri()}"',
                'metadata_format = "tsv"',
                'sample_id_column = "sample_id"',
                'read1_column = "fastq_1"',
                'read2_column = "fastq_2"',
                'read1_pattern = "{sample_id}_1.fastq.gz"',
                'read2_pattern = "{sample_id}_2.fastq.gz"',
                "",
                "[samples]",
                'ids = ["ERR3058522", "ERR3058532"]',
            ]
        ),
        encoding="utf-8",
    )

    example_dir = tmp_path / "bioinfo"
    (example_dir / "data").mkdir(parents=True)
    prepared = prepare_bioinfo_benchmark_dataset(
        example_dir=example_dir,
        manifest_path=manifest_path,
    )

    sample_lines = prepared.samples_csv.read_text(encoding="utf-8").splitlines()
    assert sample_lines[0] == "sample_id,fastq_1,fastq_2"
    assert "ERR3058522" in sample_lines[1]
    assert "benchmark_reads/ERR3058522_1.fastq.gz" in sample_lines[1]
    assert prepared.samples_csv.name == "samples.csv"


def test_compare_against_baseline_reports_failure(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(
        json.dumps(
            {
                "benchmarks": [
                    {
                        "example": "chem",
                        "mode": "cold",
                        "baseline_seconds": 10.0,
                        "max_regression_pct": 20.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    records = [
        BenchmarkRecord(
            example="chem",
            case="default",
            mode="cold",
            wall_time_seconds=15.0,
            status="succeeded",
            task_count=11,
            executed_task_count=11,
            cached_task_count=0,
            run_id="run-1",
            timestamp_utc="2026-03-31T12:00:00+00:00",
            platform="linux",
            python_version="3.11.0",
        )
    ]

    comparisons = compare_against_baseline(
        records=records,
        baseline_path=baseline_path,
    )

    assert comparisons[0]["status"] == "failed"
    assert comparisons[0]["absolute_delta_seconds"] == pytest.approx(5.0)
    assert comparisons[0]["percentage_delta"] == pytest.approx(50.0)


def test_print_benchmark_summary_renders_comparison_table(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(
        json.dumps(
            {
                "benchmarks": [
                    {
                        "example": "chem",
                        "mode": "cold",
                        "baseline_seconds": 10.0,
                        "max_regression_pct": 20.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    records = [
        BenchmarkRecord(
            example="chem",
            case="default",
            mode="cold",
            wall_time_seconds=11.5,
            status="succeeded",
            task_count=11,
            executed_task_count=11,
            cached_task_count=0,
            run_id="run-1",
            timestamp_utc="2026-03-31T12:00:00+00:00",
            platform="linux",
            python_version="3.11.0",
        )
    ]
    comparisons = compare_against_baseline(records=records, baseline_path=baseline_path)
    stream = io.StringIO()

    _print_benchmark_summary(records=records, comparisons=comparisons, stream=stream)

    output = stream.getvalue()
    assert "Benchmark Comparison" in output
    assert "baseline s" in output
    assert "observed s" in output
    assert "delta s" in output
    assert "delta %" in output
    assert "chem" in output
    assert "cold" in output
    assert "passed" in output


def test_print_benchmark_summary_renders_observed_only_table() -> None:
    records = [
        BenchmarkRecord(
            example="retail",
            case="default",
            mode="cached",
            wall_time_seconds=3.2,
            status="succeeded",
            task_count=9,
            executed_task_count=0,
            cached_task_count=9,
            run_id="run-2",
            timestamp_utc="2026-03-31T12:00:00+00:00",
            platform="linux",
            python_version="3.11.0",
        )
    ]
    stream = io.StringIO()

    _print_benchmark_summary(records=records, comparisons=[], stream=stream)

    output = stream.getvalue()
    assert "Benchmark Results" in output
    assert "observed s" in output
    assert "retail" in output
    assert "cached" in output
