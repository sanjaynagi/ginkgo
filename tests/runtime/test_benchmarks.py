"""Tests for the benchmark harness."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from benchmarks.bioinfo import prepare_bioinfo_benchmark_dataset
from benchmarks.harness import (
    BenchmarkRecord,
    _print_benchmark_summary,
    _raise_for_benchmark_failures,
    compare_against_baseline,
)
from benchmarks.sources import BenchmarkSourceManifest


def _record(*, seconds: float, executed: int = 11, **fields) -> BenchmarkRecord:
    values = {
        "example": "chem",
        "case": "default",
        "mode": "cold",
        "task_count": 11,
        "cached_task_count": 0,
        **fields,
    }
    return BenchmarkRecord(
        **values,
        wall_time_seconds=seconds,
        status="succeeded",
        executed_task_count=executed,
        run_id="run-1",
        provenance_write_seconds=0.01,
        timestamp_utc="2026-03-31T12:00:00+00:00",
        platform="linux",
        python_version="3.11.0",
    )


def _compare(tmp_path: Path, record: BenchmarkRecord, **entry) -> list[dict]:
    """Compare *record* against a one-entry chem/cold baseline of 10 s, 20 % slack."""
    baseline = {
        "example": "chem",
        "mode": "cold",
        "baseline_seconds": 10.0,
        "max_regression_pct": 20.0,
        **entry,
    }
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps({"benchmarks": [baseline]}), encoding="utf-8")
    return compare_against_baseline(records=[record], baseline_path=path)


COUNTERS = {"task_count": 11, "executed_task_count": 11, "cached_task_count": 0}


def test_source_manifest_loads_pinned_bioinfo_benchmark() -> None:
    manifest = BenchmarkSourceManifest.from_toml(
        path=Path(__file__).resolve().parents[2] / "benchmarks" / "sources" / "bioinfo_agam.toml"
    )

    assert manifest.name == "ampseeker-bioinfo"
    assert (manifest.read1_column, manifest.read2_column) == ("fastq_1", "fastq_2")
    assert len(manifest.sample_ids) == 10
    assert manifest.sample_ids[0] == "ERR3058522"


def test_prepare_bioinfo_benchmark_dataset_generates_local_sample_sheet(tmp_path: Path) -> None:
    metadata_path = tmp_path / "metadata.tsv"
    metadata_path.write_text(
        "sample_id\tcountry\nERR3058522\tGhana\nERR3058532\tGhana\n", encoding="utf-8"
    )
    reads_dir = tmp_path / "reads"
    reads_dir.mkdir()
    for sample in ("ERR3058522", "ERR3058532"):
        for mate in (1, 2):
            (reads_dir / f"{sample}_{mate}.fastq.gz").write_bytes(b"FASTQ")

    manifest_path = tmp_path / "source.toml"
    manifest_path.write_text(
        f"""[source]
name = "test-bioinfo"
repo = "owner/repo"
commit = "deadbeef"
metadata_url = "{metadata_path.as_uri()}"
reads_base_url = "{reads_dir.as_uri()}"
metadata_format = "tsv"
sample_id_column = "sample_id"
read1_column = "fastq_1"
read2_column = "fastq_2"
read1_pattern = "{{sample_id}}_1.fastq.gz"
read2_pattern = "{{sample_id}}_2.fastq.gz"

[samples]
ids = ["ERR3058522", "ERR3058532"]
""",
        encoding="utf-8",
    )

    example_dir = tmp_path / "bioinfo"
    (example_dir / "data").mkdir(parents=True)
    prepared = prepare_bioinfo_benchmark_dataset(
        example_dir=example_dir, manifest_path=manifest_path
    )

    assert prepared.samples_csv.name == "samples.csv"
    sample_lines = prepared.samples_csv.read_text(encoding="utf-8").splitlines()
    assert sample_lines[0] == "sample_id,fastq_1,fastq_2"
    assert "benchmark_reads/ERR3058522_1.fastq.gz" in sample_lines[1]


def test_a_timing_regression_fails_only_under_strict(tmp_path: Path) -> None:
    (comparison,) = _compare(tmp_path, _record(seconds=15.0), expected_counters=COUNTERS)

    assert comparison["status"] == "failed"
    assert comparison["counter_status"] == "passed"
    assert comparison["absolute_delta_seconds"] == pytest.approx(5.0)
    assert comparison["percentage_delta"] == pytest.approx(50.0)
    _raise_for_benchmark_failures(comparisons=[comparison], strict=False)
    with pytest.raises(RuntimeError, match="timing regressions"):
        _raise_for_benchmark_failures(comparisons=[comparison], strict=True)


def test_a_counter_mismatch_fails_even_without_strict(tmp_path: Path) -> None:
    (comparison,) = _compare(
        tmp_path, _record(seconds=1.0, executed=9), expected_counters=COUNTERS
    )

    assert comparison["status"] == "passed"
    assert comparison["counter_status"] == "failed"
    assert comparison["counter_failures"] == [
        {"counter": "executed_task_count", "expected": 11, "observed": 9}
    ]
    with pytest.raises(RuntimeError, match="executed_task_count expected 11 but observed 9"):
        _raise_for_benchmark_failures(comparisons=[comparison], strict=False)


@pytest.mark.parametrize(
    ("entry", "executed", "status"),
    [({"expected_counters": COUNTERS}, 11, "passed"), ({}, 3, "not_gated")],
    ids=["matching", "ungated"],
)
def test_counters_that_match_or_are_not_gated_pass(
    tmp_path: Path, entry: dict, executed: int, status: str
) -> None:
    (comparison,) = _compare(tmp_path, _record(seconds=1.0, executed=executed), **entry)

    assert comparison["counter_status"] == status
    assert comparison["counter_failures"] == []
    _raise_for_benchmark_failures(comparisons=[comparison], strict=True)


def test_print_benchmark_summary_renders_comparison_and_observed_tables(tmp_path: Path) -> None:
    record = _record(seconds=11.5)
    stream = io.StringIO()
    _print_benchmark_summary(
        records=[record], comparisons=_compare(tmp_path, record), stream=stream
    )
    compared = stream.getvalue()
    for column in ("Benchmark Comparison", "baseline s", "observed s", "delta %", "passed"):
        assert column in compared

    stream = io.StringIO()
    cached = _record(seconds=3.2, example="retail", mode="cached", executed=0)
    _print_benchmark_summary(records=[cached], comparisons=[], stream=stream)
    observed = stream.getvalue()
    for column in ("Benchmark Results", "observed s", "retail", "cached"):
        assert column in observed
