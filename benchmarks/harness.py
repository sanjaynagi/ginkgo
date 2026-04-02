"""Execution harness for example workflow benchmarks."""

from __future__ import annotations

from collections import Counter
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import time
from typing import Any, Iterator
from unittest.mock import patch

from benchmarks.bioinfo import prepare_bioinfo_benchmark_dataset
from ginkgo.cli.commands.run import run_workflow
from ginkgo.cli.workspace import discover_default_workflow
from ginkgo.envs.container import ContainerBackend
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.provenance import latest_run_dir, load_manifest


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_ROOT = REPO_ROOT / "examples"
BIOINFO_MANIFEST = REPO_ROOT / "benchmarks" / "sources" / "bioinfo_agam.toml"
EXAMPLE_NAMES = ("init", "bioinfo", "chem", "retail", "news", "supplychain", "ml")
PARTIAL_CACHE_EXAMPLES = {"init", "news"}
MOSTLY_CACHE_EXAMPLES = {"bioinfo"}


@dataclass(frozen=True, kw_only=True)
class BenchmarkRecord:
    """Structured benchmark result for one example/mode pair.

    Parameters
    ----------
    example : str
        Example workflow name.
    case : str
        Benchmark case identifier.
    mode : str
        Execution mode, typically ``cold`` or ``cached``.
    wall_time_seconds : float
        End-to-end measured wall time.
    status : str
        Run status from the manifest.
    task_count : int
        Total tasks present in the run manifest.
    executed_task_count : int
        Tasks that executed rather than returning from cache.
    cached_task_count : int
        Tasks returned from cache.
    run_id : str
        Ginkgo run identifier.
    timestamp_utc : str
        Timestamp when the benchmark record was written.
    platform : str
        Platform string for result interpretation.
    python_version : str
        Python version used for the benchmark run.
    """

    example: str
    case: str
    mode: str
    wall_time_seconds: float
    status: str
    task_count: int
    executed_task_count: int
    cached_task_count: int
    run_id: str
    timestamp_utc: str
    platform: str
    python_version: str

    def to_json(self) -> dict[str, object]:
        """Return a JSON-serializable mapping."""
        return asdict(self)


def run_example_benchmarks(
    *,
    examples: list[str],
    results_root: Path,
    baseline_path: Path | None = None,
    strict: bool = False,
    jobs: int = 4,
    cores: int = 4,
) -> Path:
    """Run the example benchmark suite and write structured results.

    Parameters
    ----------
    examples : list[str]
        Example workflow names to benchmark.
    results_root : Path
        Root directory for benchmark result output.
    baseline_path : Path | None
        Optional baseline JSON used for slowdown comparison.
    strict : bool
        When ``True``, fail on slowdown threshold violations.

    Returns
    -------
    Path
        Path to the written results JSON file.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result_dir = results_root / timestamp
    result_dir.mkdir(parents=True, exist_ok=True)

    # Run every requested example in both cold and cached modes.
    records: list[BenchmarkRecord] = []
    for example in examples:
        records.append(_benchmark_example(example=example, mode="cold", jobs=jobs, cores=cores))
        records.append(_benchmark_example(example=example, mode="cached", jobs=jobs, cores=cores))

    result_payload: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records": [record.to_json() for record in records],
        "comparisons": [],
    }
    if baseline_path is not None:
        result_payload["comparisons"] = compare_against_baseline(
            records=records,
            baseline_path=baseline_path,
        )

    results_path = result_dir / "results.json"
    results_path.write_text(json.dumps(result_payload, indent=2, sort_keys=True), encoding="utf-8")
    latest_path = results_root / "latest.json"
    latest_path.write_text(json.dumps(result_payload, indent=2, sort_keys=True), encoding="utf-8")

    _print_benchmark_summary(
        records=records,
        comparisons=result_payload["comparisons"],
        stream=sys.stdout,
    )
    _raise_for_strict_regressions(comparisons=result_payload["comparisons"], strict=strict)
    return results_path


def compare_against_baseline(
    *,
    records: list[BenchmarkRecord],
    baseline_path: Path,
) -> list[dict[str, object]]:
    """Compare benchmark records against a checked-in baseline.

    Parameters
    ----------
    records : list[BenchmarkRecord]
        Newly observed benchmark records.
    baseline_path : Path
        Baseline JSON file.
    Returns
    -------
    list[dict[str, object]]
        Comparison records with pass/fail state.
    """
    baseline_data = json.loads(baseline_path.read_text(encoding="utf-8"))
    baseline_entries = {
        f"{entry['example']}:{entry['mode']}": entry
        for entry in baseline_data.get("benchmarks", [])
    }
    comparisons: list[dict[str, object]] = []

    # Compare each benchmark record to its matching baseline entry.
    for record in records:
        key = f"{record.example}:{record.mode}"
        entry = baseline_entries.get(key)
        if entry is None:
            comparison = {
                "example": record.example,
                "mode": record.mode,
                "observed_seconds": record.wall_time_seconds,
                "baseline_seconds": None,
                "absolute_delta_seconds": None,
                "percentage_delta": None,
                "status": "missing_baseline",
            }
            comparisons.append(comparison)
            continue

        baseline_seconds = float(entry["baseline_seconds"])
        max_regression_pct = float(entry["max_regression_pct"])
        allowed_seconds = baseline_seconds * (1.0 + (max_regression_pct / 100.0))
        absolute_delta_seconds = record.wall_time_seconds - baseline_seconds
        percentage_delta = (
            (absolute_delta_seconds / baseline_seconds) * 100.0 if baseline_seconds > 0 else None
        )
        passed = record.wall_time_seconds <= allowed_seconds
        comparison = {
            "example": record.example,
            "mode": record.mode,
            "status": "passed" if passed else "failed",
            "observed_seconds": record.wall_time_seconds,
            "baseline_seconds": baseline_seconds,
            "absolute_delta_seconds": absolute_delta_seconds,
            "percentage_delta": percentage_delta,
            "allowed_seconds": allowed_seconds,
            "max_regression_pct": max_regression_pct,
        }
        comparisons.append(comparison)
    return comparisons


def _print_benchmark_summary(
    *,
    records: list[BenchmarkRecord],
    comparisons: object,
    stream: Any,
) -> None:
    """Print a readable benchmark summary table."""

    if not isinstance(comparisons, list) or len(comparisons) == 0:
        print(_render_observed_table(records=records), file=stream)
        return

    print(_render_comparison_table(comparisons=comparisons), file=stream)


def _render_comparison_table(*, comparisons: list[dict[str, object]]) -> str:
    """Return a fixed-width benchmark comparison table."""

    headers = [
        "example",
        "mode",
        "baseline s",
        "observed s",
        "delta s",
        "delta %",
        "status",
    ]
    rows = [
        [
            str(item.get("example", "—")),
            str(item.get("mode", "—")),
            _format_seconds(item.get("baseline_seconds")),
            _format_seconds(item.get("observed_seconds")),
            _format_delta_seconds(item.get("absolute_delta_seconds")),
            _format_percentage(item.get("percentage_delta")),
            str(item.get("status", "unknown")),
        ]
        for item in comparisons
    ]
    return _render_table(title="Benchmark Comparison", headers=headers, rows=rows)


def _render_observed_table(*, records: list[BenchmarkRecord]) -> str:
    """Return a fixed-width observed-results table."""

    headers = ["example", "mode", "observed s", "status"]
    rows = [
        [
            record.example,
            record.mode,
            _format_seconds(record.wall_time_seconds),
            record.status,
        ]
        for record in records
    ]
    return _render_table(title="Benchmark Results", headers=headers, rows=rows)


def _render_table(*, title: str, headers: list[str], rows: list[list[str]]) -> str:
    """Return one plain-text table."""

    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    separator = "  ".join("-" * width for width in widths)
    lines = [
        title,
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
        separator,
    ]
    lines.extend(
        "  ".join(value.ljust(widths[index]) for index, value in enumerate(row)) for row in rows
    )
    return "\n".join(lines)


def _raise_for_strict_regressions(*, comparisons: object, strict: bool) -> None:
    """Raise after summary printing when strict regression checks fail."""

    if not strict or not isinstance(comparisons, list):
        return

    failures = []
    for item in comparisons:
        if not isinstance(item, dict) or item.get("status") != "failed":
            continue
        example = str(item.get("example", "unknown"))
        mode = str(item.get("mode", "unknown"))
        observed = _format_seconds(item.get("observed_seconds"))
        allowed = _format_seconds(item.get("allowed_seconds"))
        failures.append(f"{example}:{mode} observed {observed} above allowed {allowed}")

    if failures:
        raise RuntimeError("Benchmark regressions detected:\n" + "\n".join(failures))


def _format_seconds(value: object) -> str:
    """Return one benchmark duration cell."""

    if value is None:
        return "—"
    return f"{float(value):.3f}"


def _format_delta_seconds(value: object) -> str:
    """Return one absolute-delta cell."""

    if value is None:
        return "—"
    return f"{float(value):+,.3f}"


def _format_percentage(value: object) -> str:
    """Return one percentage-delta cell."""

    if value is None:
        return "—"
    return f"{float(value):+,.1f}%"


def _benchmark_example(*, example: str, mode: str, jobs: int, cores: int) -> BenchmarkRecord:
    """Run one example benchmark in the requested mode."""
    workspace_root = REPO_ROOT / ".ginkgo" / "benchmarks" / f"{example}-{mode}"
    if workspace_root.exists():
        shutil.rmtree(workspace_root)
    workspace_root.mkdir(parents=True, exist_ok=True)
    example_dir = _copy_example(name=example, destination_root=workspace_root)

    config_paths: list[Path] = []
    if example == "bioinfo":
        prepare_bioinfo_benchmark_dataset(
            example_dir=example_dir,
            manifest_path=BIOINFO_MANIFEST,
        )

    # Use the same local-only mocks as the example integration tests.
    with _benchmark_runtime(example=example):
        if mode == "cold":
            elapsed, manifest = _timed_run(
                example_dir=example_dir,
                config_paths=config_paths,
                jobs=jobs,
                cores=cores,
            )
        elif mode == "cached":
            _timed_run(
                example_dir=example_dir,
                config_paths=config_paths,
                jobs=jobs,
                cores=cores,
            )
            elapsed, manifest = _timed_run(
                example_dir=example_dir,
                config_paths=config_paths,
                jobs=jobs,
                cores=cores,
            )
        else:
            raise ValueError(f"Unsupported benchmark mode: {mode!r}")

    _assert_cache_behavior(example=example, mode=mode, manifest=manifest)
    return _record_from_manifest(example=example, mode=mode, elapsed=elapsed, manifest=manifest)


def _record_from_manifest(
    *,
    example: str,
    mode: str,
    elapsed: float,
    manifest: dict[str, object],
) -> BenchmarkRecord:
    """Convert a run manifest into a structured benchmark record."""
    task_statuses = Counter(
        str(task["status"])
        for task in dict(manifest["tasks"]).values()  # type: ignore[arg-type]
    )
    task_count = sum(task_statuses.values())
    cached_task_count = task_statuses.get("cached", 0)
    return BenchmarkRecord(
        example=example,
        case="default" if example != "bioinfo" else "agam",
        mode=mode,
        wall_time_seconds=elapsed,
        status=str(manifest["status"]),
        task_count=task_count,
        executed_task_count=task_count - cached_task_count,
        cached_task_count=cached_task_count,
        run_id=str(manifest["run_id"]),
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        platform=platform.platform(),
        python_version=platform.python_version(),
    )


def _assert_cache_behavior(
    *,
    example: str,
    mode: str,
    manifest: dict[str, object],
) -> None:
    """Assert the benchmark run reflects the requested cache mode."""
    statuses = [str(task["status"]) for task in dict(manifest["tasks"]).values()]  # type: ignore[arg-type]
    if mode == "cold" and any(status == "cached" for status in statuses):
        raise AssertionError(f"Cold benchmark for {example} unexpectedly used cached tasks.")
    if mode != "cached":
        return

    if example in PARTIAL_CACHE_EXAMPLES:
        if not any(status == "cached" for status in statuses):
            raise AssertionError(f"Cached benchmark for {example} did not reuse any cached tasks.")
        return
    if example in MOSTLY_CACHE_EXAMPLES:
        cached_ratio = sum(status == "cached" for status in statuses) / len(statuses)
        if cached_ratio < 0.9:
            raise AssertionError(
                f"Cached benchmark for {example} reused too little cache ({cached_ratio:.0%})."
            )
        return
    if not statuses or not all(status == "cached" for status in statuses):
        raise AssertionError(f"Cached benchmark for {example} did not fully reuse cache.")


def _copy_example(*, name: str, destination_root: Path) -> Path:
    """Copy an example workflow into an isolated benchmark workspace."""
    source = EXAMPLES_ROOT / name
    destination = destination_root / name
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(
            ".ginkgo",
            "results",
            "logs",
            "__pycache__",
            ".pytest_cache",
        ),
    )
    return destination


def _timed_run(
    *,
    example_dir: Path,
    config_paths: list[Path],
    jobs: int,
    cores: int,
) -> tuple[float, dict[str, object]]:
    """Run one workflow invocation and return wall time plus manifest."""
    with _working_directory(path=example_dir):
        started = time.perf_counter()
        exit_code = run_workflow(
            workflow_path=discover_default_workflow(project_root=example_dir),
            config_paths=config_paths,
            jobs=jobs,
            cores=cores,
            memory=None,
            dry_run=False,
        )
    elapsed = time.perf_counter() - started
    if exit_code != 0:
        raise RuntimeError(
            f"Example benchmark failed for {example_dir.name} with exit code {exit_code}."
        )

    runs_root = example_dir / ".ginkgo" / "runs"
    run_dir = latest_run_dir(runs_root)
    if run_dir is None:
        raise RuntimeError(f"No run directory produced for benchmark {example_dir.name}.")
    return elapsed, load_manifest(run_dir)


@contextmanager
def _working_directory(*, path: Path) -> Iterator[None]:
    """Temporarily run code from the provided working directory."""
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextmanager
def _benchmark_runtime(*, example: str) -> Iterator[None]:
    """Apply local-only runtime mocks needed for example benchmarks."""
    with ExitStack() as stack:
        if example in {"init", "bioinfo"}:
            stack.enter_context(_mock_docker())
        if example in {"init", "retail"}:
            stack.enter_context(_mock_notebook_tools())
        yield


@contextmanager
def _mock_docker() -> Iterator[None]:
    """Mock Docker execution so container shell tasks run locally."""
    original_run_subprocess = _ConcurrentEvaluator._run_subprocess

    def _patched_run_subprocess(
        self_eval: Any,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any = None,
        on_stderr: Any = None,
    ) -> subprocess.CompletedProcess[str]:
        if isinstance(argv, list) and argv and argv[0] == "docker":
            completed = subprocess.run(
                argv[-1],
                shell=True,
                text=True,
                capture_output=True,
            )
            if on_stdout is not None and completed.stdout:
                on_stdout(completed.stdout)
            if on_stderr is not None and completed.stderr:
                on_stderr(completed.stderr)
            return subprocess.CompletedProcess(
                args=argv,
                returncode=completed.returncode,
                stdout=completed.stdout or "",
                stderr=completed.stderr or "",
            )
        return original_run_subprocess(
            self_eval,
            argv=argv,
            use_shell=use_shell,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
        )

    with (
        patch.object(_ConcurrentEvaluator, "_run_subprocess", _patched_run_subprocess),
        patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"),
        patch.object(ContainerBackend, "_image_exists_locally", return_value=True),
        patch.object(ContainerBackend, "_resolve_digest", return_value="sha256:benchmark_digest"),
    ):
        yield


@contextmanager
def _mock_notebook_tools() -> Iterator[None]:
    """Mock notebook tooling so notebook examples remain benchmarkable."""
    original_run_subprocess = _ConcurrentEvaluator._run_subprocess

    def _patched_run_subprocess(
        self_eval: Any,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any = None,
        on_stderr: Any = None,
    ) -> subprocess.CompletedProcess[str]:
        if isinstance(argv, str) and "papermill" in argv:
            parts = shlex.split(argv)
            output_path = Path(parts[4])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("executed notebook", encoding="utf-8")
            if on_stdout is not None:
                on_stdout("papermill ok\n")
            return subprocess.CompletedProcess(
                args=argv,
                returncode=0,
                stdout="papermill ok\n",
                stderr="",
            )
        if isinstance(argv, str) and "nbconvert" in argv:
            parts = shlex.split(argv)
            output_stem = parts[parts.index("--output") + 1]
            output_dir = Path(parts[parts.index("--output-dir") + 1])
            html_path = output_dir / f"{output_stem}.html"
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text("<html><body>notebook report</body></html>", encoding="utf-8")
            if on_stdout is not None:
                on_stdout("nbconvert ok\n")
            return subprocess.CompletedProcess(
                args=argv,
                returncode=0,
                stdout="nbconvert ok\n",
                stderr="",
            )
        return original_run_subprocess(
            self_eval,
            argv=argv,
            use_shell=use_shell,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
        )

    with patch.object(_ConcurrentEvaluator, "_run_subprocess", _patched_run_subprocess):
        yield
