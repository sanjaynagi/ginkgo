"""CLI entry point for Phase 16 example benchmarks."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from benchmarks.harness import EXAMPLE_NAMES, run_example_benchmarks


def main() -> int:
    """Run the benchmark CLI."""
    argv = sys.argv[1:]
    if argv[:1] == ["--"]:
        argv = argv[1:]
    args = _build_parser().parse_args(argv)
    examples = list(args.examples) if args.examples else list(EXAMPLE_NAMES)
    run_example_benchmarks(
        examples=examples,
        results_root=args.results_root,
        baseline_path=args.baseline,
        strict=args.strict,
        jobs=args.jobs,
        cores=args.cores,
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Build the benchmark CLI parser."""
    parser = argparse.ArgumentParser(description="Run Ginkgo example benchmarks.")
    parser.add_argument(
        "--results-root",
        type=Path,
        default=Path("benchmarks/results"),
        help="Directory for structured benchmark result output.",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help="Optional checked-in baseline JSON used for slowdown comparison.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any benchmark exceeds its configured slowdown threshold.",
    )
    parser.add_argument(
        "--examples",
        nargs="*",
        choices=EXAMPLE_NAMES,
        help="Optional example subset to benchmark.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=4,
        help="Concurrent job budget for benchmark runs.",
    )
    parser.add_argument(
        "--cores",
        type=int,
        default=4,
        help="Core budget for benchmark runs.",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
