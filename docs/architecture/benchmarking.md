# Benchmarking

Ginkgo includes a benchmark harness centered on the runnable workflows
under `examples/`.

- The benchmark entry point is `pixi run benchmark`, which runs
  `python -m benchmarks.run`.
- Structured benchmark results are written under `benchmarks/results/`.
- Checked-in slowdown baselines live under `benchmarks/baselines/` as JSON.
- Benchmark runs print a readable terminal summary table in addition to writing
  structured JSON results.
- A dedicated GitHub Actions workflow runs the benchmark lane separately from
  correctness and quality checks.

## Benchmark Input Provenance

Benchmark-only source manifests live under `benchmarks/sources/`.

- These manifests pin upstream repository, commit SHA, metadata URL, and read
  URL base for generated benchmark datasets.
- The heavier bioinformatics benchmark uses
  [bioinfo_agam.toml](/Users/sanjay.nagi/Software/ginkgo/benchmarks/sources/bioinfo_agam.toml)
  to fetch a pinned metadata table, inject `fastq_1` and `fastq_2`, download
  the selected FASTQs into a benchmark workspace, and point the copied
  `examples/bioinfo` workflow at the generated sample sheet via a config
  overlay.

This keeps the canonical checked-in examples stable for documentation and
correctness tests while still allowing the benchmark lane to exercise a larger
input set.
