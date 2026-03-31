# Phase 16 — Example Benchmarking and Performance Guardrails

## Problem Definition

Ginkgo currently has runnable example workflows under `examples/` and
integration tests that prove they execute correctly, but it does not yet have
a reproducible benchmark suite that answers the performance questions that
matter during runtime development:

- How long do the example workflows take from a clean workspace?
- How much faster are warm cached reruns?
- Do runtime changes introduce meaningful slowdowns across representative
  workflows?
- Can CI detect performance regressions before they accumulate?

The examples are the right benchmark source of truth because they exercise the
actual behaviors users rely on: task scheduling, cache reuse, dynamic fan-out,
notebook execution, container-backed tasks, and remote-file staging.

The benchmark design also needs to account for two practical constraints:

1. The canonical `examples/bioinfo` workflow should remain small and stable for
   docs and correctness tests.
2. GitHub Actions cannot currently stage benchmark inputs from OCI directly, so
   the heavier benchmark bioinformatics case must use CI-accessible pinned
   remote inputs instead.

## Proposed Solution

Implement Phase 16 as an example-driven benchmark system with four coordinated
parts:

1. A benchmark harness under `benchmarks/` that prepares inputs, runs the
   examples in controlled modes, and writes structured results.
2. A `pixi run benchmark` entry point that owns setup and execution.
3. A benchmark-only heavier bioinformatics case built from pinned GitHub-hosted
   FASTQs and pinned upstream metadata.
4. A separate benchmark CI lane that compares results against checked-in
   baselines and fails on significant regressions.

The design should keep correctness testing and performance measurement separate.
`tests/test_examples.py` remains the correctness gate; benchmarks should not
replace or weaken it.

---

## Scope

### In Scope

- Benchmark each runnable workflow under `examples/`
- Measure at least:
  - cold-start execution from a clean copied workspace
  - cached rerun execution after a successful first run
- Add benchmark setup for a heavier bioinformatics case without changing the
  canonical `examples/bioinfo` inputs
- Add a `pixi run benchmark` task
- Write structured benchmark outputs
- Store checked-in baselines for slowdown detection
- Add a separate benchmark CI workflow

### Out of Scope

- Snakemake-versus-Ginkgo comparative benchmarking
- OCI-dependent benchmark execution in GitHub Actions
- Per-task runtime instrumentation in the core task runner
- Broad hardware-lab benchmarking across multiple machine types

---

## Benchmark Questions

Phase 16 should answer these questions first:

- How do the example workflows perform on cold runs?
- How do they perform on cached reruns?
- Which examples are most sensitive to runtime regressions?
- Does the heavier bioinformatics case expose regressions in remote-input
  preparation and cache reuse that the smaller examples miss?
- Can a stable CI lane catch meaningful regressions without failing on normal
  runner noise?

---

## Proposed Architecture

## Part 1 — Benchmark Source Manifests

Use small checked-in manifests under `benchmarks/sources/` as the durable
source of truth for benchmark input provenance.

The current bioinformatics benchmark source manifest is:

- [bioinfo_agam.toml](/Users/sanjay.nagi/Software/ginkgo/benchmarks/sources/bioinfo_agam.toml)

The manifest should pin:

- upstream repository
- commit SHA
- metadata file URL
- reads base URL
- benchmark sample IDs
- generated read-column names
- filename patterns for read pairs

This keeps benchmark inputs reviewable without copying large data files or
duplicating full metadata tables into this repository.

## Part 2 — Benchmark Input Preparation

Add benchmark setup code under `benchmarks/` that:

1. loads the source manifest
2. downloads the pinned metadata TSV
3. validates the required columns exist, including `sample_id`
4. filters rows to the pinned sample IDs
5. injects `fastq_1` and `fastq_2` columns using the pinned reads base URL and
   filename patterns
6. writes a generated sample sheet into a benchmark workspace
7. writes any benchmark-specific config needed to point the bioinfo example at
   the generated sample sheet

This preparation step should be deterministic and should fail clearly if the
upstream pinned metadata file shape changes unexpectedly.

### Bioinformatics Benchmark Dataset

The heavier benchmark bioinformatics case should use the selected
`Anopheles gambiae` paired-end FASTQ files referenced by the pinned manifest.

The benchmark should not mutate the checked-in `examples/bioinfo/data/samples.csv`.
Instead, benchmark setup should create a generated dataset in an isolated
benchmark workspace and run the existing example workflow against that
generated input.

## Part 3 — Example Benchmark Harness

Add a benchmark harness that reuses the existing example execution pattern from
[tests/test_examples.py](/Users/sanjay.nagi/Software/ginkgo/tests/test_examples.py)
where practical.

The harness should support two modes:

- `cold`
  - copy the example into a fresh temporary workspace
  - prepare benchmark-specific inputs if needed
  - execute the first run
- `cached`
  - copy the example into a fresh temporary workspace
  - prepare benchmark-specific inputs if needed
  - run once to prime the cache
  - measure the second run
  - assert the rerun actually hit cache

The harness should preserve the current mocking strategy for notebook tooling
and Docker-backed shell tasks where needed so benchmarks remain runnable in CI
and in ordinary local developer environments.

## Part 4 — Result Schema

Benchmark output should be machine-readable and stable enough for CI
comparison. A JSON result schema is the simplest first choice.

Each benchmark record should include at least:

- example name
- benchmark case name
- mode: `cold` or `cached`
- wall-clock duration
- run status
- total task count
- executed task count
- cached task count
- benchmark command or harness version
- machine/runtime metadata needed to interpret results

Suggested output layout:

```text
benchmarks/results/
  latest.json
  YYYYMMDDTHHMMSSZ/
    results.json
```

The first implementation should optimize for clarity and comparability rather
than maximum metric depth.

## Part 5 — Checked-In Baselines

Use checked-in baseline files as the first baseline mechanism.

Suggested layout:

```text
benchmarks/baselines/
  github-actions-linux.json
```

Baseline files should store:

- the benchmark cases included in that lane
- expected durations
- allowed slowdown thresholds per benchmark
- metadata describing the runner class they apply to

Checked-in baselines are the right initial choice because they are:

- explicit
- reviewable in code review
- easy to refresh intentionally
- simple to consume in CI

## Part 6 — Slowdown Policy

CI should fail only on meaningful regressions, not on ordinary variance.

The slowdown policy should therefore:

- compare against per-benchmark baselines, not one global threshold
- support absolute or percentage thresholds per benchmark
- focus on wall-time regressions first
- allow cached and cold benchmarks to use different thresholds

The first implementation should stay simple:

- per benchmark case, store `baseline_seconds`
- per benchmark case, store `max_regression_pct`
- fail if `observed_seconds > baseline_seconds * (1 + max_regression_pct / 100)`

This is intentionally conservative and easy to reason about.

## Part 7 — Pixi Task

Add a `pixi run benchmark` task that owns the full benchmark flow:

- benchmark input preparation
- generated bioinfo sample-sheet/config creation
- benchmark execution
- result writing
- optional baseline comparison

This should be the primary local interface for running benchmarks.

If needed, add a second task later for CI-specific comparison, but the first
version should avoid unnecessary task sprawl.

## Part 8 — Separate CI Workflow

Add a dedicated benchmark CI workflow under `.github/workflows/` separate from
the main correctness CI.

Recommended shape:

- PR lane:
  - run a stable subset of example benchmarks
  - compare to the checked-in GitHub Actions baseline
  - fail on significant regressions
- scheduled or manual lane:
  - run the full example benchmark matrix, including the heavier bioinfo case
  - publish artifacts for deeper review

This keeps correctness CI fast and avoids mixing noisy performance checks into
the default test path.

---

## Risks and Tradeoffs

- GitHub-hosted remote downloads are CI-compatible, but network timing is
  noisier than local files. To keep regression checks stable, the benchmark
  should separate input preparation from workflow runtime where possible.
- If benchmark setup time and workflow runtime are combined into one number,
  regressions become harder to interpret. The harness should keep setup and run
  boundaries explicit even if it only gates on total runtime at first.
- Cached reruns can become very fast for smaller examples, making percentage
  thresholds noisy. Those examples may need looser thresholds or exclusion from
  strict gating.
- Baselines will need periodic refresh as the runtime evolves. Checked-in
  baselines make this explicit, but they still require discipline.
- The heavier bioinfo benchmark improves realism, but it also increases CI
  runtime and download volume. That is why it belongs in a dedicated benchmark
  lane rather than the main test lane.

---

## Success Criteria

- `pixi run benchmark` prepares inputs and executes the benchmark suite
  end-to-end.
- Each runnable example has at least cold and cached benchmark coverage.
- The heavier bioinformatics benchmark is generated from the pinned source
  manifest and runs without modifying the canonical example data files.
- Benchmark results are written in a structured format suitable for automated
  comparison.
- Checked-in baselines are used to compare CI runs against expected timings.
- The separate benchmark CI workflow fails on an intentionally introduced
  significant slowdown.
- Existing correctness tests remain the correctness gate and are not replaced
  by benchmark runs.

---

## Implementation Slices

Implement Phase 16 in this order:

1. Benchmark result schema and source-manifest loading
2. Bioinfo benchmark input preparation from pinned metadata and reads
3. Example benchmark harness for cold and cached modes
4. `pixi run benchmark`
5. Checked-in baseline comparison
6. Separate benchmark CI workflow

This order keeps the benchmark system usable locally before adding CI policy.

---

## Validation

- Run benchmark preparation for the bioinfo benchmark source manifest and assert
  the generated sample sheet contains the selected sample IDs plus `fastq_1`
  and `fastq_2`.
- Run the example benchmark harness locally and assert each example emits
  structured results for both cold and cached modes.
- Assert cached-mode benchmarks record cache hits rather than re-running all
  tasks.
- Compare a run against a matching checked-in baseline and assert the result is
  accepted when under threshold.
- Intentionally perturb a stored baseline or inject a slowdown and assert the
  comparison fails with a clear message identifying the regressed benchmark.
