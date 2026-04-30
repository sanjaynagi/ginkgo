# Canonical Example: Bioinformatics Workflow

The bioinformatics example is the canonical walkthrough for the docs site
because it demonstrates the main runtime boundaries in a compact, realistic
workflow.

Source files:

- `examples/bioinfo/bioinfo/workflow.py`
- `examples/bioinfo/ginkgo.toml`

## What The Workflow Does

The flow executes four stages:

1. `filter_fastq` runs `seqkit` inside a Pixi environment
2. `fastq_stats` computes QC tables in the same Pixi environment
3. `count_reads` runs inside a Docker container
4. `build_summary` merges the per-sample outputs in a local Python task

That combination makes the example useful because it shows:

- shell tasks with a named Pixi environment
- shell tasks with a container URI
- `.map()` fan-out over multiple samples
- a Python task that performs downstream aggregation

## The Flow Structure

The flow body is intentionally thin:

```python
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
    read_counts = count_reads().map(
        sample_id=samples["sample_id"],
        fastq=filtered_fastqs,
    )
    return build_summary(
        sample_ids=samples["sample_id"].tolist(),
        stats_tables=qc_tables,
        count_tables=read_counts,
    )
```

Each task owns one clear responsibility and one execution boundary.

## Why This Is A Good Canonical Example

It is small enough to read quickly, but it already demonstrates most of the
ideas that matter to new Ginkgo users:

- deferred expressions
- fan-out and fan-in
- environment-specific shell execution
- local Python post-processing
- reproducible reruns through caching

## Running The Example

From the example root:

```bash
ginkgo run
```

Then inspect:

- `results/summary.csv`
- `.ginkgo/runs/`
- `logs/`
