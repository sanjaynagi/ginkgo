# Canonical Example: Bioinformatics Workflow

This example is a small but realistic workflow. It shows the main runtime
boundaries — Pixi environments, containers, fan-out, and local aggregation —
in one place.

Source files:

- `examples/bioinfo/bioinfo/workflow.py`
- `examples/bioinfo/ginkgo.toml`

## What The Workflow Does

The flow executes four stages:

1. `filter_fastq` runs `seqkit` inside a Pixi environment
2. `fastq_stats` computes QC tables in the same Pixi environment
3. `count_reads` runs inside a Docker container
4. `build_summary` merges the per-sample outputs in a local Python task

Together the four stages show:

- shell tasks with a named Pixi environment
- shell tasks with a container URI
- `.map()` fan-out over multiple samples
- a Python task that performs downstream aggregation

## A Representative Task

Each stage is a small, typed task. `filter_fastq` is a shell task: the Python
wrapper builds a concrete command from resolved values, and only that command
runs inside the named Pixi environment.

```python
@task(kind="shell", env="bioinfo_tools")
def filter_fastq(sample_id: str, fastq: file, min_length: int) -> file:
    output = f"results/filtered/{sample_id}.fastq.gz"
    return shell(
        cmd=f"seqkit seq -m {min_length} {fastq} -o {output}",
        output=output,
        log=f"logs/filter_{sample_id}.log",
    )
```

The `file` annotations make the input and output content-addressed: Ginkgo
hashes the bytes, not the path string, so the cache key changes only when the
data actually changes.

## The Flow Structure

The flow body is intentionally thin &mdash; it composes task calls and defines
fan-out, nothing more:

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

## Running The Example

From the example root:

```bash
ginkgo run
```

Then inspect:

- `results/summary.csv`
- `.ginkgo/runs/`
- `logs/`

## See Also

- [Tasks and Flows](../guide/tasks-and-flows.md) &mdash; the authoring model
  behind this example.
- [Environments](../guide/environments.md) &mdash; how the Pixi and container
  environments used here are declared.
- [Caching and Provenance](../guide/caching-and-provenance.md) &mdash; why a
  rerun of this workflow reuses prior results.
