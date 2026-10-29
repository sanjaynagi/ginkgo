# Example: Bioinformatics Workflow

This example is a small but realistic workflow. It runs tasks in three places:
a Pixi environment, a Docker container and plain Python. It also runs the same
task once per sample.

This page walks through the code. To run the example, follow the
[Quickstart](../getting-started/quickstart.md#3-get-the-bioinformatics-example).
The code is in the Ginkgo repository, in `examples/bioinfo/workflow/flow.py`.

## Config And Samples

The workflow reads its settings and its sample sheet when the module loads:

```python
cfg = ginkgo.config("ginkgo.toml")
samples = pd.read_csv(cfg["paths"]["samples_csv"])
```

`ginkgo.toml` names the sample sheet, `data/samples.csv`, and sets
`min_length` under `[qc]`. The sample sheet has one row per sample, with the
columns `sample_id`, `fastq_1` and `fastq_2`. The two FASTQ columns hold each
sample's forward and reverse reads.

## The Tasks

The workflow has these tasks:

- `filter_fastq` drops short reads with `seqkit`, in a Pixi environment.
- `fastq_stats` computes quality-control statistics for each filtered sample,
  in the same Pixi environment.
- `count_reads` counts the reads in each filtered sample, inside a Docker
  container.
- `build_summary` merges the statistics and counts into one table, in plain
  Python.

`fastq_stats` and `count_reads` both read the filtered files and don't depend
on each other, so they run at the same time.

`filter_fastq` is a shell task. Its function builds a command from its
arguments, and Ginkgo runs that command in the `bioinfo_tools` Pixi
environment:

```python
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
        output=[out_1, out_2],
        log=f"logs/filter_{sample_id}.log",
    )
```

The `bioinfo_tools` environment is defined in
`workflow/envs/bioinfo_tools/pixi.toml`.

`count_reads` is also a shell task, but its `env` names a container image
instead of a Pixi environment. Ginkgo pulls the image and runs the command
inside it:

```python
@task(kind="shell", env="docker://ubuntu:24.04")
def count_reads(sample_id: str, fastq_1: file, fastq_2: file) -> file:
    ...
```

The `file` annotations tell Ginkgo to hash the contents of each FASTQ file,
not its path. If a file's contents change, the tasks that read it run again.
Ginkgo also reruns a task when its code changes.
[Cache Identity](../guide/caching-and-provenance.md#cache-identity) gives the
exact rule.

## The Flow

The flow only wires the tasks together and sets up the
[fan-out](../guide/concepts.md#glossary):

```python
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
    )
```

`min_length` is fixed for every sample. `.map()` runs `filter_fastq` once per
row of the sample sheet.

`filter_fastq` returns two files per sample. `filtered_pairs.output[0]` selects
the forward reads from every sample, and `filtered_pairs.output[1]` selects the
reverse reads. Passing them to `fastq_stats` and `count_reads` is what tells
Ginkgo that both depend on `filter_fastq`.

`build_summary` takes the lists of statistics and count tables. It runs once
every sample's tasks have finished, and writes `results/summary.csv`.

## See Also

- [Quickstart](../getting-started/quickstart.md): run this example, and see
  what each output folder holds.
- [Tasks and Flows](../guide/tasks-and-flows.md): the authoring model behind
  this example.
- [Environments](../guide/environments.md): how the Pixi and container
  environments used here are declared.
- [Caching and Provenance](../guide/caching-and-provenance.md): why a rerun of
  this workflow reuses earlier results.
