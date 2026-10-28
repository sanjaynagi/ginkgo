# Quickstart

This quickstart has two parts. First you run a one-task workflow that needs
nothing but the `ginkgo` command. Then you run the bioinformatics example from
the Ginkgo repository, which uses Pixi and a container.

You need the `ginkgo` command first. See [Installation](installation.md).

## 1. Run Your First Workflow

In an empty directory, save the
[minimal workflow](../docs.md#a-minimal-workflow) from the docs front page as
`flow.py`. Then run it:

```bash
ginkgo run flow.py
```

Ginkgo shows a table of tasks as they run. When the run ends, the table shows
`write_text` as `succeeded`, and `hello.txt` is in the directory.

## 2. Run It Again To See The Cache

Run the same command again:

```bash
ginkgo run flow.py
```

This time nothing runs. Ginkgo reports the task as `cached`:

```text
┌────────────┬──────────┬─────────────┬──────┐
│ Task       │ Status   │ Environment │ Time │
├────────────┼──────────┼─────────────┼──────┤
│ write_text │ ↺ cached │ local       │ 0.0s │
└────────────┴──────────┴─────────────┴──────┘

⏱ Completed in 0.1s - 0 tasks executed, 1 cached
```

Ginkgo reused the result because nothing changed. It decides this from the
task's code, its inputs and its environment.
[Cache Identity](../guide/caching-and-provenance.md#cache-identity) gives the
exact rule. Change `message` in `flow.py` and run again, and `write_text` runs
again.

## 3. Get The Bioinformatics Example

The example lives in `examples/bioinfo` in the Ginkgo repository. It is not
installed with the `ginkgo` command, so clone the repository:

```bash
git clone https://github.com/sanjaynagi/ginkgo.git
cd ginkgo/examples/bioinfo
```

The example needs two programs on your `PATH`:

- [Pixi](https://pixi.sh/). Two of the example's tasks run in a Pixi
  environment, which Ginkgo installs on first use.
- Docker or Podman. One task runs in a container image, which Ginkgo pulls on
  first use.

[Runtime Prerequisites](installation.md#runtime-prerequisites) describes both.

Open `workflow/flow.py`. The workflow has these tasks:

- `filter_fastq` filters each sample's reads with `seqkit`, in a Pixi
  environment. Pixi installs tools such as `seqkit` into an isolated
  environment.
- `fastq_stats` computes quality-control (QC) statistics for each filtered
  sample, in the same environment.
- `count_reads` counts the reads in each filtered sample, inside a Docker
  container.
- `build_summary` merges the statistics and counts into one table, in plain
  Python.

`fastq_stats` and `count_reads` both read the filtered files and don't depend
on each other, so they run at the same time.
[Example: Bioinformatics Workflow](../examples/bioinfo-workflow.md) walks
through the code.

## 4. Preview With A Dry Run

Check the workflow before you run it for real:

```bash
ginkgo run --dry-run
```

A dry run checks the workflow and prints a plan. No task runs. The plan lists
which tasks would run, which would reuse cached results, and the resources
each task declares.

## 5. Run The Workflow

```bash
ginkgo run
```

With no file named, `ginkgo run` looks for `workflow/flow.py` in the current
directory. To name the file yourself:

```bash
ginkgo run workflow/flow.py
```

## 6. Look At The Outputs

After a successful run, look at:

- `results/filtered/` for the filtered reads
- `results/qc/` for each sample's statistics
- `results/read_counts/` for the read counts made in the container
- `results/summary.csv` for the merged table
- `logs/` for the tool logs

Ginkgo also records the run, which is the run's
[provenance](../guide/concepts.md#glossary). The last line of the run output
names the run's directory under `.ginkgo/runs/`. To list your runs and show
one of them:

```bash
ginkgo runs ls
ginkgo runs show <run_id>
```

[Querying Provenance](../guide/querying-provenance.md) covers the other ways
to read the record.

## What To Read Next

Read [Core Concepts](../guide/concepts.md) next. It explains why calling a
task doesn't run it, and how a task can add new tasks while the workflow runs.
