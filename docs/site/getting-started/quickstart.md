# Quickstart

This quickstart uses the runnable bioinformatics example in `examples/bioinfo`.

## 1. Move Into The Example

```bash
cd examples/bioinfo
```

The example includes:

- a `ginkgo.toml` config file
- small FASTQ inputs
- a workflow package under `bioinfo/`
- a task-specific Pixi environment under `bioinfo/envs/bioinfo_tools/`

## 2. Inspect The Workflow Entry Point

The canonical flow lives in `examples/bioinfo/bioinfo/workflow.py`.

At a high level it:

1. filters each FASTQ in a Pixi environment
2. computes per-sample QC tables in the same environment
3. counts reads in a container-backed shell task
4. merges those outputs in a local Python task

## 3. Run The Workflow

```bash
ginkgo run
```

When you run from the example root, Ginkgo auto-discovers the canonical
`bioinfo/workflow.py` entrypoint.

If you prefer to be explicit:

```bash
ginkgo run bioinfo/workflow.py
```

## 4. Inspect Outputs

After a successful run, look at:

- `results/filtered/` for filtered FASTQs
- `results/qc/` for per-sample stats
- `results/read_counts/` for container-generated read-count tables
- `results/summary.csv` for the merged report
- `.ginkgo/runs/<run_id>/` for run provenance and logs

## 5. Re-Run To See Cache Reuse

Run the workflow again without changing inputs:

```bash
ginkgo run
```

Ginkgo should reuse cached results where the cache key still matches. The cache
identity includes task source, resolved inputs, and execution-environment
identity where relevant.

```{raw} html
<div class="section-note">
  If you are new to Ginkgo, read <a href="../../guide/concepts/">Core Concepts</a>
  next. It explains why task calls return deferred expressions and how that
  affects flow authoring.
</div>
```
