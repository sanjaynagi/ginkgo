# Caching And Provenance

Caching and provenance are central to how Ginkgo stays practical across repeated
workflow runs.

## Cache Identity

The cache lives under `.ginkgo/cache/` and is content-addressed. At a high
level, Ginkgo hashes:

- task identity
- task version
- task source
- notebook source for notebook tasks
- resolved input values
- environment identity for foreign execution

For path-like inputs, the runtime hashes the contents rather than trusting the
path string alone.

## Artifact Storage

For file and folder outputs, Ginkgo stores content-addressed artifacts under
`.ginkgo/artifacts/` and uses those as the durable backing store for cached path
outputs.

That separation matters because a task's declared output path is not the source
of truth. The artifact store is.

## What Lives In A Run Directory

Each run gets a directory under `.ginkgo/runs/<run_id>/`. This is where Ginkgo
records runtime metadata such as:

- task-level status and timing information
- logs
- notebook artifacts
- run manifests and provenance payloads

Together, the cache and the run directory answer different questions:

- cache: can this work be reused safely?
- provenance: what happened in this specific run?

## Inspecting Cache State

Use the cache subcommands to inspect or clean cache state:

```bash
ginkgo cache ls
ginkgo cache clear <cache-key>
ginkgo cache prune --older-than 30d --dry-run
```

These commands are useful when you want to understand reuse behavior without
manually navigating hidden directories.

## Bounding Cache Size

`ginkgo cache prune` supports three eviction policies, which can be combined
in one invocation:

```bash
# Time-based: remove anything older than 30 days
ginkgo cache prune --older-than 30d

# Size-based: bring total cache size down to 5 GB
ginkgo cache prune --max-size 5GB

# Count-based: keep only the newest 500 entries
ginkgo cache prune --max-entries 500

# Combined: also remove anything older than 90 days
ginkgo cache prune --older-than 90d --max-size 5GB
```

Eviction is oldest-first, and orphaned artifacts are garbage-collected at the
end of the operation. Use `--dry-run` to preview what would be removed.

## Partial Resume

When a run fails partway through, Ginkgo preserves every successfully cached
task. Rerunning the same workflow picks up where the previous run left off:
tasks whose inputs are unchanged serve from cache, and only the tasks that
failed or were never reached are re-executed. The `cache_key` column in
`ginkgo cache ls` and the cache-hit markers in `ginkgo run` output make this
reuse visible. There is no separate resume command — the cache itself is the
resume mechanism.

## Dry-Run Mode

`ginkgo run workflow.py --dry-run` validates the workflow without executing
any task body. Ginkgo resolves the expression tree, checks environments and
secrets, computes cache keys for every task, and reports which tasks would
run, which would serve from cache, and which resources they declare. Dry-run
is the fastest way to confirm that a workflow is correctly wired, that every
declared environment exists, and that planned caching aligns with intent
before committing to a real run.

## Why This Matters In Practice

Caching is only useful if users can trust it. Provenance is only useful if users
can inspect what happened after the fact. Ginkgo keeps both features explicit so
that reruns remain understandable rather than magical.
