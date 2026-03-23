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

## Why This Matters In Practice

Caching is only useful if users can trust it. Provenance is only useful if users
can inspect what happened after the fact. Ginkgo keeps both features explicit so
that reruns remain understandable rather than magical.
