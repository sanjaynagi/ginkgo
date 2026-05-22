# Environments

Ginkgo separates orchestration from foreign execution. The scheduler stays
local, while shell, script, and notebook tasks run in declared environments.

## Pixi Environments

Pixi is the default way to define reproducible task environments.

In the canonical project layout, task-specific manifests typically live under:

```text
<project_package>/envs/<env_name>/pixi.toml
```

A shell, script, or notebook task references that environment by name through
`env=`:

```python
@task(kind="shell", env="bioinfo_tools")
def fastq_stats(sample_id: str, fastq: file) -> file:
    ...
```

Ginkgo resolves the environment, executes the shell payload inside it, and folds
the environment lock identity into the cache key.

## Conda Environment Files

If you already maintain a Conda `environment.yml`, you can point a task straight
at it instead of writing a `pixi.toml`:

```python
@task(kind="shell", env="envs/genomics/environment.yml")
def call_variants(sample_id: str, bam: file) -> file:
    ...
```

Ginkgo recognises a file named `environment.yml` or `environment.yaml` and
imports it into a generated Pixi workspace (via `pixi init --import`) stored in
a neighbouring `.ginkgo-pixi/` directory. The generated workspace is reused on
later runs and regenerated automatically when the source file changes.

A Conda environment must be referenced by path rather than by bare name, so the
`env` value contains a `/` — for example `envs/genomics/environment.yml` or
`./environment.yml`.

## Container Environments

Shell tasks can also target a container image through a URI-style environment
string.

```python
@task(kind="shell", env="docker://ubuntu:24.04")
def count_reads(sample_id: str, fastq: file) -> file:
    ...
```

Container-backed execution is currently intended for shell tasks only. Python
tasks still run in the scheduler's Python environment.

## Environment Commands

The CLI includes environment inspection and cleanup commands:

```bash
ginkgo env ls
ginkgo env clear <env-name>
ginkgo env clear --all --dry-run
```

Use these when you need to inspect or reset local environment state without
clearing the workflow cache itself.

## See Also

- [Caching and Provenance](caching-and-provenance.md) &mdash; environment lock
  identity is part of every environment-backed task's cache key.
- [Tasks and Flows](tasks-and-flows.md) &mdash; how shell, script, and notebook
  tasks are authored.
