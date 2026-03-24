# Environments

Ginkgo separates orchestration from foreign execution. The scheduler stays local
while selected shell tasks run in declared environments.

## Pixi Environments

Pixi is the default way to define reproducible task environments.

In the canonical project layout, task-specific manifests typically live under:

```text
<project_package>/envs/<env_name>/pixi.toml
```

Then a shell task references that environment by name:

```python
@task(kind="shell", env="bioinfo_tools")
def fastq_stats(sample_id: str, fastq: file) -> file:
    ...
```

Ginkgo resolves the environment, executes the shell payload inside it, and folds
the environment lock identity into the cache key.

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

## Why Python Tasks Stay Local

Ginkgo treats `env=...` as a shell-task boundary. That keeps foreign execution
command-oriented and avoids requiring the Ginkgo runtime to be importable inside
every environment image.

This boundary is deliberate:

- flow construction stays scheduler-local
- validation stays scheduler-local
- cache decisions stay scheduler-local
- only the executable shell payload crosses into the foreign environment

## Environment Commands

The CLI includes environment inspection and cleanup commands:

```bash
ginkgo env ls
ginkgo env clear <env-name>
ginkgo env clear --all --dry-run
```

Use these when you need to inspect or reset local environment state without
clearing the workflow cache itself.
