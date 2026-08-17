# Environments

Ginkgo separates orchestration from foreign execution. The scheduler stays
local, while shell, script, and notebook tasks run in declared environments.

## Pixi Environments

Pixi is the default way to define reproducible task environments.

In the canonical project layout, task-specific manifests typically live under:

```text
workflow/envs/<env_name>/pixi.toml
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

### What the container can see

A container sees only what is mounted into it. Ginkgo mounts the project root,
and then whatever the task *declares*: every `file` and `folder` argument
read-only, and every declared output read-write. Each is mounted at the same
absolute path it has on the host, so the paths in your command need no
rewriting, and a symlink still resolves because Ginkgo mounts the real path at
the declared one.

This is why annotating paths matters. `fastq: file` is visible inside the
container; the same path pulled out of config and interpolated into the command
string is not, because nothing declared it. Annotating is already required for
cache correctness, and the same annotation earns the mount.

### What the container inherits

Ginkgo forwards its own computed variables and nothing else: `GINKGO_THREADS`
always, and `OMP_NUM_THREADS` and friends when the task declares
`export_thread_env=True`. The rest of your shell environment stays outside,
which is the point of running in a container at all.

The container runs as you, not as root, so its outputs stay writable by later
Python tasks and by the next run.

### Configuration

Anything the declarations cannot express goes in `ginkgo.toml`:

```toml
[container]
runtime = "docker"                          # or "podman"
pull_policy = "if-not-present"              # "always" | "never"
user = "auto"                               # host uid:gid; "root" or "1000:1000"
shell = "bash"                              # for images that ship only "sh"
auto_mount = true
extra_mounts = ["/scratch:rw", "/opt/refs:ro"]
```

Use `extra_mounts` for paths no task declares — a tool's own cache directory, a
licence file. Entries take the form `"/path"`, `"/path:rw"`, or
`"/host:/container:rw"`, and default to read-only.

Set `user = "root"` for an image that installs software at runtime and needs to
write outside its mounts. Note that its outputs will then be root-owned.

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
