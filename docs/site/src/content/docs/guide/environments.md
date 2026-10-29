---
title: "Environments"
---

A shell, script or notebook task can run in its own software environment. Its
tools and library versions then do not clash with another task's, or with your
own machine's. You declare the environment on the task with `env=`, and Ginkgo
sets it up for you.

A task runs in one of these environments:

- **A Pixi environment.** Pixi is a package manager. It builds an isolated
  environment from a `pixi.toml` file. You write the file, and Ginkgo installs
  the environment.
- **A container image**, named by a `docker://` or `oci://` URI. You name the
  image, and Ginkgo pulls it.
- **The environment the `ginkgo` CLI runs from.** This is where a task runs when
  it has no `env=`. Python tasks always run here.

## Python Tasks Run In The CLI's Own Environment

`env=` works on shell, script and notebook tasks only. A Python task with
`env=` is rejected before any work starts, and `ginkgo doctor` reports it as
`invalid_env_kind`.

A Python task runs under the same Python interpreter as the `ginkgo` CLI.
**So every import in a Python task comes from the CLI's environment, not from
your project's `pixi.toml`.** If `import pandas` fails, install pandas where
Ginkgo is installed:

- **As a uv tool** (the
  [curl installer](/ginkgo/getting-started/installation/#quick-install-curl)):
  rerun `uv tool install --force` with `--with pandas`, and keep every plugin
  you had as extras. [Plugins](/ginkgo/getting-started/installation/#plugins)
  shows the command.
- **From a Pixi environment:** add the package to its `pixi.toml` and run
  `pixi install`.
- **With `pip install -e .`:** run `pip install pandas` in that environment.

`head -1 "$(command -v ginkgo)"` shows which interpreter that is. When a task
needs a package version that conflicts with the CLI's, or a tool that isn't a
Python package, make it a script or shell task with `env=`.

## Ginkgo Sets Up Environments For You

You do not install an environment before the first run. The first time a task
needs one, Ginkgo runs `pixi install` for a Pixi environment, or pulls a
container image if it isn't already on your machine. If a Pixi install fails,
the run stops and shows the Pixi output.

So the first run takes longer. When setup takes more than a second, the run
says so:

```text
⏱ Completed in 8.8s - 1 task executed, 0 cached
⚙ Environment preparation took 7.2s (analysis_tools) - first runs install
environments, later runs reuse them
```

Later runs reuse the environment.

`pixi`, or `docker` or `podman`, must be on your `PATH`. See
[Runtime Prerequisites](/ginkgo/getting-started/installation/#runtime-prerequisites).

## Pixi Environments

Pixi environments come from the `ginkgo-pixi`
[plugin](/ginkgo/guide/concepts/#glossary), which the curl installer includes. See
[Plugins](/ginkgo/getting-started/installation/#plugins).

In a project made by `ginkgo init`, each environment has its own directory:

```text
workflow/envs/<env_name>/pixi.toml
```

A shell, script or notebook task names the environment with `env=`:

```python
@task(kind="shell", env="bioinfo_tools")
def fastq_stats(sample_id: str, fastq: file) -> file:
    ...
```

A notebook task's environment must provide `marimo`. The environment is part
of each task's cache key. See [Caching and Provenance](/ginkgo/guide/caching-and-provenance/).

### How A Named Environment Is Resolved

For `env="<name>"`, Ginkgo looks for a directory `<name>` that holds a
`pixi.toml`, or a `pyproject.toml` with a `[tool.pixi]` table. It uses the
first match from:

1. `envs/` beside the project's main `flow.py` (`workflow/envs/` in a project
   made by `ginkgo init`).
2. `envs/` in the project root.

A value that contains a `/`, such as `env="envs/genomics/pixi.toml"`, is a path
to the manifest, resolved against the project root. If Ginkgo cannot find the
environment, the error lists every directory it searched.

### Where An Environment Is Installed

Pixi installs an environment in a `.pixi/` directory beside its manifest, which
the `.gitignore` from `ginkgo init` already ignores. An environment can take
about 435 MB for Python with pandas and marimo, but Pixi shares files between
environments.

To list and remove installed Pixi environments, without touching the workflow
cache:

```bash
ginkgo env ls
ginkgo env clear <env-name>
ginkgo env clear --all --dry-run
```

The next run that needs a cleared environment installs it again. For container
images, use `docker images` and `docker image rm`, or the `podman` equivalents.
Ginkgo does not support Pixi's `detached-environments` setting.

## Conda Environment Files

If you already have a Conda `environment.yml`, point a task straight at it
instead of writing a `pixi.toml`:

```python
@task(kind="shell", env="envs/genomics/environment.yml")
def call_variants(sample_id: str, bam: file) -> file:
    ...
```

The file must be named `environment.yml` or `environment.yaml`, and the `env`
value must contain a `/`, as in `./environment.yml`. Ginkgo converts it to a
Pixi environment in a `.ginkgo-pixi/` directory beside it, and regenerates
that when the Conda file changes. Don't edit `.ginkgo-pixi/`.

Ginkgo reads no other environment files: no `requirements.txt`, plain `venv`,
or `conda activate`.

## Container Environments

A shell, script or notebook task can run inside a container image. Name the
image with a `docker://` or `oci://` URI:

```python
@task(kind="shell", env="docker://ubuntu:24.04")
def count_reads(sample_id: str, fastq: file) -> file:
    ...
```

`docker://` and `oci://` mean the same thing here. This `oci://` is unrelated
to the `oci://` URIs for Oracle Cloud object storage.

Container tasks need the Docker or Podman daemon running: on macOS and
Windows, Docker Desktop or the Podman machine. If it isn't, the task fails with
a hint on how to start it, and `ginkgo doctor` warns.

### What The Container Can See

**Annotate every path your command uses**, as a `file`, a `folder` or an
output. A container sees only the project root and the paths the task
declares, each at the same absolute path as on your machine. A path read from
config and pasted into the command string is not visible.

- **Inputs are read-only.** A `file` input also brings its directory, so a
  tool finds a neighbouring file such as `ref.fa.fai` or a `.bai`.
- **Only declared outputs are kept.** Anything written elsewhere is lost when
  the container exits.

So to create an index beside an input, declare the index as an output:

```python
@task(kind="shell", env="docker://quay.io/biocontainers/samtools:1.20--h50ea8bc_0")
def index_reference(reference: file) -> file:
    index = f"{reference}.fai"
    return shell(cmd=f"samtools faidx {shlex.quote(reference)}", output=index)
```

**Write outputs to a directory of their own, not straight into your home
directory.** Ginkgo never mounts your home directory itself, the filesystem
root, or a top-level system directory such as `/etc`. A directory inside one,
such as `~/projects/run1`, is fine.

### What The Container Inherits

Your shell variables are not passed in. The container gets `GINKGO_THREADS`,
and with `export_thread_env=True`, `OMP_NUM_THREADS` and the related
thread-count variables.

The container runs as your user, not as root, so the files it writes belong to
you.

### Configuration

Set anything the task declarations cannot express in `ginkgo.toml`:

```toml
[container]
runtime = "docker"                          # or "podman"
pull_policy = "if-not-present"              # "always" re-pulls; "never" pulls nothing
user = "auto"                               # your uid:gid; or "root", or "1000:1000"
shell = "bash"                              # "sh" for Alpine images without bash
auto_mount = true
extra_mounts = ["/scratch:rw", "/opt/refs:ro"]
```

Ginkgo rejects an unknown key or value.

- `extra_mounts` adds paths no task declares, such as a licence file. An entry
  is `"/path"`, `"/path:rw"` or `"/host:/container:rw"`, read-only by default.
  The home-directory rule above applies here too.
- `auto_mount = false` turns off the mounts that come from task declarations.
  Your `extra_mounts` still apply.
- `user = "root"` suits an image that installs software as it runs. Its outputs
  are then owned by root.

## See Also

- [Caching and Provenance](/ginkgo/guide/caching-and-provenance/) &mdash; how the
  environment affects a task's cache key.
- [Tasks and Flows](/ginkgo/guide/tasks-and-flows/) &mdash; how to write shell, script and
  notebook tasks.
