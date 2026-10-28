# Environments

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

`env=` works on shell, script and notebook tasks only. A Python task
(`kind="python"`) with `env=` is rejected before any work starts:

```text
✖ workflow.flow.summarise uses env 'analysis_tools' but is declared with
kind='python', which cannot run in a foreign environment. Use one that can:
@task('shell'), @task('script'), @task('notebook').
```

`ginkgo doctor` reports the same problem as `invalid_env_kind`, without running
anything.

A Python task runs in a worker process that uses the same Python interpreter as
the `ginkgo` CLI. **So every import in a Python task comes from the CLI's
environment, not from your project's `pixi.toml`.** If `import pandas` fails
with `ModuleNotFoundError`, install pandas in the CLI's environment. Listing it
in a task environment does not help.

To find that interpreter, read the first line of the `ginkgo` command:

```bash
head -1 "$(command -v ginkgo)"
```

```text
#!/Users/you/Software/ginkgo/.pixi/envs/default/bin/python3.14
```

How you add a package depends on how you installed Ginkgo:

- **As a uv tool** (the
  [curl installer](../getting-started/installation.md#quick-install-curl)).
  Reinstall Ginkgo with `uv tool install --force` and add `--with pandas`. Keep
  every plugin you installed before, such as `ginkgo-pixi`, in the `--with`
  list, because `--force` replaces the whole list.
  [Plugins](../getting-started/installation.md#plugins) shows the command.
- **From a Pixi workspace.** Add the package to that workspace's manifest and
  run `pixi install`.
- **With `pip install -e .`** Run `pip install pandas` in the same environment.

Ginkgo does not manage the CLI's environment for you. When a task needs
packages of its own, make it a script or shell task with `env=`. This applies to
a package version that conflicts with the CLI's, and to a tool that is not a
Python package. Ginkgo then installs the environment for you.

## Ginkgo Sets Up Environments For You

You do not install an environment before the first run. Ginkgo does it the
first time a task needs it, once per environment per run:

- Ginkgo installs a Pixi environment with
  `pixi install --manifest-path <manifest>`. If the install fails, the run stops
  and shows the Pixi output.
- Ginkgo pulls a container image with `<runtime> pull <image>`. The default
  `pull_policy = "if-not-present"` skips the pull when the image is already on
  your machine.

So the first run spends time solving, downloading and linking packages. Later
runs do not. When that time is more than a second, the run says so:

```text
⏱ Completed in 8.8s - 1 task executed, 0 cached
⚙ Environment preparation took 7.2s (analysis_tools) - first runs install
environments, later runs reuse them
```

The same workflow on the second run:

```text
⏱ Completed in 0.7s - 1 task executed, 0 cached
```

The 7.2 s on the first run is the time `pixi install` took. Pixi links packages
from a shared cache, so installing an environment whose packages are already in
the cache takes about a second.

Ginkgo runs these tools as commands, so they must be on your `PATH`. Pixi
environments need `pixi`. Container environments need `docker` or `podman`. See
[Runtime Prerequisites](../getting-started/installation.md#runtime-prerequisites).

## Pixi Environments

Pixi environments come from the `ginkgo-pixi`
[plugin](concepts.md), which the curl installer includes. See
[Plugins](../getting-started/installation.md#plugins). Without the plugin, a
task with `env="name"` fails before the run starts, and the error names the
plugin.

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

Ginkgo finds the environment, installs it if needed, and runs the shell command
inside it. A notebook task runs `python -m marimo` inside its environment, so
that environment must provide `marimo`.

The environment is part of each task's cache key. See
[Caching and Provenance](caching-and-provenance.md) for which environment
changes make a task run again.

### How A Named Environment Is Resolved

For `env="<name>"`, Ginkgo looks for a directory `<name>` that holds a
`pixi.toml`, or a `pyproject.toml` with a `[tool.pixi]` table. It searches these
places, in order, and uses the first match:

1. `envs/` beside the project's main `flow.py`. Ginkgo finds the main `flow.py`
   the same way `ginkgo run` does when you give it no path. In a project made by
   `ginkgo init`, this is `workflow/envs/`.
2. `envs/` in the project root.

Step 1 uses the main `flow.py` even when you run a different workflow file. If
the project has no single main `flow.py`, because it has none or several, step
1 searches `envs/` beside the file you are running instead.

A value that contains a `/`, such as `env="envs/genomics/pixi.toml"`, is a path
to the manifest file. Ginkgo resolves it against the project root and skips the
search.

If Ginkgo cannot find the environment, the error lists every directory it
searched and what each one holds.

### Where An Environment Is Installed

Pixi installs an environment next to its manifest, in a `.pixi/` directory:
`workflow/envs/<env_name>/.pixi/envs/default/`. The `.gitignore` that
`ginkgo init` writes already ignores `.pixi/`.

An environment can be large: about 435 MB for Python with pandas and marimo.
Pixi shares files between environments, so a second environment with the same
packages uses little extra disk.

The `ginkgo-pixi` plugin adds commands to list and remove installed
environments. They leave the workflow cache alone:

```bash
ginkgo env ls
ginkgo env clear <env-name>
ginkgo env clear --all --dry-run
```

`ginkgo env clear` removes an environment's install directory. The next run that
needs the environment installs it again.

These commands cover Pixi environments only. They don't list container images.
Use `docker images` and `docker image rm`, or the `podman` equivalents, for
those:

```text
🌿 ginkgo env ls

┌────────────────┬───────────┬────────────────────────┬────────────────────────┐
│ Env            │ Installed │ Manifest               │ Install Dir            │
├────────────────┼───────────┼────────────────────────┼────────────────────────┤
│ analysis_tools │ yes       │ /private/tmp/rnaseq/w… │ /private/tmp/rnaseq/w… │
└────────────────┴───────────┴────────────────────────┴────────────────────────┘
Container environments are managed by the container runtime and are not listed
here.
```

Ginkgo does not support Pixi's `detached-environments` setting, which installs
environments outside the project. `ginkgo env ls` and `ginkgo env clear` look
for `.pixi/` beside the manifest, so they would miss a detached environment.

## Conda Environment Files

If you already have a Conda `environment.yml`, point a task straight at it
instead of writing a `pixi.toml`:

```python
@task(kind="shell", env="envs/genomics/environment.yml")
def call_variants(sample_id: str, bam: file) -> file:
    ...
```

Ginkgo converts the file to a Pixi environment, so every environment is
installed the same way. It recognises a file named `environment.yml` or
`environment.yaml`, and imports it with `pixi init --import` into a
`.ginkgo-pixi/` directory beside it. Ginkgo reuses that directory on later runs,
and regenerates it when the Conda file changes. Don't edit `.ginkgo-pixi/`,
because Ginkgo overwrites it.

Refer to a Conda file by its path, so the `env` value contains a `/`. For
example, `envs/genomics/environment.yml` or `./environment.yml`.

Pixi manifests and Conda files are the only environment files Ginkgo reads.
There is no support for `requirements.txt`, a plain `venv`, or
`conda activate`.

## Container Environments

A shell, script or notebook task can run inside a container image. Name the
image with a `docker://` or `oci://` URI:

```python
@task(kind="shell", env="docker://ubuntu:24.04")
def count_reads(sample_id: str, fastq: file) -> file:
    ...
```

`docker://` and `oci://` mean the same thing here: an image that Docker or
Podman runs. This `oci://` is unrelated to the `oci://` URIs for Oracle Cloud
object storage, which name remote input files.

Ginkgo pulls the image the first time a task needs it, so you don't pull it by
hand.

Container tasks need the Docker or Podman daemon running. On macOS and Windows,
that means Docker Desktop or the Podman machine. Before Ginkgo pulls an image, it
checks that the daemon answers. If it does not, the task fails with a hint on
how to start it. A run whose container tasks are all cached does not need the
daemon. `ginkgo doctor` and `ginkgo run --dry-run` warn when the daemon is not
running.

### What The Container Can See

**Annotate every path your command uses**, as a `file`, a `folder` or an output.
A container sees only what Ginkgo mounts into it: the project root, plus the
paths the task declares. `fastq: file` is visible inside the container. The
same path read from config and pasted into the command string is not, because
nothing declared it. The same annotations also make caching correct.

Ginkgo mounts each path at the same absolute path it has on your machine, so
the paths in your command need no rewriting. A symlink still resolves, because
Ginkgo mounts the real path at the path you wrote. Three more rules apply:

- **Inputs are read-only.** Every `file` and `folder` input is mounted
  read-only.
- **A file input brings its directory.** A `file` input mounts the directory it
  sits in, so a tool that reads a neighbouring file finds it. For example,
  `ref.fa.fai` beside `ref.fa`, or a `.bai` beside a `.bam`.
- **Only declared outputs are kept.** Every declared output is mounted
  read-write. A container that writes anywhere else writes into its own
  filesystem, which is deleted when the container exits. The file seems to be
  written, then it is gone.

So a tool that wants to create an index beside a read-only input fails, and says
so. Declare the index as an output to make it writable and keep it:

```python
@task(kind="shell", env="docker://quay.io/biocontainers/samtools:1.20--h50ea8bc_0")
def index_reference(reference: file) -> file:
    index = f"{reference}.fai"
    return shell(cmd=f"samtools faidx {shlex.quote(reference)}", output=index)
```

**Write outputs to a directory of their own, not straight into your home
directory.** Ginkgo never mounts your home directory itself, so the image cannot
see `~/.ssh` or `~/.aws`. It also never mounts the filesystem root or a
top-level system directory such as `/etc` or `/usr`. A directory inside one of
these, such as `~/projects/run1`, is fine. The rule applies to `extra_mounts`
too.

### What The Container Inherits

The container sees only the variables Ginkgo sets for the task. It always gets
`GINKGO_THREADS`. With `export_thread_env=True`, it also gets `OMP_NUM_THREADS`
and the related thread-count variables. Your other shell variables are not
passed in.

The container runs as your user, not as root, so the files it writes belong to
you. Later Python tasks and the next run can then change them. Leave
`user = "auto"` unless an image needs root.

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

Ginkgo checks these settings when it validates a container task, and rejects an
unknown key or value. `runtime` must be `"docker"` or `"podman"`. `pull_policy`
must be `"if-not-present"`, `"always"` or `"never"`.

Use `extra_mounts` for paths no task declares, for example a tool's own cache
directory or a licence file. An entry is `"/path"`, `"/path:rw"` or
`"/host:/container:rw"`, and is read-only by default. If a task declares an
output inside a path you marked read-only, the run stops with an error.

`auto_mount = false` turns off the mounts that come from task declarations. Your
`extra_mounts` still apply.

Set `user = "root"` for an image that installs software as it runs and needs to
write outside its mounts. Its outputs are then owned by root.

## See Also

- [Caching and Provenance](caching-and-provenance.md) &mdash; how the
  environment affects a task's cache key.
- [Tasks and Flows](tasks-and-flows.md) &mdash; how to write shell, script and
  notebook tasks.
