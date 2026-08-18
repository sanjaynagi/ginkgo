# Environments

Ginkgo separates orchestration from foreign execution. The scheduler stays
local, while shell, script, and notebook tasks run in declared environments.

Three environments are in play, and it pays to keep them apart:

- **The environment the `ginkgo` CLI runs from.** It runs the scheduler and
  every `kind="python"` task body. You provision it — see
  [Python Tasks Run In The CLI's Own Environment](#python-tasks-run-in-the-clis-own-environment).
- **Pixi environments**, named or pointed at by `env=`. You write the manifest;
  Ginkgo installs it.
- **Container images**, named by a `docker://` or `oci://` URI. You name the
  image; Ginkgo pulls it.

## Ginkgo Materialises Declared Environments

You do not install a declared environment before the first run. Ginkgo does it,
on first use, once per environment per run:

- A Pixi environment is installed with `pixi install --manifest-path <manifest>`
  the first time a task needs it. A failure stops the run with the Pixi output
  attached.
- A container image is pulled with `<runtime> pull <image>` under the default
  `pull_policy = "if-not-present"` — skipped when the image is already present
  locally. `pull_policy` *controls* the pull; it does not describe one you
  performed yourself.

The first run therefore pays a solve, download, and link cost that later runs do
not. When that cost exceeds a second, the run says so:

```text
⏱ Completed in 8.8s - 1 tasks executed, 0 cached
⚙ Environment preparation took 7.2s (analysis_tools) - first runs install
environments, later runs reuse them
```

The same workflow on the second run:

```text
⏱ Completed in 0.7s - 1 tasks executed, 0 cached
```

Because Ginkgo shells out to do this, the tools must be on your `PATH`: `pixi`
for Pixi environments, and `docker` or `podman` for container environments. See
[Runtime Prerequisites](../getting-started/installation.md#runtime-prerequisites).

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

Ginkgo resolves the environment, installs it if it is not installed yet, executes
the shell payload inside it, and folds the environment lock identity into the
cache key.

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

Ginkgo pulls the image itself the first time a task needs it, so nothing has to
be pulled by hand. Container-backed execution is currently intended for shell
tasks only; Python tasks run in the environment the CLI itself runs from, covered
in [Python Tasks Run In The CLI's Own Environment](#python-tasks-run-in-the-clis-own-environment).

### What the container can see

A container sees only what is mounted into it. Ginkgo mounts the project root,
and then whatever the task *declares*: every `file` and `folder` argument
read-only, and every declared output read-write. Each is mounted at the same
absolute path it has on the host, so the paths in your command need no
rewriting, and a symlink still resolves because Ginkgo mounts the real path at
the path you wrote.

This is why annotating paths matters. `fastq: file` is visible inside the
container; the same path pulled out of config and interpolated into the command
string is not, because nothing declared it. Annotating is already required for
cache correctness, and the same annotation earns the mount.

A `file` argument mounts the directory it sits in, not just the file, so a tool
that reads a sibling — `ref.fa.fai` beside `ref.fa`, `.bai` beside `.bam` — finds
it. That directory is read-only, so a tool that wants to *create* its index will
fail and tell you. Declare the index as an output and it becomes writable:

```python
@task(kind="shell", env="docker://quay.io/biocontainers/samtools:1.20--h50ea8bc_0")
def index_reference(reference: file) -> file:
    index = f"{reference}.fai"
    return shell(cmd=f"samtools faidx {reference}", output=index)
```

Declaring it is also what keeps it. Anything a container writes to a path that
is not mounted goes into the container's own filesystem, which is discarded when
the container exits — so an undeclared index appears to be written, then quietly
isn't there.

Two directories are never mounted, however a task names them: a system directory
and your home directory. An output written straight into `$HOME` would hand the
image `~/.ssh` and `~/.aws` along with it, so give it a directory of its own.

### What the container inherits

Ginkgo forwards its own computed variables and nothing else: `GINKGO_THREADS`
always, and `OMP_NUM_THREADS` and friends when the task declares
`export_thread_env=True`. The rest of your shell environment stays outside,
which is the point of running in a container at all.

The container runs as you, not as root, so its outputs stay writable by later
Python tasks and by the next run. Under Docker that means an explicit
`uid:gid`; under rootless Podman it means passing nothing, because Podman
already maps the container's own root to your user. You should not have to think
about which — that is what `user = "auto"` is for.

### Configuration

Anything the declarations cannot express goes in `ginkgo.toml`:

```toml
[container]
runtime = "docker"                          # or "podman"
pull_policy = "if-not-present"              # "always" re-pulls; "never" pulls nothing
user = "auto"                               # host uid:gid; "root" or "1000:1000"
shell = "bash"                              # for images that ship only "sh"
auto_mount = true
extra_mounts = ["/scratch:rw", "/opt/refs:ro"]
```

Use `extra_mounts` for paths no task declares — a tool's own cache directory, a
licence file. Entries take the form `"/path"`, `"/path:rw"`, or
`"/host:/container:rw"`, and default to read-only. An entry you write is treated
as a decision: if a task declares an output inside a path you marked `ro`, the
run stops and says so rather than quietly granting write access.

`auto_mount = false` turns off the mounts derived from task declarations. Your
`extra_mounts` still apply.

Set `user = "root"` for an image that installs software at runtime and needs to
write outside its mounts. Note that its outputs will then be root-owned.

## Python Tasks Run In The CLI's Own Environment

`env=` is only available on the driver kinds — `shell`, `script`, and
`notebook`. A `kind="python"` task carrying one is rejected before any work
starts:

```text
✖ workflow.flow.summarise uses env 'analysis_tools' but is declared with
kind='python'. Foreign environments only support driver tasks — use
@task('shell'), @task('notebook'), or @task('script').
```

`ginkgo doctor` reports the same thing as `INVALID_ENV_KIND` without running
anything.

The consequence is the part worth internalising: **every import in a Python task
body resolves against the interpreter the `ginkgo` CLI runs from, not against
your project's `pixi.toml`.** A task that does `import pandas` fails with
`ModuleNotFoundError` unless pandas is installed alongside the CLI, however
thoroughly the project's environments list it.

To find that interpreter, read the console script's shebang:

```bash
head -1 "$(command -v ginkgo)"
```

```text
#!/Users/you/Software/ginkgo/.pixi/envs/default/bin/python3.14
```

How you add a package to it depends on how you installed Ginkgo:

- Installed as a uv tool (the [curl installer](../getting-started/installation.md#quick-install-curl)):
  reinstall with the extras listed, since `uv tool` keeps the tool's environment
  isolated by design —
  `uv tool install --force --with pandas "git+https://github.com/sanjaynagi/ginkgo.git@main"`.
- Running from a Pixi workspace: add the package to that workspace's manifest
  and `pixi install`.
- A plain `pip install -e .` environment: `pip install pandas` into the same
  environment.

Either way, that environment is a dependency of your workflow that Ginkgo does
not manage for you. When a step needs dependencies of its own — a version that
conflicts with the CLI's, or a tool that is not a Python package at all — move it
to `kind="script"` or `kind="shell"` with `env=`. That is the supported route to
isolated dependencies, and Ginkgo installs the environment for you.

Lifting the restriction so that `kind="python"` can take `env=` is tracked in
[issue #87](https://github.com/sanjaynagi/ginkgo/issues/87).

## Environment Commands

The CLI includes environment inspection and cleanup commands:

```bash
ginkgo env ls
ginkgo env clear <env-name>
ginkgo env clear --all --dry-run
```

Use these when you need to inspect or reset local environment state without
clearing the workflow cache itself.

**These commands cover Pixi environments only.** They work in terms of a project
directory holding a `pixi.toml` and a neighbouring `.pixi/` install directory, so
`ls` lists what it finds under the discovery roots and `clear` removes an install
directory from disk. A container image has neither, so a workflow declaring both
kinds sees only the Pixi half — which the listing says for itself:

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

To inspect or reclaim images, use the runtime directly — `docker images`,
`docker image rm`, or the `podman` equivalents.

## See Also

- [Caching and Provenance](caching-and-provenance.md) &mdash; environment lock
  identity is part of every environment-backed task's cache key.
- [Tasks and Flows](tasks-and-flows.md) &mdash; how shell, script, and notebook
  tasks are authored.
