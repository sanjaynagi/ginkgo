# Installation

Ginkgo needs Python 3.11 or later.

To write and run workflows, use the curl installer. It is the route for most
readers. To work on Ginkgo itself, clone the repository and use Pixi, as shown
in the last section of this page.

## Quick Install (curl)

Install the `ginkgo` command in one line. This needs
[uv](https://docs.astral.sh/uv/getting-started/installation/):

```bash
curl -LsSf https://raw.githubusercontent.com/sanjaynagi/ginkgo/main/install.sh | sh
```

This installs `ginkgo` and its Pixi plugin from the `main` branch, into an
isolated environment that uv manages. Run the same command again to upgrade.

The installer gives you only the `ginkgo` command. The example workflows are
not included. To get them, clone the repository:

```bash
git clone https://github.com/sanjaynagi/ginkgo.git
```

## Verify The CLI

Check that the `ginkgo` command works:

```bash
ginkgo --help
```

## Plugins

The core of Ginkgo is the `ginkgo` package on its own. Extra features come as
[plugins](../guide/concepts.md#glossary), which you install alongside it. The
curl installer includes `ginkgo-pixi`. The other first-party plugins are in the
same repository:

- `ginkgo-aws`: reads and writes `s3://` storage, and mounts it with
  mountpoint-s3
- `ginkgo-gcp`: reads and writes `gs://` storage, mounts it with gcsfuse, and
  adds the GCP Batch executor
- `ginkgo-oci`: reads and writes `oci://` storage (Oracle Cloud Object
  Storage), and mounts it with rclone
- `ginkgo-pixi`: runs tasks in Pixi environments (`env="name"`), and adds the
  `ginkgo env` command
- `ginkgo-k8s`: adds the Kubernetes executor
- `ginkgo-notebook`: adds `@task("notebook")`, which runs marimo notebooks. It
  installs `marimo`. You need it only if you use notebook tasks. Projects
  created by `ginkgo init` already list it in their `pixi.toml`.
- `ginkgo-report`: adds the `ginkgo report` command, for HTML run reports
- `ginkgo-slack`: sends run notifications to Slack

An [executor](../guide/concepts.md#glossary) runs tasks on another system,
such as a Kubernetes cluster. To mount storage means to make a bucket look like
a folder, so a task can read files from it without downloading them first.

`oci://` has a second, separate meaning. In a task's `env=`, such as
`env="oci://ubuntu:24.04"`, it names a container image, the same as
`docker://`. That use needs no plugin.

To install Ginkgo with every first-party plugin:

```bash
uv tool install --force "ginkgo[all] @ git+https://github.com/sanjaynagi/ginkgo.git@main"
```

To add a single plugin, list it with `--with`. Keep `ginkgo-pixi` in the list,
because `--force` replaces the plugins the tool had before:

```bash
uv tool install --force "ginkgo @ git+https://github.com/sanjaynagi/ginkgo.git@main" \
  --with "ginkgo-pixi @ git+https://github.com/sanjaynagi/ginkgo.git@main#subdirectory=plugins/ginkgo-pixi" \
  --with "ginkgo-k8s @ git+https://github.com/sanjaynagi/ginkgo.git@main#subdirectory=plugins/ginkgo-k8s"
```

`ginkgo plugins` lists what is installed. A task that reads remote storage, for
example through `remote_file("s3://...")`, needs the plugin for that scheme. If
it is missing, Ginkgo names the plugin to install.

[Remote Execution](../guide/remote-execution.md#installing-an-executor)
explains how to set up an executor.

## Runtime Prerequisites

Ginkgo builds the environments your tasks declare. To do that, it runs `pixi`,
`docker` or `podman` on your computer. Each program must be on your `PATH` when
you run a workflow that uses it:

- `pixi`, for tasks that declare a Pixi or Conda environment. Install it from
  [pixi.sh](https://pixi.sh/). Ginkgo runs `pixi install` for you on first use.
- `docker` or `podman`, for tasks that declare a `docker://` or `oci://` image.
  Ginkgo pulls the image for you on first use.

You need neither if your workflow declares no environments.
[Environments](../guide/environments.md) explains what Ginkgo installs, when,
and how long a first run takes.

This is a separate use of Pixi from the development setup at the end of this
page. Here, Ginkgo uses Pixi to build the environments that your tasks run in.

Python tasks don't run in a declared environment. They run in the same Python
environment as the `ginkgo` command, so install any package they import there.
With the curl install, that means reinstalling with `uv tool install --with`.
[Python Tasks Run In The CLI's Own Environment](../guide/environments.md#python-tasks-run-in-the-clis-own-environment)
gives the exact command.

## Editable Python Install

To use a plain Python environment instead, clone the repository and install
from the clone:

```bash
git clone https://github.com/sanjaynagi/ginkgo.git
cd ginkgo
pip install -e .
pip install -e plugins/ginkgo-k8s plugins/ginkgo-gcp   # optional executors
```

This installs the `ginkgo` command.

## Install For Ginkgo Development (Pixi)

The repository's `pyproject.toml` sets up Pixi for working on Ginkgo itself.
From a clone, install the environment and use Pixi to run the tests and build
the docs:

```bash
pixi install
pixi run test
pixi run docs-build
```

The default environment installs every first-party plugin in editable mode, so
`pixi run test` tests the core and the plugins. The `core` environment has no
plugins. Run `pixi run -e core test-core` to test the core alone.

To check the CLI in this setup, run:

```bash
pixi run python -m ginkgo.cli --help
```
