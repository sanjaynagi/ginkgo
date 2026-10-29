---
title: "Installation"
---

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

Extra features come as [plugins](/ginkgo/guide/concepts/#glossary), which you
install alongside the `ginkgo` package. The curl installer includes
`ginkgo-pixi`.

| Plugin | Adds |
|---|---|
| `ginkgo-pixi` | Pixi environments (`env="name"`) and the `ginkgo env` command |
| `ginkgo-notebook` | `@task("notebook")`, for marimo notebooks. Projects from `ginkgo init` list it in their `pixi.toml` |
| `ginkgo-report` | the `ginkgo report` command, for HTML run reports |
| `ginkgo-slack` | run notifications to Slack |
| `ginkgo-k8s` | the Kubernetes [executor](/ginkgo/guide/concepts/#glossary) |
| `ginkgo-gcp` | `gs://` storage and the GCP Batch executor |
| `ginkgo-aws` | `s3://` storage |
| `ginkgo-oci` | `oci://` storage (Oracle Cloud Object Storage) |

The `oci://` container images in a task's `env=` need no plugin.

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

`ginkgo plugins` lists what is installed. If a task needs a missing plugin,
Ginkgo names it. [Remote Execution](/ginkgo/guide/remote-execution/#installing-an-executor)
explains how to set up an executor.

## Runtime Prerequisites

Ginkgo builds the environments your tasks declare. To do that, it runs `pixi`,
`docker` or `podman` on your computer. Each program must be on your `PATH` when
you run a workflow that uses it:

- `pixi`, for tasks that declare a Pixi or Conda environment. Install it from
  [pixi.sh](https://pixi.sh/). Ginkgo runs `pixi install` for you on first use.
- `docker` or `podman`, for tasks that declare a `docker://` or `oci://` image.
  Ginkgo pulls the image for you on first use.

You need neither if your workflow declares no environments. See
[Environments](/ginkgo/guide/environments/).

Python tasks run in the same Python environment as the `ginkgo` command, so
install any package they import there. See
[Python Tasks Run In The CLI's Own Environment](/ginkgo/guide/environments/#python-tasks-run-in-the-clis-own-environment).

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
