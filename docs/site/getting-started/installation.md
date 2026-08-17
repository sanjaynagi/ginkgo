# Installation

Ginkgo targets Python 3.11+. Use the curl installer to get just the `ginkgo`
CLI, or Pixi for a full local development setup.

## Quick Install (curl)

Install the `ginkgo` CLI in one line. This requires [uv](https://docs.astral.sh/uv/getting-started/installation/):

```bash
curl -LsSf https://raw.githubusercontent.com/sanjaynagi/ginkgo/main/install.sh | sh
```

This installs `ginkgo` from `main` into an isolated environment via
`uv tool install`. Re-run the same command to upgrade.

## Recommended For Development: Pixi

This repository already includes a Pixi workspace. Install the environment and
use Pixi to run tests, the CLI, and the docs build.

```bash
pixi install
pixi run test
pixi run docs-build
```

## Runtime Prerequisites

Ginkgo materialises declared task environments itself, by shelling out, so the
tool behind each kind of environment must be on your `PATH` when you run a
workflow that uses it:

- `pixi`, for tasks declaring a Pixi or Conda environment. Install it from
  [pixi.sh](https://pixi.sh/). Ginkgo runs `pixi install` for you on first use.
- `docker` or `podman`, for tasks declaring a `docker://` or `oci://` image.
  Ginkgo runs `<runtime> pull` for you on first use.

Neither is needed if your workflow declares no environments. See
[Environments](../guide/environments.md) for what Ginkgo installs, when, and what
that costs on a first run.

Python task bodies are the exception: they run in the environment the `ginkgo`
CLI itself runs from, so their imports must be installed there. See
[Python Tasks Run In The CLI's Own Environment](../guide/environments.md#python-tasks-run-in-the-clis-own-environment).

## Editable Python Install

If you prefer a plain Python environment:

```bash
pip install -e .
```

That installs the `ginkgo` console script defined in the project metadata.

## Optional Notebook Support

Notebook tasks rely on optional dependencies:

- `papermill` for `.ipynb` execution
- `marimo` for marimo notebooks
- `nbconvert` for HTML export

If you are not using notebooks, you do not need those packages for basic
workflow authoring or execution.

## Verify The CLI

After installation, confirm that the CLI is available:

```bash
ginkgo --help
```

If you are using Pixi instead of installing the console script directly:

```bash
pixi run python -m ginkgo.cli --help
```
