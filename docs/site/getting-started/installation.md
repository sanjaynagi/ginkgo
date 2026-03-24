# Installation

Ginkgo targets Python 3.11+ and is easiest to develop and run locally with
Pixi.

## Recommended: Pixi

This repository already includes a Pixi workspace. Install the environment and
use Pixi to run tests, the CLI, and the docs build.

```bash
pixi install
pixi run test
pixi run docs-build
```

If your workflows use Pixi-backed task environments, `pixi` must also be
available on your `PATH` when you run those workflows.

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
