# Ginkgo

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](/Users/sanjay.nagi/Software/ginkgo/pyproject.toml)
[![Tests](https://github.com/sanjaynagi/ginkgo/actions/workflows/tests.yml/badge.svg?branch=main&event=push)](https://github.com/sanjaynagi/ginkgo/actions/workflows/tests.yml)
[![Quality](https://github.com/sanjaynagi/ginkgo/actions/workflows/quality.yml/badge.svg?branch=main&event=push)](https://github.com/sanjaynagi/ginkgo/actions/workflows/quality.yml)

Ginkgo is a Python workflow orchestrator for scientific, analytical, and
research workflows.

It is licensed under the Apache License, Version 2.0. See
[`LICENSE`](LICENSE).

It is built around four ideas:

- a Python-native authoring model with `@flow` and `@task()`
- dynamic workflow expansion from resolved task results
- reproducible execution through content-addressed caching
- isolated environments with pixi or container images
- an agent-friendly interface for autonomous analytics
- clear post-run inspection through provenance, CLI tooling, and a UI

## Documentation

The full documentation site now lives under `docs/site/`.

Build it locally with:

```bash
pixi run docs-build
```

Then open:

```text
docs/_build/dirhtml/index.html
```

The docs site covers installation, quickstart, core concepts, environments,
notebook tasks, caching, CLI usage, the local UI, and a canonical example
workflow.

## Installation

### Pixi

For local development:

```bash
pixi install
pixi run test
pixi run typecheck
```

If your workflows use Pixi-backed task environments, `pixi` must also be
available on `PATH` when you run them.

Run the CLI with either:

```bash
pixi run python -m ginkgo.cli --help
```

or:

```bash
ginkgo --help
```

### Editable install

If you prefer a plain Python environment:

```bash
pip install -e .
```

## Minimal Example

```python
from pathlib import Path

from ginkgo import flow, task


@task()
def write_text(message: str, output_path: str) -> str:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(message, encoding="utf-8")
    return str(output)


@flow
def main():
    return write_text(message="hello from ginkgo", output_path="results/hello.txt")
```

Run it with:

```bash
ginkgo run workflow.py
```

## Canonical Example

The docs and examples are centered on
[`examples/bioinfo`](examples/bioinfo), which demonstrates:

- Pixi-backed shell tasks
- a container-backed shell task
- `.map()` fan-out across samples
- a local Python aggregation task

Run it with:

```bash
cd examples/bioinfo
ginkgo run
```

## Core CLI Commands

- `ginkgo run`
- `ginkgo test --dry-run`
- `ginkgo doctor`
- `ginkgo debug`
- `ginkgo cache ls`
- `ginkgo cache clear`
- `ginkgo cache prune`
- `ginkgo env ls`
- `ginkgo ui`

## Repository Layout

```text
ginkgo/
├── core/
├── runtime/
├── envs/
├── cli/
└── ui/
```

- `core/` contains the user-facing DSL
- `runtime/` contains evaluation, scheduling, caching, provenance, and value transport
- `envs/` contains execution backends
- `cli/` contains the `ginkgo` command-line interface
- `ui/` contains the local run browser
