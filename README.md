# Ginkgo

Ginkgo is a Python workflow orchestrator for scientific, analytical, and
research workflows.

It is built around four ideas:

- dynamic workflow expansion from resolved task results
- a Python-native authoring model with `@flow` and `@task()`
- reproducible execution through content-addressed caching and declared environments
- clear post-run inspection through provenance, CLI tooling, and a local UI

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
