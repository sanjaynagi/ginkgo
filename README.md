# Ginkgo

<p align="center"><a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a> <a href="pyproject.toml"><img src="https://img.shields.io/badge/python-3.11%2B-blue.svg" alt="Python"></a> <a href="https://github.com/sanjaynagi/ginkgo/actions/workflows/tests.yml"><img src="https://github.com/sanjaynagi/ginkgo/actions/workflows/tests.yml/badge.svg?branch=main&event=push" alt="Tests"></a> <a href="https://github.com/sanjaynagi/ginkgo/actions/workflows/quality.yml"><img src="https://github.com/sanjaynagi/ginkgo/actions/workflows/quality.yml/badge.svg?branch=main&event=push" alt="Quality"></a></p>

Ginkgo is a scientific workflow orchestrator built for the AI agent era.

- `@flow` and `@task()` — define workflows in plain Python, no DSL to learn
- natively dynamic workflows — expand workflows during runtime from resolved tasks
- content-addressed caching — never recompute what hasn't changed
- isolated environments — pixi or containers, per task
- agent-friendly — built from the ground up for workflows to be built and operated by AI agents
- deep observability — provenance, CLI tooling, and a local UI

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

A population-genetics workflow that filters a VCF, computes per-population
allele frequencies, and renders a summary notebook.

```python
import numpy as np

from ginkgo import file, flow, notebook, shell, task

POPULATIONS = ["YRI", "CEU", "CHB"]


# shell task — runs bcftools in a subprocess
@task("shell", env="genomics_tools")
def filter_snps(vcf_path: file, min_maf: float) -> file:
    """Filter to biallelic SNPs above a minor-allele-frequency threshold."""
    output = "results/filtered.vcf.gz"
    return shell(
        cmd=(
            f"bcftools view -m2 -M2 -v snps -i 'MAF>={min_maf}' "
            f"{vcf_path} -Oz -o {output} && bcftools index {output}"
        ),
        output=output,
    )


# python task — uses scikit-allel, fanned out per population via .map()
@task()
def allele_frequencies(vcf_path: file, population: str) -> file:
    """Compute per-SNP alt-allele frequencies for one population."""
    import allel

    callset = allel.read_vcf(str(vcf_path), fields=["calldata/GT"])
    ac = allel.GenotypeArray(callset["calldata/GT"]).count_alleles()
    freqs = ac.to_frequencies()[:, 1]  # alt allele frequency

    output = f"results/af_{population}.npy"
    np.save(output, freqs)
    return file(output)


# notebook task — renders an HTML report from a Jupyter notebook
@task("notebook")
def population_structure(af_files: list[file], populations: list[str]) -> file:
    """Render an HTML population-genetics summary notebook."""
    return notebook("notebooks/population_structure.ipynb")


# flow
@flow
def main():
    filtered = filter_snps(vcf_path="data/chr22.vcf.gz", min_maf=0.05)
    af_results = allele_frequencies(vcf_path=filtered).map(population=POPULATIONS)
    return population_structure(af_files=af_results, populations=POPULATIONS)
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

---

Ginkgo is licensed under the Apache License, Version 2.0. See
[`LICENSE`](LICENSE).
