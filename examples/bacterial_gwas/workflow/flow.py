"""Bacterial GWAS: from a folder of assemblies to resistance-associated unitigs.

1. Discover the cohort: the assemblies on disk that have a phenotype. Each one
   is cleaned of short contigs by its own task, so one bad file fails alone.
2. Build a graph genome: a coloured de Bruijn graph of the cohort, with
   unitig-caller from a Pixi environment.
3. QC: drop incomplete or contaminated samples, then decide which antibiotics
   have enough resistant samples to test. The graph grows here, at run time:
   QC returns one GWAS task per antibiotic that passes.
4. GWAS: treeWAS (treewas-rs) on each antibiotic's phenotype.
"""

import csv
from pathlib import Path
from shlex import quote

import ginkgo
from ginkgo import Out, file, flow, folder, shell, task
from workflow.modules.unitigs import (
    load_unitigs,
    n_cases,
    passing_samples,
    write_genotypes,
    write_phenotypes,
)

min_cases = ginkgo.param(
    "min_cases", type=int, default=10, help="Resistant samples needed to test a drug"
)


@task(kind="shell", env="graph", on_failure="ignore")
def clean_assembly(sample: str, assembly: file, cleaned: Out[file]):
    """Drop contigs shorter than 200 bp."""
    return shell(cmd=f"seqkit seq -m 200 {quote(assembly)} -o {quote(cleaned)}")


@task()
def discover_cohort(assemblies: folder, phenotypes: file) -> list[file]:
    """Find the assemblies that have a phenotype, and clean each one."""
    phenotyped = {row["sample"] for row in csv.DictReader(open(phenotypes))}
    return [
        clean_assembly(sample=p.stem, assembly=str(p), cleaned=f"results/clean/{p.stem}.fasta")
        for p in sorted(Path(assemblies).glob("*.fasta"))
        if p.stem in phenotyped
    ]


@task(kind="shell", env="graph", threads=4, memory="2Gi")
def build_graph(assemblies: list[file], graph: Out[folder]):
    """Build a coloured de Bruijn graph and call unitigs across the cohort."""
    refs = f"{graph}/refs.txt"
    return shell(
        cmd=f"mkdir -p {quote(graph)} && printf '%s\\n' {' '.join(map(quote, assemblies))} > {quote(refs)} "
        f"&& unitig-caller --call --refs {quote(refs)} --out {quote(graph)}/unitigs "
        "--threads $GINKGO_THREADS",
        log="logs/build_graph.log",
    )


@task()
def qc(graph: folder, phenotypes: file, min_cases: int, qc_dir: Out[folder]) -> list[folder]:
    """Drop poor samples, then run a GWAS for each antibiotic with enough cases."""
    unitigs = load_unitigs(graph)
    samples = passing_samples(unitigs, report=f"{qc_dir}/samples.tsv")
    genotypes = write_genotypes(unitigs, samples, path=f"{qc_dir}/genotypes.tsv")

    tests = []
    for drug, phenotype in write_phenotypes(phenotypes, samples, qc_dir).items():
        if n_cases(phenotype) >= min_cases:
            results = f"results/gwas/{drug}"
            tests.append(
                gwas(drug=drug, genotypes=genotypes, phenotype=phenotype, results=results)
            )
    return tests


@task(kind="shell", threads=4, memory="1Gi")
def gwas(drug: str, genotypes: file, phenotype: file, results: Out[folder]):
    """Phylogenetic GWAS with treeWAS: which unitigs track resistance?"""
    return shell(
        cmd=f"treewas --genotypes {quote(genotypes)} --phenotype {quote(phenotype)} "
        f"--seed 1 --n-null 3000000 --threads $GINKGO_THREADS --out {quote(results)}",
        log=f"logs/gwas_{drug}.log",
    )


@task()
def summarise(results: list[folder], hits: Out[file]) -> file:
    """Count the significant unitigs each antibiotic's GWAS found."""
    with open(hits, "w") as out:
        out.write("antibiotic\tsignificant_unitigs\n")
        for result in results:
            tables = list(Path(result).glob("significant_*.tsv"))
            loci = {line.split("\t")[0] for t in tables for line in t.read_text().splitlines()[1:]}
            out.write(f"{Path(result).name}\t{len(loci)}\n")
    return file(hits)


@flow
def main():
    assemblies = discover_cohort(assemblies="data/assemblies", phenotypes="data/phenotypes.csv")
    graph = build_graph(assemblies=assemblies, graph="results/graph")
    tests = qc(
        graph=graph, phenotypes="data/phenotypes.csv", min_cases=min_cases, qc_dir="results/qc"
    )
    return summarise(results=tests, hits="results/hits.tsv")
