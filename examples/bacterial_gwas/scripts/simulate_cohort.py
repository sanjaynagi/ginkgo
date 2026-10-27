"""Simulate a small bacterial cohort for the GWAS demo.

Evolves a genome down a random tree: core SNPs on every branch, accessory
genes gained and lost, and three resistance genes gained on chosen branches.
Writes one assembly per sample and a table of resistance phenotypes. Two
samples are deliberately bad, so QC has something to catch: one assembly is
truncated and one is contaminated with a foreign sequence. A third assembly
has lost its first header line, so it is not valid FASTA at all.

Usage: python scripts/simulate_cohort.py [n_samples]
"""

import csv
import random
import sys
from pathlib import Path

SEED = 7
CORE_LENGTH = 400_000
GENE_LENGTH = 900
N_ACCESSORY = 30
SNPS_PER_UNIT = 60
# Antibiotic, resistance gene, independent gains, and the clade size of each.
RESISTANCE = [
    ("ciprofloxacin", "gyrA_mut", 3, (3, 8)),
    ("tetracycline", "tetM", 2, (4, 9)),
    ("ampicillin", "blaTEM", 1, (3, 3)),
]
TRUNCATED, CONTAMINATED, HEADERLESS = "S07", "S19", "S33"

rng = random.Random(SEED)


def random_seq(length: int) -> str:
    return "".join(rng.choice("ACGT") for _ in range(length))


def build_tree(leaves: list[str]) -> dict:
    """Split the leaves at random until each node has one."""
    if len(leaves) == 1:
        return {"leaves": leaves, "children": [], "length": rng.uniform(0.2, 1.0)}
    cut = rng.randint(1, len(leaves) - 1)
    children = [build_tree(leaves[:cut]), build_tree(leaves[cut:])]
    return {"leaves": leaves, "children": children, "length": rng.uniform(0.2, 1.0)}


def nodes(tree: dict) -> list[dict]:
    return [tree] + [n for child in tree["children"] for n in nodes(child)]


def mutate(core: list[str], length: float) -> list[str]:
    core = core.copy()
    for _ in range(int(rng.expovariate(1 / (length * SNPS_PER_UNIT)))):
        i = rng.randrange(len(core))
        core[i] = rng.choice([b for b in "ACGT" if b != core[i]])
    return core


def evolve(tree: dict, core: list[str], genes: set[str], gains: dict, out: dict) -> None:
    core = mutate(core, tree["length"])
    genes = set(genes)
    for gene in list(genes):
        if gene.startswith("acc") and rng.random() < 0.08 * tree["length"]:
            genes.discard(gene)
    if rng.random() < 0.3 * tree["length"]:
        genes.add(f"acc{rng.randrange(N_ACCESSORY):02d}")
    genes |= gains.get(id(tree), set())
    if not tree["children"]:
        out[tree["leaves"][0]] = (core, genes)
    for child in tree["children"]:
        evolve(child, core, genes, gains, out)


def main() -> None:
    n_samples = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    samples = [f"S{i:02d}" for i in range(1, n_samples + 1)]
    rng.shuffle(samples)
    tree = build_tree(samples)

    # Gain each resistance gene on clades of the requested size.
    gains: dict[int, set[str]] = {}
    for _, gene, n_gains, (low, high) in RESISTANCE:
        clades = [n for n in nodes(tree) if low <= len(n["leaves"]) <= high]
        for clade in rng.sample(clades, n_gains):
            gains.setdefault(id(clade), set()).add(gene)

    sequences = {f"acc{i:02d}": random_seq(GENE_LENGTH) for i in range(N_ACCESSORY)}
    sequences |= {gene: random_seq(GENE_LENGTH) for _, gene, _, _ in RESISTANCE}
    root_genes = {f"acc{i:02d}" for i in rng.sample(range(N_ACCESSORY), 12)}
    genomes: dict[str, tuple[list[str], set[str]]] = {}
    evolve(tree, list(random_seq(CORE_LENGTH)), root_genes, gains, genomes)

    assemblies = Path("data/assemblies")
    assemblies.mkdir(parents=True, exist_ok=True)
    rows = []
    for sample in sorted(genomes):
        core, genes = genomes[sample]
        genome = "".join(core) + "".join(sequences[g] for g in sorted(genes))
        if sample == TRUNCATED:
            genome = genome[: len(genome) // 3]
        if sample == CONTAMINATED:
            genome += random_seq(100_000)
        cuts = sorted(rng.sample(range(1, len(genome)), rng.randint(1, 4)))
        contigs = [genome[a:b] for a, b in zip([0, *cuts], [*cuts, len(genome)])]
        with open(assemblies / f"{sample}.fasta", "w") as handle:
            for i, contig in enumerate(contigs, 1):
                if not (sample == HEADERLESS and i == 1):
                    handle.write(f">{sample}_contig{i}\n")
                handle.write(f"{contig}\n")

        # A resistance gene confers resistance; 5% of calls are flipped.
        row = {"sample": sample}
        for drug, gene, _, _ in RESISTANCE:
            resistant = (gene in genes) != (rng.random() < 0.05)
            row[drug] = "R" if resistant else "S"
        rows.append(row)

    with open("data/phenotypes.csv", "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["sample", *[r[0] for r in RESISTANCE]])
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
