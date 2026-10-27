"""Plain helpers for the QC step: reading unitigs, sample QC, and tables for treeWAS."""

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Unitigs:
    """unitig-caller's output: each unitig's sequence, and the samples that carry it."""

    sequences: list[str]
    carriers: list[set[str]]

    def samples(self) -> list[str]:
        return sorted(set().union(*self.carriers))


def load_unitigs(graph: str) -> Unitigs:
    """Read unitig-caller's pyseer-format output: `SEQ | S01:1 S02:1 ...`."""
    sequences, carriers = [], []
    for line in (Path(graph) / "unitigs.pyseer").read_text().splitlines():
        sequence, calls = line.split(" | ")
        sequences.append(sequence)
        carriers.append({call.split(":")[0] for call in calls.split()})
    return Unitigs(sequences, carriers)


def passing_samples(unitigs: Unitigs, report: str) -> list[str]:
    """Keep samples that are complete and uncontaminated, and write a QC report.

    Completeness is the share of core unitigs (carried by 90% of samples) a
    sample carries. Contamination shows as sequence no other sample carries.
    """
    samples = unitigs.samples()
    core = [c for c in unitigs.carriers if len(c) >= 0.9 * len(samples)]
    passed = []
    Path(report).parent.mkdir(parents=True, exist_ok=True)
    with open(report, "w") as out:
        out.write("sample\tcompleteness\tunique_kb\tpass\n")
        for sample in samples:
            completeness = sum(sample in c for c in core) / len(core)
            unique_kb = (
                sum(len(s) for s, c in zip(unitigs.sequences, unitigs.carriers) if c == {sample})
                / 1000
            )
            ok = completeness >= 0.8 and unique_kb < 30
            out.write(f"{sample}\t{completeness:.2f}\t{unique_kb:.1f}\t{ok}\n")
            if ok:
                passed.append(sample)
    return passed


def write_genotypes(unitigs: Unitigs, samples: list[str], path: str) -> str:
    """Write the unitigs that vary across `samples` as a samples x unitigs 0/1 table."""
    keep = set(samples)
    variable = [
        (s, c & keep)
        for s, c in zip(unitigs.sequences, unitigs.carriers)
        if 0 < len(c & keep) < len(keep)
    ]
    with open(path, "w") as out:
        out.write("sample\t" + "\t".join(s for s, _ in variable) + "\n")
        for sample in samples:
            out.write(sample + "\t" + "\t".join(str(int(sample in c)) for _, c in variable) + "\n")
    return path


def write_phenotypes(phenotypes: str, samples: list[str], directory: str) -> dict[str, str]:
    """Write one 0/1 resistance table per antibiotic; return each table's path."""
    rows = [row for row in csv.DictReader(open(phenotypes)) if row["sample"] in samples]
    paths = {}
    for drug in [column for column in rows[0] if column != "sample"]:
        paths[drug] = f"{directory}/{drug}.tsv"
        with open(paths[drug], "w") as out:
            out.write("sample\tresistant\n")
            out.writelines(f"{row['sample']}\t{int(row[drug] == 'R')}\n" for row in rows)
    return paths


def n_cases(phenotype: str) -> int:
    """Count the resistant samples in a 0/1 phenotype table."""
    return sum(line.endswith("\t1") for line in Path(phenotype).read_text().splitlines())
