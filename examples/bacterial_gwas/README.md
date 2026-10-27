# Bacterial GWAS

A small bacterial genome-wide association study, and the workflow behind the
[walkthrough video](../../docs/demo/walkthrough.mp4).

1. **Discover the cohort:** the assemblies on disk that have a phenotype.
2. **Build a graph genome:** unitig-caller builds a coloured de Bruijn graph of
   the cohort, from the `graph` Pixi environment.
3. **QC:** drop incomplete or contaminated samples, then pick the antibiotics
   with at least `--min-cases` resistant samples. QC returns one GWAS task per
   antibiotic, so the graph grows while the workflow runs.
4. **GWAS:** [treewas-rs](https://github.com/EITORG-Pathogens/treewas-rs) tests
   every unitig against each antibiotic's phenotype.

The data is simulated: 65 genomes evolved down a random tree, with resistance
genes gained on a few branches, one truncated assembly and one contaminated one.
The last five are a later batch in `data/incoming/`, and one of them is not
valid FASTA. The QC logic lives in plain helpers in `workflow/modules/unitigs.py`.

```bash
python scripts/simulate_cohort.py
ginkgo run
ginkgo run --min-cases 3                            # also test ampicillin
mv data/incoming/*.fasta data/assemblies/ && ginkgo run   # S63 fails alone
```

`treewas` must be on your `PATH`. Ginkgo installs the Pixi environment itself.

To re-render the video, run `vhs docs/demo/walkthrough.tape` from the
repository root. It needs [VHS](https://github.com/charmbracelet/vhs) and
[bat](https://github.com/sharkdp/bat).
