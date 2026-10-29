# Ginkgo walkthrough: a bacterial GWAS, from assemblies to hits.
#
# Render from the repository root (needs vhs, bat, pixi and treewas-rs):
#   vhs docs/demo/walkthrough.tape

Output docs/site/_static/walkthrough.mp4

Set Shell bash
Set FontSize 17
Set Width 1920
Set Height 1080
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms

# Copy the example to a clean workspace, with ginkgo and treewas on the PATH.
Hide
Type `export PATH="$PWD/.pixi/envs/default/bin:$HOME/Software/treewas-rs/target/release:$PATH"` Enter
Type `export PS1='\[\e[32m\]❯\[\e[0m\] ' BAT_THEME=ansi` Enter
Type `alias bat='bat --paging=never --style=numbers,grid'` Enter
Type "rm -rf /tmp/bacterial_gwas && rsync -a --exclude data --exclude results --exclude logs --exclude .ginkgo --exclude .pixi examples/bacterial_gwas /tmp/" Enter
Type "cd /tmp/bacterial_gwas && python3 scripts/simulate_cohort.py" Enter
Type "clear" Enter
Show

Type "# A bacterial GWAS: 60 assemblies and their resistance phenotypes" Enter
Sleep 500ms
Type "ls data/assemblies | head -4 && head -4 data/phenotypes.csv" Enter
Sleep 3s

Type "# The flow: discover the cohort, build a graph genome, QC, then GWAS" Enter
Sleep 500ms
Type "bat -r 100: workflow/flow.py" Enter
Sleep 5s
Type "clear" Enter

Type "# QC runs a GWAS for each antibiotic with enough resistant samples" Enter
Sleep 500ms
Type "bat -r 61:75 workflow/flow.py" Enter
Sleep 7s
Type "clear" Enter

Type "# The graph genome tools come from a pixi environment" Enter
Sleep 500ms
Type "bat workflow/envs/graph/pixi.toml" Enter
Sleep 4s
Type "clear" Enter

Type "# Run it. Ginkgo installs the pixi environment on first use" Enter
Sleep 500ms
Type "ginkgo run" Enter
Sleep 17s
Type "# The flow planned 4 tasks. Discovery added a cleaning task per assembly," Enter
Type "# and QC added the two GWAS tasks, while the workflow ran" Enter
Sleep 4s

Type "# Run it again: nothing changed, so everything comes from the cache" Enter
Sleep 500ms
Type "ginkgo run" Enter
Sleep 4s
Type "clear" Enter

Type "# Lower the QC bar. QC reruns and adds a GWAS for ampicillin;" Enter
Type "# the graph genome and the existing GWAS stay cached" Enter
Sleep 500ms
Type "ginkgo run --min-cases 3" Enter
Sleep 7s
Type "cat results/hits.tsv" Enter
Sleep 4s
Type "clear" Enter

Type "# Provenance: every run is recorded" Enter
Sleep 500ms
Type "ginkgo runs ls" Enter
Sleep 3s
Type "# Which task, run and inputs produced this file?" Enter
Sleep 500ms
Type "ginkgo why results/hits.tsv" Enter
Sleep 5s
Type "clear" Enter

Type "# And why did QC rerun in the last run?" Enter
Sleep 500ms
Type "ginkgo cache explain | sed -n '/^qc/,/^$/p'" Enter
Sleep 6s
Type "clear" Enter

Type "# Resource usage is measured on every run. Was build_graph sized right?" Enter
Sleep 500ms
Type "ginkgo history build_graph --resources" Enter
Sleep 6s
Type "clear" Enter
Type "# And each GWAS, per antibiotic" Enter
Sleep 500ms
Type "ginkgo history gwas --resources --by-label" Enter
Sleep 9s
Type "clear" Enter

Type "# A new batch of 5 assemblies arrives. Each is cleaned in its own task" Enter
Sleep 500ms
Type "mv data/incoming/*.fasta data/assemblies/ && ginkgo run" Enter
Sleep 8s
Type "# One new assembly is broken. The other four ran, and are cached" Enter
Sleep 4s
Type "clear" Enter

Type "head -c 120 data/assemblies/S63.fasta; echo" Enter
Sleep 2s
Type "# Its FASTA header is missing. Put it back and rerun" Enter
Sleep 500ms
Type "{ echo '>S63_contig1'; cat data/assemblies/S63.fasta; } > fixed.fasta && mv fixed.fasta data/assemblies/S63.fasta" Enter
Sleep 500ms
Type "ginkgo run" Enter
Sleep 14s
Type "# Only S63 and the steps after it ran: 64 tasks came from the cache" Enter
Sleep 5s
