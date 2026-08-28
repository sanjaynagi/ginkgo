# Ginkgo user testing — 2026-08-17

Five independent agents played new-user scientists, same rules as [2026-08-10](user-testing-2026-08-10.md):
each built and ran a real workflow using only `README.md`, `docs/site/`, `examples/`,
`ginkgo init` + generated skills, and CLI `--help`. Source reading was forbidden. All
tested against the current `main` (commit `2471fcd`, editable install).

| # | Persona | Domain | Outcome |
|---|---------|--------|---------|
| 1 | RNA-seq bioinformatician | 17-task QC/DE pipeline, 3 environments (local/pixi/docker), notebook report | Ran end to end; would adopt with reservations |
| 2 | ML researcher | Hyperparameter grid sweep, leaderboard, model asset | Ran end to end; blocked from using `model()` asset kind |
| 3 | Materials scientist (env-isolation focus) | Diffusion sweep, Pixi + Docker, pandas aggregation | Ran end to end; hit a silent wrong-data bug |
| 4 | Ecologist (notebooks/reporting focus) | Multi-site trend fitting, notebook report | Ran end to end; `ginkgo report` drops cached-run assets |
| 5 | True beginner (never used any orchestrator) | Synthetic 3D geometry, fan-out, comparison | Ran end to end; hit a cache-correctness bug in hour one |

The five raw reports were lost when `scratchpad/usertest-2026-08-17/` was deleted mid-session by a
concurrent process (untracked, unrecoverable). Everything of substance from them is preserved in
this document and, with verbatim evidence, in the issues filed below.

## Validation and filed issues

Every finding below was then independently validated against source by seven agents, each of which
reproduced the behaviour itself rather than trusting the report. Sixteen issues were filed
(#194–#209). Validation changed the picture materially in several places — three tester findings
were **refuted**, and four defects the testers never saw were **discovered** in the process.

| Issue | Finding | Severity |
|---|---|---|
| #196 | `ginkgo report --out <dir>` silently deletes the directory's contents | data loss |
| #198 | `product_map()` Cartesian-products a derived `output_path`, writing wrong data to correctly-named files | data corruption |
| #199 | `table()` asset read as garbage by a shell task; same pattern crashes a script task | silent wrong data |
| #194 | Env-backed tasks re-execute on run 2: cache key computed before `prepare()`, when `env_identity` is `None` | correctness |
| #197 | CLI discards the traceback for every error outside a task body | diagnosability |
| #202 | Notebook export breaks on an unreadable system Jupyter path; cached rerun replays it against the wrong run | correctness |
| #203 | Explicit asset names silently prefixed for non-`file` kinds; `asset show` rejects them | usability + docs |
| #201 | `model()` accepts only five ML frameworks, contract undocumented | capability gap |
| #200 | `Expr` unpack error names no task and no idiom | usability |
| #204 | Live run table labels undispatched fan-out branches numerically | cosmetic |
| #205 | `init` scaffolds an unusable `pixi.toml` and an untagged notebook | first contact |
| #195 | `init` prints a `cd` into the directory you're in; groups error instead of showing help | first contact |
| #208 | Report section numbers hardcoded; `--single-file` figures inlined as `application/octet-stream` | cosmetic |
| #206, #207, #209 | Documentation: environments page, README/report wording, `test` vs `run --dry-run` | docs |

**Refuted by validation — do not action:**

- **`ginkgo report` does not drop assets on a cached run.** Agent 4's P0 finding below could not be
  reproduced: all six of their runs were re-rendered and every fully- and partially-cached run
  produced the same 13 asset cards, 3 sections and 4 figures as the fresh run. Their numbers came
  from a *different* run — an aborted one (`1 cached, 19 pending`, killed by a `BrokenPipeError`
  when stdout was closed mid-run) that legitimately held one asset. Because `ginkgo report` with no
  run id renders the *latest* run, a bare invocation after that abort reproduces their output
  exactly. The `06 → 08` numbering gap they cited as corroboration is unrelated and present in
  every clean run (#208).
- **`docs/site/reference/api.md` is not a gap.** It builds and publishes 192 rendered signatures via
  Sphinx to GitHub Pages, including `model()`'s full payload contract. The residual — `ginkgo.core.asset`
  has no `automodule` entry, so its accurate docstrings never publish — is folded into #201.
- **`ginkgo secrets` is not the odd one out.** Every subcommand group and bare `ginkgo` itself print
  the same raw argparse error, so it is one shared fix (#195), not a special case.

**Also corrected:** the `.product_map()` + `expand()` finding was filed by its tester as a missing
docs example. It is a data-corruption bug (#198) — the construction the docs' own pairing rule
endorses silently writes each output file several times with different parameter values. Two
testers' accounts of the scaffold notebook conflicted; the template has no `parameters`-tagged cell
(#205).

**Notable regressions checked and *not* reproduced this round** — worth tracking as
possible fixes since the 08-10 report: `ginkgo test --dry-run` no longer fails to
import the scaffolded package (previously the single most consistent finding, 4/5
agents); no agent hit the `table()` cache-replay crash-on-rerun (Agent 4 specifically
reran an all-cached workflow three times looking for it); no agent hit the `str`-vs-
`file` silent-stale-cache footgun (though Agent 5 flagged it as the scariest paragraph
in the docs on a purely theoretical read).

---

## P0 — correctness and silent-wrong-answer bugs

### 1. Environment-backed tasks (Pixi/Docker) spuriously re-execute on an unchanged rerun — 2 of 5 agents
Agents 1 and 5 independently hit the same shape of bug, on unrelated workflows:

- Agent 1 (genomics): reran an already-fully-cached 17-task workflow three times with
  zero changes. Run 2 re-executed exactly the two environment-backed tasks
  (`normalize_counts` — pixi script, `flag_low_expression_genes` — docker shell); run 3
  finally stabilized at `0 executed, 17 cached`. `cache ls` showed genuinely distinct
  cache keys created for the same task/inputs across runs 1 and 2.
- Agent 5 (beginner): same shape on the `ginkgo init` scaffold itself — `build_brief`
  (pixi script) re-executed on run 2, then a *different* task, `package_brief[beta]`
  (docker shell), re-executed on run 3, before settling fully stable on run 4.

Both agents traced it to the cache key's environment-identity component via `cache
explain` (`reason: input_changed` / `cache_key_changed`) cross-referenced against
`cache ls` — plain local Python/shell tasks cached correctly every time; only
Pixi/Docker-backed tasks were affected, and only for the first one or two reruns after
a cold or partially-failed run. `cache explain` gives no detail on *what* about the key
changed, so both agents had to infer this externally. This directly contradicts the
docs' "Partial Resume" guarantee (`caching-and-provenance.md`) and undermines the
caching pitch specifically for the environments (Pixi, containers) Ginkgo advertises as
a differentiator. Agents 2 and 3 also exercised Pixi/Docker tasks and did *not* see
this, so it looks intermittent/timing-dependent rather than universal — worth
reproducing with an eye on env-resolution timing (first-install vs. already-resolved).

### 2. `table()` asset consumed by a shell/Docker command silently produces wrong data (Agent 3)
`table()` stores its payload as Parquet — documented (`assets.md:38-44`) — but nothing
stops a downstream `shell`/`script` task from piping that asset's `.artifact_path` into
a tool (`awk`) that expects CSV. The task ran to completion, exit code 0, `✓ succeeded`,
while writing garbage:
```
$ cat results/flagged_candidates.csv
PAR1�xL   `
```
No error, no warning, no cache-correctness signal — wrong data flows forward through
the rest of the graph. This is the worst finding of the round precisely because it's
silent: a scientist would have to manually inspect an intermediate file to catch it.
Worth a runtime guard (warn when a non-`file`-kind asset's `artifact_path` is
interpolated into a shell/Docker command) or at minimum a `--dry-run`/`doctor` note.

### 3. ~~`ginkgo report` drops nearly all assets/figures for a fully-cached run~~ (Agent 4) — REFUTED

> **Validation outcome: not reproduced.** See the Validation section above. The tester was reading a
> report for an aborted run (`1 cached, 19 pending`), not their run 2. Cached-run asset rendering is
> correct at every layer — `mark_cached` writes the full `assets` payload, and `_build_assets`
> explicitly handles replayed versions. The original report is retained below for the record.
Reproduced cleanly:
```
ginkgo run                     # fresh: report shows 3 groups, 13 assets, 5 <img> tags — correct
ginkgo run                     # identical rerun: 20/20 cached
ginkgo report <run_id_2>       # now shows 1 group, 1 asset, 1 <img> tag
```
Confirmed as a report-rendering bug, not a provenance bug: the run's own
`manifest.yaml` carries the full `assets:` payload for every cached task, identical in
shape to a freshly-executed task's. The report's own section numbering skips a number
(`06 Assets` → `08 Environment`, no `07`), suggesting a whole subsection silently
disappears. This matters because the normal iterative-science loop ("tweak one thing,
`ginkgo run`, look at the report") is majority-cached — the report is only reliable on
a from-scratch run, which undercuts the "deep observability" pitch during exactly the
workflow it's meant to serve.

### 4. `model()` asset kind rejects plain payloads and enforces an undocumented 5-framework allowlist (Agent 2)
`assets.md:36` documents `model(payload)` as accepting "a trained model object," no
restriction stated. In practice:
```
TypeError: model() does not support payload of type builtins.dict
...
Reason  model() framework must be one of ['keras', 'lightgbm', 'pytorch', 'sklearn', 'xgboost'], got 'pure-python-logreg'
```
Any model outside those five frameworks (hand-rolled, statsmodels, JAX/Flax) cannot use
`model()` at all, and therefore cannot appear in `ginkgo models` — a whole advertised,
ML-specific feature silently disabled for a large share of real ML work. The only way
to discover the contract was reading a leaked internal file path
(`ginkgo/runtime/artifacts/asset_kinds.py`) inside the traceback — there is no docs page
or error-message hint listing the allowlist.

### 5. Tuple-unpacking a multi-output task crashes with a location-free error (Agent 1)
Given `simulate_counts(...) -> tuple[file, file]`, the natural first attempt,
`counts, samples = simulate_counts(...)`, fails with only:
```
✖ cannot unpack non-iterable Expr object
```
No task name, no file:line, no traceback — nothing to localize the problem beyond
already knowing to look for `.output[i]` in `tasks-and-flows.md`. The type annotation
itself invites this exact mistake and Python's type checker won't catch it either.

---

## P1 — silent behavior, observability, and env-isolation gaps

- **Notebook HTML export permission failure, and a stale error pointer on cached reruns**
  (Agents 1, 4, 5 — 3/5). `nbconvert` fails deterministically on this machine
  (`PermissionError: /usr/local/share/jupyter/conf.json`) — likely a local-environment
  issue rather than a Ginkgo defect, but Ginkgo's degradation is graceful (notebook
  execution still succeeds, a readable HTML stub with the traceback is written, task
  marked `succeeded`). The compounding bug found independently by Agents 1 and 5: on a
  **cached** rerun, the terminal still prints "⚠ HTML export failed," still pointing at
  the *original* failed run's file path rather than the current run's — confusing
  during troubleshooting, and reminiscent of previously-filed #137 (cached notebook path
  printed as corrupted concatenation) — possibly the same underlying stale-path issue in
  a new guise.
- **Python tasks run in the scheduler's own ambient interpreter, not the project's Pixi
  env — and the scaffold ships with no numpy/pandas** (Agents 2, 4 — 2/5). Undocumented
  as an ML-specific trap; a fresh `ginkgo init` scaffold's own `pixi.toml` has nothing
  but `python>=3.11`, so any real data-science Python task fails on a clean machine the
  moment it imports numpy/pandas/scipy. For a tool whose headline audience reaches for
  those on task #1, this deserves prominent callout, not one matter-of-fact line in a
  task-kinds table.
- **Asset keys are silently prefixed with the producing task's name, and `asset show`
  fails on the name users would naturally try** (Agents 2, 4 — 2/5). `table(frame,
  name="sites/x/trend")` shows up in `asset ls` as `table:fit_site_trend.sites/x/trend`
  — undocumented namespacing. `asset show` with the bare `name=` string (or even the
  fully-namespaced string without the exact kind prefix) fails with a misleading
  "no versions registered for asset **file**:..." error that assumes the wrong kind by
  default. Only the exact string from `asset ls`'s own output column works.
- **`ginkgo doctor`/`--dry-run` miss statically-knowable errors** (Agents 3, 4 — 2/5): a
  `table()`-returning task feeding a `file`-annotated consumer (documented failure mode,
  `assets.md:44`) only surfaces after a full real run, Docker pull and Pixi build
  included; the notebook-export permission problem is deterministic and knowable in
  advance but `doctor` reports clean regardless. Echoes the 08-10 report's #136 finding
  that `doctor` under-delivers on its advertised scope.
- **`.product_map()` has no worked example for a grid with a per-cell derived output
  path** (Agent 3) — the single most natural real use of a grid sweep. Docs cover
  `.map()`+`zip_expand()` and `.product_map()`+`expand()` as paired idioms but never
  show a `.product_map()` call that *also* takes an `expand()`-built `output_path`
  alongside the swept parameters; had to fall back to manual `itertools.product` +
  `.map()`.
- **Fan-out branch labels regress to numeric indices for not-yet-run branches queued
  behind an unrelated failure** (Agent 4): `plot_site_trend[2]`, `[3]`, `[4]` instead of
  `[meadow]`, `[wetland]`, `[urban]`, even though the fan-out key is a plain
  compile-time-known string unrelated to the failure. Cosmetic but confusing when
  scanning a failure report. Distinct from, but in the same family as, the 08-10
  report's #131 (`.map()` labels pick only the first varying argument).
- **`ginkgo env ls` doesn't list the Docker environment a run actually used** (Agent 3)
  — only the Pixi env showed up, despite both being exercised in the same run; may be
  intentional scope but the docs describe `env ls`/`env clear` as general environment
  inspection without carving out containers.
- **`ginkgo secrets` with no subcommand exits with a bare argparse error** (Agents 2, 5
  — 2/5), the one top-level command that doesn't work bare or degrade gracefully, unlike
  its siblings (`cache`, `asset`, `env`).

---

## P2 — unintuitive patterns

- **`ginkgo test --dry-run` and `ginkgo run --dry-run` are materially different commands
  that read as interchangeable** (Agent 5, echoed by Agent 1's C1). The quickstart
  pushes `run --dry-run`; the top-level README and the scaffold's own generated README
  push `test --dry-run`. The latter silently validates `tests/workflows/smoke.py`, not
  the user's real flow, with no indication in its own output of *which* file it checked.
- **`README.md`'s CLI command list is incomplete** (Agents 1, 2 — 2/5): omits `inspect`,
  `asset`, `report`, `models`, `secrets`, `notebooks`, and `cache explain` — several of
  which (`inspect workflow`, `cache explain`, `report`) were essential to both testers.
  Echoes the 08-10 report's #127.
- **`docs/site/reference/api.md` is literally unrendered Sphinx markup** when read
  directly, as the exercise's own rules direct (Agent 2) — `{eval-rst}` / `automodule`
  directives, no actual signatures. Presumably fine once built into the hosted site, but
  it's the one place that might have documented the `model()` allowlist, and reading it
  as instructed yields nothing.
- **Reverting a file to byte-identical content resurrects the old cache entry** (Agent
  1) — correct content-addressed behavior, but surprising the first time: "I just
  edited this file" doesn't guarantee a rerun if the net effect is a no-op. The same
  property delighted Agent 5 on their own workflow (switching a parameter back to its
  default reused the original cache keys) — same mechanism, read as a bug the first time
  and a feature the second.
- **Papermill "unknown parameter" warnings print on every successful notebook run**
  (Agent 1) with no doc note that they're expected/benign for the untagged-cell shape
  `ginkgo init` itself scaffolds.
- **`ginkgo init`'s printed "cd `<dirname>`" next step is wrong when you passed `.`
  inside an already-created target directory** (Agent 5) — literally following it fails
  (`cd: no such file or directory`).
- **A notebook task's Python-signature parameters and its `parameters`-tagged cell are
  two independent declarations of the same names, unchecked against each other**
  (Agent 4) — nothing would catch a typo'd default in the notebook cell alone.
- **`ginkgo report`'s "self-contained" claim reads as "one file"** (Agent 5) but produces
  a directory (`index.html` + assets/js/css/fonts subfolders); needs the whole directory
  zipped to actually share as one unit.

---

## Documentation findings

- The `model()` framework allowlist and payload-type contract exist nowhere in the docs
  (`assets.md` or the unrendered `reference/api.md`) — the single highest-impact ML doc
  gap this round (Agent 2).
- `assets.md` never states the `<kind>:<producer_task>.<name>` asset-key namespacing
  rule, so a user scripting against `asset show` predicts the wrong key from the docs
  alone (Agents 2, 4).
- No worked example anywhere covers `.product_map()` with a derived, `expand()`-built
  per-cell output path — the single most natural real use of a grid sweep (Agent 3).
- `README.md`'s Canonical Example section never mentions `asset()`/`table()` even though
  the example it points to (`examples/bioinfo`) uses both throughout, and the
  `file`/`object`/`AssetRef` distinction is one of the trickiest concepts in the system
  (Agent 3).
- `environments.md` doesn't mention that Pixi/Docker environment prep happens
  transparently (and can be slow) on first use — the terminal UI's own "first runs
  install environments" line is more informative than the docs page (Agent 3).
- `environments.md` states Python tasks run in "the scheduler's Python environment" but
  never says what that environment needs to contain, or how a user is meant to get
  numpy/pandas/scipy into it (Agents 2, 4).
- Quickstart and README disagree on which command confirms a workflow is wired
  correctly (`run --dry-run` vs `test --dry-run`) — never stated as two different things
  in one place (Agent 5).
- `caching-and-provenance.md` states cache identity includes "environment identity for
  foreign execution" with no caveat that this component appears unstable across the
  first couple of reruns for Pixi/Docker tasks (Agents 1, 5 — see P0 #1).
- `caching-and-provenance.md`'s "Partial Resume" and "A Typical Loop" guarantees don't
  caveat that `ginkgo report` may not reflect a mostly-cached run's assets (Agent 4 — see
  P0 #3).
- `docs/site/guide/cli.md` documents `ginkgo secrets` as directly usable without noting
  a subcommand is mandatory (Agents 2, 5).

---

## What landed well

Unprompted, all five agents praised the same core:

- **`ginkgo init`'s scaffold is excellent teaching material**, every time. All five
  agents pattern-matched their real workflow onto its module layout, task kinds, and
  idioms without ever needing to consult source — several called out specific idioms
  (`expand()`, `.output[i]`, `AssetRef`) that they learned faster from the generated
  code than from prose docs.
- **Fan-out (`.map()`, `.product_map()`, `expand()`) worked correctly, first try**,
  across every domain — 6-way, 12-way, 18/24-way grids, tuple-output alignment,
  multi-arg simultaneous variation — with output ordering staying aligned with inputs.
- **Cache invalidation was surgically precise under real edits, every time it was
  tested cleanly** (i.e. outside the P0 #1 environment-identity bug): a single config
  value change re-ran exactly its downstream-affected tasks and nothing else, across
  four independent workflows and four independent editing scenarios (Agents 1, 2, 3, 4).
- **Content-addressing is real, not just "did the file change"**: reverting a file to
  byte-identical content, or a parameter back to its default, correctly resurrected the
  original cache entries rather than treating the revert as new (Agents 1, 5).
- **`ginkgo debug`** consistently gave exact resolved inputs, the real traceback, and
  (for env-backed tasks) proof that isolation was genuinely in effect — Agent 3's broken
  import traceback resolved against the actual `.pixi/envs/.../site-packages/...` path,
  concretely confirming Pixi isolation isn't aspirational.
- **Failure reporting and CLI argument validation were called out as excellent** by
  multiple agents — categorized failure panels, exact resolved inputs per failing
  fan-out branch, and "unrecognized arguments" errors that name the workflow's actual
  declared parameters instead of a generic usage dump.
- **`--agent-output` JSONL** was clean, self-describing, and directly consumable by
  every agent that tried it — no parsing tricks needed.
- **`ginkgo report`** produced genuinely useful, good-looking HTML output with zero extra
  flags, including a "Peak RSS" column and correctly embedded tables/figures — on fresh,
  partially-cached and fully-cached runs alike, as validation confirmed.
- **Graceful degradation on the (locally-caused) notebook export failure**: execution
  still succeeds, the real notebook output is preserved, and a readable HTML stub with
  the actual traceback replaces the failed export — multiple agents called this the
  right way to fail.
- **`ginkgo test --dry-run` no longer crashes on the untouched scaffold** — the most
  consistent P0 finding from 08-10 (#118) was not reproduced by any of the 5 agents this
  round.

## Verdicts

Agents 1, 2, 3, and 4 would adopt Ginkgo for real work, each with a specific reservation
tied to their P0 finding (environment cache-key instability; the `model()` gap; the
silent `table()`-as-CSV footgun; and the reporting behaviour that validation later
refuted, respectively). Agent 5, the true beginner, was the most cautious: hitting a
cache-correctness bug in the very first hour — on the stock scaffold, before writing any
of their own code — is a hard thing to build trust back from, even though their own
hand-written workflow ran and cached perfectly afterward.

The pattern from 08-10 repeats: **the core engine — the authoring model, fan-out,
content-addressed caching under normal edits, failure reporting, provenance — is strong
and independently delighted every agent again.** The failures continue to cluster in the
surfaces around it, and have shifted since the last round: `ginkgo test`'s import crash
and the `table()` cache-replay crash both look fixed.

The sharper lesson from this round is about **where the remaining defects live and who
finds them**. The three worst problems — a command that deletes user directories, a fan-out
idiom that writes wrong data to right-looking files, and an asset consumed as the wrong
format by a shell task — all share a shape: the system had the information needed to know
something was wrong (the artifact's recorded extension, the column length, the
user-supplied output path) and said nothing. Two of the three were found by *validation*,
not by the testers, because a scientist evaluating a tool checks whether it ran, not
whether every file's contents match its name. That asymmetry is the real risk surface: the
failures most likely to survive user testing are exactly the ones that look like success.

## Suggested order of work

Revised after validation. The top three all destroy or corrupt user data silently, and none
of them were the findings the testers rated most severe.

1. **#196** — `ginkgo report --out` deleting an existing directory's contents. One-line fix
   (pass `overwrite=False` from the CLI), worst consequence of anything here.
2. **#198** — `product_map()` multiplying a derived `output_path`. Green runs write wrong data
   to correctly-named files while following the documented pairing rule.
3. **#199** — `table()` asset consumed as text by a shell task, plus the `script`-task crash on
   the same documented pattern.
4. **#194** — environment-identity cache-key instability for Pixi/Docker tasks. Contradicts the
   Partial Resume guarantee and fires on the stock scaffold's second run.
5. **#197** — restore tracebacks for errors outside a task body. Cheap, additive, and makes every
   future authoring bug diagnosable.
6. **#202** — set `JUPYTER_PLATFORM_DIRS` for the export subprocess, and attribute replayed
   notebook artifacts to their producing run.
7. **#203**, **#201** — asset-key prefixing and lookup; `model()` widening plus its docs.
8. **#205**, **#195**, **#200**, **#204**, **#208** — first-contact and cosmetic fixes, all small
   and several tagged good-first-issue.
9. **#206**, **#207**, **#209** — documentation.

Deliberately not filed: making `ginkgo doctor` chattier on success. Validation confirmed #136 is
fully closed and that `doctor` does real work (params, secrets, envs, unreachable calls, FUSE
probes) while reporting only problems. The more valuable version of that tester complaint is
giving `ginkgo inspect workflow` a human-readable mode and surfacing it in the docs — worth
considering separately.

Also deferred, with reasoning recorded in #199: the `table`→`file` annotation mismatch cannot be
caught soundly at `doctor` time, because the produced asset kind is a property of the runtime
return value and no annotation can declare it. The tractable pieces are validating probe-resolved
args on a warm `--dry-run`, and eventually letting a producer declare its asset kind.
