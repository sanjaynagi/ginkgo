# Ginkgo user testing — 2026-08-10

Five independent agents played new-user scientists. Each built and ran a real
workflow using only `README.md`, `docs/site/`, `examples/`, `ginkgo init` +
generated skills, and CLI `--help`. Source reading was forbidden.

| # | Persona | Domain | Outcome |
|---|---------|--------|---------|
| 1 | Popgen analyst | VCF filtering, windowed Fst, 3-way fan-out | Ran end to end; would adopt |
| 2 | ML researcher | 3×3 model/seed sweep, leaderboard, notebook report | Ran end to end first try |
| 3 | Cheminformatician (env-isolation focus) | RDKit descriptors, Ro5 filter, ranking | Blocked by env bug; worked around |
| 4 | Ecologist (notebooks/provenance focus) | Multi-site trend fitting, notebook report | Ran cold; **crashed on second run** |
| 5 | Structural biologist (true beginner) | PDB-like geometry, fan-out, aggregate | Ran end to end; hit silent stale cache |

Full reports: `scratchpad/usertest/reports/{genomics,ml,chem-envs,notebooks-reporting,beginner-onboarding}.md`

## Filed issues

Every finding below was independently reproduced and traced to source before filing.

| Issue | Finding |
|---|---|
| #118 | `ginkgo test` fails on a fresh scaffold — package not importable |
| #119 | Pixi env discovery anchored on the workflow file's parent |
| #120 | `script` tasks silently ignore `env=` for the interpreter |
| #121 | `str`-annotated boundaries silently serve stale results |
| #122 | Task calls unreachable from the flow return value silently dropped |
| #123 | Rehydrated `table()` asset crashes a `file`-annotated consumer on rerun |
| #124 | `table()` from a `-> file` task fails with a misleading message |
| #125 | Closed-capture-stream writes leak CPython teardown noise |
| #126 | docs: caching-correctness guidance |
| #127 | docs: guide omits public API the scaffold teaches |
| #128 | `W#` typo in the scaffolded skills template |
| #129 | Container tasks labelled `pixi:docker://...` |
| #130 | No progress indication during pixi env prepare |
| #131 | `.map()` branch labels use only the first varying argument |
| #132 | docs: quickstart never mentions `--dry-run` |
| #133 | `ginkgo init <dir>` produces `<dir>/<dir>/` |
| #134 | `cache explain --run` vs positional run id |
| #135 | `ginkgo debug` reports a failed run as clean |
| #136 | `ginkgo doctor`: `--json` can't express success; bad `env=` passes |
| #137 | Cached notebook path printed as a corrupted concatenation |
| #138 | Notebook success reported when HTML export fails |
| #139 | User-code exceptions categorised `scheduler_error` |
| #140 | `env ls` / `env clear` look in the wrong directory |

**Corrections from investigation.** Two findings in this report did not reproduce and were not filed: `examples/bioinfo` does **not** ship run artifacts in git (`git ls-files` returns 9 files; `.gitignore:13-15` already covers them — the tester was inspecting a tree that had been run in), and `ginkgo.config()` **does** return native TOML types (the scaffold's `str(cfg[...])` is a type-checker cast, not a coercion workaround). One was partly wrong: the `str`/`file` footgun *is* documented publicly at `docs/site/faq.md:245-257` for the positive case — the gap is the consequence of not using `file`.

Two findings were upgraded by investigation. #120 turned out to affect marimo notebook tasks and container envs too, with a confirmed silent wrong-version execution. #123 explained why the cohort disagreed about `table()`: the trigger is the *consumer's* annotation, not `table()` itself.

---

## P0 — correctness and first-contact failures

### 1. `ginkgo test --dry-run` fails on an untouched scaffold — 4 of 5 agents
The command the generated README and `skills/commands.md` name as the *first*
thing to run:

```
$ ginkgo test --dry-run
🌿 ginkgo run smoke.py --dry-run
✖ No module named 'w1'          # also 'w2', 'chemscreen', 'protein_analysis'
```

`tests/workflows/smoke.py` does `from <pkg>.workflow import main`, but `ginkgo
test` doesn't put the project root on `sys.path` — while `ginkgo run` resolves
the same module fine from the same directory. Agent 3 found a second layer:
with `PYTHONPATH=$(pwd)` the import succeeds, then `env=` resolves relative to
`tests/workflows/` instead of the package:

```
✖ Pixi environment 'cheminformatics' not found. Expected a pixi.toml at
  .../chemscreen/tests/workflows/envs/cheminformatics/pixi.toml. Available environments: []
```

This is the single most consistent finding across the cohort, and it's the worst
possible placement for a bug — first command, zero user code.

### 2. `str` vs `file` annotation silently serves stale results (Agent 5)
Annotate a task boundary `str` instead of `file` and the downstream cache key
becomes the path string, not the content hash. Reproduced: changed
`n_residues` 60→90 in `ginkgo.toml`; `make_structure` correctly re-ran and wrote
91-line CSVs, but `analyze_structure`, `aggregate_summaries` and
`plot_comparison` all reported `↺ cached`, and `results/comparison.csv` kept
serving `n_residues=60` values. No error, no warning, `ginkgo doctor` clean.

The footgun is documented — but only in the agent-facing
`skills/workflow-patterns.md`, not on the public docs site. For a tool whose
pitch is reproducibility, this is the one bug that undermines the pitch.

### 3. Cache replay of a `table()` task crashes the run (Agent 4)
```
ginkgo run --cores 4     # cold: 12/12 succeeded
ginkgo run --cores 4     # identical, no edits: CRASHES
✖ [Errno 63] File name too long: '/private/tmp/.../w4/        site  month_index ... \n0  forest_a ...'
```
The cache-hit path reconstructs the output as `str(<DataFrame>)` and treats it
as a path. Confirmed across two different sites' cache entries. Note the cohort
disagrees here — Agents 3 and 5 also used `table()` and got clean cache hits —
so the trigger is likely payload-shape-specific (DataFrame vs path) rather than
`table()` per se. Agent 4 also hit the same "File name too long" on a *cold*
run when passing a raw DataFrame as the `table()` payload, which the docs
appear to sanction (`docs/site/guide/assets.md:32`: "a dataframe or tabular
file"). Worth reproducing both paths.

A CI smoke test that runs any example workflow **twice** would have caught this.

### 4. `@task("script", env=...)` executes outside its declared environment (Agent 3)
Ginkgo resolved and built the Pixi env correctly (rdkit genuinely on disk), the
status table printed `pixi:cheminformatics` — and then ran the script with the
scheduler's own base interpreter:

```
ModuleNotFoundError: No module named 'rdkit'
# manifest shows: /Users/.../miniconda3/bin/python
```

An equivalent `kind="shell"` task with the same `env=` works correctly
(verified in an isolated minimal project), so the bug is specific to `script`
env routing. The scary variant isn't the crash: if the base env happens to
satisfy the imports, the task silently runs against undeclared dependency
versions while the CLI claims otherwise.

---

## P1 — silent behaviour and observability gaps

- **Unreachable task calls vanish with no warning** (Agents 4, 5). Writing
  `plot_comparison(summary=comparison)` as a bare statement produced a 9-task
  graph instead of 10 — no message. Coming from imperative Python, "I called it
  so it runs" is the natural assumption. Related: passing a *literal path
  string* instead of the upstream task's return value silently drops the
  dependency edge, with `--dry-run` still reporting a plausible wave structure.
- **`ginkgo debug` misses run-level failures** (Agent 4). After the crash in
  #3, `ginkgo inspect run <id>` shows `"status": "failed"` with a full
  traceback, but `ginkgo debug <id>` prints `✓ No failed tasks found`. The tool
  named `debug` reports a failed run as clean.
- **Notebook task reports success when HTML export fails** (Agent 4). nbconvert
  died on a machine-local permission issue; ginkgo degraded gracefully into a
  placeholder HTML containing the traceback (a nice touch) — but the run table
  said `✓ succeeded`, the banner said `📓 Notebooks materialised (1)`, and
  `ginkgo notebooks` listed it identically to a fully-rendered notebook.
- **Cached notebook path is concatenated garbage** (Agent 2). On a cache hit the
  banner prints the *new* run directory with the *previous* run's absolute path
  glued underneath — a path that doesn't exist. Reproduced twice.
- **User-code exceptions are labelled `scheduler_error`** (Agents 1, 2). A plain
  `RuntimeError` in a `@task()` body surfaces as
  `"kind": "scheduler_error"` in `ginkgo debug --json`. Since `--json`/`--agent`
  exist for automated triage, `task_error` vs `scheduler_error` matters.
- **`ginkgo env ls` / `env clear` look in `./envs/`** (Agent 3), not
  `<package>/envs/` as `docs/site/guide/environments.md:10-14` documents and as
  `ginkgo run` actually resolves. Both report "No Pixi environments found" for
  an env the run just built in that same directory.
- **`ginkgo doctor` under-delivers on its advertised scope** (Agents 1, 5). It
  missed a nonexistent `env=` reference (caught, well, at run time) and the
  str/file footgun; `ginkgo doctor --json` returns a bare `[]` on success with
  no status field, indistinguishable from "the check produced no output".
- **CPython object-repr noise after a task failure** (Agent 5):
  `object address : 0x12201f280 … object repr : ValueError('I/O operation on
  closed file.') / lost sys.stderr`, printed *after* an otherwise excellent
  failure report. Reads like ginkgo itself crashed.

---

## P2 — unintuitive patterns

- **Shared module-level config amplifies invalidation to the whole fan-out**
  (Agent 2). Changing only `random_forest`'s entry in a module-level
  `MODEL_HYPERPARAMS` dict re-ran all 9 fan-out branches, not 3. Consistent
  with `caching-and-provenance.md:21-24`, but it's precisely the trap the
  natural instinct (centralise config) walks into, and nothing warns that it
  defeats the headline caching feature.
- **Non-deterministic inputs silently defeat caching** (Agent 1). Seeding with
  `hash(chrom)` meant every run saw changed inputs (`cache explain` →
  `input_changed`, `0 cached` forever). Ginkgo was right; the user was wrong;
  nothing flags that `hash()`, `uuid4()`, timestamps are unsafe near task
  inputs. Diagnosed in under a minute via `cache explain` — a real win for that
  command.
- **`asset()` is a separate opt-in registry from the artifact store** (Agent 1).
  Five `file`-returning tasks were cached and staged correctly but never appeared
  in `ginkgo asset ls`, which was surprising.
- **`file | AssetRef` unions everywhere downstream** (Agent 4). Any consumer of
  an asset-returning task must type `X | AssetRef` and branch on
  `isinstance(...)` to reach `.artifact_path`. Learnable only from the generated
  scaffold — `assets.md` never shows a downstream consumer.
- **`ginkgo init <dir>` nests `dir/dir/`** (Agents 1, 3), since package name
  defaults to the basename. Reads as a mistake the first time you see it.
- **Python tasks cannot declare `env=`** (Agent 3), so any pure-Python step
  needing isolated deps must be laundered through `shell`/`script` — documented,
  but the natural first instinct is a dead end.
- **`.map()` progress labels pick the first argument**, so path-shaped args
  crowd out the identifying one: `simulate_variants[chr1]` beside
  `filter_variants[results/raw/chr1.csv]` and a truncated third (Agent 1).
- **`script()` converts `--param_name` to `--param-name`** — inferred from a
  failure log, not documented (Agent 3).
- **`cache explain --run <id>` takes a flag** where `debug <id>` and
  `inspect run <id>` take a positional (Agent 3).
- **`ginkgo.config()` returns uncoerced TOML values** — every read needed an
  explicit `int()`/`float()` cast (Agent 2).

---

## Documentation findings

- `skills/workflow-patterns.md:1` reads `W# Workflow patterns` — a stray `W`
  breaks the H1. Found by 3 agents independently; verified with `od -c`. Ships
  in every scaffolded project.
- The str/file cache footgun lives only in `skills/workflow-patterns.md`, absent
  from the public site. Agent 5 called this the highest-impact doc gap found.
- `expand()` is used by `ginkgo init`'s own generated `workflow.py` but appears
  nowhere in the site guide.
- `.output[i]` indexing for multi-output tasks is undocumented — Agent 2
  reverse-engineered it from the scaffold.
- `docs/site/guide/assets.md:32` — "a dataframe or tabular file" for
  `table(payload)` is true in isolation but no example shows the DataFrame form
  inside a `file`-returning task, which is the combination that crashes.
- `quickstart.md` never mentions `--dry-run`, arguably the best onboarding
  command; it only appears in `caching-and-provenance.md:95-103`.
- `examples/bioinfo` ships a populated `.ginkgo/`, `results/` and `logs/` in
  git, so a reader following "run it, then inspect outputs" is looking at
  someone else's stale run (Agent 5).
- `README.md:56` and the progress table label a Docker shell task
  `pixi:docker://ubuntu:24.04`, conflating the two execution patterns the docs
  present as distinct (Agent 1).
- No caching-hygiene callout on non-deterministic inputs; thin coverage of
  "task returns a plain dict/DataFrame as an intermediate value".

---

## What landed well

Unprompted, all five agents praised the same things:

- **The quickstart works verbatim.** Agent 5 (the beginner) followed
  `examples/bioinfo` literally, first try, no improvised setup, 7/7 in 3.4s,
  and the rerun reported exactly the documented "0 executed, 7 cached".
- **`ginkgo cache explain`** — per-task, human-readable invalidation reasons
  (`source_hash_changed`, `input_changed`, `all_inputs_match`). Named as a
  highlight by 4 agents; it's what let Agent 1 self-diagnose the `hash()` bug.
- **`ginkgo debug <run_id>`** reconstructs a full failure report — task, exit
  code, traceback with correct file/line, resolved inputs, log path — purely
  from stored provenance, no re-run. Agent 2: "some of the best CLI failure
  reporting I've seen in a workflow tool."
- **Content-addressed partial resume genuinely works at value level.** Agent 2
  fixed one failed fan-out branch; only it and `build_leaderboard` re-ran, and
  because the leaderboard came out byte-identical, the downstream notebook
  *still* served from cache. That's real content addressing.
- **`ginkgo run --dry-run`** wave-by-wave plans with fan-out counts and sensible
  truncation — 3 agents called it excellent and wished it were in the quickstart.
- **`ginkgo asset show` / `inspect` / `report`.** Agent 4: schema, row count,
  producer, run id, content hash and check results in one command "genuinely
  answers 'where did this number come from' better than most tools I've used."
  `ginkgo report --single-file` produced a browsable DAG + asset-card HTML with
  zero config.
- **`--agent` JSONL** is clean and directly consumable, no parsing tricks.
- **The `init` scaffold as a teaching tool** — Python, shell, script/Pixi,
  Docker, notebook, fan-out/fan-in and `expand()` in one small readable
  project. Multiple agents wrote correct real workflows from it plus the docs
  and nothing else.
- **Pixi isolation works** once routed correctly: ~25-27s first resolve, 0s
  cached, no leakage either direction. (No progress indicator during the
  resolve, so the first run looks stuck.)
- **Two error messages were called out as exemplary:** the `kind='shell' and
  must return shell(...)` mismatch, and the Pixi-env-not-found message that
  lists valid environment names. Both self-correctable with no doc lookup.

## Verdicts

Agents 1 and 2 would adopt Ginkgo today. Agent 5 called the happy path
"close to friction-free for someone who has never used an orchestrator."
Agents 3 and 4 would not yet trust it — Agent 3 because `env=` on a `script`
task silently lies, Agent 4 because the second run of a workflow crashed and
`debug` said nothing was wrong.

The pattern is clear: **the core engine — graph model, content addressing,
cache explanation, provenance, failure reporting — is strong and repeatedly
delighted people. The failures cluster in the surfaces around it**: the
scaffold's own test command, `env` routing for one task kind, one cache-replay
path, and a set of silent no-ops where the system knows something is wrong and
says nothing.

## Suggested order of work

1. `ginkgo test` sys.path + env resolution (4/5 agents, first command).
2. Reproduce and fix the `table()` cache-replay crash; add a run-twice CI test.
3. `script`-kind `env=` execution routing.
4. Warn on `str`-annotated path arguments crossing task boundaries, and lift the
   footgun into the public docs.
5. Warn on task calls unreachable from the flow return value.
6. `ginkgo debug` to report run-level failures; `task_error` vs
   `scheduler_error`; `doctor --json` status field.
7. Notebook cached-path concatenation; notebook partial-failure status.
8. `env ls`/`env clear` path resolution.
9. Docs: `expand()`, `.output[i]`, `AssetRef` consumption, `--dry-run` in
   quickstart, non-determinism callout, the `W#` typo, untrack
   `examples/bioinfo` run artifacts.
