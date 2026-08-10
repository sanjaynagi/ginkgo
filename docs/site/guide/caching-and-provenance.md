# Caching And Provenance

Ginkgo caches task results so repeated runs reuse prior work, and records
provenance so you can inspect what happened in any run.

## Cache Identity

The cache lives under `.ginkgo/cache/` and is content-addressed. At a high
level, Ginkgo hashes:

- task identity
- task version
- task source and its statically imported local Python modules
- notebook source for notebook tasks
- resolved input values
- environment identity for foreign execution

Inputs annotated `file` or `folder` are hashed by content; everything else is
hashed from its `repr`. See [Cache Correctness](#cache-correctness) for why that
distinction decides whether the cache stays correct.

Local-import tracking is conservative: changing a reachable helper module
invalidates tasks that import it, even when the changed symbol is not called.
Dynamic imports and other runtime dependencies cannot be tracked this way; set
or increment `version=` on the task when those dependencies change.

The conservative closure has a practical consequence for fan-out. If tasks read
their parameters from a shared module-level structure, every consumer's cache
identity is coupled to every other consumer's parameters — editing one model's
entry in a shared `MODEL_HYPERPARAMS` dict invalidates every task that imports
the module, so the whole fan-out re-runs rather than the affected branch. Pass
such parameters as task arguments instead; arguments are hashed per call, so
only the branches whose values changed are invalidated.

(cache-correctness)=
## Cache Correctness

### Annotate Path Boundaries `file` Or `folder`, Not `str`

A path that flows between tasks must be annotated `file` (or `folder`) at both
ends — the producer's return and the consumer's parameter. Content hashing is
dispatched on that annotation.

**A `str`-annotated path boundary makes the cache key path-identity only.** The
key incorporates the path string, not the file's contents, so if an upstream
task rewrites the file at the same path the downstream task still matches its
old key: it reports `↺ cached` and serves a stale result as current. Nothing
warns, because from the cache's point of view nothing changed.

```python
from ginkgo import file, task

# WRONG — coords is keyed on the path string, so a rewritten file still hits
@task()
def analyze(coords: str, output_path: str) -> str: ...

# CORRECT — coords is keyed on the file's contents
@task()
def analyze(coords: file, output_path: str) -> file: ...
```

The producer's annotation matters as much as the consumer's: a task declared
`-> str` returns a plain `str` at runtime, which is hashed by `repr` even if the
consumer asks for `file`. Note that `file` and `folder` are `str` subclasses, so
a `str` annotation is indistinguishable from the correct one at the type level
while behaving oppositely at the cache level — no type checker will catch this.

Output paths stay `str`. The file does not exist when the key is computed, so
there is nothing to hash; annotate the return `file` when the produced path
should be content-tracked and stored as an artifact.

`Path` and `pathlib.Path` annotations are **not** content-hashed either. Use
`file` and `folder`.

### Keep Inputs Deterministic

An input that changes on every run defeats caching entirely — the key never
repeats, so every task re-executes. Avoid computing task inputs (or values
derived into them, such as RNG seeds) from:

- `hash()` on a `str`, `bytes` or other salted type — Python randomises string
  hashing per process unless `PYTHONHASHSEED` is fixed, so `hash(chrom)` yields
  a different seed on every run
- `uuid4()` and other random identifiers
- `datetime.now()`, timestamps, and run counters
- `random` without an explicit seed

Use a stable function of the input instead, such as an index into the fan-out
list or a fixed seed passed as an argument.

If tasks re-run when you expected a cache hit, `ginkgo cache explain <run-id>`
reports a reason per task. `input_changed` means the resolved input hashes
differ from the most recent prior entry for that task, which is the signature of
a non-deterministic input.

## Artifact Storage

For file and folder outputs, Ginkgo stores content-addressed artifacts under
`.ginkgo/artifacts/` and uses those as the durable backing store for cached path
outputs.

A task's declared output path is not the source of truth — the artifact store
is.

## What Lives In A Run Directory

Each run gets a directory under `.ginkgo/runs/<run_id>/`. This is where Ginkgo
records runtime metadata such as:

- task-level status and timing information
- logs
- notebook artifacts
- run manifests and provenance payloads

Together, the cache and the run directory answer different questions:

- cache: can this work be reused safely?
- provenance: what happened in this specific run?

## Inspecting Cache State

Use the cache subcommands to inspect or clean cache state:

```bash
ginkgo cache ls
ginkgo cache clear <cache-key>
ginkgo cache prune --older-than 30d --dry-run
```

These commands report reuse behavior without navigating the hidden cache
directory by hand.

## Bounding Cache Size

`ginkgo cache prune` supports three eviction policies, which can be combined
in one invocation:

```bash
# Time-based: remove anything older than 30 days
ginkgo cache prune --older-than 30d

# Size-based: bring total cache size down to 5 GB
ginkgo cache prune --max-size 5GB

# Count-based: keep only the newest 500 entries
ginkgo cache prune --max-entries 500

# Combined: also remove anything older than 90 days
ginkgo cache prune --older-than 90d --max-size 5GB
```

Eviction is oldest-first, and orphaned artifacts are garbage-collected at the
end of the operation. Use `--dry-run` to preview what would be removed.

## Partial Resume

When a run fails partway through, Ginkgo preserves every successfully cached
task. Rerunning the same workflow picks up where the previous run left off:
tasks whose inputs are unchanged serve from cache, and only the tasks that
failed or were never reached are re-executed. The `cache_key` column in
`ginkgo cache ls` and the cache-hit markers in `ginkgo run` output make this
reuse visible. There is no separate resume command — the cache itself is the
resume mechanism.

## Dry-Run Mode

`ginkgo run workflow.py --dry-run` validates the workflow without executing
any task body. Ginkgo resolves the expression tree, checks environments and
secrets, computes cache keys for every task, and reports which tasks would
run, which would serve from cache, and which resources they declare. Dry-run
is the fastest way to confirm that a workflow is correctly wired, that every
declared environment exists, and that planned caching aligns with intent
before committing to a real run.
