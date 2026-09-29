# Caching And Provenance

Ginkgo caches task results so repeated runs reuse prior work, and records
provenance so you can inspect what happened in any run.

## Cache Identity

The cache lives under `.ginkgo/cache/` and is content-addressed. At a high
level, Ginkgo hashes:

- task identity
- task version
- task source, the module the task is defined in, and the local Python modules
  that module statically imports
- notebook source for notebook tasks
- resolved input values
- environment identity for foreign execution

Inputs annotated `file` or `folder` are hashed by content; everything else is
hashed from its `repr`. See [Cache Correctness](#cache-correctness) for why that
distinction decides whether the cache stays correct.

The import walk starts at the task's own module, so the unit of cache identity
is one file plus the local modules that file imports — not one function. Two
`@task` functions in the same `flow.py` are therefore coupled even though
neither imports the other: editing either one invalidates both, and a task can
re-run when nothing inside it changed. That is deliberate. Module-level state —
a constant, a lookup table, a helper the task calls — reaches the cache key
only through this hash, so hashing function bodies alone would serve stale
results whenever such state changed. When a task is expensive and the code
beside it is under active edit, move it into its own module; that is the only
way to decouple them. `ginkgo cache explain <run_id>` reports
`source_hash_changed` for a task invalidated this way.

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

Any path a task *reads* must be annotated `file` (or `folder`) — whether that
path is produced by another task's return value or written down as a literal
input path in the flow. When it does come from another task, annotate both
ends: the producer's return and the consumer's parameter. Content hashing is
dispatched on that annotation.

**A `str`-annotated path boundary makes the cache key path-identity only.** The
key incorporates the path string, not the file's contents, so if an upstream
task rewrites the file at the same path the downstream task still matches its
old key: it reports `↺ cached` and serves a stale result as current. Nothing
warns, because from the cache's point of view nothing changed.

```python
from ginkgo import Out, file, task

# WRONG — coords is keyed on the path string, so a rewritten file still hits
@task()
def analyze(coords: str, output_path: str) -> str: ...

# CORRECT — coords is keyed on the file's contents, output_path is declared a write
@task()
def analyze(coords: file, output_path: Out[file]) -> file: ...
```

The producer's annotation matters as much as the consumer's: a task declared
`-> str` returns a plain `str` at runtime, which is hashed by `repr` even if the
consumer asks for `file`. Note that `file` and `folder` are `str` subclasses, so
a `str` annotation is indistinguishable from the correct one at the type level
while behaving oppositely at the cache level — no type checker will catch this.

Never leave an output path `str`: annotate the parameter `Out[file]` /
`Out[folder]` (below), naming it as a write rather than a read. Annotating the
return `file` on top of that content-tracks and stores the produced path as an
artifact.

`pathlib.Path` is rejected outright on a task parameter, since it is neither
path- nor content-tracked: it is hashed as an opaque pickled object. Use
`file`, `folder`, or `Out[file]` instead.

### Reads vs. Writes: `file` vs. `Out[file]`

`file` and `folder` mean *read*: the path must already exist before the task
runs, and its bytes are what the cache key hashes. Neither can annotate a path
the task is about to *write* — a first run would fail validation before the
file exists, and even if it didn't, cache-keying on the previous run's bytes
would make every run after the first look like a miss (or, worse, make
deleting the output invalidate its own producer).

`Out[...]` is the write-side counterpart, orthogonal to kind:

```python
from ginkgo import Out, file, folder, task

@task()
def align(reads: file, bam: Out[file], qc_dir: Out[folder]) -> file: ...
```

- `Out[file]` / `Out[folder]` compose with containers and optionals —
  `Out[list[file]]`, `Out[file | None]` — but must be the outermost wrapper:
  `list[Out[file]]` is rejected, `Out[list[file]]` is not.
- **Before execution:** an `Out[...]` argument need not exist yet — it is
  validated to be a local, path-like value, and if something already exists at
  the path it must be the declared kind (`Out[file]` pointing at an existing
  directory is an error).
- **Cache key:** contributes its path string only, never content — the same
  as leaving the parameter `str`, but declared rather than implied.
- **After execution:** the path must exist, with the right kind, or the task
  fails naming the parameter and the path. A cache hit whose declared output
  is missing on disk (or the wrong kind) is treated as a miss and the task
  re-runs.
- Supported for remote tasks (`remote=True` / `executor=`) too: with a
  `[remote.artifacts] store` configured, each declared path is rewritten to a
  worker-local scratch path, staged back through the same channel returned
  files use once the task body writes it, and restored at its declared
  driver path before the post-execution check runs. Without a configured
  store, the declared path is sent to the worker unchanged, which only works
  when the worker shares the driver's filesystem.
- The parent directory of every declared `Out[...]` path is created
  automatically before the task body runs (`mkdir(parents=True)`), so a task
  writing into a fresh subdirectory needs no boilerplate of its own. For
  `Out[folder]` only the *parent* is created, not the folder itself — tools
  differ on whether they want the target directory to already exist.

A literal path elsewhere in the graph that matches an `Out[...]` path is now
also a real dependency edge: if another task's argument is the same path
string (or a path inside a produced `Out[folder]`, or a `folder`-annotated
argument containing a produced path), Ginkgo infers that it must run after
the task that writes it — no `.output[...]` reference or return value needed.
Two tasks declaring the same (or an overlapping) `Out[...]` path is a clear
error before anything runs. A path only *computed* at runtime, from an
upstream value rather than written down as a literal string, still needs to
be passed through the graph (the producer's return value or an
`.output[...]` reference) — that is not inferable from a static argument.

### Inferring the return from `Out[...]`

A task with at least one `Out[...]` parameter and *no return annotation at
all* has its return value inferred, so `return file(output_path)` is no
longer needed just to make an output content-tracked, cached, and restorable:

```python
@task()
def align(reads: file, bam: Out[file], qc_dir: Out[folder]):
    ...  # writes bam and qc_dir; no return statement needed
```

- One `Out[...]` parameter infers that parameter's own value (`file` /
  `folder`, or a list/tuple of them for a container `Out`, `None` for an
  absent optional one).
- Several infer a tuple of their values, in parameter declaration order —
  effectively `-> tuple[file, folder]` for the example above.
- An explicit `-> None` is not inferred — it means what it says. A python
  task body that returns anything other than `None` under inference is a
  clear error: add an explicit return annotation if the task needs to return
  something else.
- For a `shell`/`script`/`notebook` task, the directive's own executed result
  becomes the inferred value the same way — see below.

Once inferred, the value goes through the same machinery an explicit `->
file` return does: content tracking, artifact storage, cache restoration,
and dependency edges to downstream consumers.

### Selecting one output: `.output["name"]`

A call whose task declares `Out[...]` parameters can select one of them by
name, in addition to the existing positional `.output[i]`:

```python
@task(kind="shell")
def normalize(src: file, dest: Out[file], check: Out[file]):
    return shell(cmd=f"tr a-z A-Z < {src} > {dest} && shasum {dest} > {check}")

norm = normalize(src="in.txt", dest="out.txt", check="out.sha")
normalized = norm.output["dest"]
```

`.output["name"]` resolves to that `Out[...]` parameter's own resolved
argument (coerced to `file`/`folder`) — not the task's return value — so it
works whether the return is explicit or inferred, and creates a real
dependency edge on the producing node, same as integer indexing. It works on
a `.map()` result too, yielding the per-branch list. A name that is not one
of the task's `Out[...]` parameters is a clear error at flow-construction
time, naming the task's actual `Out[...]` parameters.

### How Is Each Input Tracked?

Every task input contributes to the cache key in one of a few ways, and
Ginkgo names which:

- **content** — a `file` / `folder` annotation or instance: the bytes are
  hashed.
- **asset** — an `AssetRef` (or a remote reference): tracked by its version id.
- **path** — a plain value that happens to name an existing path, but is
  annotated as an ordinary scalar: tracked by the path *string* only. This is
  the silent-staleness trap above — nothing is wrong syntactically, so nothing
  warns.
- **value** — an ordinary scalar or object: tracked by its own `repr` or
  pickle digest.
- **output** — an `Out[...]` parameter: tracked by its declared path string
  only, by design (it names what the task is about to write, not something it
  reads).
- **untracked** — `tmp_dir`: excluded from the key entirely.

A container (`list`, `tuple`, `dict`) is labelled by its least-tracked
element — one `path` buried inside `inputs=[a, b]` labels the whole parameter
`path`, since that is the element the cache key does not really watch.

`ginkgo cache explain <run_id>` shows every input's label next to it, with a
`path` label highlighted and a reminder to annotate `file` / `folder` instead:

```
analyze (task_0002)
  cache key: fddb71a9…
  reason: all_inputs_match
  inputs:
    coords: path (tracked by path string only — annotate `file`/`folder` to track contents)
    output_path: output
```

Pass `--json` for the same data as JSON, under `input_labels`. A run recorded
before this label existed shows nothing for its inputs rather than a guess.

After a real `ginkgo run`, if any task's input was labelled `path`, the run
prints one dim summary line — `N inputs are tracked by path only; see
\`ginkgo cache explain\`` — so the trap surfaces without having to go looking
for it. A workflow with nothing tracked by path stays quiet.

## Artifact Storage

For file and folder outputs, Ginkgo stores content-addressed artifacts under
`.ginkgo/artifacts/` and uses those as the durable backing store for cached path
outputs.

A task's declared output path is not the source of truth — the artifact store
is.

## Where Provenance Lives

What happened goes into one SQLite database per workspace, at
`.ginkgo/ginkgo.db`: an append-only event log, plus the tables `ginkgo inspect
run`, `ginkgo debug` and `ginkgo report` read. All three work on a run that is
still going.

Each run also gets a directory under `.ginkgo/runs/<run_id>/` holding the bytes
— per-task logs, notebook artifacts, copies of the environment lock files, and
a `manifest.yaml` snapshot of everything the database recorded for the run.

Together, the cache and the ledger answer different questions:

- cache: can this work be reused safely?
- provenance: what happened in this specific run?

## Maintaining The Provenance Database

```bash
ginkgo db path                # where the database is
ginkgo db check               # schema version, integrity, rows against bytes
ginkgo db migrate             # create or upgrade it
ginkgo db prune --events-older-than 90d --dry-run
ginkgo db prune --staging-older-than 30d    # staged remote inputs, and their bytes
ginkgo db vacuum              # give the freed space back
```

`ginkgo db check` asks every index whether its rows and the files they name
still agree — the cache, the artifact store, the run directories, the staged
remote inputs — and reports both directions: a row whose bytes are gone, and
bytes no row can find. It never repairs anything.

`ginkgo db prune --events-older-than 90d` deletes the raw event stream of runs
that finished more than 90 days ago. Everything `ginkgo runs show`, the report
and `ginkgo history` read is left alone; what goes is the per-event detail
`ginkgo export events` prints. Add `--digest-memo-older-than` to drop memoised
file digests, which cost only a re-hash to lose, `--staging-older-than` to
evict downloaded remote inputs that nothing has read for a while — bytes and
row together, and the only eviction the staging cache has — and `--dry-run` to
see the counts first. Deleting rows does not shrink the database file; `ginkgo
db vacuum` does.

`ginkgo db check` reads; it never creates. In a directory nobody has run a
workflow in it says so and succeeds.

If the database is gone but `.ginkgo/runs/` still holds run directories, it
says that instead — how many runs are stranded, and that their provenance
cannot be read until the ledger is restored. `ginkgo runs ls` and
`ginkgo doctor` report the same thing. It is a warning rather than a failure,
so all three still exit 0: nothing is corrupt, and the fix is to restore the
file (or delete the directories if the history is not wanted).

### Upgrading from a pre-ledger workspace

Workspaces recorded before `.ginkgo/ginkgo.db` existed are **not** migrated.
Their runs, cache entries and asset catalog lived in files ginkgo no longer
reads, so they are invisible to every command. If you have one, delete
`.ginkgo/` and run the workflow again; there is no import path, and nothing in
the old layout is read by mistake.

`ginkgo.db` is the record of your runs and of your cache; back it up as you
would `.git`. If it is lost, the run history goes with it and the cache goes
cold: the cached bytes are still under `.ginkgo/cache/`, but the keys that find
them were rows in the database. `ginkgo db check` lists those stranded
directories and `ginkgo cache clear --orphans` removes them.

Each run directory still holds a `manifest.yaml` of what that run did, which is
there to be read rather than re-imported: ginkgo does not load it back.

`GINKGO_DB=<path>` relocates the database. Do that if `.ginkgo` is on a network
filesystem: SQLite locking is unreliable over NFS, Lustre, SMB and FUSE, and
ginkgo prints one warning when it notices.

Two `ginkgo run` processes can share a workspace; the ledger is built for it.

## Inspecting Cache State

Use the cache subcommands to inspect or clean cache state:

```bash
ginkgo cache ls
ginkgo cache stats
ginkgo cache clear <cache-key>
ginkgo cache prune --older-than 30d --dry-run
```

These commands report reuse behavior without navigating the hidden cache
directory by hand. `ginkgo cache stats` adds the aggregate picture: how many
entries there are, how much they take, how often they are hit, and how much is
held by entries nothing has ever reused. They read the database read-only, so
they answer while a run is in progress.

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

# Give up what nobody has used lately, rather than what is oldest
ginkgo cache prune --max-size 5GB --least-recently-hit
```

Eviction is oldest-first unless you pass `--least-recently-hit`, which gives up
the entries with the oldest last hit first — an old entry that hits on every run
is worth more than a young one nothing has touched. Orphaned artifacts are
garbage-collected at the end of the operation. Use `--dry-run` to preview what
would be removed.

## Partial Resume

When a run fails partway through, Ginkgo preserves every successfully cached
task. Rerunning the same workflow picks up where the previous run left off:
tasks whose inputs are unchanged serve from cache, and only the tasks that
failed or were never reached are re-executed. The `cache_key` column in
`ginkgo cache ls` and the cache-hit markers in `ginkgo run` output make this
reuse visible. There is no separate resume command — the cache itself is the
resume mechanism.

## Dry-Run Mode

`ginkgo run flow.py --dry-run` validates the workflow without executing
any task body. Ginkgo resolves the expression tree, checks environments and
secrets, computes cache keys for every task, and reports which tasks would
run, which would serve from cache, and which resources they declare. Dry-run
is the fastest way to confirm that a workflow is correctly wired, that every
declared environment exists, and that planned caching aligns with intent
before committing to a real run.
