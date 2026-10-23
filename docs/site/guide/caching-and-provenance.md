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

Nothing else reaches the key (see
[A Task Sees Only Its Arguments](#a-task-sees-only-its-arguments)).

Inputs annotated `file` or `folder` are hashed by content. A remote input
(`remote_file`, `remote_folder`, or a remote URI under a `file`/`folder`
annotation) is keyed by what its store reports instead, so checking the cache
never downloads it: its `version_id` when pinned, otherwise its ETag and size
(one HEAD) or, for a folder, the digest of its listing (one LIST). Re-uploading
identical bytes changes the ETag on most stores, so it is a miss. A plain `str` (or
`Any`/`list[str]`/...) value that names an existing *file* and reads as a path
(a separator or a file extension) is hashed by content too, whatever its
annotation — everything else is hashed from its `repr`. See
[Cache Correctness](#cache-correctness) for the exact rule and why it exists.

A list, tuple or dict that holds no file, folder or remote object is hashed as
one digest of its elements, so a 200,000-number input costs the cache entry and
the ledger one digest, and `cache explain` reports the parameter as changed
rather than naming the element. A container holding a file keeps an entry per
element, which is how `cache explain` and `ginkgo why` name the file that
moved. Recorded inputs (`runs show --json`, `debug`, the manifest) keep the
first 20 items of a long container, with its length and the number left out.

Hashing a large file on every run would be slow, so each digest is memoised
under the file's device, inode, size and modification time, and reused while
those four agree. A rewrite that keeps the size and the modification time —
a tool that restores timestamps, `touch -r`, or a filesystem with coarse
timestamps — is therefore not re-hashed, and the old digest stands. `touch`
the file to make the next run hash it again. The same memo checks a cached
output on a cache hit, so an output edited that way is not restored either (see
[What The Cache Cannot See](#what-the-cache-cannot-see)).

### A Task Sees Only Its Arguments

A task's cache key covers its arguments, its source and `version=`. Anything
else the task reads must come in as an argument.

- Files go in as `file` or `folder` arguments. Their contents are hashed, and a
  file another task writes orders the graph, so the reader runs after the
  writer.
- Environment variables are read in the flow body, which runs while the graph
  is built, and passed as plain arguments:

  ```python
  @flow
  def main():
      threshold = float(os.environ.get("PEAK_THRESHOLD", "0.05"))
      return call_peaks(bam="data/a.bam", out="results/a.bed", threshold=threshold)
  ```

  Workflow parameters are the other route (see
  [Workflow Parameters](cli.md#workflow-parameters)). An argument's value is
  recorded in provenance, so pass a secret with `ginkgo.secret(...)` instead
  (see
  [the FAQ](../faq.md#how-do-i-reference-a-secret-without-leaking-it-into-logs-provenance-or-the-report)).

A file the task body opens by its own path, or a variable it reads from
`os.environ`, is invisible to the cache: changing it serves the cached result.

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
`module_source_changed` for a task invalidated this way — its own text is
unchanged — and `source_hash_changed` when the task's own definition moved. The hash covers the
module's raw text, so editing only a comment or whitespace in it also re-runs
every task the module defines.

Local-import tracking is conservative: changing a reachable helper module
invalidates tasks that import it, even when the changed symbol is not called.
An `import` statement inside a function body counts too, whether or not it has
run yet. Dynamic imports (`importlib.import_module`) and other runtime
dependencies cannot be tracked this way; set or increment `version=` on the
task when those dependencies change. A task that returns other tasks needs no
bump for its children: on a cache hit each returned task is looked up on its
own, so a child whose module, script or inputs changed runs again.

The conservative closure has a practical consequence for fan-out. If tasks read
their parameters from a shared module-level structure, every consumer's cache
identity is coupled to every other consumer's parameters — editing one model's
entry in a shared `MODEL_HYPERPARAMS` dict invalidates every task that imports
the module, so the whole fan-out re-runs rather than the affected branch. Pass
such parameters as task arguments instead; arguments are hashed per call, so
only the branches whose values changed are invalidated.

### Early Cutoff

A downstream task's key is built from the *values* its upstream tasks produced,
not from the upstream tasks' own cache keys. So when an upstream task re-runs
(an input changed, or its `version=` was bumped) and produces the same bytes
for a `file` output, or an equal plain value such as an `int` or `str`, every
task below it still hits the cache. The change stops spreading at the first task
whose output did not change, so only the tasks whose inputs actually differ
re-run. (Tasks defined in the same module share a source hash, so editing that
module's source still invalidates all of them; see [Cache Identity](#cache-identity).)

(cache-correctness)=
## Cache Correctness

### Path Boundaries: `file`/`folder` Are Still Preferred, But `str` Is No Longer A Trap

Annotate a path a task *reads* `file` (or `folder`) when you can — it is
explicit, it validates existence and kind before execution, and it is what
`ginkgo cache explain` and the doctor's `path_like_str_param` check expect.
But since #307 phase 2, a plain `str` (or `Any`/`list[str]`/...) parameter is
no longer a silent-staleness trap for a *file*: if the value is a
`str`/`PathLike` that reads as a path (a separator, `data/x.tsv`, or a file
extension, `x.tsv`) and names an existing regular file, its BLAKE3 content
digest goes into the cache key automatically, exactly as `file` would.
Editing the file — by hand, or an upstream task returning a different one —
now invalidates the consumer whether or not the annotation says `file`.

```python
from ginkgo import Out, file, task

# Both are keyed on coords' file contents now — file is still the clearer,
# type-checked choice, but str is no longer a correctness trap for a file.
@task()
def analyze(coords: str, output_path: Out[file]) -> file: ...

@task()
def analyze(coords: file, output_path: Out[file]) -> file: ...
```

**A directory is the one case this does not cover.** A plain `str` naming an
existing directory — `"."`, `"results"` — is *not* auto-hashed: doing so
would silently hash an entire tree as a side effect of an ordinary scalar
parameter. It stays keyed by its path string only, same as before. Annotate
it `folder` to track its contents; Ginkgo still warns when a directory
crosses a task boundary as `str` (an upstream task's return reaching a
consumer's `str` parameter), naming both ends and the `folder` annotation to
add.

A bare word with neither a separator nor an extension (`"alpha"`) does not
qualify either, even if a same-named file exists — nothing about the string
itself suggests a path, so it stays tracked by its own value.

**Use `ginkgo.untracked` to deliberately opt a path out of content
tracking** — a shared log file several tasks append to, a staging path that
is not itself the tracked artifact, or anything whose content should not
participate in the key. `untracked` never content-hashes, whatever exists on
disk, and never triggers a doctor warning: it is a declared choice, not the
trap these checks exist to flag.

Never leave an *output* path plain `str` without `Out[...]`: annotate the
parameter `Out[file]` / `Out[folder]` (below), naming it as a write rather
than a read. A file a task writes through a plain `str` path is
content-hashed as an input, so the task invalidates its own cache entry and
re-runs every time; Ginkgo notices the write after the run and prints a
notice naming the path, and the fix is `Out[...]` (or `untracked` for a
side-channel file such as a log).

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
- `Out[file | None]` means the *argument* may be `None`, in which case the
  task has nothing to write there. A path that is passed must still be
  written. For a shell, script or notebook output that may legitimately not
  be written, use [`optional()`](tasks-and-flows.md#outputs-that-may-not-be-written)
  on its `output=` entry instead.
- **Before execution:** an `Out[...]` argument need not exist yet — it is
  validated to be a local, path-like value, and if something already exists at
  the path it must be the declared kind (`Out[file]` pointing at an existing
  directory is an error).
- **Cache key:** contributes its path string only, never content — the same
  as leaving the parameter `str`, but declared rather than implied.
- **After execution:** the path must exist, with the right kind, or the task
  fails naming the parameter and the path. On a cache hit, a tracked output
  file or folder that is missing or modified on disk is restored from the
  artifact store without re-running the task and without a log line.
  "Modified" is judged as for inputs: a file whose size or modification time
  moved is re-hashed, and one whose size and modification time both survived
  the edit is taken as unchanged (see
  [What The Cache Cannot See](#what-the-cache-cannot-see)). This
  covers `file(...)` returns and every declared `Out[...]` path, whatever the
  task's return annotation says (`-> str`, `-> None`, or a dynamic task
  returning tasks). The restore overwrites hand edits: an edited output is
  reset to the cached bytes.
- Supported for remote tasks (`remote=True` / `executor=`) too: with a
  `[remote.artifacts] store` configured, each declared path is rewritten to a
  worker-local scratch path, staged back through the same channel returned
  files use once the task body writes it, and restored at its declared
  driver path before the post-execution check runs. Without a configured
  store, the declared path is sent to the worker unchanged, which only works
  when the worker shares the driver's filesystem.
- Before the task body runs, whatever is at every declared `Out[...]` path is
  removed (a file, or a folder with everything in it), for every task kind,
  so a task never reads its own stale output: a body globbing its
  `Out[folder]` sees only the files this run wrote. The parent directory is
  then created (`mkdir(parents=True)`), so a task writing into a fresh
  subdirectory needs no boilerplate of its own. For `Out[folder]` only the
  *parent* is created, not the folder itself — tools differ on whether they
  want the target directory to already exist.

A literal path elsewhere in the graph that matches an `Out[...]` path is now
also a real dependency edge: if another task's argument is the same path
string (or a path inside a produced `Out[folder]`, or a `folder`-annotated
argument containing a produced path), Ginkgo infers that it must run after
the task that writes it — no `.output[...]` reference or return value needed.
Two tasks declaring the same (or an overlapping) `Out[...]` path is a clear
error before anything runs. So is one task whose `Out[...]` path overlaps one
of its own `file`/`folder` inputs: the same path, an input inside an output
folder (`Out[folder]` at `results/` with an input `results/samples.tsv`), or
an output inside an input folder. Running it would overwrite the input and
cache the damaged file as the input of a successful run. Write the output to
a path outside every input instead. A plain `str` input is not checked, since
it may be a label that happens to match a path. A path only *computed* at
runtime, from an
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
    return shell(
        cmd=f"tr a-z A-Z < {shlex.quote(src)} > {shlex.quote(dest)} && shasum {shlex.quote(dest)} > {shlex.quote(check)}"
    )

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

- **content** — a `file` / `folder` annotation or instance, *or* (since
  #307 phase 2) a plain value that names an existing regular file and reads
  as a path: the bytes are hashed.
- **version** — a remote reference, or a remote file streamed through FUSE:
  tracked by its version id, or by the ETag and size (a folder: its listing)
  the store reports.
- **path** — a value that names an existing path but is not content-hashed: a
  directory (never auto-hashed — annotate `folder`), a bare word with no
  separator or extension. Tracked by the path *string* only.
- **value** — an ordinary scalar or object: tracked by its own `repr` or
  pickle digest.
- **output** — an `Out[...]` parameter: tracked by its declared path string
  only, by design (it names what the task is about to write, not something it
  reads).
- **untracked** — `tmp_dir`, or a parameter explicitly annotated
  `ginkgo.untracked`: excluded from content tracking, by design.

A container (`list`, `tuple`, `dict`) is labelled by its least-tracked
element — one `path` buried inside `inputs=[a, b]` labels the whole parameter
`path`, since that is the element the cache key does not really watch.

`ginkgo cache explain <run_id>` shows every input's label next to it, with a
`path` label highlighted and a reminder that a directory needs `folder` to
track its contents:

```
analyze (task_0002)
  cache key: fddb71a9…
  reason: all_inputs_match
  inputs:
    readings: path (tracked by path string only — a directory needs `folder` to track
      its contents; use `untracked` if that is deliberate)
    output_path: output
```

Pass `--json` for the same data as JSON, under `input_labels`. A run recorded
before this label existed shows nothing for its inputs rather than a guess.

After a real `ginkgo run`, if any task's input was labelled `path`, the run
prints one dim summary line — `N inputs are tracked by path only; see
\`ginkgo cache explain\`` — so a remaining directory or bare-word case
surfaces without having to go looking for it. A workflow with nothing
tracked by path stays quiet.

### What The Cache Cannot See

The cache key is built from the task's arguments and source, and only declared
outputs are stored. These sit outside both.

**A file written to a path the task neither declares nor returns.** A task
that writes `results/side.txt` to a hard-coded path, with no `Out[...]`
parameter for it and without returning it, leaves that file invisible to the
cache. It is not tracked or stored, so on a cache hit the task does not re-run
and a deleted or edited copy is not restored. Nothing about the file feeds a
cache key either, so a change to it invalidates nothing downstream. Ginkgo
prints nothing about it. Declare the path as an `Out[file]` parameter, or
return it as `file(...)`.

**A file or environment variable read inside a task body.** A task that opens
`Path("data/genes.tsv")` or reads `os.environ["PEAK_THRESHOLD"]` itself depends
on something its key never sees, so a change to it serves the cached result.
Pass the file in as a `file` argument, and read the variable in the flow body
and pass its value in (see
[A Task Sees Only Its Arguments](#a-task-sees-only-its-arguments)).

**A script or tool a shell command runs.** A shell task's key covers the
command it builds and its arguments, not the files the command executes. A
task running `python scripts/filter.py {reads}` or `bash tools/annotate.sh`
does not re-run when that script changes, and Snakemake behaves the same way.
Pass the script in as a `file` argument and use it in the command, so its bytes
are hashed; or make it a script task (`@task("script")`), whose script is part
of the key; or bump `version=` on the task when you change it.

**A file a notebook reads by a literal path.** A notebook task's key covers the
notebook source and the task's arguments. A notebook that calls
`mo.image("results/figures/north.png")` or reads `pd.read_csv("results/x.csv")`
itself is served from the cache after that file changes. Declare the file as an
`Out[file]` output of the task that writes it and pass that result to the
notebook task as a `file` argument (see
[Figures From Another Task](tasks-and-flows.md#figures-from-another-task)).

**An edit that keeps a file's size and modification time.** A digest is reused
while a file's device, inode, size and modification time agree (see
[Cache Identity](#cache-identity)), so such an edit leaves the old digest in
place. For an input, the task is served from the cache. For a cached output,
the hit takes the file as already holding the cached bytes and does not restore
it, so the hand edit stays in the working tree. `ginkgo cache explain` compares
those same recorded digests, so it reports the task as cached
(`all_inputs_match`) and cannot point at the edit. `touch` the file, or delete
an output, to make the next run hash or restore it. Checking every output by
content on every hit would read every cached output's bytes on every run: at
about 0.5 s per GiB from a warm page cache, a workflow with 100 GiB of outputs
would spend close to a minute re-reading them before deciding nothing changed,
and much longer on network storage.

## Artifact Storage

For file and folder outputs, Ginkgo stores content-addressed artifacts under
`.ginkgo/artifacts/` and uses those as the durable backing store for cached path
outputs.

A task's declared output path is not the source of truth — the artifact store
is. If `.ginkgo/artifacts/` is deleted while the outputs themselves are intact,
the next cache hit stores them again from the working tree; an output that is
gone or edited by then cannot be restored, so its task re-runs.

### Intermediate Files

Ginkgo has no equivalent of Snakemake's `temp()`. Every declared output is
stored, and a cache hit puts a deleted output back in the working tree, so
deleting an intermediate between runs does not keep it away. Three ways to
handle an intermediate you do not want to keep:

- Produce and consume it inside one task, in a `tmp_dir` scratch directory.
  Ginkgo creates the directory for each execution and deletes it when the task
  succeeds, and nothing in it is stored or restored.
- Pass the data between tasks as a Python value instead of a file, when it is
  small enough to hold in memory.
- Leave it as a declared output, and bound what the store keeps with
  `ginkgo cache prune` (see [Bounding Cache Size](#bounding-cache-size)).

## Where Provenance Lives

What happened goes into one SQLite database per workspace, at
`.ginkgo/ginkgo.db`: an append-only event log, plus the tables `ginkgo inspect
run`, `ginkgo debug` and `ginkgo report` read. All three work on a run that is
still going.

Each run also gets a directory under `.ginkgo/runs/<run_id>/` holding the bytes
— per-task logs, rendered notebook pages, copies of the environment lock files, and
a `manifest.yaml` snapshot of everything the database recorded for the run.

Together, the cache and the ledger answer different questions:

- cache: can this work be reused safely?
- provenance: what happened in this specific run?

## Maintaining The Provenance Database

```bash
ginkgo db path                # where the database is
ginkgo db check               # schema version, integrity, rows against bytes
ginkgo db check --verify      # also rehash every stored artifact byte
ginkgo db check --fix         # first remove what nothing can use any more
ginkgo db prune --events-older-than 90d --dry-run
ginkgo db prune --staging-older-than 30d    # staged remote inputs, and their bytes
ginkgo db vacuum              # give the freed space back
```

`ginkgo db check` asks every index whether its rows and the files they name
still agree — the cache, the artifact store, the run directories, the staged
remote inputs — and reports both directions: a row whose bytes are gone, and
bytes no row can find. On its own it repairs nothing; `--fix` first removes
what nothing can use any more — run directories and cache directories the
ledger has no row for, cache entries whose bytes are missing or unreadable
(each would only miss), the artifacts no entry names, and a `ginkgo.db-wal` /
`-shm` left behind by a database that was moved or deleted — then reports what
is left. A run row whose directory is gone is kept: it is the only record of
that run. `ginkgo doctor` reports what `db check` fails on as well: a ledger
SQLite cannot read is an error, and cache drift a warning pointing at `--fix`.
Those checks compare rows with file names; they cannot see a stored file whose bytes changed in place.
`--verify` rehashes every stored blob and directory member against its digest
and reports each mismatch, exiting non-zero. It reads the whole store, so it
takes as long as the store is large.

A cache hit checks the bytes it restores anyway: they are hashed as they are
copied, and a copy that does not match its digest is deleted with a warning,
the corrupt stored bytes are discarded, and the task re-runs as a cache miss.

`ginkgo db prune --events-older-than 90d` deletes the raw event stream of runs
that finished more than 90 days ago. Everything `ginkgo runs show`, the report
and `ginkgo history` read is left alone; what goes is the per-event detail
`ginkgo export events` prints. Add `--digest-memo-older-than` to drop memoised
file digests, which cost only a re-hash to lose, `--staging-older-than` to
evict downloaded remote inputs that nothing has read for a while — bytes and
row together, and the only eviction the staging cache has — and `--dry-run` to
see the counts first. Deleting rows does not shrink the database file; `ginkgo
db vacuum` does, and reports the file and its write-ahead log together.
`ginkgo cache clear` and `cache prune` likewise free their entries' rows for
reuse without shrinking the file; the space they report freed is what the
cached outputs and artifacts they deleted occupied on disk, in allocated
blocks as `du` counts them.

Every run adds to the ledger, a fully cached one too: about 4 KiB per task
(3.5–4.5 KiB in our measurements, more for a task with many inputs), of which
the raw events are between a third and two thirds, so a 1,000-task workflow run
nightly adds about 1.5 GB a year. `ginkgo doctor` warns once the ledger passes
1 GiB. Nothing is pruned unless you ask: prune events older than a few months
and vacuum, or opt the workspace in to pruning after every run in
`ginkgo.toml`:

```toml
[ledger]
keep_events_for = "90d"   # each run drops the events of runs finished over 90 days ago
```

That is `ginkgo db prune --events-older-than 90d` run as each run finishes; the
runs themselves stay listed, and `ginkgo db vacuum` still returns the space to
the disk.

`ginkgo db check` reads; it never creates. In a directory nobody has run a
workflow in it says so and succeeds.

If the database is gone but `.ginkgo/runs/` still holds run directories, it
says that instead — how many runs are stranded, and that their provenance
cannot be read until the ledger is restored. `ginkgo runs ls` and
`ginkgo doctor` report the same thing. It is a warning rather than a failure,
so all three still exit 0: nothing is corrupt, and the fix is to restore the
file — or, if the history is not wanted, `ginkgo db check --fix`, which removes
the stranded run directories and any write-ahead log the database left behind.

### Upgrading from a pre-ledger workspace

Workspaces recorded before `.ginkgo/ginkgo.db` existed are **not** migrated.
Their runs and cache entries lived in files ginkgo no longer reads, so they
are invisible to every command. If you have one, delete `.ginkgo/` and run the
workflow again; there is no import path, and nothing in the old layout is read
by mistake.

The same holds between schema versions before 1.0: there are no migrations,
so a database written at a different schema version is refused with an error
naming both versions. Delete `.ginkgo/` and run again.

`ginkgo.db` is the record of your runs and of your cache; back it up as you
would `.git`. If it is lost, the run history goes with it and the cache goes
cold: the cached bytes are still under `.ginkgo/cache/`, but the keys that find
them were rows in the database. `ginkgo db check` lists those stranded
cache and run directories; `ginkgo db check --fix` removes both, and
`ginkgo cache clear --orphans` (or any `cache prune`) the cache ones.

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

An entry's size is its stored return value plus the files and folders it
restores, which live in the artifact store. An artifact two entries share
counts toward both, so the per-entry sizes can add up to more than the store
holds; `cache stats` also reports the artifact store's own size, each byte
counted once.

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
is worth more than a young one nothing has touched. `--max-size` counts each
entry's artifacts as well as its return value — including a large value kept
in the artifact store, such as a DataFrame's Parquet; `5GB` means 5,000,000,000 bytes
and `5GiB` means 5 × 1024³. Sizes are printed in binary units (KiB, MiB, GiB). Artifacts no remaining entry
names are garbage-collected at the end of the operation, and the space
reported as freed is what was actually deleted, measured on disk: an artifact a
surviving entry still names stays. Entry sizes in `cache ls` and the totals in
`cache stats` are apparent sizes — what the files hold — while `cache stats`
also reports the space the cache takes on disk, which for many small entries is
far larger. A prune also removes cache directories the database has no row
for. Use `--dry-run` to preview what would be removed.

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
