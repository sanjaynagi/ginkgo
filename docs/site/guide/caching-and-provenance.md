# Caching And Provenance

Ginkgo caches task results, so a repeat run reuses earlier work. It also
records what happened in each run in the [provenance](concepts.md#glossary) database,
`.ginkgo/ginkgo.db`. This page covers the cache.
[Querying Provenance](querying-provenance.md) covers reading the record, and
[Maintaining The Database And Cache](maintenance.md) covers checking and
pruning both.

## Partial Resume

If a run fails partway through, the tasks that finished are already cached.
Run the same workflow again and it picks up where it stopped. Tasks whose
inputs are unchanged come from the cache, and only the tasks that failed or
never ran execute. There is no separate resume command. `ginkgo run` marks
each task it serves from the cache.

## Cache Identity

Ginkgo gives each task call a cache key. The key is a hash of everything that
can change the task's result. If an earlier run stored a result under the same
key, Ginkgo reuses it instead of running the task. The key covers:

- the task's name and the module it is defined in
- the task's `version=`
- the task's source: its own code, the rest of its module, and the local
  Python modules that module imports
- the notebook or script file, for a notebook or script task
- the values of the task's arguments
- the identity of the environment, for a task that runs in a declared
  environment

Nothing else goes into the key.

Ginkgo stores the cache in two places. The provenance database holds a row for
each cache entry, found by its key. The stored results are files under
`.ginkgo/cache/`, and copies of output files and folders are under
`.ginkgo/artifacts/`.

### A Task Sees Only Its Arguments

A task's cache key covers its arguments, its source and `version=`. Anything
else the task reads must come in as an argument.

- Pass files in as `file` or `folder` arguments. Ginkgo hashes their contents.
  If another task writes the file, the reader runs after the writer.
- Read environment variables in the flow body, which runs while the graph is
  built, and pass the values as plain arguments:

  ```python
  @flow
  def main():
      threshold = float(os.environ.get("PEAK_THRESHOLD", "0.05"))
      return call_peaks(bam="data/a.bam", out="results/a.bed", threshold=threshold)
  ```

  Workflow parameters are the other route (see
  [Workflow Parameters](cli.md#workflow-parameters)). Ginkgo records each
  argument's value in the provenance database, so pass a secret with
  `ginkgo.secret(...)` instead (see
  [the FAQ](../faq.md#how-do-i-reference-a-secret-without-leaking-it-into-logs-provenance-or-the-report)).

The cache can't see a file that the task body opens by its own path, or a
variable it reads from `os.environ`. If you change one, Ginkgo still serves
the cached result.

### What The Source Hash Covers

A task's source hash covers the task's own code, the rest of its module, and
the local Python modules that module imports. It leaves out the bodies of the
other tasks and flows in those files. Calling a task does not run its body, so
another task's body can't change this task's result.

- Editing another task's body, or the flow's body, does not re-run this task.
- Renaming another task, or changing its signature or decorator arguments,
  re-runs every task in the module.
- Editing anything else in the module re-runs every task in it. That includes
  a helper function, a constant, a lookup table, and a comment or whitespace
  outside a task or flow body.

To share code between tasks, put it in a plain helper function, not in
another task. Ginkgo hashes a helper with the rest of the module.

The `ginkgo init` layout puts tasks in `workflow/modules/`, not in `flow.py`.
`flow.py` imports the task modules, not the other way round, so editing
`flow.py` never re-runs a task.

Ginkgo tracks imports conservatively:

- Changing a local module re-runs every task whose module imports it, even if
  the task never calls the changed code.
- An `import` inside a function body counts too, whether or not it has run.
- Ginkgo can't track a dynamic import (`importlib.import_module`) or another
  dependency found at run time. Bump the task's `version=` when one of those
  changes.

A task can return other tasks, which then run (see
[Dynamic Graphs](tasks-and-flows.md#dynamic-graphs-returning-tasks-from-a-task)).
On a cache hit, Ginkgo looks up each returned task in the cache on its own. A
returned task whose code or inputs changed runs again, with no `version=` bump
needed.

A shared module-level value couples every task that imports the module. Take a
[fan-out](concepts.md#glossary), where one task runs over many inputs. If each branch reads its
settings from a shared `MODEL_HYPERPARAMS` dict, editing one model's entry
re-runs every branch, not just that model's. Pass such values as task
arguments instead. Arguments are hashed per call, so only the branches whose
values changed run again.

### Early Cutoff

A task's key holds the values its upstream tasks produced, not their cache
keys. So when an upstream task re-runs and produces the same output, the tasks
below it still come from the cache. The same output means the same bytes for a
`file`, or an equal plain value such as an `int` or `str`. Only the tasks
whose inputs actually changed run again.

(cache-correctness)=
## Cache Correctness

### How Is Each Input Tracked?

Ginkgo tracks each task input in one of the ways below. `ginkgo cache explain`
shows each input's label.

- **content**: Ginkgo hashes the bytes. This covers a `file` or `folder`
  argument. It also covers a plain value, such as a `str`, that names an
  existing file and looks like a path. A value looks like a path when it
  contains a separator, as in `data/x.tsv`, or has a file extension, as in
  `x.tsv`.
- **version**: a remote input, such as a `remote_file`, a `remote_folder`, or
  a remote URI passed as a `file` or `folder`. This includes a remote file
  streamed to the task rather than downloaded. Ginkgo uses what the store
  reports, so checking the cache never downloads the input. That is the
  object's version id if it is pinned, or else its ETag and size. For a
  folder, it is a digest of the folder's listing.
- **path**: a value that names an existing path but is not hashed by content.
  Ginkgo tracks only the path string. This covers a `str` that names a
  directory, such as `"results"`. It also covers a bare word with no separator
  or extension, such as `"alpha"`, that matches a file name.
- **value**: an ordinary value, such as a number, a string or an object.
  Ginkgo hashes the value itself.
- **output**: an `Out[...]` parameter. Ginkgo tracks only its path string,
  because the task writes the path rather than reads it.
- **untracked**: a `tmp_dir`, or a parameter annotated `ginkgo.untracked`.
  Ginkgo never hashes its content.

A remote input is tracked by where it is and what its store says about it,
not by its content. So the same bytes copied to another URI miss the cache.
So does a re-upload of the same bytes, on a store whose ETag changes on every
write.

A list, tuple or dict takes the label of its least-tracked element. One `path`
element in `inputs=[a, b]` labels the whole parameter `path`, because the
cache key does not watch that element's content.

A list or dict that holds only plain values is hashed as a whole, so
`cache explain` reports the parameter as changed, not the element. A container
that holds files keeps an entry for each file. `cache explain` and
`ginkgo why` can then name the file that changed.

Ginkgo does not re-read a large file on every run. It reuses the file's hash
while its size and modification time are unchanged. So it misses an edit that
keeps both. `touch` the file to make the next run hash it again.

`ginkgo cache explain <run_id>` shows each input's label next to it. A `path`
label is highlighted, with a reminder that a directory needs `folder`:

```
analyze (task_0002)
  cache key: fddb71a9…
  reason: all_inputs_match
  inputs:
    readings: path (tracked by path string only — a directory needs `folder` to track
      its contents; use `untracked` if that is deliberate)
    output_path: output
```

Pass `--json` for the same data as JSON, under `input_labels`. After a run, if
any input is labelled `path`, `ginkgo run` prints a one-line summary that
points to `ginkgo cache explain`.

### Reads vs. Writes: `file` vs. `Out[file]`

Choose each path's annotation by what the task does with it:

- Annotate a path the task reads as `file` or `folder`. Ginkgo then checks
  that it exists, and is the right kind, before the task runs. A plain `str`
  that names a file is hashed by content too. `file` still states the intent,
  and it is what `ginkgo doctor` expects.
- Annotate a directory the task reads as `folder`. A plain `str` that names a
  directory is tracked by its path only, so a change inside it does not re-run
  the task. Ginkgo warns when a directory passes from one task to another
  task's `str` parameter, and names both tasks.
- Annotate a path the task writes as `Out[file]` or `Out[folder]`. Don't leave
  it a plain `str`. Ginkgo would hash the file as an input, so the task would
  invalidate its own cache entry and re-run every time. Ginkgo prints a notice
  naming the path after such a run. See [Declaring Outputs](outputs.md) for
  how `Out[...]` works.
- Annotate a path `ginkgo.untracked` to keep its content out of the key on
  purpose. Examples are a log file several tasks append to, or a scratch path.
  `ginkgo doctor` does not warn about it.
- Don't annotate a task parameter as `pathlib.Path`. Ginkgo rejects it, because
  it would be hashed as an opaque object, by neither its path nor its content.
  Use `file`, `folder` or `Out[file]` instead.

```python
from ginkgo import Out, file, task

# Either signature hashes the contents of the file coords names.
# file is the clearer choice.
@task()
def analyze(coords: file, output_path: Out[file]) -> file: ...

# or
@task()
def analyze(coords: str, output_path: Out[file]) -> file: ...
```

### What The Cache Cannot See

The cache key is built from the task's arguments and source, and only declared
outputs are stored. These sit outside both.

**A file written to a path the task neither declares nor returns.** Say a
task writes `results/side.txt` to a hard-coded path. It has no `Out[...]`
parameter for the file, and does not return it. Then the file is invisible to
the cache. Ginkgo does not track or store it. On a cache hit the task does not
re-run, and a deleted or edited copy is not restored. The file feeds no cache
key either, so a change to it invalidates nothing downstream. Ginkgo prints
nothing about it. Declare the path as an `Out[file]` parameter, or return it
as `file(...)`.

**A file or environment variable read inside a task body.** A task that opens
`Path("data/genes.tsv")` or reads `os.environ["PEAK_THRESHOLD"]` itself
depends on something its key never sees. A change to it serves the cached
result. Pass the file in as a `file` argument. Read the variable in the flow
body and pass its value in (see
[A Task Sees Only Its Arguments](#a-task-sees-only-its-arguments)).

**A script or tool a shell command runs.** A shell task's key covers the
command it builds and its arguments, not the files the command runs. A task
running `python scripts/filter.py {reads}` or `bash tools/annotate.sh` does
not re-run when that script changes. Fix this in one of these ways:

- Pass the script in as a `file` argument and use it in the command, so its
  bytes are hashed.
- Make it a script task (`@task("script")`), whose script is part of the key.
- Bump `version=` on the task when you change the script.

**A file a notebook reads by a literal path.** A notebook task's key covers the
notebook source and the task's arguments. A notebook that calls
`mo.image("results/figures/north.png")` or reads
`pd.read_csv("results/x.csv")` itself is served from the cache after that file
changes. Declare the file as an `Out[file]` output of the task that writes it.
Then pass that result to the notebook task as a `file` argument (see
[Figures From Another Task](tasks-and-flows.md#figures-from-another-task)).

**An edit that keeps a file's size and modification time.** Ginkgo reuses a
file's hash while its size and modification time are unchanged. A tool that
restores timestamps, `touch -r`, or a filesystem with coarse timestamps can
make an edit that keeps both. For an input, the task is then served from the
cache. For a cached output, the hit does not restore the file, so the hand
edit stays. `ginkgo cache explain` compares the same recorded hashes, so it
reports the task as `all_inputs_match`. `touch` the file, or delete an output,
to make the next run hash or restore it. Ginkgo does not re-read every output
on every hit, because that is slow for a large workflow.

## Why Did A Task Run Again?

`ginkgo cache explain` answers this for every task in a run. Leave out the run
id to explain the most recent run:

```bash
ginkgo cache explain             # the most recent run
ginkgo cache explain <run_id>
ginkgo cache explain <run_id> --json
```

For each task, it prints a reason and each input's label. For a task that
re-ran, it also names the parts of the cache key that changed, and shows each
one's earlier and current value. A short value, such as a number, a bool or a
string of up to 40 characters, is shown as itself, as in
`inputs.min_cases (changed: 10 → 3)`. Files, folders, secrets and longer values
are shown by their digests. So is a string that names a file, because the
file's contents are what Ginkgo tracks. The reasons are:

| Reason | Meaning |
| --- | --- |
| `all_inputs_match` | Cache hit. Every input matched the stored entry. |
| `rerun_requested` | `--rerun` named the task, so Ginkgo did not check the cache. |
| `no_prior_entry` | No earlier run of this call to compare against. |
| `no_entry_for_key` | Nothing the key covers changed, but the entry is gone. It was removed by `cache prune` or `cache clear`, or never written because the earlier run failed. |
| `prior_entry_removed` | The key changed, and the previous entry has since been removed, so Ginkgo can't name what changed. |
| `task_moved` | The task moved to another module or was renamed. Its name is part of its identity, so it starts with no cache entry. The old name is in `moved_from`. |
| `source_hash_changed` | The task's own code changed. |
| `module_source_changed` | The task's own code is unchanged, but its module or a local module it imports changed. For example, another task in the same file was renamed. |
| `version_bump` | The task's `version=` changed. |
| `env_changed` | The declared environment changed. |
| `input_changed` | One or more inputs changed. |
| `cache_key_changed` | The key changed in a part none of the reasons above covers. |

When more than one part changed, the reasons are listed in a fixed order:
`source_hash_changed`, `version_bump`, `env_changed`, `input_changed`. The
first of them is the summary reason.

`ginkgo run --dry-run` shows the same reasons before a run, in shorter words:
`no prior entry`, `source changed`, `version changed`, `environment changed`,
`input changed`, `cache key changed` and `--rerun`. Any other reason appears as
its code. See [Checking A Workflow](cli.md#checking-a-workflow).

### The JSON Shape

`--json` prints the same data. One task's entry looks like this:

```json
{
  "task_name": "produce",
  "cache_key": "fddb71a9…",
  "input_labels": {"samples": "path", "threads": "value"},
  "compared_with": {"cache_key": "4388619b…", "strategy": "same_node"},
  "reason": "input_changed",
  "details": ["input_changed"],
  "components": [
    {
      "component": "inputs.samples",
      "status": "changed",
      "current": {"type": "str", "digest": "2f64dc9d…"},
      "prior": {"type": "str", "digest": "f7a9858a…"}
    }
  ]
}
```

- `reason` is the summary reason, and `details` lists every reason.
- A changed short value also carries `prior_value` and `current_value`.
- `components` lists the parts of the key that differ. Each `status` is
  `changed`, or `added` or `removed` for a parameter that appeared or went
  away. The part names follow the cache key:
  - `task`, `version` and `source_hash`
  - `extra_source_hash`, the notebook or script the task runs
  - `env`, and `env_hash.pixi_lock` for the identity of the declared
    environment
  - one `inputs.<parameter>` per argument
- `compared_with` names the entry this one was compared with. Its `strategy`
  is usually `same_node`: the key this task used in the most recent earlier
  run of the workflow. For a task that is new, or whose label changed, it is
  `most_similar_by_function`. That is the earlier entry of the same task that
  differs least, which may be another branch of a fan-out. The `reason` is then
  `no_prior_entry`, and `components` shows how the task differs from that
  branch.

A task with `all_inputs_match`, `rerun_requested`, `no_prior_entry`,
`no_entry_for_key` or `prior_entry_removed` has a reason only, with no
`components`.

## Artifact Storage

An artifact is a stored copy of a file or folder a task wrote. Ginkgo keeps
artifacts under `.ginkgo/artifacts/`, and restores outputs from them on a cache
hit.

If you delete `.ginkgo/artifacts/` but the outputs are intact, the next cache
hit stores them again. If an output is also gone or edited by then, it can't
be restored, so its task re-runs.

### Intermediate Files

Ginkgo stores every declared output, and a cache hit puts a deleted output
back. So deleting an intermediate file between runs does not keep it away. To
avoid keeping an intermediate file, do one of these:

- Produce and use it inside one task, in a `tmp_dir` scratch directory.
  Ginkgo creates the directory for each run of the task and deletes it when
  the task succeeds. Nothing in it is stored or restored.
- Pass the data between tasks as a Python value instead of a file, when it is
  small enough to hold in memory.
- Leave it as a declared output, and limit what the store keeps with
  `ginkgo cache prune` (see
  [Bounding Cache Size](maintenance.md#bounding-cache-size)).
