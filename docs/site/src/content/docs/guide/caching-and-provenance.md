---
title: "Caching And Provenance"
---

Ginkgo caches task results, so a repeat run reuses earlier work. It also
records what happened in each run in the [provenance](/ginkgo/guide/concepts/#glossary) database,
`.ginkgo/ginkgo.db`. This page covers the cache.
[Querying Provenance](/ginkgo/guide/querying-provenance/) covers reading the record, and
[Maintaining The Database And Cache](/ginkgo/guide/maintenance/) covers checking and
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
else the task reads must come in as an argument, or the cache can't see it
change.

- Pass files in as `file` or `folder` arguments. Ginkgo hashes their contents,
  and runs the task that writes a file before the task that reads it.
- Read environment variables in the flow body, and pass the values as plain
  arguments:

  ```python
  @flow
  def main():
      threshold = float(os.environ.get("PEAK_THRESHOLD", "0.05"))
      return call_peaks(bam="data/a.bam", out="results/a.bed", threshold=threshold)
  ```

  [Workflow parameters](/ginkgo/guide/cli/#workflow-parameters) are the other route.
  Argument values are recorded in the provenance database, so pass a secret
  with `ginkgo.secret(...)` (see
  [the FAQ](/ginkgo/faq/#how-do-i-reference-a-secret-without-leaking-it-into-logs-provenance-or-the-report)).

### What The Source Hash Covers

A task's source hash covers its own code, the rest of its module, and the
local Python modules that module imports. It leaves out the bodies of the other
tasks and flows in those files.

| You edit | What re-runs |
| --- | --- |
| Another task's body, or the flow's body | Nothing |
| Anything else in the module, such as a helper, a constant, a comment outside a task body, or another task's name, signature or decorator | Every task in the module |
| A local module that the task's module imports, even code the task never calls | Every task in the importing module |
| `flow.py` in the `ginkgo init` layout, which keeps tasks in `workflow/modules/` | Nothing |

- Ginkgo can't track a dynamic import (`importlib.import_module`) or another
  dependency found at run time. Bump the task's `version=` when one changes.
- To share logic between tasks, call a plain helper function, not another
  task. A helper is hashed like any other module code.
- A shared module-level value couples every task that imports the module. If
  each branch of a [fan-out](/ginkgo/guide/concepts/#glossary) reads a shared
  `MODEL_HYPERPARAMS` dict, editing one entry re-runs every branch. Pass such values as arguments
  instead, so only the branches whose values changed run again.
- A task that a cached task returned is looked up in the cache on its own, so
  a change to it re-runs it with no `version=` bump (see
  [Dynamic Graphs](/ginkgo/guide/tasks-and-flows/#dynamic-graphs-returning-tasks-from-a-task)).

### Early Cutoff

A task's key holds the values its upstream tasks produced, not their cache
keys. So when an upstream task re-runs and produces the same output, the tasks
below it still come from the cache. The same output means the same bytes for a
`file`, or an equal plain value such as an `int` or `str`. Only the tasks
whose inputs actually changed run again.

<a id="cache-correctness"></a>
## Cache Correctness

### How Is Each Input Tracked?

Ginkgo tracks each task input in one of these ways:

| Label | Inputs | What Ginkgo tracks |
| --- | --- | --- |
| `content` | A `file` or `folder` argument. Also a plain value that names an existing file and looks like a path, as in `data/x.tsv` or `x.tsv`. | The bytes |
| `version` | A remote input: a `remote_file`, a `remote_folder`, or a remote URI passed as a `file` or `folder` | What the store reports: the pinned version id, or else the ETag and size. Checking never downloads the input. |
| `path` | A `str` that names a directory, such as `"results"`, or a bare word that matches a file name, such as `"alpha"` | The path string only |
| `value` | Any other value, such as a number, a string or an object | The value |
| `output` | An `Out[...]` parameter | The path string only |
| `untracked` | A `tmp_dir`, or a parameter annotated `ginkgo.untracked` | Nothing |

- A remote input is tracked by its URI and its store's metadata, not its
  bytes. The same bytes at another URI miss the cache, as does a re-upload to
  a store whose ETag changes on every write.
- A list, tuple or dict takes the label of its least-tracked element. One
  `path` element in `inputs=[a, b]` labels the whole parameter `path`.

`ginkgo cache explain` shows each input's label, and flags a `path` label:

```
analyze (task_0002)
  cache key: fddb71a9…
  reason: all_inputs_match
  inputs:
    readings: path (tracked by path string only — a directory needs `folder` to track
      its contents; use `untracked` if that is deliberate)
    output_path: output
```

After a run with any `path` input, `ginkgo run` prints a one-line reminder to
check `ginkgo cache explain`.

### Reads vs. Writes: `file` vs. `Out[file]`

Choose each path's annotation by what the task does with it:

- **A file it reads:** `file`. Ginkgo checks that it exists before the task
  runs. A plain `str` naming a file is hashed too, but `file` states the
  intent, and `ginkgo doctor` expects it.
- **A directory it reads:** `folder`. A plain `str` naming a directory is
  tracked by its path only, so a change inside it does not re-run the task.
- **A path it writes:** `Out[file]` or `Out[folder]`. As a plain `str`, the
  output would be hashed as an input, and the task would re-run every time.
  See [Declaring Outputs](/ginkgo/guide/outputs/).
- **A path to leave out of the key on purpose,** such as a shared log file:
  `ginkgo.untracked`.

Don't annotate a parameter as `pathlib.Path`. Ginkgo rejects it, because it
would be hashed by neither its path nor its content.

```python
from ginkgo import Out, file, task

@task()
def analyze(coords: file, output_path: Out[file]) -> file: ...
```

### What The Cache Cannot See

The cache key is built from the task's arguments and source, and only declared
outputs are stored. These sit outside both.

**A file written to a path the task neither declares nor returns.** Ginkgo
doesn't store or restore it, and a change to it invalidates nothing
downstream. Declare it as an `Out[file]` parameter, or return it as
`file(...)`.

**A file or environment variable read inside a task body.** A change to it
still serves the cached result. Pass it in as an argument (see
[A Task Sees Only Its Arguments](#a-task-sees-only-its-arguments)).

**A script or tool a shell command runs.** A shell task's key covers the
command, not the files it runs, so `python scripts/filter.py {reads}` does not
re-run when `filter.py` changes. Pass the script in as a `file` argument, make
it a script task (`@task("script")`), or bump `version=`.

**A file a notebook reads by a literal path.** Declare the file as an
`Out[file]` output of the task that writes it, and pass that result to the
notebook task as a `file` argument (see
[Figures From Another Task](/ginkgo/guide/tasks-and-flows/#figures-from-another-task)).

**An edit that keeps a file's size and modification time.** Ginkgo reuses a
file's hash while both are unchanged. So an edit made with `touch -r`, by a
tool that restores timestamps, or on a filesystem with coarse timestamps is
missed: an input is served from the cache, and an edited output is not
restored. `touch` the file, or delete the output, to make the next run hash or
restore it.

## Why Did A Task Run Again?

`ginkgo cache explain` answers this for every task in a run. Leave out the run
id to explain the most recent run:

```bash
ginkgo cache explain             # the most recent run
ginkgo cache explain <run_id>
ginkgo cache explain <run_id> --json
```

For each task, it prints a reason and each input's label. For a task that
re-ran, it names the parts of the cache key that changed, with their earlier
and current values. A short value is shown as itself, as in
`inputs.min_cases (changed: 10 → 3)`. Files, secrets and longer values are
shown by their digests.

| Reason | Meaning |
| --- | --- |
| `all_inputs_match` | Cache hit. Every input matched the stored entry. |
| `rerun_requested` | `--rerun` named the task, so Ginkgo did not check the cache. |
| `no_prior_entry` | No earlier run of this call to compare against. |
| `no_entry_for_key` | Nothing changed, but the entry is gone: removed by `cache prune` or `cache clear`, or never written because the earlier run failed. |
| `prior_entry_removed` | The key changed, but the previous entry was removed, so Ginkgo can't name what changed. |
| `task_moved` | The task was renamed or moved to another module, so it has no cache entry. The old name is in `moved_from`. |
| `source_hash_changed` | The task's own code changed. |
| `module_source_changed` | The task's code is unchanged, but its module or a local module it imports changed. |
| `version_bump` | The task's `version=` changed. |
| `env_changed` | The declared environment changed. |
| `input_changed` | One or more inputs changed. |
| `cache_key_changed` | The key changed in a part none of the reasons above covers. |

`ginkgo run --dry-run` shows the same reasons before a run, in shorter words.
See [Checking A Workflow](/ginkgo/guide/cli/#checking-a-workflow).

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
- `components` lists each part of the key that differs, with a `status` of
  `changed`, `added` or `removed`. Part names follow the cache key: `task`,
  `version`, `source_hash`, `extra_source_hash` (the notebook or script),
  `env`, `env_hash.pixi_lock`, and `inputs.<parameter>`. A changed short value
  also carries `prior_value` and `current_value`.
- `compared_with` names the entry this one was compared with. Its `strategy`
  is usually `same_node`, the key from the most recent earlier run. For a new
  task it is `most_similar_by_function`: the closest earlier entry of the same
  task, which may be another branch of a fan-out.

## Artifact Storage

An artifact is a stored copy of a file or folder a task wrote. Ginkgo keeps
artifacts under `.ginkgo/artifacts/`, and restores outputs from them on a cache
hit.

If you delete `.ginkgo/artifacts/` but the outputs are intact, the next cache
hit stores them again. If an output is also gone or edited by then, it can't
be restored, so its task re-runs.

### Intermediate Files

A cache hit puts a deleted output back, so deleting an intermediate file
between runs does not keep it away. To avoid keeping one, do one of these:

- Produce and use it inside one task, in a `tmp_dir` scratch directory, which
  is never stored and is deleted when the task succeeds.
- Pass the data between tasks as a Python value instead of a file, when it is
  small enough.
- Leave it as a declared output, and limit what the store keeps with
  `ginkgo cache prune` (see
  [Bounding Cache Size](/ginkgo/guide/maintenance/#bounding-cache-size)).
