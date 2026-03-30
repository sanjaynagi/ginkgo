# Immutable Cache, Writable Working Tree

## Problem Definition

Ginkgo currently stores managed file and folder outputs in the immutable
artifact store and then materializes those outputs back into the working tree
as symlinks. This keeps cache-backed outputs cheap to rematerialize and makes
immutability visible, but it creates a mismatch between Ginkgo's internal
storage model and user expectations for normal workflow files.

The working tree plays two conflicting roles:

- it is the place where tasks write outputs during execution
- it is also used as a cache-backed materialization of immutable artifacts

That collision causes rerun failures when a task tries to overwrite a
previously materialized managed output. The failure is especially visible for
shell tasks and for Python tasks that return managed file outputs. It also
leaks runtime implementation details into workflow authoring, which is the
wrong abstraction boundary for Ginkgo.

Ginkgo's core product goals are:

- keep the cache and artifact store immutable
- preserve a simple DSL
- keep dynamic tasks natural rather than forcing static path planning
- allow the working tree to behave like a normal writable filesystem view

This design should therefore separate canonical cached state from the mutable
working tree instead of making the working tree itself the canonical cache
materialization.

## Proposed Solution

Adopt a new runtime contract:

- the artifact store remains the only canonical immutable storage layer
- the working tree contains normal writable files and directories
- cache hits restore working-tree outputs only when they are missing or differ
  from the cached artifact content
- successful task execution stores managed outputs in the artifact store but
  leaves the working-tree files in place rather than replacing them with
  symlinks

This makes the working tree a mutable materialized view of the cache rather
than the cache itself.

### High-level behavior

On fresh execution:

1. the task writes normal files/directories into the working tree
2. Ginkgo stores managed returned outputs in the artifact store
3. Ginkgo records output path -> artifact id mappings in cache metadata
4. Ginkgo leaves the working-tree outputs as writable files/directories

On cache hit:

1. Ginkgo loads the cached result value
2. for each managed returned output, Ginkgo checks whether the working-tree
   path exists and matches the cached artifact content
3. Ginkgo restores only missing or mismatched outputs
4. if the working-tree output already matches the cached artifact, Ginkgo does
   nothing

This preserves immutable canonical storage while avoiding unnecessary copying
on every cache hit.

## Scope

### In scope

- stop replacing managed outputs with symlinks after task completion
- add writable file and directory restoration from the artifact store
- change cache-hit validation from symlink integrity checks to content/type
  validation
- rematerialize only missing or mismatched outputs
- keep `AssetRef.artifact_path` pointing at canonical artifact-store content
- preserve existing cache-key semantics
- preserve dynamic task behavior
- add regression tests for reruns, cache hits, missing outputs, and modified
  outputs

### Out of scope

- changing the public DSL
- requiring explicit output declarations for all Python tasks
- changing asset identity/versioning semantics
- remote cache publication protocol changes
- asset pruning or retention policy changes

## Design

### Part 1 — Artifact Store Materialization Modes

**File:** `ginkgo/runtime/artifact_store.py`

The artifact store currently exposes retrieval behavior that materializes
managed outputs as symlinks. The writable-working-tree model needs a separate
materialization path for normal writable outputs.

Add a second materialization API for artifact restoration:

- restore blob artifacts as regular files
- restore tree artifacts as regular directories/files
- preserve the existing canonical artifact metadata and content digests

This should be an explicit API rather than silently changing the meaning of the
existing retrieval method. The runtime should clearly distinguish:

- canonical retrieval / identity-oriented materialization
- writable working-tree restoration

### Part 2 — Cache Save Path

**File:** `ginkgo/runtime/cache.py`

Keep the current behavior that stores managed returned outputs in the artifact
store and records output path -> artifact id mappings in cache metadata.

Change post-save behavior:

- do not replace working-tree outputs with symlinks
- leave the freshly produced outputs in place

The artifact store remains authoritative. The working tree is simply the latest
local materialization.

### Part 3 — Cache Hit Validation

**File:** `ginkgo/runtime/cache.py`

Replace symlink-based validation with writable-working-tree validation.

For file outputs:

- if the path is missing, restore it
- if the path exists and hashes to the cached artifact digest, keep it
- if the path exists but differs, replace it with a restored copy
- if the path exists but is the wrong type, replace it

For folder outputs:

- if the path is missing, restore it
- if the path exists and recursively matches the cached artifact content, keep
  it
- if the path exists but differs, replace it with a restored directory
- if the path exists but is the wrong type, replace it

Validation should be content-based rather than timestamp-based.

### Part 4 — Conditional Rematerialization

This phase should avoid copying outputs on every cache hit.

The cache-hit path should materialize outputs only when needed:

- output missing
- output content differs
- output type differs

This keeps the working tree cheap to maintain while still behaving like a
normal filesystem view.

### Part 5 — Python Task Semantics

This design intentionally avoids requiring Python tasks to declare outputs up
front.

That means the runtime fix should focus on the managed-output contract that
already exists:

- if a Python task returns a path-like managed output, Ginkgo stores it in the
  artifact store and can restore it later on cache hits
- if a Python task writes undeclared side-effect files that are not part of the
  returned output, those files remain unmanaged

This preserves Ginkgo's dynamic-task model and keeps the DSL simple.

### Part 6 — Asset Semantics

Assets continue to be layered over the artifact store.

- `AssetRef.artifact_path` should continue to refer to canonical artifact-store
  content
- the working tree may contain a writable materialized copy of the same content
- cache-hit restoration of working-tree files should not change asset version
  identity or artifact identity

The working tree is not canonical for asset identity. The artifact store
remains canonical.

## Key Design Points

- The cache must remain immutable even though the working tree becomes
  writable.
- The working tree is a local materialized view, not the source of truth.
- Manual edits to working-tree outputs are allowed but non-authoritative.
  Future cache hits may replace them if they differ from the cached artifact.
- The design should not require new DSL ceremony for ordinary Python tasks.
- Dynamic tasks remain first-class; no global move toward static path planning
  is required in this phase.
- If later phases add explicit Python output declarations, they should be an
  optional strengthening of the contract, not a prerequisite for this model.

## Risks and Tradeoffs

### Benefits

- simpler user mental model: outputs in `results/` are normal writable files
- reruns do not fail because a path is an immutable symlink
- preserves dynamic Python task behavior
- keeps the cache and artifact store canonical and immutable

### Costs

- more filesystem I/O than symlink materialization
- more disk usage in the working tree
- cache-hit validation now requires content comparison
- manual edits to managed outputs can be overwritten on later cache restore

### Main tradeoff

This design intentionally prefers a simpler workflow-authoring model over the
storage elegance of direct symlink-backed materialization.

## Implementation Sequence

| Step | Scope | Dependencies |
|------|-------|-------------|
| 1 | Add writable artifact restoration API in `artifact_store.py` | None |
| 2 | Update cache save path to stop symlinking outputs after execution | Step 1 |
| 3 | Replace cache-hit symlink validation with content/type validation | Steps 1-2 |
| 4 | Add conditional restore for missing/mismatched file outputs | Step 3 |
| 5 | Add conditional restore for missing/mismatched folder outputs | Step 3 |
| 6 | Verify `AssetRef` behavior remains canonical to artifact-store paths | Steps 2-5 |
| 7 | Add regression tests for reruns, cache hits, modified outputs, and folders | Steps 1-6 |
| 8 | Remove or narrow symlink-specific runtime assumptions elsewhere | Steps 1-7 |

## Validation

The implementation is complete when all of the following are true:

1. A Python task returning a managed file output can run, be cached, and rerun
   without failing because of a prior cache materialization.
2. A shell task with declared outputs can run, be cached, and rerun without
   manual cleanup in workflow code.
3. On a cache hit, if the working-tree output already matches the cached
   artifact, Ginkgo does not rewrite it.
4. On a cache hit, if the working-tree output is missing, Ginkgo restores it.
5. On a cache hit, if the working-tree output has been manually modified,
   Ginkgo replaces it with the cached artifact content.
6. Folder outputs follow the same rules as file outputs.
7. The artifact store remains immutable and authoritative throughout.
8. Asset-backed workflows continue to resolve asset identity through canonical
   artifact-store content rather than through mutable working-tree files.

## Success Criteria

- Managed outputs in the working tree behave like ordinary writable
  files/directories.
- The cache remains immutable and content-addressed.
- Dynamic Python workflows continue to work without mandatory output
  declarations.
- Cache-hit rematerialization is conditional rather than unconditional.
- No workflow code needs to remove cache-managed symlinks or work around
  internal artifact materialization behavior.
