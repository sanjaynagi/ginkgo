# Phase 6 Plan: Remote References and Staged Access

## Problem Definition

Ginkgo's current runtime is path-oriented. Tasks consume `file` and `folder`
values as ordinary local filesystem paths, cached outputs are restored from the
local artifact store, and shell and notebook execution assume those paths are
usable without provider-specific I/O code.

Phase 6A and Phase 6B established two important foundations:

- the local artifact store now uses explicit blob/tree records
- workflows can declare `s3://...` and `oci://...` inputs through first-class
  remote references and have them staged locally before execution

Those changes were originally framed as the first half of a larger plan that
would later add FUSE-like mounted access and a remote-capable managed artifact
store. That framing is too broad for the current needs of the project.

The immediate problem to solve is narrower:

- users need to pass object storage inputs into tasks without writing manual
  download code
- tasks must continue to receive ordinary local paths
- the design must remain compatible with future Kubernetes and cloud execution,
  where workers should stage inputs into worker-local storage rather than rely
  on a shared scheduler filesystem

## Proposed Scope

Phase 6 should now focus on one correctness path only:

- external remote references are resolved by staging them into worker-local
  storage before task execution

Phase 6 should not currently include:

- FUSE-like mounted access as an active implementation target
- publication of all managed artifacts into a remote store
- a general multi-driver access engine where mount and stage are treated as
  interchangeable runtime modes

Those may still happen later, but they should be treated as follow-on work
built on top of the staged-access model rather than part of the active Phase 6
deliverable.

## Goals

- Allow workflow authors to pass remote object storage URIs into `file(...)`
  and `folder(...)` inputs without explicit staging code.
- Preserve local-path task semantics for Python, shell, notebook, and future
  remote-worker execution.
- Make staging the single correctness path for remote inputs.
- Keep remote input identity, cache keys, and provenance explicit and
  reproducible.
- Use a storage adapter layer that can support `fsspec`-style backends such as
  `s3fs` and `ocifs`.
- Keep the design compatible with future worker execution on Kubernetes or
  other cloud platforms.
- Preserve extension points for a future FUSE-like optimization without
  forcing mount-specific complexity into the current runtime.

## Non-Goals

- Implementing a mounted virtual filesystem in this phase.
- Requiring Linux FUSE, macFUSE, or any other platform-specific mount support.
- Turning object storage into a distributed POSIX filesystem.
- Publishing every task output to object storage as part of the active Phase 6
  scope.
- Solving high-performance streaming, byte-range reads, or lazy hydration in
  this phase.

## Summary of the Proposed Solution

Phase 6 should define a simple, stable contract:

- workflows may declare external remote references
- the evaluator resolves those references before task launch
- remote files and folders are staged into worker-local storage
- tasks receive normal local paths
- cache identity is based on immutable remote metadata where possible and on
  content digests after staging

The runtime should be built around a small backend abstraction for object
storage operations. That abstraction may be implemented with `fsspec`,
`s3fs`, and `ocifs`, but the task-facing contract remains local-path-based.

## Design Principles

- Preserve the current task contract: tasks open local paths, not remote
  streams.
- Treat staging as the correctness layer, not as a fallback beneath a future
  mount.
- Keep remote input handling separate from the managed local artifact store.
- Prefer worker-local caches and explicit transfer steps over shared mutable
  filesystems.
- Preserve explicit remote reference objects so future access modes can be
  added without changing workflow code.
- Keep scheduler and worker responsibilities compatible with Kubernetes-style
  deployment, where workers may run in isolated pods with ephemeral local disk.

## User-Facing Model

```python
from ginkgo import flow, task, file, remote_file


@task
def count_lines(path: file) -> int:
    with open(path) as handle:
        return sum(1 for _ in handle)


@flow
def main() -> int:
    return count_lines(remote_file("s3://my-bucket/data/sample.txt"))
```

The task still receives a normal local path. Internally, Ginkgo stages the
remote file into worker-local storage before the task starts.

`remote_file()` remains the canonical constructor for file-shaped remote
inputs, and `remote_folder()` remains the canonical constructor for
folder-shaped remote inputs.

Annotation-aware coercion remains useful, but it should stay narrow:

- if a parameter is annotated as `file` or `folder`
- and the caller passes a supported remote URI string such as `s3://...` or
  `oci://...`
- the evaluator may coerce that string into a remote reference and stage it

Plain `str` parameters must remain plain strings.

## Architecture

### 1. Two Distinct Domains

Phase 6 should keep a clear separation between:

- managed local artifacts produced by Ginkgo
- external remote references declared by the user

Managed local artifacts continue to use the local blob/tree CAS introduced in
Phase 6A. External remote references are staged into a separate local cache.

This separation matters because the current requirement is remote input
consumption, not a full remote artifact lifecycle for all outputs.

### 2. Remote Reference Model

Remote references remain explicit immutable values:

- `RemoteFileRef`
- `RemoteFolderRef`

They represent provider URI, remote object identity hints, and enough metadata
for the runtime to select the right backend and reproducibility policy.

This explicit model should stay even though mounting is deferred. It is the
main compatibility point for future enhancements such as:

- FUSE-like path exposure
- task-aware prefetch
- remote worker transfer planning
- stricter pinning and provenance policies

### 3. Worker-Local Staging Contract

The evaluator should resolve each remote input into a local staged path before
task dispatch.

For files:

- download into a local staging cache
- compute or confirm content identity
- hand the staged local path to the task

For folders:

- enumerate the remote prefix
- materialize the directory tree into worker-local storage
- hand the local directory path to the task

This contract works locally today and maps directly to cloud execution later:

- on a local machine, the worker-local cache may live under `.ginkgo/staging/`
- on Kubernetes, the same cache contract can point at pod-local ephemeral
  storage or another node-local writable volume

### 4. Backend Abstraction

The runtime should keep a small backend protocol for object storage operations:

- inspect object metadata
- download one object
- list a prefix
- optionally upload in later phases

The current implementation may use provider-specific clients, but the target
direction for this phase is to support an adapter implementation based on
`fsspec`, including:

- `s3fs` for S3-compatible object storage
- `ocifs` for OCI Object Storage

Using a backend abstraction keeps evaluator logic independent from transport
details and avoids coupling the task model to Python-only file interfaces.

### 5. Identity and Reproducibility

Remote URIs are mutable and must not be treated as stable cache identity.

The staging cache should prefer the following identity policy:

1. use an explicit version ID when present
2. otherwise use provider metadata such as ETag when trustworthy
3. materialize and hash the content when immutable metadata is insufficient

For folders, Phase 6 should be explicit that folder identity is harder than
file identity. A folder/prefix should eventually resolve to a manifest-like
identity derived from the listed objects and their metadata, not just the URI
string.

### 6. Cloud and Kubernetes Compatibility

The staged-access design should assume the long-term deployment model may
include isolated workers running on Kubernetes or similar systems.

That implies:

- no assumption that workers can see the scheduler's local artifact root
- no dependence on privileged FUSE support in containers
- credentials should come from environment, mounted secrets, or provider-native
  identity, not from hard-coded local machine assumptions
- staging paths should be configurable so workers can use pod-local writable
  volumes

This is a better fit for cloud execution than treating mounted access as the
primary path.

## Why This Still Leaves Room for Future FUSE-Like Access

Deferring mounted access does not invalidate the work already done.

Phase 6A remains useful because blob/tree CAS and explicit artifact metadata
are still the right internal representation for managed artifacts.

Phase 6B remains useful because explicit remote references and staging are the
correct baseline for any future optimization.

If Ginkgo later adds a FUSE-like system, it should build on top of the same
reference and staging model:

- remote refs stay the user-facing abstraction
- staging remains the correctness baseline and fallback path
- any mount implementation becomes a worker-local optimization layer

That future system should be justified by a specific execution environment and
performance need, not treated as a prerequisite for remote input support.

## Implemented Work and Required Adjustments

The repository already contains meaningful Phase 6 work. Most of it still fits
the revised plan, but some parts should be adjusted or re-scoped.

### Work That Still Fits the Revised Plan

- `ginkgo/core/remote.py`
  Explicit `RemoteFileRef` and `RemoteFolderRef` types still match the desired
  architecture.
- `ginkgo/runtime/evaluator.py`
  Remote ref staging and annotation-aware coercion still fit the active Phase 6
  design.
- `ginkgo/remote/staging.py`
  A dedicated staging cache remains central to the revised plan.
- Phase 6A blob/tree CAS
  The local managed artifact model remains valid even without mount support.

### Implemented Areas That Need Adjustment

- `ginkgo/remote/publisher.py` and evaluator-driven remote publisher loading
  should no longer be treated as part of active Phase 6 scope.
  This work should move into a later phase focused on remote publication of
  managed artifacts.

- `ginkgo/remote/s3_compat.py` and `ginkgo/remote/resolve.py` currently encode
  a boto3-based S3-compatible path for both S3 and OCI.
  That is workable in the short term, but it does not yet reflect the desired
  backend direction of `fsspec` plus provider-specific adapters such as
  `s3fs` and `ocifs`.
  The Phase 6 plan should therefore treat the backend implementation as
  replaceable behind the protocol.

- `ginkgo/remote/staging.py` currently stages folders under a URI-hash-based
  directory.
  That is adequate as a first implementation, but it is weaker than the file
  identity model.
  The revised plan should call for stronger folder identity based on a listed
  object manifest rather than URI alone.

- OCI backend configuration currently depends on environment-specific endpoint
  construction.
  That should be tightened so cloud and Kubernetes deployments can resolve
  credentials and endpoints cleanly through runtime configuration and
  provider-native mechanisms.

## Implementation Stages

### Stage 1: Consolidate the staged-access contract

- Keep `remote_file()` and `remote_folder()` as the canonical remote input
  constructors.
- Keep annotation-aware coercion limited to `file` and `folder` parameters.
- Remove Phase 6 references to mount-mode selection and remote output
  publication as active deliverables.
- Document staging as the sole correctness path for remote inputs.

### Stage 2: Stabilize the worker-local staging cache

- Keep a dedicated staging cache separate from the managed artifact store.
- Ensure staged files are content-addressed and reusable across tasks on the
  same worker.
- Make the staging root configurable for cloud and Kubernetes workers.
- Tighten folder staging semantics so folder identity can later be derived from
  a manifest-like listing rather than only a URI hash.

### Stage 3: Standardize backend integration

- Keep the backend protocol narrow and evaluator-facing.
- Support S3 and OCI through replaceable backends.
- Prefer an implementation direction compatible with `fsspec`, `s3fs`, and
  `ocifs`.
- Keep provider-specific credential handling outside core task logic.

### Stage 4: Strengthen reproducibility and provenance

- Record remote identity hints such as URI, version ID, ETag, and resolved
  digest in staging metadata.
- Ensure cache keys for remote file inputs use stable content identity.
- Define the provenance contract for folder refs clearly enough to support
  future remote-worker execution.

### Stage 5: Prepare for cloud worker execution

- Ensure remote input resolution does not assume shared local scheduler state.
- Treat worker-local staging as the execution boundary for future Kubernetes
  workers.
- Keep extension points for later optimizations such as prefetch, sparse
  hydration, or FUSE-like mounted access, but do not implement them in this
  phase.

## Risks and Tradeoffs

- Staging large inputs has latency and local disk costs.
  That is an accepted tradeoff for a simpler and more portable correctness
  model.

- Deferring mounted access means some large-file workloads may be less
  efficient than a mature Fusion-like system.
  That is acceptable until there is a concrete performance problem in a target
  deployment environment.

- Folder identity remains more subtle than file identity.
  This needs explicit follow-up so remote folder caching does not become too
  URI-dependent.

- Backend standardization around `fsspec` may still require provider-specific
  exceptions.
  The protocol boundary should absorb those differences.

## Success Criteria

- A Python task can consume `remote_file("s3://...")` through a normal local
  path without explicit download code.
- A shell or notebook task can consume the same staged path without any
  provider-specific logic.
- A `file` or `folder` parameter can accept a supported remote URI string
  through annotation-aware coercion.
- Re-running against the same pinned remote object reuses staged content on the
  same worker.
- Changing remote object identity invalidates the cache deterministically.
- The staging root can be configured for worker-local storage in future cloud
  deployments.
- The Phase 6 design remains compatible with a future FUSE-like optimization,
  but does not require it.
