# Phase 6B Plan: External Remote References and Staging-Based Access

## Problem Definition

After Phase 6A, the local artifact store uses a blob/tree CAS model with
metadata records. But workflows can only consume local filesystem paths.
A bioinformatics user who wants to run a task against
`s3://my-bucket/samples/reads.fastq.gz` or
`oci://namespace/bucket/object` must write their own download logic before
passing the path to a Ginkgo task.

Phase 6B adds first-class remote file references so that workflows can
declare remote URIs and have Ginkgo stage them transparently before task
execution.

## Goals

- `remote_file("s3://...")` / `remote_file("oci://...")` and
  `remote_folder(...)` as value constructors that workflows can pass
  directly to `file` and `folder` task parameters.
- Transparent staging: remote refs are downloaded to a local staging cache
  before the task runs. The task receives a normal local path.
- Cache identity: remote input identity participates correctly in cache
  keys using content digests, not mutable URIs.
- Annotation-aware coercion: raw `s3://` and `oci://` strings passed to
  `file`/`folder` parameters are auto-upgraded to remote references.
- Output publishing: task outputs can be published to a remote artifact
  store so downstream tasks or future runs on other machines can consume
  them.
- Both AWS S3 and Oracle Cloud Infrastructure (OCI) Object Storage from
  the start, since test data lives in OCI.

## Non-Goals

- FUSE-mounted access (that is Phase 6C).
- Parallel transfer or byte-range reads (that is Phase 6D).
- Remote worker bootstrap (that is Phase 6E).

## Proposed Solution

### User-Facing API

```python
from ginkgo import task, flow, file, remote_file

@task()
def count_lines(path: file) -> int:
    with open(path) as handle:
        return sum(1 for _ in handle)

@flow
def main():
    # Explicit remote references.
    s3_input = remote_file("s3://my-bucket/data/sample.txt")
    oci_input = remote_file("oci://namespace/bucket/path/to/object.csv")

    # Or via annotation-aware coercion (same effect):
    return count_lines(path="s3://my-bucket/data/sample.txt")
```

The task still receives a normal local path. Ginkgo downloads the file
into a staging cache before task execution.

### Remote Reference Types

```python
@dataclass(frozen=True, kw_only=True)
class RemoteRef:
    """Base for remote object references."""
    uri: str
    scheme: str           # "s3", "oci"

@dataclass(frozen=True, kw_only=True)
class RemoteFileRef(RemoteRef):
    """Remote reference to a single file."""

@dataclass(frozen=True, kw_only=True)
class RemoteFolderRef(RemoteRef):
    """Remote reference to a directory prefix."""
```

These are **not** subclasses of `file`/`folder` (which are `str` subclasses
that validate local existence). They are opaque references that the
evaluator materializes into local `file`/`folder` values before task
dispatch.

URI parsing is scheme-specific:
- **S3**: `s3://bucket/key` → bucket + key, optional `?versionId=...`
- **OCI**: `oci://namespace/bucket/key` → namespace + bucket + key

### S3-Compatible Protocol

Both AWS S3 and OCI Object Storage implement the S3-compatible API. OCI
Object Storage supports S3 compatibility mode natively, so a single
`boto3`-based client can serve both backends by varying the endpoint URL.

The backend resolution therefore maps:
- `s3://` → S3-compatible client with default AWS endpoint
- `oci://` → S3-compatible client with OCI-specific endpoint
  (`https://<namespace>.compat.objectstorage.<region>.oraclecloud.com`)

This means we need only one storage backend implementation, parameterized
by endpoint and credential source.

### Staging Cache

Remote files are staged into a local content-addressed cache:

```text
.ginkgo/staging/
  blobs/
    <digest>                    # cached remote file bytes
  metadata/
    <uri_hash>.json             # { uri, digest, etag, version_id, size, staged_at }
```

The staging cache is separate from the artifact store. The artifact store
holds task outputs; the staging cache holds downloaded remote inputs.

Staging metadata records the content digest, ETag, and version ID (when
available) so that cache identity is based on content, not the mutable URI.

### Reproducibility Policy for Remote Inputs

Remote URIs are mutable. Cache identity must be based on content, not URI.

Resolution order:
1. If the remote ref has `version_id`, use it as an immutable pin.
2. Otherwise, fetch the object's ETag via HEAD.
3. On first access, download and hash the content. Store the digest in
   staging metadata.
4. On subsequent access with matching ETag, skip re-download.
5. If ETag changes, re-download and recompute the digest.

This works identically for S3 and OCI Object Storage since both support
ETag and versioning through the S3-compatible API.

For cache keys, the input hash for a remote file is its content digest
(same as for local files), computed after staging.

### Storage Backend Protocol

```python
@dataclass(frozen=True, kw_only=True)
class RemoteObjectMeta:
    """Metadata returned by remote storage operations."""
    uri: str
    size: int
    etag: str | None = None
    digest: str | None = None
    version_id: str | None = None

class RemoteStorageBackend(Protocol):
    def head(self, *, uri: str) -> RemoteObjectMeta:
        """Return metadata without downloading."""
        ...

    def download(self, *, uri: str, dest_path: Path) -> RemoteObjectMeta:
        """Download an object to a local path."""
        ...

    def upload(self, *, src_path: Path, uri: str) -> RemoteObjectMeta:
        """Upload a local file to a remote URI."""
        ...

    def list_prefix(self, *, uri: str) -> list[RemoteObjectMeta]:
        """List objects under a prefix (for folder refs)."""
        ...
```

A single `S3CompatibleBackend` class implements this protocol for both
S3 and OCI Object Storage. The backend is constructed with an endpoint URL
and credentials appropriate to the scheme.

### Backend Resolution

A `resolve_backend(scheme: str) -> RemoteStorageBackend` dispatcher
constructs the correct client:

- `s3://` → `S3CompatibleBackend(endpoint=None)` (default AWS endpoint)
- `oci://` → `S3CompatibleBackend(endpoint=oci_s3_compat_endpoint)`

The OCI endpoint and namespace are resolved from configuration or
environment variables.

### Evaluator Integration

The materialization happens in `_resolve_task_args()`, after
`_materialize()` resolves expression dependencies but before validation
and cache-key building.

Flow:
1. `_materialize()` returns the raw value (a `RemoteFileRef` or string).
2. A new `_stage_remote_refs()` step checks each resolved arg:
   - If it's a `RemoteFileRef`/`RemoteFolderRef`, download it to the
     staging cache and replace the value with a local `file(staged_path)`
     or `folder(staged_path)`.
   - If it's a raw `s3://...` or `oci://...` string and the parameter
     annotation is `file` or `folder`, auto-coerce to a remote ref and
     stage it.
3. Validation and cache-key building proceed against the local staged path.

### Output Publishing

When the workflow is configured with a remote artifact store, task outputs
are published after execution:

1. Task produces a local `file`/`folder` output.
2. The local artifact store stores it as before (blob/tree CAS).
3. If a remote store is configured, the blob(s) are uploaded to the
   remote prefix.
4. The `ArtifactRecord` gains a `remote_uri` field pointing to the
   published location.

This makes outputs consumable from other machines.

### Configuration

Remote store configuration lives in `ginkgo.toml`:

```toml
[remote]
store = "s3://my-bucket/ginkgo-artifacts/"
region = "eu-west-1"  # optional, defaults to boto3 resolution

# For OCI Object Storage:
# store = "oci://namespace/bucket/ginkgo-artifacts/"
# region = "uk-london-1"
```

Credentials are resolved through provider-native chains:
- S3: boto3 chain (env vars, AWS config, IAM roles)
- OCI: OCI config (`~/.oci/config`) or S3-compat credentials via env vars

Ginkgo does not manage credentials directly.

## Implementation Stages

### Stage 1: Remote reference types and constructors

- Add `ginkgo/core/remote.py` with `RemoteRef`, `RemoteFileRef`,
  `RemoteFolderRef`.
- Add `remote_file()` and `remote_folder()` constructor functions with
  URI parsing for `s3://` and `oci://` schemes.
- Export from `ginkgo/__init__.py`.
- Unit tests for construction and URI parsing.

### Stage 2: S3-compatible storage backend

- Add `ginkgo/remote/__init__.py`, `ginkgo/remote/backend.py` with
  `RemoteStorageBackend` protocol and `RemoteObjectMeta`.
- Add `ginkgo/remote/s3_compat.py` with `S3CompatibleBackend` using
  `boto3`. Parameterized by endpoint URL to support both S3 and OCI.
- Add `ginkgo/remote/resolve.py` with `resolve_backend(scheme)`.
- Implement `head()`, `download()`, `upload()`, `list_prefix()`.
- Unit tests with mocked boto3 client.

### Stage 3: Staging cache

- Add `ginkgo/remote/staging.py` with the staging cache implementation.
- Content-addressed staging under `.ginkgo/staging/blobs/<digest>`.
- Staging metadata records under `.ginkgo/staging/metadata/<uri_hash>.json`.
- ETag-based freshness checks to avoid redundant downloads.
- Unit tests for stage/lookup/freshness.

### Stage 4: Evaluator integration

- Add `_stage_remote_refs()` to the evaluator, called between
  `_materialize()` and `_validate_inputs()`.
- Remote refs are replaced with local `file(staged_path)` /
  `folder(staged_path)` values.
- Update `_contains_dynamic_expression()` to handle remote ref types.
- Annotation-aware coercion: raw `s3://` and `oci://` strings in
  `file`/`folder` params auto-upgrade to remote refs before staging.
- Integration tests with mocked backends.

### Stage 5: Cache identity for remote inputs

- Remote file inputs are hashed by their staged content digest (same
  as local files -- the staging step already produces a local path).
- Provenance records both the user-facing URI and the resolved content
  digest.
- Test that changing a remote object's content invalidates the cache.

### Stage 6: Output publishing to remote store

- Add optional remote publishing after local artifact storage.
- If `[remote] store` is configured, upload blobs after task completion.
- Record `remote_uri` in `ArtifactRecord`.
- Integration test: publish an output, verify it exists in mocked backend.

## Risks and Tradeoffs

- **boto3 dependency**: adds a significant transitive dependency. Keep it
  optional (import on first use, clear error if missing).
- **Staging latency**: downloads happen synchronously before task execution.
  Phase 6D adds prefetch and parallel transfer.
- **Large files**: no byte-range or parallel download in this phase. Files
  are downloaded whole. Acceptable for a first cut.
- **Credential management**: deferred to provider-native chains. Phase 13
  may add Ginkgo-managed secrets for credentials.
- **Folder refs**: listing a prefix and downloading all objects is
  inherently slower than a single file. Consider warning for large
  prefixes.

## Success Criteria

- `remote_file("s3://bucket/key")` passed to a `file` parameter works:
  the file is staged locally and the task receives a local path.
- `remote_file("oci://namespace/bucket/key")` works the same way via the
  S3-compatible endpoint.
- `remote_folder("s3://bucket/prefix/")` works for directory-shaped inputs.
- A raw `"s3://..."` or `"oci://..."` string passed to a `file` parameter
  is auto-coerced.
- Cache identity uses content digest, not the mutable URI.
- Changing the remote object invalidates the cache.
- Two tasks consuming the same remote file share the staged copy.
- Task outputs can be published to remote storage when configured.
- All existing local-only workflows continue to work unchanged.
