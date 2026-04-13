"""Remote-backed artifact store with local CAS and remote fallback.

Wraps a ``LocalArtifactStore`` with a ``RemoteStorageBackend`` so that
artifacts produced locally are published to a remote object store and
artifacts produced by other machines (e.g. Kubernetes workers) can be
transparently downloaded on read.
"""

from __future__ import annotations

import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from ginkgo.remote.backend import RemoteStorageBackend
from ginkgo.runtime.artifacts.artifact_model import (
    ArtifactRecord,
    deserialize_tree_manifest,
)
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class RemoteArtifactStore:
    """Artifact store backed by local CAS with remote fallback.

    Writes go to both local and remote. Reads try local first, then
    download from remote on miss.

    Parameters
    ----------
    local : LocalArtifactStore
        Local CAS store for immediate reads and writes.
    backend : RemoteStorageBackend
        Remote storage backend for uploads and downloads.
    bucket : str
        Remote bucket name.
    prefix : str
        Key prefix under which artifacts are stored remotely.
    scheme : str
        URI scheme for the remote store (e.g. ``"gs"``, ``"s3"``).
    """

    local: LocalArtifactStore
    backend: RemoteStorageBackend
    bucket: str
    prefix: str
    scheme: str
    transfer_concurrency: int = 16

    # --- ArtifactStore protocol -----------------------------------------------

    def store(self, *, src_path: Path) -> ArtifactRecord:
        """Store locally and publish to remote."""
        record = self.local.store(src_path=src_path)
        return self._publish(record)

    def retrieve(self, *, artifact_id: str, dest_path: Path) -> None:
        """Retrieve via local store, downloading from remote on miss."""
        self._ensure_local(artifact_id)
        self.local.retrieve(artifact_id=artifact_id, dest_path=dest_path)

    def restore(self, *, artifact_id: str, dest_path: Path) -> None:
        """Restore as writable content, downloading from remote on miss."""
        self._ensure_local(artifact_id)
        self.local.restore(artifact_id=artifact_id, dest_path=dest_path)

    def matches(self, *, artifact_id: str, path: Path) -> bool:
        """Check content match (local only, no remote download)."""
        if not self.local.exists(artifact_id=artifact_id):
            return False
        return self.local.matches(artifact_id=artifact_id, path=path)

    def exists(self, *, artifact_id: str) -> bool:
        """Check existence locally, then remotely."""
        if self.local.exists(artifact_id=artifact_id):
            return True
        return self._remote_ref_exists(artifact_id)

    def delete(self, *, artifact_id: str) -> None:
        """Delete from local store only (remote is immutable)."""
        self.local.delete(artifact_id=artifact_id)

    def artifact_path(self, *, artifact_id: str) -> Path:
        """Return local path, downloading from remote on miss."""
        self._ensure_local(artifact_id)
        return self.local.artifact_path(artifact_id=artifact_id)

    def store_bytes(self, *, data: bytes, extension: str) -> ArtifactRecord:
        """Store raw bytes locally and publish to remote."""
        record = self.local.store_bytes(data=data, extension=extension)
        return self._publish(record)

    def read_bytes(self, *, artifact_id: str) -> bytes:
        """Read bytes, downloading from remote on miss."""
        self._ensure_local(artifact_id)
        return self.local.read_bytes(artifact_id=artifact_id)

    # --- Publishing (local → remote) -----------------------------------------

    def _publish(self, record: ArtifactRecord) -> ArtifactRecord:
        """Upload artifact content and ref to remote.

        Uploads content first (blobs/trees), then the ref JSON last so
        that the artifact is only "visible" remotely once all content is
        present.
        """
        if record.kind == "blob":
            self._upload_blob(record.digest_hex)
        else:
            self._upload_tree(record.digest_hex)

        # Upload ref JSON last (visibility marker).
        updated = ArtifactRecord(
            artifact_id=record.artifact_id,
            kind=record.kind,
            digest_algorithm=record.digest_algorithm,
            digest_hex=record.digest_hex,
            extension=record.extension,
            size=record.size,
            created_at=record.created_at,
            storage_backend=record.storage_backend,
            remote_uri=f"{self.scheme}://{self.bucket}/{self.prefix}refs/{record.artifact_id}.json",
        )
        ref_path = self.local._refs_dir / f"{record.artifact_id}.json"
        ref_path.write_text(updated.to_json(), encoding="utf-8")
        self.backend.upload(
            src_path=ref_path,
            bucket=self.bucket,
            key=f"{self.prefix}refs/{record.artifact_id}.json",
        )
        return updated

    def _upload_blob(self, digest_hex: str) -> None:
        """Upload a single blob to remote."""
        blob_path = self.local._blobs_dir / digest_hex
        if blob_path.exists():
            self.backend.upload(
                src_path=blob_path,
                bucket=self.bucket,
                key=f"{self.prefix}blobs/{digest_hex}",
            )

    def _upload_tree(self, digest_hex: str) -> None:
        """Upload a tree manifest and all its constituent blobs to remote.

        Blob uploads are content-addressed and idempotent, so we fan them
        out across a thread pool to saturate network bandwidth. The tree
        manifest is uploaded last so it only becomes visible once every
        referenced blob is present.
        """
        tree_path = self.local._trees_dir / f"{digest_hex}.json"
        if not tree_path.exists():
            return

        tree_ref = deserialize_tree_manifest(tree_path.read_text(encoding="utf-8"))
        unique_digests = {entry.blob_digest for entry in tree_ref.entries}
        self._run_parallel(self._upload_blob, unique_digests)

        self.backend.upload(
            src_path=tree_path,
            bucket=self.bucket,
            key=f"{self.prefix}trees/{digest_hex}.json",
        )

    # --- Downloading (remote → local) ----------------------------------------

    def _ensure_local(self, artifact_id: str) -> None:
        """Download artifact from remote into local store if not present."""
        if self.local.exists(artifact_id=artifact_id):
            return

        # Download the ref JSON to learn the artifact structure.
        ref_key = f"{self.prefix}refs/{artifact_id}.json"
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            self.backend.download(bucket=self.bucket, key=ref_key, dest_path=tmp_path)
            record = ArtifactRecord.from_json(tmp_path.read_text(encoding="utf-8"))
        finally:
            tmp_path.unlink(missing_ok=True)

        # Download the content.
        if record.kind == "blob":
            self._download_blob(record.digest_hex)
        else:
            self._download_tree(record.digest_hex)

        # Write the local ref file so LocalArtifactStore can find it.
        local_ref_path = self.local._refs_dir / f"{artifact_id}.json"
        local_ref_path.parent.mkdir(parents=True, exist_ok=True)
        local_ref_path.write_text(record.to_json(), encoding="utf-8")

    def _download_blob(self, digest_hex: str) -> None:
        """Download a single blob from remote into local store."""
        dest = self.local._blobs_dir / digest_hex
        if dest.exists():
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        self.backend.download(
            bucket=self.bucket,
            key=f"{self.prefix}blobs/{digest_hex}",
            dest_path=dest,
        )
        # Match LocalArtifactStore convention: blobs are read-only.
        dest.chmod(0o444)

    def _download_tree(self, digest_hex: str) -> None:
        """Download a tree manifest and all its blobs from remote."""
        tree_dest = self.local._trees_dir / f"{digest_hex}.json"
        tree_dest.parent.mkdir(parents=True, exist_ok=True)
        if not tree_dest.exists():
            self.backend.download(
                bucket=self.bucket,
                key=f"{self.prefix}trees/{digest_hex}.json",
                dest_path=tree_dest,
            )

        tree_ref = deserialize_tree_manifest(tree_dest.read_text(encoding="utf-8"))
        unique_digests = {entry.blob_digest for entry in tree_ref.entries}
        self._run_parallel(self._download_blob, unique_digests)

    def _run_parallel(self, fn, digests) -> None:
        """Run a per-blob transfer callable across a bounded thread pool."""
        digest_list = list(digests)
        if not digest_list:
            return
        if self.transfer_concurrency <= 1 or len(digest_list) == 1:
            for digest in digest_list:
                fn(digest)
            return
        workers = min(self.transfer_concurrency, len(digest_list))
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="ginkgo-artifact-transfer"
        ) as pool:
            # list() forces exceptions from any worker to propagate.
            list(pool.map(fn, digest_list))

    def _remote_ref_exists(self, artifact_id: str) -> bool:
        """Check whether a ref JSON exists in remote storage."""
        ref_key = f"{self.prefix}refs/{artifact_id}.json"
        try:
            self.backend.head(bucket=self.bucket, key=ref_key)
            return True
        except (FileNotFoundError, OSError):
            return False


def load_remote_artifact_store(
    *,
    local: LocalArtifactStore,
) -> RemoteArtifactStore | None:
    """Construct a ``RemoteArtifactStore`` from project configuration.

    Reads the ``[remote.artifacts]`` section of ``ginkgo.toml``. Returns
    ``None`` when no remote artifact store is configured.

    Config example::

        [remote.artifacts]
        store = "gs://my-bucket/artifacts/"

    Parameters
    ----------
    local : LocalArtifactStore
        The local artifact store to wrap.

    Returns
    -------
    RemoteArtifactStore | None
    """
    from ginkgo.config import load_runtime_config
    from ginkgo.core.remote import _parse_uri
    from ginkgo.remote.resolve import resolve_backend

    config = load_runtime_config(project_root=Path.cwd())
    artifacts_config = config.get("remote", {}).get("artifacts", {})
    store_uri = artifacts_config.get("store") if isinstance(artifacts_config, dict) else None
    if store_uri is None:
        return None

    parsed = _parse_uri(store_uri)
    backend = resolve_backend(parsed["scheme"])

    prefix = parsed["key"]
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    return RemoteArtifactStore(
        local=local,
        backend=backend,
        bucket=parsed["bucket"],
        prefix=prefix,
        scheme=parsed["scheme"],
    )
