"""Remote artifact publisher.

Uploads locally stored artifacts to a remote object store, making them
available to other machines or future runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ginkgo.remote.backend import RemoteStorageBackend
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord, deserialize_tree_manifest


@dataclass(kw_only=True)
class RemotePublisher:
    """Publishes local artifacts to a remote object store.

    Parameters
    ----------
    backend : RemoteStorageBackend
        The remote storage backend to use for uploads.
    bucket : str
        Destination bucket name.
    prefix : str
        Key prefix under which artifacts are published.
    local_blobs_dir : Path
        Local blobs directory to read from.
    local_trees_dir : Path
        Local trees directory to read from.
    local_refs_dir : Path
        Local refs directory to update with remote URIs.
    """

    backend: RemoteStorageBackend
    bucket: str
    prefix: str
    local_blobs_dir: Path
    local_trees_dir: Path
    local_refs_dir: Path

    def publish(self, *, record: ArtifactRecord) -> ArtifactRecord:
        """Upload an artifact's content to the remote store.

        For blob artifacts, uploads the blob file.  For tree artifacts,
        uploads all constituent blobs and the tree manifest.

        Parameters
        ----------
        record : ArtifactRecord
            The local artifact record to publish.

        Returns
        -------
        ArtifactRecord
            Updated record with ``remote_uri`` set.
        """
        if record.remote_uri is not None:
            return record

        if record.kind == "blob":
            return self._publish_blob(record)
        return self._publish_tree(record)

    def _publish_blob(self, record: ArtifactRecord) -> ArtifactRecord:
        """Upload a single blob."""
        blob_path = self.local_blobs_dir / record.digest_hex
        remote_key = f"{self.prefix}blobs/{record.digest_hex}"
        self.backend.upload(src_path=blob_path, bucket=self.bucket, key=remote_key)

        remote_uri = f"s3://{self.bucket}/{remote_key}"
        updated = ArtifactRecord(
            artifact_id=record.artifact_id,
            kind=record.kind,
            digest_algorithm=record.digest_algorithm,
            digest_hex=record.digest_hex,
            extension=record.extension,
            size=record.size,
            created_at=record.created_at,
            storage_backend=record.storage_backend,
            remote_uri=remote_uri,
        )

        # Update the local ref file with the remote URI.
        ref_path = self.local_refs_dir / f"{record.artifact_id}.json"
        if ref_path.exists():
            ref_path.write_text(updated.to_json(), encoding="utf-8")

        return updated

    def _publish_tree(self, record: ArtifactRecord) -> ArtifactRecord:
        """Upload a tree manifest and all its constituent blobs."""
        # Upload individual blobs.
        tree_path = self.local_trees_dir / f"{record.digest_hex}.json"
        if tree_path.exists():
            tree_ref = deserialize_tree_manifest(tree_path.read_text(encoding="utf-8"))
            for entry in tree_ref.entries:
                blob_path = self.local_blobs_dir / entry.blob_digest
                if blob_path.exists():
                    remote_key = f"{self.prefix}blobs/{entry.blob_digest}"
                    self.backend.upload(src_path=blob_path, bucket=self.bucket, key=remote_key)

            # Upload the tree manifest itself.
            manifest_key = f"{self.prefix}trees/{record.digest_hex}.json"
            self.backend.upload(src_path=tree_path, bucket=self.bucket, key=manifest_key)

        remote_uri = f"s3://{self.bucket}/{self.prefix}trees/{record.digest_hex}.json"
        updated = ArtifactRecord(
            artifact_id=record.artifact_id,
            kind=record.kind,
            digest_algorithm=record.digest_algorithm,
            digest_hex=record.digest_hex,
            extension=record.extension,
            size=record.size,
            created_at=record.created_at,
            storage_backend=record.storage_backend,
            remote_uri=remote_uri,
        )

        ref_path = self.local_refs_dir / f"{record.artifact_id}.json"
        if ref_path.exists():
            ref_path.write_text(updated.to_json(), encoding="utf-8")

        return updated
