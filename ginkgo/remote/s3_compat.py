"""S3-compatible storage backend.

Supports both AWS S3 and Oracle Cloud Infrastructure Object Storage via
the S3-compatible API.  The backend is parameterized by endpoint URL to
support both providers with a single implementation.
"""

from __future__ import annotations

from pathlib import Path

from ginkgo.remote.backend import RemoteObjectMeta


class S3CompatibleBackend:
    """S3-compatible object storage backend.

    Parameters
    ----------
    endpoint_url : str | None
        Custom endpoint URL for non-AWS providers (e.g. OCI Object Storage).
        ``None`` uses the default AWS S3 endpoint.
    region : str | None
        AWS region or OCI region.  Defaults to ``boto3`` resolution.
    """

    def __init__(
        self,
        *,
        endpoint_url: str | None = None,
        region: str | None = None,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._region = region
        self._client = None

    def _get_client(self) -> object:
        """Lazily create the boto3 S3 client."""
        if self._client is not None:
            return self._client

        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for remote storage support. Install it with: pip install boto3"
            ) from exc

        kwargs: dict[str, object] = {"service_name": "s3"}
        if self._endpoint_url is not None:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._region is not None:
            kwargs["region_name"] = self._region

        self._client = boto3.client(**kwargs)  # type: ignore[arg-type]
        return self._client

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        """Return metadata for an object without downloading it.

        Parameters
        ----------
        bucket : str
            Bucket name.
        key : str
            Object key.

        Returns
        -------
        RemoteObjectMeta
        """
        client = self._get_client()
        response = client.head_object(Bucket=bucket, Key=key)  # type: ignore[union-attr]
        return RemoteObjectMeta(
            uri=f"s3://{bucket}/{key}",
            size=response["ContentLength"],
            etag=response.get("ETag", "").strip('"'),
            version_id=response.get("VersionId"),
        )

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        """Download an object to a local path.

        Parameters
        ----------
        bucket : str
            Bucket name.
        key : str
            Object key.
        dest_path : Path
            Local destination path.

        Returns
        -------
        RemoteObjectMeta
        """
        client = self._get_client()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        response = client.get_object(Bucket=bucket, Key=key)  # type: ignore[union-attr]

        with dest_path.open("wb") as handle:
            for chunk in response["Body"].iter_chunks(chunk_size=65536):
                handle.write(chunk)

        return RemoteObjectMeta(
            uri=f"s3://{bucket}/{key}",
            size=response["ContentLength"],
            etag=response.get("ETag", "").strip('"'),
            version_id=response.get("VersionId"),
        )

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        """Upload a local file to remote storage.

        Parameters
        ----------
        src_path : Path
            Local file to upload.
        bucket : str
            Destination bucket.
        key : str
            Destination key.

        Returns
        -------
        RemoteObjectMeta
        """
        client = self._get_client()
        client.upload_file(str(src_path), bucket, key)  # type: ignore[union-attr]

        # Fetch metadata for the uploaded object.
        return self.head(bucket=bucket, key=key)

    def list_prefix(self, *, bucket: str, prefix: str) -> list[RemoteObjectMeta]:
        """List objects under a key prefix.

        Parameters
        ----------
        bucket : str
            Bucket name.
        prefix : str
            Key prefix to list.

        Returns
        -------
        list[RemoteObjectMeta]
        """
        client = self._get_client()
        paginator = client.get_paginator("list_objects_v2")  # type: ignore[union-attr]
        results: list[RemoteObjectMeta] = []

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                results.append(
                    RemoteObjectMeta(
                        uri=f"s3://{bucket}/{obj['Key']}",
                        size=obj["Size"],
                        etag=obj.get("ETag", "").strip('"'),
                    )
                )

        return results
