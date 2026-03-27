"""Remote object references for workflow inputs.

Provides ``remote_file()`` and ``remote_folder()`` constructors that allow
workflow authors to declare remote object storage URIs as task inputs.
The evaluator materializes these references into local paths before task
execution.

Supported URI schemes:

- ``s3://bucket/key`` — AWS S3
- ``oci://namespace/bucket/key`` — Oracle Cloud Infrastructure Object Storage
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

_SUPPORTED_SCHEMES = frozenset({"s3", "oci"})


@dataclass(frozen=True, kw_only=True)
class RemoteRef:
    """Base for remote object references.

    Parameters
    ----------
    uri : str
        Full URI (e.g. ``s3://bucket/key``).
    scheme : str
        URI scheme (``"s3"`` or ``"oci"``).
    bucket : str
        Bucket/container authority used by the storage backend.
        For S3 this is the bucket name. For OCI this is normalized to
        ``"<bucket>@<namespace>"``.
    key : str
        Object key within the bucket.
    namespace : str | None
        Optional OCI namespace. ``None`` for S3.
    version_id : str | None
        Optional immutable version pin.
    """

    uri: str
    scheme: str
    bucket: str
    key: str
    namespace: str | None = None
    version_id: str | None = None


@dataclass(frozen=True, kw_only=True)
class RemoteFileRef(RemoteRef):
    """Remote reference to a single file."""


@dataclass(frozen=True, kw_only=True)
class RemoteFolderRef(RemoteRef):
    """Remote reference to a directory prefix."""


def remote_file(uri: str, *, version_id: str | None = None) -> RemoteFileRef:
    """Construct a remote file reference from a URI.

    Parameters
    ----------
    uri : str
        Remote URI (e.g. ``s3://bucket/key`` or ``oci://namespace/bucket/key``).
    version_id : str | None
        Optional version ID for immutable pinning.

    Returns
    -------
    RemoteFileRef

    Raises
    ------
    ValueError
        If the URI scheme is unsupported or the URI is malformed.
    """
    parsed = _parse_uri(uri)
    return RemoteFileRef(
        uri=uri,
        scheme=parsed["scheme"],
        bucket=parsed["bucket"],
        key=parsed["key"],
        namespace=parsed.get("namespace"),
        version_id=version_id,
    )


def remote_folder(uri: str, *, version_id: str | None = None) -> RemoteFolderRef:
    """Construct a remote folder reference from a URI.

    Parameters
    ----------
    uri : str
        Remote URI pointing to a prefix (e.g. ``s3://bucket/prefix/``).
    version_id : str | None
        Optional version ID for immutable pinning.

    Returns
    -------
    RemoteFolderRef

    Raises
    ------
    ValueError
        If the URI scheme is unsupported or the URI is malformed.
    """
    parsed = _parse_uri(uri)
    return RemoteFolderRef(
        uri=uri,
        scheme=parsed["scheme"],
        bucket=parsed["bucket"],
        key=parsed["key"],
        namespace=parsed.get("namespace"),
        version_id=version_id,
    )


def is_remote_uri(value: str) -> bool:
    """Check whether a string looks like a supported remote URI.

    Parameters
    ----------
    value : str
        String to check.

    Returns
    -------
    bool
        ``True`` if the string starts with a supported scheme.
    """
    try:
        scheme = value.split("://", 1)[0].lower()
    except (AttributeError, IndexError):
        return False
    return scheme in _SUPPORTED_SCHEMES


def _parse_uri(uri: str) -> dict[str, str]:
    """Parse a remote URI into scheme, bucket, and key components.

    Parameters
    ----------
    uri : str
        Remote URI to parse.

    Returns
    -------
    dict[str, str]
        Parsed components with keys ``scheme``, ``bucket``, ``key``.

    Raises
    ------
    ValueError
        If the scheme is unsupported or the URI is malformed.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme not in _SUPPORTED_SCHEMES:
        raise ValueError(
            f"Unsupported remote URI scheme {scheme!r} in {uri!r}. "
            f"Supported schemes: {', '.join(sorted(_SUPPORTED_SCHEMES))}"
        )

    if scheme == "s3":
        return _parse_s3_uri(parsed, uri)
    if scheme == "oci":
        return _parse_oci_uri(parsed, uri)

    raise ValueError(f"Unsupported scheme: {scheme!r}")


def _parse_s3_uri(parsed: object, uri: str) -> dict[str, str]:
    """Parse an S3 URI: ``s3://bucket/key``.

    Parameters
    ----------
    parsed : ParseResult
        Result from ``urlparse``.
    uri : str
        Original URI for error messages.

    Returns
    -------
    dict[str, str]
    """
    bucket = parsed.netloc  # type: ignore[union-attr]
    if not bucket:
        raise ValueError(f"S3 URI missing bucket: {uri!r}")

    key = parsed.path.lstrip("/")  # type: ignore[union-attr]
    if not key:
        raise ValueError(f"S3 URI missing key: {uri!r}")

    return {"scheme": "s3", "bucket": bucket, "key": key}


def _parse_oci_uri(parsed: object, uri: str) -> dict[str, str]:
    """Parse an OCI Object Storage URI.

    Supports both of these forms:

    - ``oci://namespace/bucket/key``
    - ``oci://bucket@namespace/key``

    Parameters
    ----------
    parsed : ParseResult
        Result from ``urlparse``.
    uri : str
        Original URI for error messages.

    Returns
    -------
    dict[str, str]
    """
    authority = parsed.netloc  # type: ignore[union-attr]
    if not authority:
        raise ValueError(f"OCI URI missing namespace or bucket: {uri!r}")

    key = parsed.path.lstrip("/")  # type: ignore[union-attr]
    if not key:
        raise ValueError(
            "OCI URI must have the form oci://namespace/bucket/key "
            f"or oci://bucket@namespace/key: {uri!r}"
        )

    if "@" in authority:
        bucket, namespace = authority.split("@", 1)
        if not bucket or not namespace:
            raise ValueError(f"OCI URI must have the form oci://bucket@namespace/key: {uri!r}")
        return {
            "scheme": "oci",
            "bucket": f"{bucket}@{namespace}",
            "key": key,
            "namespace": namespace,
        }

    path_parts = key.split("/", 1)
    if len(path_parts) < 2 or not path_parts[1]:
        raise ValueError(
            "OCI URI must have the form oci://namespace/bucket/key "
            f"or oci://bucket@namespace/key: {uri!r}"
        )

    namespace = authority
    bucket = path_parts[0]
    object_key = path_parts[1]
    return {
        "scheme": "oci",
        "bucket": f"{bucket}@{namespace}",
        "key": object_key,
        "namespace": namespace,
    }
