"""Remote object references for workflow inputs.

Provides ``remote_file()`` and ``remote_folder()`` constructors that allow
workflow authors to declare remote object storage URIs as task inputs.
The evaluator materializes these references into local paths before task
execution.

A URI's scheme selects the object store plugin that parses and serves it:
``s3://`` comes from ginkgo-aws, ``gs://`` from ginkgo-gcp and ``oci://`` from
ginkgo-oci. Core knows no scheme by name.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlparse

_VALID_ACCESS_MODES = frozenset({"stage", "fuse"})


@dataclass(frozen=True, kw_only=True)
class RemoteRef:
    """Base for remote object references.

    Parameters
    ----------
    uri : str
        Full URI (e.g. ``s3://bucket/key``).
    scheme : str
        URI scheme, e.g. ``"s3"``.
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
    access : str | None
        Preferred access mode (``"stage"`` or ``"fuse"``). Participates in
        ``__repr__`` but is excluded from equality / hashing so toggling
        access does not invalidate cache entries.
    """

    uri: str
    scheme: str
    bucket: str
    key: str
    namespace: str | None = None
    version_id: str | None = None
    access: str | None = field(default=None, compare=False)


@dataclass(frozen=True, kw_only=True)
class RemoteFileRef(RemoteRef):
    """Remote reference to a single file."""


@dataclass(frozen=True, kw_only=True)
class RemoteFolderRef(RemoteRef):
    """Remote reference to a directory prefix."""


def remote_file(
    uri: str,
    *,
    version_id: str | None = None,
    access: str | None = None,
) -> RemoteFileRef:
    """Construct a remote file reference from a URI.

    Parameters
    ----------
    uri : str
        Remote URI (e.g. ``s3://bucket/key`` or ``oci://namespace/bucket/key``).
    version_id : str | None
        Optional version ID for immutable pinning.
    access : str | None
        Preferred access mode (``"stage"`` or ``"fuse"``). ``None`` defers
        the choice to the configured default / policy resolver.

    Returns
    -------
    RemoteFileRef

    Raises
    ------
    PluginNotFoundError
        If no installed plugin provides an object store for the URI's scheme.
    ValueError
        If the URI is malformed, or ``access`` is not one of ``"stage"`` /
        ``"fuse"``.
    """
    location = parse_remote_uri(uri)
    _validate_access(access)
    return RemoteFileRef(
        uri=uri,
        scheme=location.scheme,
        bucket=location.bucket,
        key=location.key,
        namespace=location.namespace,
        version_id=version_id,
        access=access,
    )


def remote_folder(
    uri: str,
    *,
    version_id: str | None = None,
    access: str | None = None,
) -> RemoteFolderRef:
    """Construct a remote folder reference from a URI.

    Parameters
    ----------
    uri : str
        Remote URI pointing to a prefix (e.g. ``s3://bucket/prefix/``).
    version_id : str | None
        Optional version ID for immutable pinning.
    access : str | None
        Preferred access mode (``"stage"`` or ``"fuse"``). ``None`` defers
        the choice to the configured default / policy resolver.

    Returns
    -------
    RemoteFolderRef

    Raises
    ------
    PluginNotFoundError
        If no installed plugin provides an object store for the URI's scheme.
    ValueError
        If the URI is malformed, or ``access`` is not one of ``"stage"`` /
        ``"fuse"``.
    """
    location = parse_remote_uri(uri)
    _validate_access(access)
    return RemoteFolderRef(
        uri=uri,
        scheme=location.scheme,
        bucket=location.bucket,
        key=location.key,
        namespace=location.namespace,
        version_id=version_id,
        access=access,
    )


def _validate_access(access: str | None) -> None:
    """Raise ``ValueError`` if ``access`` is not a supported mode."""
    if access is None:
        return
    if access not in _VALID_ACCESS_MODES:
        raise ValueError(
            f"Unsupported access mode {access!r}. "
            f"Supported modes: {', '.join(sorted(_VALID_ACCESS_MODES))}"
        )


@dataclass(frozen=True, kw_only=True)
class RemoteLocation:
    """Where a remote URI points: the store, the bucket and the object key.

    Parameters
    ----------
    scheme : str
        URI scheme, e.g. ``"s3"``.
    bucket : str
        Bucket authority the store addresses. A store may normalise it (OCI
        uses ``"<bucket>@<namespace>"``).
    key : str
        Object key or prefix within the bucket.
    namespace : str | None
        Store-specific namespace, when the scheme has one.
    """

    scheme: str
    bucket: str
    key: str
    namespace: str | None = None


def uri_scheme(value: str) -> str | None:
    """Return the lower-cased scheme of ``scheme://...``, or ``None`` if there is none."""
    scheme, separator, _ = value.partition("://")
    return scheme.lower() if separator and scheme else None


def is_remote_uri(value: str) -> bool:
    """Check whether a string is a URI for an object store.

    A scheme counts when an installed plugin provides a store for it, or a
    first-party plugin would: ``s3://`` without ginkgo-aws is still a remote
    URI, so using it fails with the plugin to install rather than being
    mistaken for a local path.

    Parameters
    ----------
    value : str
        String to check.

    Returns
    -------
    bool
    """
    from ginkgo.plugins.api import ObjectStoreType
    from ginkgo.plugins.registry import first_party_provider, plugin_registry

    scheme = uri_scheme(value) if isinstance(value, str) else None
    if scheme is None:
        return False
    if scheme in plugin_registry().keys(ObjectStoreType):
        return True
    return first_party_provider(ObjectStoreType, scheme) is not None


def parse_remote_uri(uri: str) -> RemoteLocation:
    """Parse a remote URI with the object store plugin its scheme selects.

    Raises
    ------
    PluginNotFoundError
        If no installed plugin provides an object store for the scheme.
    ValueError
        If the URI has no scheme or is malformed for its store.
    """
    from ginkgo.plugins.api import ObjectStoreType
    from ginkgo.plugins.registry import plugin_registry

    scheme = uri_scheme(uri)
    if scheme is None:
        raise ValueError(f"Remote URI must have the form scheme://...: {uri!r}")
    return plugin_registry().get(ObjectStoreType, scheme).parse_uri(uri)


def parse_bucket_key_uri(uri: str) -> RemoteLocation:
    """Parse ``scheme://bucket/key``, the layout S3 and GCS share.

    An object store plugin whose URIs have this shape passes this as its
    ``parse_uri``.

    Raises
    ------
    ValueError
        If the bucket or key is missing.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if not parsed.netloc:
        raise ValueError(f"{scheme}:// URI missing bucket: {uri!r}")
    key = parsed.path.lstrip("/")
    if not key:
        raise ValueError(f"{scheme}:// URI missing key: {uri!r}")
    return RemoteLocation(scheme=scheme, bucket=parsed.netloc, key=key)
