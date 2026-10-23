"""The ``oci://`` object store, on ``ocifs``, and its URI layout."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

from ginkgo.core.remote import RemoteLocation
from ginkgo.plugins.api import ObjectStoreContext
from ginkgo.remote.backend import ObjectStore
from ginkgo.remote.fsspec_backends import FsspecBackend
from ginkgo.remote.resolve import config_string, config_table, first_defined, resolve_backend

# How a store signs its requests, by the name ginkgo's config uses, with the
# ``iam_type`` ocifs takes for it.
AUTH_IAM_TYPES = {
    "api_key": "api_key",  # pragma: allowlist secret
    "oke_workload_identity": "oke_principal",
    "instance_principal": "instance_principal",
    "resource_principal": "resource_principal",
}


class OCIFileSystemBackend(FsspecBackend):
    """OCI Object Storage backend implemented with ``ocifs``.

    Parameters
    ----------
    auth : str
        How requests are signed: a key of :data:`AUTH_IAM_TYPES`. Only
        ``api_key`` reads a config file.
    config_path : str | None
        Path to the OCI config file, for ``api_key`` auth.
    profile : str | None
        OCI config profile name, for ``api_key`` auth.
    region : str | None
        OCI region override.
    """

    def __init__(
        self,
        *,
        auth: str = "api_key",
        config_path: str | None = None,
        profile: str | None = None,
        region: str | None = None,
    ) -> None:
        super().__init__(scheme="oci")
        if auth not in AUTH_IAM_TYPES:
            raise ValueError(
                f"Unknown OCI auth {auth!r}; expected one of {', '.join(sorted(AUTH_IAM_TYPES))}"
            )
        self._auth = auth
        self._config_path = config_path
        self._profile = profile
        self._region = region

    def filesystem(self) -> Any:
        """Return the ``ocifs.OCIFileSystem`` this store reads and writes through."""
        return self._get_filesystem()

    def _get_filesystem(self) -> Any:
        if self._filesystem is not None:
            return self._filesystem

        # Imported on first use: the plugin module loads on every CLI run.
        import ocifs

        kwargs: dict[str, Any] = {"iam_type": AUTH_IAM_TYPES[self._auth]}
        if self._auth == "api_key":
            if self._config_path is not None:
                kwargs["config"] = os.path.expanduser(self._config_path)
            if self._profile is not None:
                kwargs["profile"] = self._profile
        if self._region is not None:
            kwargs["region"] = self._region

        self._filesystem = ocifs.OCIFileSystem(**kwargs)
        return self._filesystem


def filesystem() -> Any:
    """Return an ``ocifs.OCIFileSystem`` signed the way the ``oci://`` store is.

    For workflow code that reads or writes OCI Object Storage itself, such as a
    task uploading its results to a staging prefix, so it uses the auth, profile
    and region the run is configured with.

    Returns
    -------
    ocifs.OCIFileSystem
        The configured filesystem.
    """
    backend = resolve_backend("oci")
    assert isinstance(backend, OCIFileSystemBackend)
    return backend.filesystem()


def build_oci_store(context: ObjectStoreContext) -> ObjectStore:
    """Build the OCI store from ``GINKGO_REMOTE_OCI_*``/``OCI_*`` or ``[remote.oci]``."""
    remote = config_table(context.config, "remote")
    oci = config_table(remote, "oci")
    return OCIFileSystemBackend(
        auth=first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_AUTH"),
            config_string(oci, "auth"),
        )
        or "api_key",
        config_path=first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_CONFIG"),
            os.environ.get("OCI_CONFIG_FILE"),
            config_string(oci, "config"),
            "~/.oci/config",
        ),
        profile=first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_PROFILE"),
            os.environ.get("OCI_CONFIG_PROFILE"),
            config_string(oci, "profile"),
        ),
        region=context.region
        or first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_REGION"),
            os.environ.get("OCI_REGION"),
            config_string(oci, "region"),
            config_string(remote, "region"),
        ),
    )


def parse_oci_uri(uri: str) -> RemoteLocation:
    """Parse an OCI Object Storage URI.

    Both forms are accepted: ``oci://namespace/bucket/key`` and
    ``oci://bucket@namespace/key``. The bucket is normalised to
    ``"<bucket>@<namespace>"``, the authority ``ocifs`` addresses.

    Raises
    ------
    ValueError
        If the namespace, bucket or key is missing.
    """
    parsed = urlparse(uri)
    authority = parsed.netloc
    if not authority:
        raise ValueError(f"OCI URI missing namespace or bucket: {uri!r}")

    form = "oci://namespace/bucket/key or oci://bucket@namespace/key"
    key = parsed.path.lstrip("/")
    if not key:
        raise ValueError(f"OCI URI must have the form {form}: {uri!r}")

    if "@" in authority:
        bucket, namespace = authority.split("@", 1)
        if not bucket or not namespace:
            raise ValueError(f"OCI URI must have the form oci://bucket@namespace/key: {uri!r}")
        return RemoteLocation(
            scheme="oci", bucket=f"{bucket}@{namespace}", key=key, namespace=namespace
        )

    bucket, _, object_key = key.partition("/")
    if not object_key:
        raise ValueError(f"OCI URI must have the form {form}: {uri!r}")
    return RemoteLocation(
        scheme="oci", bucket=f"{bucket}@{authority}", key=object_key, namespace=authority
    )
