"""The s3:// object store contributed by ginkgo-aws."""

from __future__ import annotations

import pytest

from ginkgo.core.remote import remote_file
from ginkgo.plugins.api import ObjectStoreContext
from ginkgo.remote.fsspec_backends import FsspecBackend
from ginkgo.remote.resolve import resolve_backend
from ginkgo_aws.store import S3FileSystemBackend, build_s3_store


def test_the_installed_plugin_resolves_s3() -> None:
    backend = resolve_backend("s3")

    assert isinstance(backend, S3FileSystemBackend)
    assert isinstance(backend, FsspecBackend)
    assert backend._scheme == "s3"


def test_s3_uris_parse_as_bucket_and_key() -> None:
    ref = remote_file("s3://my-bucket/data/sample.txt")

    assert (ref.scheme, ref.bucket, ref.key) == ("s3", "my-bucket", "data/sample.txt")


def test_region_prefers_the_override_then_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-2")
    config = {"remote": {"region": "eu-west-1"}}

    from_env = build_s3_store(ObjectStoreContext(config=config))
    overridden = build_s3_store(ObjectStoreContext(config=config, region="ap-south-1"))

    assert isinstance(from_env, S3FileSystemBackend)
    assert isinstance(overridden, S3FileSystemBackend)
    assert from_env._region == "us-east-2"
    assert overridden._region == "ap-south-1"
