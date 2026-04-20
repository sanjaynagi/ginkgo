"""Unit tests for the Phase 9 remote-input-access layer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ginkgo.core.remote import (
    RemoteFileRef,
    RemoteFolderRef,
    remote_file,
    remote_folder,
)
from ginkgo.remote.access import (
    AccessConfig,
    AccessStats,
    MountedAccess,
    PerInputStats,
    StagedAccess,
    TaskAccessPolicy,
    decode_fuse_ref,
    encode_fuse_ref,
    is_fuse_ref,
    resolve_access,
)
from ginkgo.remote.access.drivers.base import (
    DriverUnavailableError,
    MountFailedError,
    MountSpec,
)


# ---------------------------------------------------------------------------
# Ref access field
# ---------------------------------------------------------------------------


class TestRefAccessField:
    def test_default_access_is_none(self) -> None:
        ref = remote_file("s3://b/k.txt")
        assert ref.access is None

    def test_constructor_sets_access(self) -> None:
        ref = remote_file("s3://b/k.txt", access="fuse")
        assert ref.access == "fuse"

    def test_folder_constructor_sets_access(self) -> None:
        ref = remote_folder("s3://b/prefix/", access="stage")
        assert ref.access == "stage"

    def test_invalid_access_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported access mode"):
            remote_file("s3://b/k.txt", access="magic")

    def test_access_excluded_from_hash(self) -> None:
        fused = remote_file("s3://b/k.txt", access="fuse")
        staged = remote_file("s3://b/k.txt", access="stage")
        bare = remote_file("s3://b/k.txt")
        # Equality / hash must only consider identity fields so cache entries
        # do not invalidate when streaming is toggled.
        assert fused == staged == bare
        assert hash(fused) == hash(staged) == hash(bare)

    def test_access_shows_in_repr(self) -> None:
        ref = remote_file("s3://b/k.txt", access="fuse")
        assert "fuse" in repr(ref)


# ---------------------------------------------------------------------------
# Policy resolver
# ---------------------------------------------------------------------------


class TestResolveAccess:
    def test_explicit_ref_access_wins(self) -> None:
        ref = remote_file("s3://b/k.txt", access="fuse")
        assert resolve_access(ref=ref) == "fuse"

    def test_explicit_stage_beats_task_fuse(self) -> None:
        ref = remote_file("s3://b/k.txt", access="stage")
        policy = TaskAccessPolicy(remote_input_access="fuse")
        assert resolve_access(ref=ref, task_policy=policy) == "stage"

    def test_task_default_applies_when_ref_is_bare(self) -> None:
        ref = remote_file("s3://b/k.txt")
        policy = TaskAccessPolicy(remote_input_access="fuse")
        assert resolve_access(ref=ref, task_policy=policy) == "fuse"

    def test_config_default_applies(self) -> None:
        ref = remote_file("s3://b/k.txt")
        config = AccessConfig(default="stage")
        assert resolve_access(ref=ref, config=config) == "stage"

    def test_pattern_default_applies(self) -> None:
        ref = remote_file("s3://b/sample.fastq.gz")
        config = AccessConfig(
            default="stage",
            pattern_defaults=(("*.fastq.gz", "fuse"),),
        )
        assert resolve_access(ref=ref, config=config) == "fuse"

    def test_auto_enable_heuristic(self) -> None:
        ref = remote_file("s3://b/huge.fastq")
        config = AccessConfig(auto_fuse=True, auto_fuse_min_bytes=1024)
        assert (
            resolve_access(
                ref=ref,
                config=config,
                known_size=2048,
            )
            == "fuse (auto)"
        )

    def test_auto_enable_gated_on_size(self) -> None:
        ref = remote_file("s3://b/small.txt")
        config = AccessConfig(auto_fuse=True, auto_fuse_min_bytes=1024)
        assert resolve_access(ref=ref, config=config, known_size=100) == "stage"

    def test_auto_enable_blocked_by_streaming_incompatible(self) -> None:
        ref = remote_file("s3://b/huge.fastq")
        config = AccessConfig(auto_fuse=True, auto_fuse_min_bytes=1024)
        task_policy = TaskAccessPolicy(streaming_compatible=False)
        assert (
            resolve_access(
                ref=ref,
                config=config,
                task_policy=task_policy,
                known_size=2048,
            )
            == "stage"
        )

    def test_driver_missing_falls_back(self) -> None:
        ref = remote_file("s3://b/k.txt", access="fuse")
        assert resolve_access(ref=ref, driver_available=False) == "stage"

    def test_streaming_incompatible_blocks_explicit_fuse(self) -> None:
        ref = remote_file("s3://b/k.txt", access="fuse")
        policy = TaskAccessPolicy(streaming_compatible=False)
        assert resolve_access(ref=ref, task_policy=policy) == "stage"


class TestAccessConfig:
    def test_from_mapping_parses_patterns(self) -> None:
        config = AccessConfig.from_mapping(
            {
                "default": "stage",
                "auto_fuse": True,
                "auto_fuse_min_bytes": 1000,
                "default_for_pattern": [
                    {"glob": "*.bam", "access": "fuse"},
                    {"glob": "*.bai", "access": "stage"},
                ],
            }
        )
        assert config.default == "stage"
        assert config.auto_fuse is True
        assert config.auto_fuse_min_bytes == 1000
        assert config.pattern_defaults == (
            ("*.bam", "fuse"),
            ("*.bai", "stage"),
        )

    def test_from_mapping_handles_none(self) -> None:
        assert AccessConfig.from_mapping(None).default == "stage"


# ---------------------------------------------------------------------------
# Fuse ref encoding
# ---------------------------------------------------------------------------


class TestFuseRefEncoding:
    def test_encode_then_decode_file(self) -> None:
        ref = remote_file("s3://b/k.bam", access="fuse", version_id="v1")
        encoded = encode_fuse_ref(ref=ref, policy="fuse")
        assert is_fuse_ref(encoded)
        # Survives JSON round-trip.
        round_tripped = json.loads(json.dumps(encoded))
        decoded, policy = decode_fuse_ref(round_tripped)
        assert isinstance(decoded, RemoteFileRef)
        assert decoded.uri == ref.uri
        assert decoded.version_id == "v1"
        assert policy == "fuse"

    def test_encode_then_decode_folder(self) -> None:
        ref = remote_folder("gs://b/prefix/", access="fuse")
        encoded = encode_fuse_ref(ref=ref, policy="fuse (auto)")
        decoded, policy = decode_fuse_ref(encoded)
        assert isinstance(decoded, RemoteFolderRef)
        assert decoded.bucket == "b"
        assert policy == "fuse (auto)"

    def test_non_fuse_dict_is_not_recognised(self) -> None:
        assert is_fuse_ref({"__ginkgo_type__": "file", "value": "/x"}) is False
        assert is_fuse_ref("s3://b/k") is False


# ---------------------------------------------------------------------------
# AccessStats + StagedAccess
# ---------------------------------------------------------------------------


class TestStagedAccess:
    def test_materialize_file_records_bytes(self, tmp_path: Path) -> None:
        src = tmp_path / "staged.txt"
        src.write_text("hello world")

        class _FakeCache:
            def stage_file(self, *, ref: RemoteFileRef) -> Path:  # noqa: ARG002
                return src

            def stage_folder(self, *, ref: RemoteFolderRef) -> Path:  # noqa: ARG002
                raise NotImplementedError

        strategy = StagedAccess(cache=_FakeCache(), policy="stage")
        ref = remote_file("s3://b/k.txt")
        path = strategy.materialize_file(ref=ref)
        assert path == src
        stats = strategy.stats()
        assert stats.policy == "stage"
        assert stats.per_input[ref.uri].bytes_read == len("hello world")

    def test_materialize_folder_records_tree_size(self, tmp_path: Path) -> None:
        staged = tmp_path / "folder"
        staged.mkdir()
        (staged / "a.txt").write_text("abc")
        (staged / "b.txt").write_text("defg")

        class _FakeCache:
            def stage_folder(self, *, ref: RemoteFolderRef) -> Path:  # noqa: ARG002
                return staged

            def stage_file(self, *, ref: RemoteFileRef) -> Path:  # noqa: ARG002
                raise NotImplementedError

        strategy = StagedAccess(cache=_FakeCache())
        ref = remote_folder("s3://b/prefix/")
        path = strategy.materialize_folder(ref=ref)
        assert path == staged
        stats = strategy.stats()
        assert stats.per_input[ref.uri].bytes_read == 3 + 4


class TestAccessStatsSerialization:
    def test_to_dict_round_trip(self) -> None:
        stats = AccessStats(policy="fuse")
        stats.per_input["s3://b/k"] = PerInputStats(
            uri="s3://b/k", bytes_read=10, range_requests=2, cache_hits=1, cache_bytes=4
        )
        payload = stats.to_dict()
        decoded = json.loads(json.dumps(payload))
        assert decoded["policy"] == "fuse"
        assert decoded["per_input"]["s3://b/k"]["bytes_read"] == 10


# ---------------------------------------------------------------------------
# MountedAccess with a stub driver
# ---------------------------------------------------------------------------


class _StubDriver:
    name = "stub"

    def __init__(self) -> None:
        self.mounted: list[MountSpec] = []
        self.unmounted: list[Path] = []

    def health_check(self) -> None:
        return

    def mount(self, *, spec: MountSpec) -> int:
        spec.mount_point.mkdir(parents=True, exist_ok=True)
        self.mounted.append(spec)
        # Seed mount content so ``materialize_file`` returns something plausible.
        (spec.mount_point / "file.txt").write_text("streamed")
        return 0

    def unmount(self, *, mount_point: Path) -> None:
        self.unmounted.append(mount_point)


class TestMountedAccess:
    def test_materialize_file_mounts_bucket_once(self, tmp_path: Path) -> None:
        driver = _StubDriver()
        strategy = MountedAccess(
            mount_root=tmp_path / "mounts",
            cache_root=tmp_path / "cache",
            driver_factory=lambda scheme: driver,  # noqa: ARG005
        )
        ref1 = remote_file("s3://bucket/path/one.txt", access="fuse")
        ref2 = remote_file("s3://bucket/path/two.txt", access="fuse")
        path1 = strategy.materialize_file(ref=ref1)
        path2 = strategy.materialize_file(ref=ref2)

        assert path1 == tmp_path / "mounts" / "s3" / "bucket" / "path/one.txt"
        assert path2 == tmp_path / "mounts" / "s3" / "bucket" / "path/two.txt"
        # One mount, two materialisations.
        assert len(driver.mounted) == 1
        assert driver.mounted[0].bucket == "bucket"
        assert driver.mounted[0].scheme == "s3"

        strategy.close()
        assert len(driver.unmounted) == 1

    def test_close_records_fallback_reason_on_unmount_failure(self, tmp_path: Path) -> None:
        class _FailingUnmountDriver(_StubDriver):
            def unmount(self, *, mount_point: Path) -> None:  # noqa: ARG002
                raise MountFailedError("boom")

        driver = _FailingUnmountDriver()
        strategy = MountedAccess(
            mount_root=tmp_path / "mounts",
            cache_root=tmp_path / "cache",
            driver_factory=lambda scheme: driver,  # noqa: ARG005
        )
        ref = remote_file("s3://bucket/path/one.txt", access="fuse")
        strategy.materialize_file(ref=ref)
        strategy.close()
        stats = strategy.stats()
        assert stats.fallback_reason is not None
        assert "boom" in stats.fallback_reason

    def test_unavailable_driver_raises(self, tmp_path: Path) -> None:
        class _DeadDriver(_StubDriver):
            def health_check(self) -> None:
                raise DriverUnavailableError("missing")

        strategy = MountedAccess(
            mount_root=tmp_path / "mounts",
            cache_root=tmp_path / "cache",
            driver_factory=lambda scheme: _DeadDriver(),  # noqa: ARG005
        )
        ref = remote_file("s3://bucket/path/one.txt", access="fuse")
        with pytest.raises(DriverUnavailableError):
            strategy.materialize_file(ref=ref)


# ---------------------------------------------------------------------------
# Worker-side hydration (fuse markers → file paths)
# ---------------------------------------------------------------------------


class TestWorkerHydration:
    def test_markers_become_file_values(self, tmp_path: Path) -> None:
        from ginkgo.core.types import file
        from ginkgo.remote.access.worker_hydration import hydrate_fuse_refs

        ref = remote_file("s3://bucket/path/one.txt", access="fuse")
        marker = encode_fuse_ref(ref=ref, policy="fuse")

        driver = _StubDriver()
        mounted = MountedAccess(
            mount_root=tmp_path / "mounts",
            cache_root=tmp_path / "cache",
            driver_factory=lambda scheme: driver,  # noqa: ARG005
        )
        args = {"fastq": marker, "threads": 4}
        rewritten, m = hydrate_fuse_refs(args=args, mounted_access=mounted)
        assert m is mounted
        assert isinstance(rewritten["fastq"], file)
        assert rewritten["fastq"].endswith("path/one.txt")
        assert rewritten["threads"] == 4

    def test_markers_in_nested_containers(self, tmp_path: Path) -> None:
        from ginkgo.remote.access.worker_hydration import hydrate_fuse_refs

        ref_a = remote_file("s3://bucket/a.txt", access="fuse")
        ref_b = remote_file("s3://bucket/b.txt", access="fuse")
        marker_a = encode_fuse_ref(ref=ref_a, policy="fuse")
        marker_b = encode_fuse_ref(ref=ref_b, policy="fuse")

        driver = _StubDriver()
        mounted = MountedAccess(
            mount_root=tmp_path / "mounts",
            cache_root=tmp_path / "cache",
            driver_factory=lambda scheme: driver,  # noqa: ARG005
        )
        args = {"files": [marker_a, marker_b]}
        rewritten, _ = hydrate_fuse_refs(args=args, mounted_access=mounted)
        assert len(rewritten["files"]) == 2
        assert all("bucket" in str(p) for p in rewritten["files"])

    def test_mount_failure_falls_back_to_staged(self, tmp_path: Path) -> None:
        from ginkgo.core.types import file
        from ginkgo.remote.access.staged import StagedAccess as StagedStrategy
        from ginkgo.remote.access.worker_hydration import hydrate_fuse_refs

        class _FailingDriver:
            name = "failing"

            def health_check(self) -> None:
                raise DriverUnavailableError("missing driver")

            def mount(self, *, spec: MountSpec) -> int:  # noqa: ARG002
                raise RuntimeError("never called")

            def unmount(self, *, mount_point: Path) -> None:  # noqa: ARG002
                return

        mounted = MountedAccess(
            mount_root=tmp_path / "mounts",
            cache_root=tmp_path / "cache",
            driver_factory=lambda scheme: _FailingDriver(),  # noqa: ARG005
        )

        staged_path = tmp_path / "staged.txt"
        staged_path.write_text("content")

        class _FakeCache:
            def stage_file(self, *, ref: RemoteFileRef) -> Path:  # noqa: ARG002
                return staged_path

            def stage_folder(self, *, ref: RemoteFolderRef) -> Path:  # noqa: ARG002
                raise NotImplementedError

        fallback = StagedStrategy(cache=_FakeCache(), policy="stage (fallback)")
        ref = remote_file("s3://bucket/one.txt", access="fuse")
        marker = encode_fuse_ref(ref=ref, policy="fuse")
        rewritten, m = hydrate_fuse_refs(
            args={"in": marker},
            mounted_access=mounted,
            fallback_access=fallback,
        )
        assert rewritten["in"] == file(str(staged_path))


# ---------------------------------------------------------------------------
# Executor fuse detection
# ---------------------------------------------------------------------------


class TestExecutorFuseDetection:
    def test_kubernetes_fuse_detection(self) -> None:
        from ginkgo.remote.kubernetes import _payload_requires_fuse

        ref = remote_file("s3://b/k.bam", access="fuse")
        marker = encode_fuse_ref(ref=ref, policy="fuse")

        assert _payload_requires_fuse({"args": {}}) is False
        assert _payload_requires_fuse({"args": {"fastq": marker}}) is True
        assert _payload_requires_fuse({"args": {"files": [marker]}}) is True

    def test_gcp_batch_fuse_detection(self) -> None:
        from ginkgo.remote.gcp_batch import _payload_requires_fuse

        ref = remote_file("s3://b/k.bam", access="fuse")
        marker = encode_fuse_ref(ref=ref, policy="fuse")
        assert _payload_requires_fuse({"args": {"fastq": marker}}) is True
        assert _payload_requires_fuse({"args": {"raw": "plain"}}) is False


# ---------------------------------------------------------------------------
# RemoteStager integration
# ---------------------------------------------------------------------------


@dataclass
class _MockTaskDef:
    """Minimal TaskDef stand-in for RemoteStager routing tests."""

    remote: bool = True
    gpu: int = 0
    remote_input_access: str | None = None
    streaming_compatible: bool = True
    fuse_prefetch: tuple = ()
    type_hints: dict[str, Any] = None  # type: ignore[assignment]
    signature: Any = None

    def __post_init__(self) -> None:
        from inspect import Parameter, Signature

        if self.type_hints is None:
            self.type_hints = {}
        if self.signature is None:
            self.signature = Signature(
                parameters=[
                    Parameter("data", Parameter.POSITIONAL_OR_KEYWORD),
                ]
            )


class TestCacheKeyStability:
    """Fuse markers must hash identically to the equivalent RemoteRef."""

    def test_marker_hashes_same_as_ref(self, tmp_path: Path) -> None:
        from ginkgo.runtime.caching.cache import CacheStore

        store = CacheStore(root=tmp_path)
        ref = remote_file("s3://bucket/key.bam", version_id="v42")
        marker = encode_fuse_ref(
            ref=remote_file("s3://bucket/key.bam", version_id="v42", access="fuse"),
            policy="fuse",
        )
        ref_hash = store._hash_value(annotation=file, value=ref)
        marker_hash = store._hash_value(annotation=file, value=marker)
        assert ref_hash == marker_hash

    def test_marker_stat_value_same_as_ref(self, tmp_path: Path) -> None:
        from ginkgo.runtime.caching.cache import CacheStore

        store = CacheStore(root=tmp_path)
        ref = remote_file("s3://bucket/key.bam", version_id="v42")
        marker = encode_fuse_ref(
            ref=remote_file("s3://bucket/key.bam", version_id="v42", access="fuse"),
            policy="fuse",
        )
        assert store._stat_value(annotation=file, value=ref) == store._stat_value(
            annotation=file, value=marker
        )


# Put ``file`` in scope for the tests above. Imported lazily so the module
# remains importable under restricted environments that skip the codec.
from ginkgo.core.types import file  # noqa: E402


class TestRemoteStagerFuseRouting:
    def test_fuse_ref_becomes_marker(self, tmp_path: Path) -> None:
        from ginkgo.remote.access.resolver import AccessConfig
        from ginkgo.runtime.remote_input_resolver import RemoteStager

        stager = RemoteStager(access_config=AccessConfig(default="stage"))
        task_def = _MockTaskDef(
            type_hints={"data": RemoteFileRef},
            signature=None,
        )
        from inspect import Parameter, Signature

        task_def.signature = Signature(
            parameters=[Parameter("data", Parameter.POSITIONAL_OR_KEYWORD)]
        )
        ref = remote_file("s3://bucket/k.txt", access="fuse")
        staged = stager.stage_remote_refs(
            task_def=task_def,
            resolved_args={"data": ref},
        )
        assert is_fuse_ref(staged["data"])
        assert staged["data"]["uri"] == "s3://bucket/k.txt"

    def test_non_remote_task_forces_stage(self, tmp_path: Path, monkeypatch) -> None:
        from ginkgo.remote.access.resolver import AccessConfig
        from ginkgo.runtime.remote_input_resolver import RemoteStager

        # The stager must route through StagingCache.stage_file; patch it to
        # avoid a real download.
        from ginkgo.remote import staging

        class _FakeCache:
            def stage_file(self, *, ref: RemoteFileRef) -> Path:  # noqa: ARG002
                return tmp_path / "staged.bin"

            def stage_folder(self, *, ref: RemoteFolderRef) -> Path:  # noqa: ARG002
                raise NotImplementedError

        (tmp_path / "staged.bin").write_bytes(b"")
        monkeypatch.setattr(staging, "StagingCache", lambda: _FakeCache())

        stager = RemoteStager(access_config=AccessConfig(default="stage"))
        task_def = _MockTaskDef(
            remote=False,
            type_hints={"data": RemoteFileRef},
            signature=None,
        )
        from inspect import Parameter, Signature

        task_def.signature = Signature(
            parameters=[Parameter("data", Parameter.POSITIONAL_OR_KEYWORD)]
        )
        ref = remote_file("s3://bucket/k.txt", access="fuse")
        staged = stager.stage_remote_refs(
            task_def=task_def,
            resolved_args={"data": ref},
        )
        # Local task ignores fuse; output is a staged local path (file instance).
        from ginkgo.core.types import file as file_type

        assert isinstance(staged["data"], file_type)
        assert not is_fuse_ref(staged["data"])
