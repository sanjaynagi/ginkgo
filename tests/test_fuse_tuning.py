"""Tests for the gcsfuse performance defaults and fallback-warning wiring.

Covers two user-facing improvements to the remote-input-access layer:

1. The gcsfuse driver now assembles a tuned flag set (metadata caches,
   parallel range-read downloads, bigger chunks) rather than relying on
   gcsfuse's conservative defaults. Tests verify the flags appear in the
   subprocess ``cmd`` so a regression here would be caught immediately,
   and that caller-supplied ``extra_args`` still land on the command
   line so users can override.
2. ``_default_mount_root`` prefers a known NVMe/SSD mount when one is
   present — that is where gcsfuse's read-through cache earns its keep.
3. The evaluator emits a user-visible ``TaskNotice`` when a worker
   reports ``fallback_reason`` on its access stats, so a silent
   fuse→stage downgrade can't hide behind successful task completion.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ginkgo.remote.access.drivers.base import MountSpec
from ginkgo.remote.access.drivers.gcsfuse import GcsFuseDriver
from ginkgo.remote.access.mounted import _default_mount_root
from ginkgo.runtime.events import TaskNotice


class TestGcsFuseFlags:
    """Inspect the subprocess ``cmd`` built by ``GcsFuseDriver.mount``."""

    @pytest.fixture
    def captured_cmd(self, tmp_path: Path) -> list[str]:
        """Run ``mount`` with a stubbed subprocess and return the cmd."""
        driver = GcsFuseDriver()
        spec = MountSpec(
            scheme="gs",
            bucket="my-bucket",
            mount_point=tmp_path / "mnt",
            cache_dir=tmp_path / "cache",
            cache_max_bytes=2 * 1024 * 1024 * 1024,
        )
        calls: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            result = MagicMock()
            result.returncode = 0
            result.stderr = b""
            return result

        with patch("ginkgo.remote.access.drivers.gcsfuse.subprocess.run", side_effect=fake_run):
            with patch(
                "ginkgo.remote.access.drivers.gcsfuse.shutil.which",
                return_value="/usr/bin/gcsfuse",
            ):
                driver.health_check()
                driver.mount(spec=spec)
        assert calls, "driver.mount should invoke subprocess.run exactly once"
        return calls[0]

    def test_metadata_cache_ttl_set(self, captured_cmd: list[str]) -> None:
        assert "--stat-cache-ttl" in captured_cmd
        assert "--type-cache-ttl" in captured_cmd

    def test_parallel_downloads_enabled(self, captured_cmd: list[str]) -> None:
        assert "--file-cache-enable-parallel-downloads" in captured_cmd
        assert "--file-cache-parallel-downloads-per-file" in captured_cmd

    def test_chunk_size_configured(self, captured_cmd: list[str]) -> None:
        assert "--file-cache-download-chunk-size-mb" in captured_cmd

    def test_cache_size_limit_threads_through(self, captured_cmd: list[str]) -> None:
        # 2 GiB → 2048 MiB (spec above).
        assert "--file-cache-max-size-mb" in captured_cmd
        idx = captured_cmd.index("--file-cache-max-size-mb")
        assert captured_cmd[idx + 1] == "2048"

    def test_range_read_caching_enabled(self, captured_cmd: list[str]) -> None:
        assert "--file-cache-cache-file-for-range-read" in captured_cmd

    def test_extra_args_still_applied(self, tmp_path: Path) -> None:
        driver = GcsFuseDriver()
        spec = MountSpec(
            scheme="gs",
            bucket="b",
            mount_point=tmp_path / "mnt",
            extra_args=("--foo-override", "42"),
        )
        captured: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            captured.append(cmd)
            result = MagicMock()
            result.returncode = 0
            result.stderr = b""
            return result

        with patch("ginkgo.remote.access.drivers.gcsfuse.subprocess.run", side_effect=fake_run):
            with patch(
                "ginkgo.remote.access.drivers.gcsfuse.shutil.which",
                return_value="/usr/bin/gcsfuse",
            ):
                driver.mount(spec=spec)
        assert "--foo-override" in captured[0]
        # Extra args must land after the tuning flags so gcsfuse's
        # "last occurrence wins" rule allows users to override defaults.
        foo_idx = captured[0].index("--foo-override")
        ttl_idx = captured[0].index("--stat-cache-ttl")
        assert foo_idx > ttl_idx


class TestDefaultMountRoot:
    """NVMe-aware selection of the FUSE mount root."""

    def test_env_override_wins(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GINKGO_FUSE_ROOT", str(tmp_path / "custom"))
        assert _default_mount_root() == tmp_path / "custom"

    def test_prefers_nvme_when_writable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("GINKGO_FUSE_ROOT", raising=False)
        fake_nvme = tmp_path / "fake-ssd"
        fake_nvme.mkdir()

        # Pretend /mnt/disks/ssd exists and is writable by mapping the
        # first probed candidate to our tmp directory.
        real_is_dir = Path.is_dir
        real_access = os.access

        def fake_is_dir(self: Path) -> bool:
            if str(self) == "/mnt/disks/ssd":
                return True
            return real_is_dir(self)

        def fake_access(path, mode) -> bool:
            if str(path) == "/mnt/disks/ssd":
                return True
            return real_access(path, mode)

        with patch.object(Path, "is_dir", fake_is_dir), patch("os.access", fake_access):
            result = _default_mount_root()
        assert result == Path("/mnt/disks/ssd/ginkgo-fuse")

    def test_falls_back_to_tmp_when_no_nvme(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GINKGO_FUSE_ROOT", raising=False)

        def no_nvme(self: Path) -> bool:
            return False

        with patch.object(Path, "is_dir", no_nvme):
            # /tmp branch only fires if /tmp exists; simulate no nvme but real /tmp.
            with patch("pathlib.Path.exists", return_value=True):
                result = _default_mount_root()
        assert result == Path("/tmp/ginkgo-fuse")


class TestFallbackNotice:
    """Evaluator should emit a TaskNotice when access stats report a fallback."""

    def test_notice_emitted_when_fallback_reason_present(self) -> None:
        from ginkgo.runtime.evaluator import _ConcurrentEvaluator

        evaluator = _ConcurrentEvaluator.__new__(_ConcurrentEvaluator)
        evaluator.event_bus = MagicMock()
        evaluator.profiler = MagicMock()
        evaluator.profiler.timed.return_value.__enter__ = lambda self: None
        evaluator.profiler.timed.return_value.__exit__ = lambda self, *a: None
        evaluator.provenance = MagicMock()
        evaluator.provenance.run_id = "r1"

        node = MagicMock()
        node.node_id = 0
        node.task_def.name = "task.x"
        node.attempt = 1
        node.display_label = None

        evaluator._warn_on_access_fallback(
            node=node,
            access_stats={
                "policy": "fuse",
                "fallback_reason": "gcsfuse: /dev/fuse not accessible",
            },
        )

        assert evaluator.event_bus.emit.called
        event = evaluator.event_bus.emit.call_args.args[0]
        assert isinstance(event, TaskNotice)
        assert "fell back to staging" in event.message
        assert "/dev/fuse" in event.message

    def test_no_notice_when_fallback_reason_absent(self) -> None:
        from ginkgo.runtime.evaluator import _ConcurrentEvaluator

        evaluator = _ConcurrentEvaluator.__new__(_ConcurrentEvaluator)
        evaluator.event_bus = MagicMock()
        evaluator.profiler = MagicMock()
        evaluator.profiler.timed.return_value.__enter__ = lambda self: None
        evaluator.profiler.timed.return_value.__exit__ = lambda self, *a: None
        evaluator.provenance = MagicMock()
        evaluator.provenance.run_id = "r1"

        node = MagicMock()

        evaluator._warn_on_access_fallback(
            node=node,
            access_stats={"policy": "fuse", "fallback_reason": None},
        )

        assert not evaluator.event_bus.emit.called
