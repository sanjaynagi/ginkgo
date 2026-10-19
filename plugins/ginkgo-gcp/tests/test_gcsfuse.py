"""The gcsfuse driver's tuned flag set.

The driver assembles metadata caches, parallel range-read downloads and bigger
chunks rather than relying on gcsfuse's conservative defaults, and still applies
caller-supplied ``extra_args`` after them so users can override.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ginkgo.remote.access.drivers.base import MountSpec
from ginkgo_gcp.gcsfuse import GcsFuseDriver


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

        with patch("ginkgo_gcp.gcsfuse.subprocess.run", side_effect=fake_run):
            with patch(
                "ginkgo_gcp.gcsfuse.shutil.which",
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

        with patch("ginkgo_gcp.gcsfuse.subprocess.run", side_effect=fake_run):
            with patch(
                "ginkgo_gcp.gcsfuse.shutil.which",
                return_value="/usr/bin/gcsfuse",
            ):
                driver.mount(spec=spec)
        assert "--foo-override" in captured[0]
        # Extra args must land after the tuning flags so gcsfuse's
        # "last occurrence wins" rule allows users to override defaults.
        foo_idx = captured[0].index("--foo-override")
        ttl_idx = captured[0].index("--stat-cache-ttl")
        assert foo_idx > ttl_idx
