"""Tests for the gcsfuse performance defaults and fallback-warning wiring.

The gcsfuse flag tests live with the driver, in ginkgo-gcp. These cover:

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

from ginkgo.remote.access.mounted import _default_mount_root
from ginkgo.runtime.events import TaskNotice


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
        from ginkgo.runtime.evaluator import ConcurrentEvaluator

        evaluator = ConcurrentEvaluator.__new__(ConcurrentEvaluator)
        evaluator.event_bus = MagicMock()
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
        from ginkgo.runtime.evaluator import ConcurrentEvaluator

        evaluator = ConcurrentEvaluator.__new__(ConcurrentEvaluator)
        evaluator.event_bus = MagicMock()
        evaluator.provenance = MagicMock()
        evaluator.provenance.run_id = "r1"

        node = MagicMock()

        evaluator._warn_on_access_fallback(
            node=node,
            access_stats={"policy": "fuse", "fallback_reason": None},
        )

        assert not evaluator.event_bus.emit.called
