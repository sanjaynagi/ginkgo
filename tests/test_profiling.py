"""Tests for the runtime profile recorder."""

from __future__ import annotations

import time

from ginkgo.runtime.profiling import ProfileRecorder


def test_disabled_recorder_records_nothing() -> None:
    recorder = ProfileRecorder(enabled=False)
    with recorder.timed("phase_a"):
        time.sleep(0.001)
    recorder.record(phase="phase_b", seconds=0.5)
    assert recorder.snapshot() == {}


def test_enabled_recorder_accumulates_seconds_and_counts() -> None:
    recorder = ProfileRecorder(enabled=True)
    recorder.record(phase="phase_a", seconds=0.1)
    recorder.record(phase="phase_a", seconds=0.2)
    recorder.record(phase="phase_b", seconds=0.05)

    snapshot = recorder.snapshot()
    assert snapshot["phase_a"]["count"] == 2
    assert snapshot["phase_a"]["seconds"] == 0.3
    assert snapshot["phase_b"]["count"] == 1
    assert snapshot["phase_b"]["seconds"] == 0.05


def test_timed_context_records_elapsed() -> None:
    recorder = ProfileRecorder(enabled=True)
    with recorder.timed("phase"):
        time.sleep(0.005)
    snapshot = recorder.snapshot()
    assert snapshot["phase"]["count"] == 1
    assert snapshot["phase"]["seconds"] >= 0.005


def test_negative_seconds_are_ignored() -> None:
    recorder = ProfileRecorder(enabled=True)
    recorder.record(phase="phase", seconds=0.0)
    recorder.record(phase="phase", seconds=-1.0)
    assert recorder.snapshot() == {}
