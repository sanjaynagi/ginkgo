"""Coarse phase-timer aggregation for the ``--profile`` runtime mode.

The :class:`ProfileRecorder` is a single object that accumulates wall-time
samples for named phases. When the recorder is disabled (the default), every
public method short-circuits to a no-op so the default ``ginkgo run`` path
incurs no measurable overhead.

The recorder is intentionally coarse: it aggregates per-phase totals and call
counts, not function-level traces. Detailed function-level profiling is
deliberately out of scope to keep the recording cost negligible and to prevent
the act of measurement from distorting what is being measured.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator


@dataclass(kw_only=True)
class ProfileRecorder:
    """Accumulate wall time and call counts for named runtime phases.

    Parameters
    ----------
    enabled : bool
        When ``False``, all recording methods are no-ops. The default value is
        ``False`` so the recorder can be created unconditionally without cost.
    """

    enabled: bool = False
    _seconds: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _counts: dict[str, int] = field(default_factory=dict, init=False, repr=False)

    def record(self, *, phase: str, seconds: float) -> None:
        """Add one observation of ``seconds`` to ``phase``."""
        if not self.enabled or seconds <= 0:
            return
        self._seconds[phase] = self._seconds.get(phase, 0.0) + float(seconds)
        self._counts[phase] = self._counts.get(phase, 0) + 1

    @contextmanager
    def timed(self, phase: str) -> Iterator[None]:
        """Time the wrapped block and record it under ``phase``.

        When the recorder is disabled this yields immediately and performs no
        measurement.
        """
        if not self.enabled:
            yield
            return
        started = time.perf_counter()
        try:
            yield
        finally:
            self.record(phase=phase, seconds=time.perf_counter() - started)

    def snapshot(self) -> dict[str, dict[str, float | int]]:
        """Return the current per-phase totals as a JSON-serializable mapping.

        Returns
        -------
        dict
            Mapping of ``phase`` to ``{"seconds": float, "count": int}``.
        """
        return {
            phase: {
                "seconds": round(self._seconds[phase], 6),
                "count": self._counts[phase],
            }
            for phase in self._seconds
        }

    def merge_into(self, other: "ProfileRecorder") -> None:
        """Add this recorder's totals into ``other``.

        Used to flush a pre-recorder buffer into the canonical recorder once it
        becomes available later in the run.
        """
        if not other.enabled:
            return
        for phase, seconds in self._seconds.items():
            other._seconds[phase] = other._seconds.get(phase, 0.0) + seconds
            other._counts[phase] = other._counts.get(phase, 0) + self._counts[phase]
