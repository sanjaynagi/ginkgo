"""Shared helpers for the VW (validation-walkthrough) scenario tests.

The VW tests prove ordering, concurrency, and resource-contention properties
by having tasks record what they did to disk, then reading those records back.
These helpers centralise that record-and-replay machinery so each VW file only
contains its scenario, not a private copy of the plumbing.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any


def append_line(path: str, line: str) -> None:
    """Append ``line`` (plus a newline) to the text file at ``path``."""
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


def write_interval(
    events_dir: str,
    name: str,
    *,
    started_at: float,
    ended_at: float,
    **dimensions: float | bool,
) -> None:
    """Record one task's execution interval as a JSON file.

    Parameters
    ----------
    events_dir : str
        Directory to hold the per-task JSON records; created if absent.
    name : str
        Unique record name; also the file stem (``{name}.json``).
    started_at, ended_at : float
        Wall-clock start/end timestamps for the interval.
    **dimensions : float | bool
        Optional named resource dimensions (e.g. ``threads``, ``memory_gb``,
        ``heavy``) to fold into the record for peak computation.
    """
    Path(events_dir).mkdir(parents=True, exist_ok=True)
    payload = {"name": name, "start": started_at, "end": ended_at, **dimensions}
    Path(events_dir, f"{name}.json").write_text(json.dumps(payload), encoding="utf-8")


def load_intervals(events_dir: str, *, prefix: str | None = None) -> list[dict]:
    """Load interval records from ``events_dir``.

    Parameters
    ----------
    events_dir : str
        Directory previously populated by :func:`write_interval`.
    prefix : str | None
        When given, only records whose name starts ``{prefix}-`` are returned.

    Returns
    -------
    list[dict]
        One payload dict per record, each with ``name``, ``start``, ``end``,
        and any recorded dimensions. Sorted by name for determinism.
    """
    pattern = f"{prefix}-*.json" if prefix is not None else "*.json"
    paths = sorted(Path(events_dir).glob(pattern))
    return [json.loads(path.read_text(encoding="utf-8")) for path in paths]


def compute_peaks(
    intervals: Iterable[Mapping[str, Any]],
    *,
    dimensions: Sequence[str] = (),
) -> dict[str, int]:
    """Return the peak concurrent load over a set of intervals.

    A sweep line counts how many intervals are simultaneously active. Intervals
    that merely touch (one ends exactly as another starts) are not treated as
    overlapping.

    Parameters
    ----------
    intervals : iterable of mapping
        Records with ``start``/``end`` and any dimensions named in *dimensions*.
    dimensions : sequence of str
        Extra per-interval quantities to sum across active intervals (bools are
        counted as 1). For example ``("threads", "memory_gb")``.

    Returns
    -------
    dict[str, int]
        ``{"tasks": peak_concurrent_count, <dim>: peak_summed_value, ...}``.
    """
    keys = ("tasks", *dimensions)
    points: list[tuple[float, int, dict[str, int]]] = []
    for interval in intervals:
        contribution = {"tasks": 1}
        for dim in dimensions:
            contribution[dim] = int(interval[dim])
        points.append((float(interval["start"]), 1, contribution))
        points.append((float(interval["end"]), -1, {k: -v for k, v in contribution.items()}))

    # Sort by (time, sign) only; ends (-1) precede starts (+1) at equal times.
    points.sort(key=lambda item: (item[0], item[1]))

    active = dict.fromkeys(keys, 0)
    peak = dict.fromkeys(keys, 0)
    for _, _, contribution in points:
        for key, delta in contribution.items():
            active[key] += delta
            peak[key] = max(peak[key], active[key])
    return peak


def peak_concurrency(intervals: Iterable[Mapping[str, Any]]) -> int:
    """Return the peak number of simultaneously active intervals."""
    return compute_peaks(intervals)["tasks"]
