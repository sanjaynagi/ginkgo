"""Record-and-replay helpers for the scenario tests.

Scenario tasks prove ordering, concurrency and resource limits by recording
what they did to disk; the tests read those records back.
"""

from __future__ import annotations

import json
import time
from pathlib import Path


def append_line(path: str, line: str) -> None:
    """Append ``line`` (plus a newline) to the text file at ``path``."""
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


def timed(events_dir: str, name: str, *, seconds: float, **dimensions: float | bool) -> None:
    """Sleep for ``seconds`` and record the interval as ``events_dir/name.json``.

    ``dimensions`` (``threads``, ``memory_gb``, a bool flag, ...) are stored
    with the interval so :func:`peaks` can sum them across overlapping tasks.
    """
    started_at = time.time()
    time.sleep(seconds)
    record = {"name": name, "start": started_at, "end": time.time(), **dimensions}
    Path(events_dir).mkdir(parents=True, exist_ok=True)
    Path(events_dir, f"{name}.json").write_text(json.dumps(record), encoding="utf-8")


def load_intervals(events_dir: str, *, prefix: str = "") -> list[dict]:
    """Return the intervals recorded under ``events_dir`` whose name starts ``prefix``."""
    paths = sorted(Path(events_dir).glob(f"{prefix}*.json"))
    return [json.loads(path.read_text(encoding="utf-8")) for path in paths]


def peaks(intervals: list[dict], *dimensions: str) -> dict[str, int]:
    """Return the peak concurrent task count, and the peak sum of each dimension.

    Intervals that merely touch (one ends exactly as another starts) do not
    overlap: at equal times, ends sort before starts.
    """
    points = []
    for interval in intervals:
        amounts = {"tasks": 1, **{dim: int(interval[dim]) for dim in dimensions}}
        points.append((interval["start"], 1, amounts))
        points.append((interval["end"], -1, {k: -v for k, v in amounts.items()}))
    active = dict.fromkeys(("tasks", *dimensions), 0)
    peak = dict(active)
    for _, _, amounts in sorted(points, key=lambda point: point[:2]):
        for key, delta in amounts.items():
            active[key] += delta
            peak[key] = max(peak[key], active[key])
    return peak
