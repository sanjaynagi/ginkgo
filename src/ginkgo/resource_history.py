"""Cross-run resource distributions, aggregated from measured task runs.

``ginkgo history <task> --resources`` answers "how much memory does this task
really need", which is a question about many runs rather than one. Every
execution already records its peak RSS and CPU time; this module turns a
sequence of those measurements into the distribution a user sizes a declaration
from.

Three things keep the answer honest, and each of them is a way the naive
aggregate lies:

* **Cache hits are not measurements.** A task with forty runs and thirty-eight
  cache hits has two samples, and the sample count travels with the numbers so
  nobody reads confidence into ``n=2``.
* **Failed attempts are censored.** A task killed at its 16 GiB ceiling needs
  *more than* 16 GiB, not 16, so folding it into a percentile drags the
  distribution towards the value that already failed. Those samples are counted
  and reported, never averaged in.
* **Percentiles are nearest-rank.** Interpolating between two samples invents
  precision the sampler does not have — local RSS is periodic ``ps`` sampling of
  a process tree, which can miss a short spike entirely.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

__all__ = [
    "Distribution",
    "ResourceHistory",
    "ResourceSample",
    "group_by_label",
    "samples_from_rows",
    "summarize",
]


@dataclass(frozen=True, kw_only=True)
class ResourceSample:
    """One measured execution of a task.

    Attributes
    ----------
    label : str | None
        The task's fan-out display label, when it had one.
    peak_rss_bytes : int | None
        Measured peak resident set size.
    cpu_seconds : float | None
        Measured CPU time.
    duration_s : float | None
        Wall-clock seconds the execution took.
    declared_memory_gb, effective_memory_gb : float | None
        What the task declared, and what its last attempt ran against.
    declared_threads : int | None
        Threads declared, after site overrides.
    succeeded : bool
        Whether the execution finished successfully. A failed execution's peak
        is a lower bound, not a measurement, and is excluded from the
        distribution.
    """

    label: str | None = None
    peak_rss_bytes: int | None = None
    cpu_seconds: float | None = None
    duration_s: float | None = None
    declared_memory_gb: float | None = None
    effective_memory_gb: float | None = None
    declared_threads: int | None = None
    succeeded: bool = True


@dataclass(frozen=True, kw_only=True)
class Distribution:
    """Nearest-rank percentiles over one measured quantity.

    Attributes
    ----------
    n : int
        Samples the percentiles were computed from.
    p50, p95, max, total : float
        The distribution. ``total`` is the sum, meaningful for CPU seconds
        (what the task has cost in aggregate) and not for a peak.
    """

    n: int
    p50: float
    p95: float
    max: float
    total: float

    def to_payload(self) -> dict[str, Any]:
        """Return the distribution as JSON-ready data."""
        return {
            "n": self.n,
            "p50": self.p50,
            "p95": self.p95,
            "max": self.max,
            "total": self.total,
        }


@dataclass(frozen=True, kw_only=True)
class ResourceHistory:
    """What one task's measured executions say about its resource needs.

    Attributes
    ----------
    label : str | None
        The fan-out branch these samples came from, or ``None`` when the
        history pools every branch.
    runs : int
        Task rows this history covers, measured or not. The denominator that
        makes a small ``n`` legible: forty runs and two samples is a cache
        working, not a measurement failing.
    n : int
        Successful measured executions behind the distributions.
    cached : int
        Runs served from the cache. These measure nothing, and are reported so
        a small ``n`` next to a long run history is explicable rather than
        surprising.
    failed : int
        Measured executions that failed. Counted, never folded into the
        distribution: their peaks are lower bounds.
    peak_rss_bytes, cpu_seconds : Distribution | None
        ``None`` when no successful execution measured that quantity.
    declared_memory_gb, effective_memory_gb : float | None
        From the most recent measured execution.
    declared_threads : int | None
        From the most recent measured execution.
    declaration_varied : bool
        Whether the declaration changed across the samples, which makes a
        single "declared" figure a summary of more than one number.
    failed_peak_floor_bytes : int | None
        The largest peak seen among failed executions — the floor those
        censored samples establish, when there are any.
    """

    label: str | None = None
    runs: int = 0
    n: int = 0
    cached: int = 0
    failed: int = 0
    peak_rss_bytes: Distribution | None = None
    cpu_seconds: Distribution | None = None
    declared_memory_gb: float | None = None
    effective_memory_gb: float | None = None
    declared_threads: int | None = None
    declaration_varied: bool = False
    failed_peak_floor_bytes: int | None = None

    @property
    def headroom(self) -> float | None:
        """Return p95 peak RSS as a fraction of the declared memory.

        ``None`` when either half is missing. A value of ``0.3`` means the task
        is using under a third of what it reserves; above ``1.0`` means it has
        been exceeding its declaration and surviving only because the executor
        did not enforce it.
        """
        if self.peak_rss_bytes is None or not self.declared_memory_gb:
            return None
        declared_bytes = self.declared_memory_gb * (1024**3)
        if declared_bytes <= 0:
            return None
        return self.peak_rss_bytes.p95 / declared_bytes

    def to_payload(self) -> dict[str, Any]:
        """Return the history as JSON-ready data."""
        return {
            "label": self.label,
            "runs": self.runs,
            "n": self.n,
            "cached": self.cached,
            "failed": self.failed,
            "peak_rss_bytes": None
            if self.peak_rss_bytes is None
            else self.peak_rss_bytes.to_payload(),
            "cpu_seconds": None if self.cpu_seconds is None else self.cpu_seconds.to_payload(),
            "declared_memory_gb": self.declared_memory_gb,
            "effective_memory_gb": self.effective_memory_gb,
            "declared_threads": self.declared_threads,
            "declaration_varied": self.declaration_varied,
            "failed_peak_floor_bytes": self.failed_peak_floor_bytes,
            "headroom": self.headroom,
        }


def samples_from_rows(rows: Sequence[Any]) -> tuple[list[ResourceSample], int]:
    """Split task history rows into measured samples and a cache-hit count.

    A row measured nothing when it hit the cache, never ran, or predates the
    projection. Those are not samples with a missing value — they are not
    samples — so they are counted rather than carried through as nulls that
    every consumer would have to filter again.

    Parameters
    ----------
    rows : Sequence[Any]
        :class:`~ginkgo.query.TaskRow` values, newest first.

    Returns
    -------
    tuple[list[ResourceSample], int]
        The measured samples, and how many rows were served from the cache.
    """
    samples: list[ResourceSample] = []
    cached = 0
    for row in rows:
        if row.cached:
            cached += 1
            continue
        if row.peak_rss_bytes is None and row.cpu_seconds is None:
            continue
        samples.append(
            ResourceSample(
                label=row.display_label,
                peak_rss_bytes=row.peak_rss_bytes,
                cpu_seconds=row.cpu_seconds,
                duration_s=row.duration_s,
                declared_memory_gb=row.declared_memory_gb,
                effective_memory_gb=row.effective_memory_gb,
                declared_threads=row.declared_threads,
                succeeded=row.status == "succeeded",
            )
        )
    return samples, cached


def summarize(
    samples: Sequence[ResourceSample],
    *,
    cached: int = 0,
    runs: int | None = None,
    label: str | None = None,
) -> ResourceHistory:
    """Aggregate measured executions into one task's resource history.

    Parameters
    ----------
    samples : Sequence[ResourceSample]
        Measured executions, newest first. Order matters only for reporting the
        declaration, which comes from the most recent sample.
    cached : int, optional
        Runs that hit the cache and so measured nothing.
    runs : int | None, optional
        Task rows behind these samples. Defaults to the samples plus the cache
        hits, which is right unless rows were dropped for measuring nothing.
    label : str | None, optional
        The fan-out branch these samples belong to.

    Returns
    -------
    ResourceHistory
        With ``n=0`` and null distributions when nothing measured successfully.
    """
    succeeded = [sample for sample in samples if sample.succeeded]
    failed = [sample for sample in samples if not sample.succeeded]
    failed_peaks = [
        sample.peak_rss_bytes for sample in failed if sample.peak_rss_bytes is not None
    ]

    declared_memory = [
        sample.declared_memory_gb for sample in samples if sample.declared_memory_gb is not None
    ]

    return ResourceHistory(
        label=label,
        runs=len(samples) + cached if runs is None else runs,
        n=len(succeeded),
        cached=cached,
        failed=len(failed),
        peak_rss_bytes=_distribution(
            [sample.peak_rss_bytes for sample in succeeded if sample.peak_rss_bytes is not None]
        ),
        cpu_seconds=_distribution(
            [sample.cpu_seconds for sample in succeeded if sample.cpu_seconds is not None]
        ),
        declared_memory_gb=_most_recent(samples, "declared_memory_gb"),
        effective_memory_gb=_most_recent(samples, "effective_memory_gb"),
        declared_threads=_most_recent(samples, "declared_threads"),
        declaration_varied=len(set(declared_memory)) > 1,
        failed_peak_floor_bytes=max(failed_peaks) if failed_peaks else None,
    )


def group_by_label(rows: Sequence[Any]) -> list[ResourceHistory]:
    """Summarize each fan-out branch of one task separately, widest peak first.

    One branch of a fan-out over chromosomes is not like another, so pooling
    them hides the branch that actually drives the declaration. Each group
    counts its own runs and cache hits: a branch that ran once inside a
    forty-run history has ``runs=1``, not forty.

    Parameters
    ----------
    rows : Sequence[Any]
        :class:`~ginkgo.query.TaskRow` values, newest first.

    Returns
    -------
    list[ResourceHistory]
        One per distinct display label, ordered by peak so the branch that
        sizes the declaration comes first.
    """
    grouped: dict[str | None, list[Any]] = {}
    for row in rows:
        grouped.setdefault(row.display_label, []).append(row)

    histories = []
    for label, group in grouped.items():
        samples, cached = samples_from_rows(group)
        histories.append(summarize(samples, cached=cached, runs=len(group), label=label))
    return sorted(
        histories,
        key=lambda item: item.peak_rss_bytes.max if item.peak_rss_bytes else -1.0,
        reverse=True,
    )


def _distribution(values: Sequence[float]) -> Distribution | None:
    """Return nearest-rank percentiles over *values*, or ``None`` when empty."""
    if not values:
        return None
    ordered = sorted(values)
    return Distribution(
        n=len(ordered),
        p50=_percentile(ordered, 0.50),
        p95=_percentile(ordered, 0.95),
        max=ordered[-1],
        total=sum(ordered),
    )


def _percentile(ordered: Sequence[float], fraction: float) -> float:
    """Return the nearest-rank percentile of an already-sorted sequence.

    Nearest-rank rather than interpolated: every value returned is a value that
    was actually measured. With the handful of samples a real task history
    holds, interpolation would report a peak RSS that no run ever reached.
    """
    rank = max(1, min(len(ordered), _ceil(fraction * len(ordered))))
    return ordered[rank - 1]


def _ceil(value: float) -> int:
    """Return the smallest integer not less than *value*."""
    truncated = int(value)
    return truncated if truncated == value else truncated + 1


def _most_recent(samples: Sequence[ResourceSample], attribute: str) -> Any:
    """Return the newest non-null value of *attribute*, or ``None``."""
    for sample in samples:
        value = getattr(sample, attribute)
        if value is not None:
            return value
    return None
