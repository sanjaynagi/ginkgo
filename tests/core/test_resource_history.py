"""Aggregating measured task runs into a distribution worth sizing from.

Every test here is a way the naive aggregate would lie: counting cache hits as
samples, averaging in the attempt that died at its ceiling, interpolating a
percentile between two measurements, or comparing a peak against a declaration
the attempt was no longer bound by.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from ginkgo.resource_history import (
    ResourceSample,
    group_by_label,
    samples_from_rows,
    summarize,
)

GIB = 1024**3


@dataclass
class Row:
    """The parts of ``query.TaskRow`` the aggregation reads."""

    display_label: str | None = None
    status: str = "succeeded"
    cached: bool = False
    peak_rss_bytes: int | None = None
    cpu_seconds: float | None = None
    duration_s: float | None = None
    declared_memory_gb: float | None = None
    effective_memory_gb: float | None = None
    declared_threads: int | None = None


def _sample(peak_gb: float, **overrides) -> ResourceSample:
    return ResourceSample(peak_rss_bytes=int(peak_gb * GIB), **overrides)


def _history(rows: list[Row]):
    samples, cached = samples_from_rows(rows)
    return summarize(samples, cached=cached, runs=len(rows))


def test_percentiles_are_values_that_were_actually_measured() -> None:
    """Nearest-rank, not interpolated: with a handful of samples an interpolated
    p95 reports a peak no run ever reached."""
    peaks = summarize([_sample(peak) for peak in (1.0, 2.0, 3.0, 4.0)]).peak_rss_bytes
    single = summarize([_sample(3.0)]).peak_rss_bytes

    measured = {int(peak * GIB) for peak in (1.0, 2.0, 3.0, 4.0)}
    assert {int(peaks.p50), int(peaks.p95)} <= measured
    assert peaks.max == 4.0 * GIB
    assert (single.n, single.p50, single.max) == (1, 3.0 * GIB, 3.0 * GIB)


def test_cpu_time_reports_a_total_as_well() -> None:
    """A peak has no meaningful sum; CPU seconds are what the task cost."""
    history = summarize([ResourceSample(cpu_seconds=s) for s in (10.0, 20.0, 30.0)])

    assert history.cpu_seconds.total == 60.0


def test_cache_hits_and_tasks_that_never_ran_are_not_samples() -> None:
    """Forty runs and two executions is n=2, and the run count says why."""
    rows = [Row(cached=True) for _ in range(37)] + [Row(status="skipped")]
    rows += [Row(peak_rss_bytes=2 * GIB), Row(peak_rss_bytes=3 * GIB)]

    history = _history(rows)

    assert (history.n, history.cached, history.runs) == (2, 37, 40)


def test_nothing_measured_is_no_distribution_rather_than_zero() -> None:
    history = _history([Row(cached=True) for _ in range(5)])
    payload = summarize([]).to_payload()

    assert (history.n, history.cached, history.peak_rss_bytes) == (0, 5, None)
    assert payload["peak_rss_bytes"] is payload["cpu_seconds"] is payload["headroom"] is None


def test_a_failed_attempt_is_a_floor_not_a_sample() -> None:
    """A task killed at its ceiling needs more than the ceiling, not the ceiling."""
    mixed = summarize([_sample(2.0), _sample(2.0), _sample(16.0, succeeded=False)])
    only_failures = summarize([_sample(16.0, succeeded=False)] * 2)

    assert (mixed.n, mixed.failed, mixed.peak_rss_bytes.max) == (2, 1, 2.0 * GIB)
    assert mixed.failed_peak_floor_bytes == 16 * GIB
    assert (only_failures.n, only_failures.failed, only_failures.peak_rss_bytes) == (0, 2, None)
    assert only_failures.failed_peak_floor_bytes == 16 * GIB
    (sample,), _ = samples_from_rows([Row(status="failed", peak_rss_bytes=16 * GIB)])
    assert sample.succeeded is False


def test_an_escalated_retry_is_compared_against_its_own_budget() -> None:
    """Comparing 30 GiB against the 16 GiB declaration would cry overrun: the
    attempt ran against 32 after ``memory_retry_multiplier`` escalated it."""
    history = summarize([_sample(30.0, declared_memory_gb=16.0, effective_memory_gb=32.0)])

    assert (history.declared_memory_gb, history.effective_memory_gb) == (16.0, 32.0)
    assert history.headroom > 1.0  # over the declaration, under what it ran on


def test_headroom_is_p95_against_the_declaration() -> None:
    assert summarize([_sample(4.0, declared_memory_gb=16.0)] * 4).headroom == pytest.approx(0.25)
    assert summarize([_sample(4.0)]).headroom is None


def test_the_declaration_comes_from_the_most_recent_run_and_changes_are_flagged() -> None:
    """Samples arrive newest first; the current declaration is the one a user edits."""
    varied = summarize(
        [_sample(4.0, declared_memory_gb=8.0), _sample(4.0, declared_memory_gb=16.0)]
    )
    stable = summarize([_sample(4.0, declared_memory_gb=8.0)] * 3)

    assert varied.declared_memory_gb == 8.0
    assert varied.declaration_varied is True
    assert stable.declaration_varied is False


def test_each_branch_is_summarized_separately_widest_first() -> None:
    """The widest branch of a fan-out is the one that has to size the declaration."""
    rows = [
        Row(display_label="small", peak_rss_bytes=GIB),
        Row(display_label="huge", peak_rss_bytes=40 * GIB),
        Row(display_label="medium", peak_rss_bytes=8 * GIB),
        Row(display_label="medium", cached=True),
    ]

    histories = group_by_label(rows)

    assert [item.label for item in histories] == ["huge", "medium", "small"]
    assert [(item.runs, item.cached) for item in histories] == [(1, 0), (2, 1), (1, 0)]


def test_unlabelled_rows_group_together() -> None:
    (history,) = group_by_label([Row(peak_rss_bytes=GIB) for _ in range(3)])

    assert (history.label, history.n) == (None, 3)


def test_the_payload_carries_the_counts_beside_the_numbers() -> None:
    payload = _history(
        [
            Row(cached=True),
            Row(peak_rss_bytes=2 * GIB, cpu_seconds=4.0, declared_memory_gb=16.0),
            Row(status="failed", peak_rss_bytes=16 * GIB),
        ]
    ).to_payload()

    assert (payload["n"], payload["cached"], payload["failed"], payload["runs"]) == (1, 1, 1, 3)
    assert payload["peak_rss_bytes"]["max"] == 2 * GIB
    assert payload["declared_memory_gb"] == 16.0
    assert payload["failed_peak_floor_bytes"] == 16 * GIB
