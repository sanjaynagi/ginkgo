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
class FakeRow:
    """The parts of ``TaskRow`` the aggregation reads."""

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


class TestDistribution:
    """Percentiles over measured peaks."""

    def test_percentiles_are_values_that_were_actually_measured(self) -> None:
        """Nearest-rank, not interpolated.

        With a handful of samples an interpolated p95 reports a peak no run
        ever reached, from a sampler that already misses short spikes.
        """
        samples = [_sample(peak) for peak in (1.0, 2.0, 3.0, 4.0)]

        history = summarize(samples)

        assert history.peak_rss_bytes is not None
        measured = {int(peak * GIB) for peak in (1.0, 2.0, 3.0, 4.0)}
        assert int(history.peak_rss_bytes.p50) in measured
        assert int(history.peak_rss_bytes.p95) in measured
        assert history.peak_rss_bytes.max == 4.0 * GIB

    def test_a_single_sample_is_its_own_distribution(self) -> None:
        history = summarize([_sample(3.0)])

        assert history.peak_rss_bytes is not None
        assert history.peak_rss_bytes.n == 1
        assert history.peak_rss_bytes.p50 == history.peak_rss_bytes.max == 3.0 * GIB

    def test_cpu_time_reports_a_total_as_well(self) -> None:
        """A peak has no meaningful sum; CPU seconds are what the task cost."""
        samples = [
            ResourceSample(cpu_seconds=10.0),
            ResourceSample(cpu_seconds=20.0),
            ResourceSample(cpu_seconds=30.0),
        ]

        history = summarize(samples)

        assert history.cpu_seconds is not None
        assert history.cpu_seconds.total == 60.0

    def test_nothing_measured_is_no_distribution_rather_than_zero(self) -> None:
        history = summarize([])

        assert history.n == 0
        assert history.peak_rss_bytes is None
        assert history.cpu_seconds is None


class TestSampleCounting:
    """A distribution is only as honest as the count travelling with it."""

    def test_cache_hits_contribute_no_samples(self) -> None:
        """Forty runs and two executions is n=2, and the run count says why."""
        rows = [FakeRow(cached=True) for _ in range(38)]
        rows += [FakeRow(peak_rss_bytes=2 * GIB), FakeRow(peak_rss_bytes=3 * GIB)]

        samples, cached = samples_from_rows(rows)
        history = summarize(samples, cached=cached, runs=len(rows))

        assert history.n == 2
        assert history.cached == 38
        assert history.runs == 40

    def test_a_task_that_never_ran_is_not_a_sample(self) -> None:
        """A skipped or never-started task measured nothing and is not a zero."""
        rows = [FakeRow(status="skipped"), FakeRow(peak_rss_bytes=GIB)]

        samples, _ = samples_from_rows(rows)

        assert len(samples) == 1

    def test_an_all_cached_history_measures_nothing(self) -> None:
        rows = [FakeRow(cached=True) for _ in range(5)]

        samples, cached = samples_from_rows(rows)
        history = summarize(samples, cached=cached, runs=len(rows))

        assert history.n == 0
        assert history.peak_rss_bytes is None
        assert history.cached == 5


class TestCensoredSamples:
    """A task killed at its ceiling needs more than the ceiling, not the ceiling."""

    def test_failed_attempts_stay_out_of_the_distribution(self) -> None:
        samples = [
            _sample(2.0),
            _sample(2.0),
            _sample(16.0, succeeded=False),
        ]

        history = summarize(samples)

        assert history.n == 2
        assert history.failed == 1
        assert history.peak_rss_bytes is not None
        assert history.peak_rss_bytes.max == 2.0 * GIB

    def test_the_failed_peak_is_reported_as_a_floor(self) -> None:
        """The censored samples still say something: it needs more than this."""
        samples = [_sample(2.0), _sample(16.0, succeeded=False)]

        history = summarize(samples)

        assert history.failed_peak_floor_bytes == 16 * GIB

    def test_a_history_of_only_failures_has_no_distribution(self) -> None:
        """Nothing here is a measurement, so there is no percentile to report."""
        samples = [_sample(16.0, succeeded=False), _sample(16.0, succeeded=False)]

        history = summarize(samples)

        assert history.n == 0
        assert history.peak_rss_bytes is None
        assert history.failed == 2
        assert history.failed_peak_floor_bytes == 16 * GIB

    def test_a_failed_row_is_read_from_its_status(self) -> None:
        rows = [FakeRow(status="failed", peak_rss_bytes=16 * GIB)]

        samples, _ = samples_from_rows(rows)

        assert samples[0].succeeded is False


class TestDeclaration:
    """What the task asked for, and what it was actually given."""

    def test_an_escalated_retry_is_compared_against_its_own_budget(self) -> None:
        """Comparing 30 GiB against the 16 GiB declaration would cry overrun.

        The attempt ran against 32 after ``memory_retry_multiplier`` escalated
        it, and fitted.
        """
        samples = [
            _sample(30.0, declared_memory_gb=16.0, effective_memory_gb=32.0),
        ]

        history = summarize(samples)

        assert history.declared_memory_gb == 16.0
        assert history.effective_memory_gb == 32.0
        assert history.headroom is not None
        assert history.headroom > 1.0  # over the declaration, under what it ran on

    def test_headroom_is_p95_against_the_declaration(self) -> None:
        samples = [_sample(4.0, declared_memory_gb=16.0) for _ in range(4)]

        history = summarize(samples)

        assert history.headroom == pytest.approx(0.25)

    def test_headroom_is_unknown_without_a_declaration(self) -> None:
        history = summarize([_sample(4.0)])

        assert history.headroom is None

    def test_the_declaration_comes_from_the_most_recent_run(self) -> None:
        """Samples arrive newest first, and the current declaration is the one
        a user would edit."""
        samples = [
            _sample(4.0, declared_memory_gb=8.0),
            _sample(4.0, declared_memory_gb=16.0),
        ]

        history = summarize(samples)

        assert history.declared_memory_gb == 8.0

    def test_a_changed_declaration_is_flagged(self) -> None:
        """One "declared" figure over runs that declared differently is a summary."""
        samples = [
            _sample(4.0, declared_memory_gb=8.0),
            _sample(4.0, declared_memory_gb=16.0),
        ]

        assert summarize(samples).declaration_varied is True

    def test_a_stable_declaration_is_not_flagged(self) -> None:
        samples = [_sample(4.0, declared_memory_gb=8.0) for _ in range(3)]

        assert summarize(samples).declaration_varied is False


class TestGroupingByLabel:
    """One branch of a fan-out over chromosomes is not like another."""

    def test_each_branch_is_summarized_separately(self) -> None:
        rows = [
            FakeRow(display_label="align[chr1]", peak_rss_bytes=2 * GIB),
            FakeRow(display_label="align[chr2]", peak_rss_bytes=9 * GIB),
        ]

        histories = group_by_label(rows)

        assert [item.label for item in histories] == ["align[chr2]", "align[chr1]"]

    def test_the_widest_branch_comes_first(self) -> None:
        """It is the branch that has to size the declaration."""
        rows = [
            FakeRow(display_label="small", peak_rss_bytes=GIB),
            FakeRow(display_label="huge", peak_rss_bytes=40 * GIB),
            FakeRow(display_label="medium", peak_rss_bytes=8 * GIB),
        ]

        histories = group_by_label(rows)

        assert [item.label for item in histories] == ["huge", "medium", "small"]

    def test_a_branch_counts_only_its_own_runs(self) -> None:
        """A branch that ran once inside a four-run history has runs=1."""
        rows = [
            FakeRow(display_label="a", peak_rss_bytes=GIB),
            FakeRow(display_label="b", peak_rss_bytes=GIB),
            FakeRow(display_label="b", cached=True),
        ]

        histories = {item.label: item for item in group_by_label(rows)}

        assert histories["a"].runs == 1
        assert histories["b"].runs == 2
        assert histories["b"].cached == 1

    def test_unlabelled_rows_group_together(self) -> None:
        """A fan-out without labels is one group, not one group per row."""
        rows = [FakeRow(peak_rss_bytes=GIB) for _ in range(3)]

        histories = group_by_label(rows)

        assert len(histories) == 1
        assert histories[0].label is None
        assert histories[0].n == 3


class TestPayload:
    """The JSON a consumer aggregates over."""

    def test_the_payload_carries_the_counts_beside_the_numbers(self) -> None:
        rows = [
            FakeRow(cached=True),
            FakeRow(peak_rss_bytes=2 * GIB, cpu_seconds=4.0, declared_memory_gb=16.0),
            FakeRow(status="failed", peak_rss_bytes=16 * GIB),
        ]
        samples, cached = samples_from_rows(rows)

        payload = summarize(samples, cached=cached, runs=len(rows)).to_payload()

        assert payload["n"] == 1
        assert payload["cached"] == 1
        assert payload["failed"] == 1
        assert payload["runs"] == 3
        assert payload["peak_rss_bytes"]["max"] == 2 * GIB
        assert payload["declared_memory_gb"] == 16.0
        assert payload["failed_peak_floor_bytes"] == 16 * GIB

    def test_an_empty_history_serialises_as_nulls_not_zeros(self) -> None:
        payload = summarize([]).to_payload()

        assert payload["peak_rss_bytes"] is None
        assert payload["cpu_seconds"] is None
        assert payload["headroom"] is None
