"""Unit tests for keyed grouping and joins (issue #97, static-keys slice)."""

import pytest
from tests._vw_support import append_line

from ginkgo import Expr, ExprList, evaluate, keyed, task
from ginkgo.core.keyed import KeyedExprList, KeyedGroups


@task()
def filter_reads(sample: str, lane: str) -> str:
    return f"{sample}-{lane}"


@task()
def merge_lanes(sample: str, parts: list) -> str:
    return f"{sample}:" + ",".join(parts)


@task()
def merge_lanes_logged(sample: str, parts: list, log_path: str) -> str:
    append_line(log_path, f"merge:{sample}")
    return f"{sample}:" + ",".join(parts)


@task()
def merge_treatment(sample: str, library: str, treatment: str, parts: list) -> str:
    return f"{sample}/{library}/{treatment}:" + ",".join(sorted(parts))


@task()
def annotate(prefix: str, value: str) -> str:
    return f"{prefix}:{value}"


@task()
def combine_pair(a: str, b: str) -> str:
    return f"{a}+{b}"


def _lane_samplesheet():
    """Three samples × libraries × lanes, in samplesheet order.

    sample, library, lane:
      S1, L1, lane1
      S1, L1, lane2
      S1, L2, lane1
      S2, L1, lane1
    """
    return [
        {"sample": "S1", "library": "L1", "lane": "lane1"},
        {"sample": "S1", "library": "L1", "lane": "lane2"},
        {"sample": "S1", "library": "L2", "lane": "lane1"},
        {"sample": "S2", "library": "L1", "lane": "lane1"},
    ]


def _keyed_reads():
    rows = _lane_samplesheet()
    reads = filter_reads().map(
        sample=[r["sample"] for r in rows],
        lane=[r["lane"] for r in rows],
    )
    return keyed(reads, rows)


class TestKeyedConstruction:
    def test_keyed_aligns_exprs_and_keys(self):
        k = _keyed_reads()
        assert len(k) == 4
        assert k.keys == _lane_samplesheet()

    def test_keyed_length_mismatch_raises(self):
        reads = filter_reads().map(sample=["S1", "S2"], lane=["L1", "L1"])
        with pytest.raises(ValueError, match="1:1"):
            keyed(reads, [{"sample": "S1"}])

    def test_keyed_rejects_non_scalar_key_value(self):
        reads = filter_reads().map(sample=["S1"], lane=["L1"])
        with pytest.raises(TypeError, match="hashable scalar"):
            keyed(reads, [{"sample": ["not", "hashable"]}])

    def test_keyed_rejects_expr_as_key_value(self):
        reads = filter_reads().map(sample=["S1"], lane=["L1"])
        upstream = filter_reads(sample="x", lane="y")
        with pytest.raises(TypeError, match="hashable scalar"):
            keyed(reads, [{"sample": upstream}])

    def test_with_keys_method_matches_keyed_function(self):
        rows = _lane_samplesheet()
        reads = filter_reads().map(
            sample=[r["sample"] for r in rows],
            lane=[r["lane"] for r in rows],
        )
        assert reads.with_keys(rows).keys == keyed(reads, rows).keys

    def test_keys_property_returns_independent_copies(self):
        k = _keyed_reads()
        first = k.keys
        first[0]["sample"] = "mutated"
        assert k.keys[0]["sample"] == "S1"

    def test_unkey_returns_plain_exprlist(self):
        k = _keyed_reads()
        plain = k.unkey()
        assert isinstance(plain, ExprList)
        assert list(plain) == list(k)


class TestGroupBy:
    def test_group_by_library_merges_lanes_per_library(self):
        groups = _keyed_reads().group_by("sample", "library")
        assert isinstance(groups, KeyedGroups)
        assert groups.keys == [
            {"sample": "S1", "library": "L1"},
            {"sample": "S1", "library": "L2"},
            {"sample": "S2", "library": "L1"},
        ]
        sizes = [len(group.members) for group in groups]
        assert sizes == [2, 1, 1]

    def test_group_by_preserves_first_appearance_and_member_order(self):
        # Reversed samplesheet order still yields first-appearance group
        # order and in-group member order matching the *reversed* input.
        rows = list(reversed(_lane_samplesheet()))
        reads = filter_reads().map(
            sample=[r["sample"] for r in rows],
            lane=[r["lane"] for r in rows],
        )
        groups = keyed(reads, rows).group_by("sample", "library")
        assert groups.keys[0] == {"sample": "S2", "library": "L1"}
        s1_l1 = next(g for g in groups if g.key == {"sample": "S1", "library": "L1"})
        assert [expr.args["lane"] for expr in s1_l1.members] == ["lane2", "lane1"]

    def test_group_by_missing_field_raises_key_error(self):
        k = _keyed_reads()
        with pytest.raises(KeyError):
            k.group_by("treatment")

    def test_group_by_requires_at_least_one_field(self):
        with pytest.raises(ValueError):
            _keyed_reads().group_by()

    def test_group_by_treatment_distinguishes_library_merges(self):
        rows = [
            {"sample": "S1", "library": "L1", "treatment": "control"},
            {"sample": "S1", "library": "L1", "treatment": "treated"},
            {"sample": "S1", "library": "L2", "treatment": "control"},
        ]
        reads = filter_reads().map(
            sample=[r["sample"] for r in rows],
            lane=["lane1", "lane1", "lane1"],
        )
        groups = keyed(reads, rows).group_by("sample", "library", "treatment")
        assert len(groups) == 3
        assert all(len(group.members) == 1 for group in groups)


class TestMapGroups:
    def test_map_groups_passes_members_as_list_and_keys_task_by_group(self):
        groups = _keyed_reads().group_by("sample", "library")
        merged = groups.map_groups(merge_lanes, param="parts")
        assert isinstance(merged, KeyedExprList)
        assert merged.keys == groups.keys
        by_key = dict(zip((tuple(k.values()) for k in merged.keys), merged))
        s1_l1 = by_key[("S1", "L1")]
        assert isinstance(s1_l1, Expr)
        assert s1_l1.args["sample"] == "S1"
        assert len(s1_l1.args["parts"]) == 2
        assert all(isinstance(part, Expr) for part in s1_l1.args["parts"])

    def test_map_groups_display_label_carries_key_values(self):
        groups = _keyed_reads().group_by("sample", "library")
        merged = groups.map_groups(merge_lanes, param="parts")
        s1_l1 = next(
            e for e, k in zip(merged, merged.keys) if k == {"sample": "S1", "library": "L1"}
        )
        assert s1_l1.display_label_parts == ("sample=S1", "library=L1")
        assert s1_l1.display_label == "merge_lanes[sample=S1,library=L1]"

    def test_map_groups_explicit_fixed_arg_overrides_key_value(self):
        @task()
        def merge_named(sample: str, parts: list) -> str:
            return sample

        groups = _keyed_reads().group_by("sample", "library")
        merged = groups.map_groups(merge_named, param="parts", sample="OVERRIDDEN")
        assert all(expr.args["sample"] == "OVERRIDDEN" for expr in merged)

    def test_map_groups_deterministic_ordering_across_runs(self):
        groups_a = _keyed_reads().group_by("sample", "library")
        groups_b = _keyed_reads().group_by("sample", "library")
        assert groups_a.keys == groups_b.keys


class TestKeyedMap:
    def test_map_applies_task_per_element_and_retains_keys(self):
        k = _keyed_reads()
        annotated = k.map(annotate, param="value", prefix="qc")
        assert isinstance(annotated, KeyedExprList)
        assert annotated.keys == k.keys
        assert all(expr.args["prefix"] == "qc" for expr in annotated)
        assert all(isinstance(expr.args["value"], Expr) for expr in annotated)


class TestJoin:
    def test_join_matches_on_field_in_left_order(self):
        left_rows = [{"sample": "S1"}, {"sample": "S2"}]
        right_rows = [{"sample": "S2"}, {"sample": "S1"}]
        left = keyed(filter_reads().map(sample=["S1", "S2"], lane=["L1", "L1"]), left_rows)
        right = keyed(filter_reads().map(sample=["S2", "S1"], lane=["L2", "L2"]), right_rows)

        joined = left.join(right, on="sample")
        assert joined.keys == [{"sample": "S1"}, {"sample": "S2"}]
        first_pair = joined[0]
        assert first_pair[0].args == {"sample": "S1", "lane": "L1"}
        assert first_pair[1].args == {"sample": "S1", "lane": "L2"}

    def test_join_duplicate_key_raises_naming_key_and_side(self):
        left = keyed(
            filter_reads().map(sample=["S1", "S1"], lane=["L1", "L2"]),
            [{"sample": "S1"}, {"sample": "S1"}],
        )
        right = keyed(filter_reads().map(sample=["S1"], lane=["L1"]), [{"sample": "S1"}])
        with pytest.raises(ValueError, match="duplicate left key"):
            left.join(right, on="sample")

    def test_join_missing_key_one_side_raises_naming_key(self):
        left = keyed(
            filter_reads().map(sample=["S1", "S2"], lane=["L1", "L1"]),
            [{"sample": "S1"}, {"sample": "S2"}],
        )
        right = keyed(filter_reads().map(sample=["S1"], lane=["L1"]), [{"sample": "S1"}])
        with pytest.raises(ValueError, match="only on the left side") as excinfo:
            left.join(right, on="sample")
        assert "S2" in str(excinfo.value)

    def test_join_result_map_zips_pair_into_two_params(self):
        left = keyed(filter_reads().map(sample=["S1"], lane=["L1"]), [{"sample": "S1"}])
        right = keyed(filter_reads().map(sample=["S1"], lane=["L2"]), [{"sample": "S1"}])
        joined = left.join(right, on="sample")

        combined = joined.map(combine_pair, param=("a", "b"))
        assert len(combined) == 1
        expr = combined[0]
        assert set(expr.args) == {"a", "b"}
        assert isinstance(expr.args["a"], Expr)
        assert isinstance(expr.args["b"], Expr)

    def test_join_result_unkey_raises(self):
        left = keyed(filter_reads().map(sample=["S1"], lane=["L1"]), [{"sample": "S1"}])
        right = keyed(filter_reads().map(sample=["S1"], lane=["L2"]), [{"sample": "S1"}])
        joined = left.join(right, on="sample")
        with pytest.raises(TypeError, match="joined"):
            joined.unkey()

    def test_join_requires_at_least_one_field(self):
        left = keyed(filter_reads().map(sample=["S1"], lane=["L1"]), [{"sample": "S1"}])
        with pytest.raises(ValueError):
            left.join(left, on=[])


class TestEvaluatorIntegration:
    def test_grouped_lane_merge_runs_and_caches(self) -> None:
        log_path = "merge-events.log"
        groups = _keyed_reads().group_by("sample", "library")
        merged = groups.map_groups(merge_lanes_logged, param="parts", log_path=log_path)

        first = evaluate(merged.unkey())
        assert sorted(first) == sorted(["S1:S1-lane1,S1-lane2", "S1:S1-lane1", "S2:S2-lane1"])

        # Re-run: same graph, so every merge is served from cache.
        second = evaluate(merged.unkey())
        assert second == first

        from pathlib import Path

        lines = Path(log_path).read_text(encoding="utf-8").splitlines()
        assert sorted(lines) == ["merge:S1", "merge:S1", "merge:S2"]
