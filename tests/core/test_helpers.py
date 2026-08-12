"""Unit tests for workflow authoring helpers."""

from __future__ import annotations

import pytest

from ginkgo import expand, flatten, slug, zip_expand


class TestExpand:
    def test_expands_single_wildcard(self) -> None:
        assert expand("results/{sample}.txt", sample=["a", "b", "c"]) == [
            "results/a.txt",
            "results/b.txt",
            "results/c.txt",
        ]

    def test_expands_cartesian_product_in_template_order(self) -> None:
        assert expand("{a}_{b}", b=["x", "y"], a=[1, 2]) == [
            "1_x",
            "1_y",
            "2_x",
            "2_y",
        ]

    def test_reuses_repeated_placeholder(self) -> None:
        assert expand("{sample}/{sample}.txt", sample=["alpha", "beta"]) == [
            "alpha/alpha.txt",
            "beta/beta.txt",
        ]

    def test_returns_template_when_no_placeholders_are_present(self) -> None:
        assert expand("results/static.txt") == ["results/static.txt"]

    def test_raises_for_missing_wildcard(self) -> None:
        with pytest.raises(ValueError, match="references wildcard"):
            expand("results/{sample}.txt", batch=["a"])

    def test_raises_for_extra_wildcard(self) -> None:
        with pytest.raises(ValueError, match="do not appear"):
            expand("results/{sample}.txt", sample=["a"], batch=["b"])

    def test_raises_for_non_simple_placeholder(self) -> None:
        with pytest.raises(ValueError, match="simple named placeholders"):
            expand("results/{sample.name}.txt", sample=[{"name": "a"}])

    def test_raises_for_string_wildcard_value(self) -> None:
        with pytest.raises(ValueError, match="must be an iterable of values"):
            expand("results/{sample}.txt", sample="abc")


class TestZipExpand:
    def test_expands_positionally(self) -> None:
        assert zip_expand("results/{sample}_{lane}.txt", sample=["a", "b"], lane=[1, 2]) == [
            "results/a_1.txt",
            "results/b_2.txt",
        ]

    def test_reuses_expand_validation_for_non_simple_placeholders(self) -> None:
        with pytest.raises(ValueError, match="simple named placeholders"):
            zip_expand("results/{sample.name}.txt", sample=["a"])

    def test_raises_for_unequal_lengths(self) -> None:
        with pytest.raises(ValueError, match="equal lengths"):
            zip_expand("results/{sample}_{lane}.txt", sample=["a"], lane=[1, 2])

    def test_returns_template_when_no_placeholders_are_present(self) -> None:
        assert zip_expand("results/static.txt") == ["results/static.txt"]


class TestSlug:
    def test_normalizes_mixed_content(self) -> None:
        assert slug("Business Desk / Q1!") == "business_desk_q1"

    def test_collapses_repeated_separators(self) -> None:
        assert slug("alpha---beta___gamma") == "alpha_beta_gamma"

    def test_returns_empty_string_for_separator_only_input(self) -> None:
        assert slug(" --- ") == ""


class TestFlatten:
    def test_flattens_nested_lists_and_tuples(self) -> None:
        assert flatten([1, (2, 3), [4, [5, 6]]]) == [1, 2, 3, 4, 5, 6]

    def test_preserves_strings_and_scalars(self) -> None:
        assert flatten(["ab", ["cd"], 3]) == ["ab", "cd", 3]

    def test_returns_empty_list_for_empty_input(self) -> None:
        assert flatten([]) == []
