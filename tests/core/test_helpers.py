"""Unit tests for workflow authoring helpers."""

from __future__ import annotations

import pytest

from ginkgo import expand, flatten, per_branch, slug, zip_expand
from ginkgo.wildcards import ExpandedTemplate


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


class TestExpandedTemplateMarker:
    def test_expand_result_remembers_its_template(self) -> None:
        result = expand("{a}_{b}", a=[1, 2], b=["x"])
        assert isinstance(result, ExpandedTemplate)
        assert result == ["1_x", "2_x"]
        assert result.template == "{a}_{b}"
        assert result.function_name == "expand"
        assert result.placeholders == ("a", "b")

    def test_zip_expand_result_remembers_its_template(self) -> None:
        result = zip_expand("{a}_{b}", a=[1], b=["x"])
        assert isinstance(result, ExpandedTemplate)
        assert result.function_name == "zip_expand"

    def test_respells_placeholders_with_fan_out_argument_names(self) -> None:
        result = expand("results/{t}_{d}.json", t=[300], d=[0.01])
        assert (
            result.as_per_branch_template(["temperature", "defect_density"])
            == "results/{temperature}_{defect_density}.json"
        )

    def test_respelling_keeps_template_when_counts_differ(self) -> None:
        result = expand("results/{t}_{d}.json", t=[300], d=[0.01])
        assert result.as_per_branch_template(["temperature"]) == "results/{t}_{d}.json"


class TestPerBranch:
    def test_renders_from_branch_values(self) -> None:
        template = per_branch("results/{sample}_{rep}.txt")
        assert template.render({"sample": "a", "rep": 2, "unused": 9}) == "results/a_2.txt"

    def test_reports_placeholder_names(self) -> None:
        assert per_branch("{sample}/{rep}").placeholder_names() == ["sample", "rep"]

    def test_rejects_template_without_placeholders(self) -> None:
        with pytest.raises(ValueError, match="no placeholders"):
            per_branch("results/out.txt")

    def test_rejects_non_simple_placeholder(self) -> None:
        with pytest.raises(ValueError, match="simple named placeholders"):
            per_branch("results/{sample.name}.txt")

    def test_rejects_non_string_template(self) -> None:
        with pytest.raises(TypeError, match="must be a string"):
            per_branch(["results/{sample}.txt"])  # type: ignore[arg-type]


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
