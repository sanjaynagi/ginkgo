"""Unit tests for workflow authoring helpers."""

from __future__ import annotations

import pytest
import yaml

from ginkgo import expand, flatten, per_branch, slug, zip_expand
from ginkgo.runtime.event_values import render_value
from ginkgo.runtime.task_runners.shell import serialize_cli_argument_value
from ginkgo.wildcards import ExpandedTemplate


def test_expand_is_a_cartesian_product_in_template_order() -> None:
    assert expand("results/{sample}.txt", sample=["a", "b"]) == ["results/a.txt", "results/b.txt"]
    assert expand("{a}_{b}", b=["x", "y"], a=[1, 2]) == ["1_x", "1_y", "2_x", "2_y"]
    assert expand("{s}/{s}.txt", s=["alpha"]) == ["alpha/alpha.txt"]
    assert expand("results/static.txt") == ["results/static.txt"]


def test_zip_expand_is_positional() -> None:
    assert zip_expand("{sample}_{lane}", sample=["a", "b"], lane=[1, 2]) == ["a_1", "b_2"]
    assert zip_expand("results/static.txt") == ["results/static.txt"]


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda: expand("results/{sample}.txt", batch=["a"]), "references wildcard"),
        (lambda: expand("results/{sample}.txt", sample=["a"], batch=["b"]), "do not appear"),
        (lambda: expand("{sample.name}", sample=[{"name": "a"}]), "simple named placeholders"),
        (lambda: expand("results/{sample}.txt", sample="abc"), "must be an iterable of values"),
        (lambda: zip_expand("{sample.name}", sample=["a"]), "simple named placeholders"),
        (lambda: zip_expand("{sample}_{lane}", sample=["a"], lane=[1, 2]), "equal lengths"),
        (lambda: per_branch("results/out.txt"), "no placeholders"),
        (lambda: per_branch("results/{sample.name}.txt"), "simple named placeholders"),
    ],
)
def test_malformed_templates_raise(call, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        call()


def test_expanded_results_remember_their_template() -> None:
    result = expand("{a}_{b}", a=[1, 2], b=["x"])

    assert isinstance(result, ExpandedTemplate)
    assert result == ["1_x", "2_x"]
    assert (result.template, result.function_name, result.placeholders) == (
        "{a}_{b}",
        "expand",
        ("a", "b"),
    )
    assert zip_expand("{a}", a=[1]).function_name == "zip_expand"


def test_unresolved_placeholders_are_matched_by_name_not_position() -> None:
    result = expand("results/{t}_{d}.json", t=[300], d=[0.01])

    assert result.unresolved_placeholders(["temperature", "d"]) == ("t",)
    assert result.unresolved_placeholders(["temperature", "defect_density"]) == ("t", "d")
    assert expand("{temperature}", temperature=[1]).unresolved_placeholders(["temperature"]) == ()


def test_serialised_arguments_rebuild_plain_lists() -> None:
    """`ExpandedTemplate` is not safe_dump-able, so normalisation must stay.

    Both paths that serialise resolved task arguments rebuild plain lists, so a
    refactor dropping that fails here rather than mid-run (PR #216 review).
    """
    paths = expand("results/{sample}.txt", sample=["a", "b"])
    with pytest.raises(yaml.YAMLError):
        yaml.safe_dump(paths)

    serialized = serialize_cli_argument_value(paths)
    rendered = render_value({"paths": paths})

    assert type(serialized) is list and type(rendered["paths"]) is list
    assert yaml.safe_load(yaml.safe_dump(serialized)) == ["results/a.txt", "results/b.txt"]
    assert yaml.safe_load(yaml.safe_dump(rendered)) == {
        "paths": ["results/a.txt", "results/b.txt"]
    }


def test_per_branch_renders_from_branch_values() -> None:
    template = per_branch("results/{sample}_{rep}.txt")

    assert template.render({"sample": "a", "rep": 2, "unused": 9}) == "results/a_2.txt"
    assert template.placeholder_names() == ["sample", "rep"]
    with pytest.raises(TypeError, match="must be a string"):
        per_branch(["results/{sample}.txt"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("text", "expected"),
    [("Business Desk / Q1!", "business_desk_q1"), ("alpha---beta___gamma", "alpha_beta_gamma")],
)
def test_slug(text: str, expected: str) -> None:
    assert slug(text) == expected
    assert slug(" --- ") == ""


def test_flatten() -> None:
    assert flatten([1, (2, 3), [4, [5, 6]]]) == [1, 2, 3, 4, 5, 6]
    assert flatten(["ab", ["cd"], 3]) == ["ab", "cd", 3]
    assert flatten([]) == []
