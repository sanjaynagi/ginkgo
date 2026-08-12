"""Tests for workflow parameter declaration and resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

import ginkgo
from ginkgo.cli.workflow_params import params_table
from ginkgo.config import config_session
from ginkgo.params import (
    ParamContext,
    ParamDecl,
    ParamError,
    extract_flag_values,
    find_global_param_reads,
    flag_for,
    format_param_help,
    resolve_param,
)


def test_flag_for_dashes_underscores():
    assert flag_for("n_replicates") == "--n-replicates"
    assert flag_for("region") == "--region"


def test_cli_beats_config_beats_default():
    with config_session(param_config={"a": 5, "b": 6}, cli_extras=["--a", "1"]):
        assert ginkgo.param("a", type=int, default=99) == 1
        assert ginkgo.param("b", type=int, default=99) == 6
        assert ginkgo.param("c", type=int, default=99) == 99


def test_sources_recorded():
    with config_session(param_config={"b": 6}, cli_extras=["--a", "1"]) as session:
        ginkgo.param("a", type=int, default=0)
        ginkgo.param("b", type=int, default=0)
        ginkgo.param("c", type=int, default=0)
        assert session.param_sources() == {"a": "cli", "b": "config", "c": "default"}


def test_resolved_params_collects_values():
    with config_session(cli_extras=["--a", "2"]) as session:
        ginkgo.param("a", type=int, default=0)
        ginkgo.param("z", default="zz")
        assert session.resolved_params() == {"a": 2, "z": "zz"}


def test_inline_equals_form():
    with config_session(cli_extras=["--label=wide"]):
        assert ginkgo.param("label", default="") == "wide"


def test_negative_number_value():
    with config_session(cli_extras=["--offset", "-3"]):
        assert ginkgo.param("offset", type=int, default=0) == -3


def test_negative_float_value():
    with config_session(cli_extras=["--offset", "-1.5e3"]):
        assert ginkgo.param("offset", type=float, default=0.0) == -1500.0


def test_single_dash_flag_is_not_taken_as_a_value():
    with config_session(cli_extras=["--label", "-x"]):
        with pytest.raises(ParamError, match="--label expects a value"):
            ginkgo.param("label", default="")


def test_type_conversion_applied():
    with config_session(cli_extras=["--ratio", "0.25", "--out", "results/x.txt"]):
        assert ginkgo.param("ratio", type=float, default=1.0) == 0.25
        assert ginkgo.param("out", type=Path, default=Path(".")) == Path("results/x.txt")


def test_config_string_coerced_to_declared_type():
    with config_session(param_config={"n": "12"}):
        assert ginkgo.param("n", type=int, default=0) == 12


def test_config_native_scalar_kept():
    with config_session(param_config={"n": 12}):
        assert ginkgo.param("n", type=int, default=0) == 12


def test_multiple_repeats_into_tuple():
    with config_session(cli_extras=["--item", "a", "--item=b", "--item", "c"]):
        assert ginkgo.param("item", multiple=True, default=()) == ("a", "b", "c")


def test_multiple_from_config_sequence():
    with config_session(param_config={"item": ["x", "y"]}):
        assert ginkgo.param("item", multiple=True, default=()) == ("x", "y")


def test_multiple_default_wrapped_in_tuple():
    with config_session():
        assert ginkgo.param("item", multiple=True, default=["a"]) == ("a",)


def test_repeated_flag_without_multiple_is_an_error():
    with config_session(cli_extras=["--a", "1", "--a", "2"]):
        with pytest.raises(ParamError, match="not declared multiple=True"):
            ginkgo.param("a", default="")


def test_config_sequence_without_multiple_is_an_error():
    with config_session(param_config={"a": [1, 2]}):
        with pytest.raises(ParamError, match="not declared multiple=True"):
            ginkgo.param("a", default="")


@pytest.mark.parametrize(
    ("tokens", "expected"),
    [
        (["--on"], True),
        (["--on", "true"], True),
        (["--on", "yes"], True),
        (["--on", "1"], True),
        (["--on", "false"], False),
        (["--on", "no"], False),
        (["--on", "0"], False),
    ],
)
def test_bool_literals(tokens, expected):
    with config_session(cli_extras=tokens):
        assert ginkgo.param("on", type=bool, default=None) is expected


def test_bool_rejects_ambiguous_literal():
    with config_session(cli_extras=["--on=maybe"]):
        with pytest.raises(ParamError, match="Cannot read 'maybe' as a boolean"):
            ginkgo.param("on", type=bool, default=False)


def test_bare_bool_flag_does_not_swallow_following_token():
    with config_session(cli_extras=["--on", "--label", "x"]) as session:
        assert ginkgo.param("on", type=bool, default=False) is True
        assert ginkgo.param("label", default="") == "x"
        assert session.unconsumed_extras() == []


def test_coercion_failure_names_parameter_and_value():
    with config_session(cli_extras=["--n", "oops"]):
        with pytest.raises(ParamError, match="Cannot read 'oops' as int for parameter 'n'"):
            ginkgo.param("n", type=int, default=0)


def test_missing_value_is_an_error():
    with config_session(cli_extras=["--n"]):
        with pytest.raises(ParamError, match="--n expects a value"):
            ginkgo.param("n", default="")


def test_choices_enforced():
    with config_session(cli_extras=["--mode", "sideways"]):
        with pytest.raises(ParamError, match="is not one of"):
            ginkgo.param("mode", choices=["fast", "slow"], default="fast")


def test_choices_allow_permitted_value():
    with config_session(cli_extras=["--mode", "slow"]):
        assert ginkgo.param("mode", choices=["fast", "slow"], default="fast") == "slow"


def test_empty_choices_rejected():
    with config_session():
        with pytest.raises(ParamError, match="empty choices"):
            ginkgo.param("mode", choices=[], default="x")


def test_required_missing_raises():
    with config_session():
        with pytest.raises(ParamError, match="is required but was not supplied"):
            ginkgo.param("region")


def test_required_supplied_resolves():
    with config_session(cli_extras=["--region", "2L"]):
        assert ginkgo.param("region") == "2L"


def test_lenient_session_resolves_required_to_none():
    with config_session(require_params=False):
        assert ginkgo.param("region") is None


def test_invalid_names_rejected():
    for name in ("9bad", "has-dash", "", "has space"):
        with config_session():
            with pytest.raises(ParamError, match="Invalid parameter name"):
                ginkgo.param(name, default=1)


def test_identical_redeclaration_is_a_noop():
    with config_session(cli_extras=["--a", "3"]) as session:
        assert ginkgo.param("a", type=int, default=1) == 3
        assert ginkgo.param("a", type=int, default=1) == 3
        assert len(session.declarations) == 1


def test_conflicting_redeclaration_rejected():
    with config_session():
        ginkgo.param("a", type=int, default=1)
        with pytest.raises(ParamError, match="declared twice with different settings"):
            ginkgo.param("a", type=str, default="x")


def test_no_session_returns_default():
    assert ginkgo.param("a", type=int, default=7) == 7


def test_no_session_required_explains_itself():
    with pytest.raises(ParamError, match="no ginkgo run context is active"):
        ginkgo.param("region")


def test_unconsumed_extras_reported():
    with config_session(cli_extras=["--a", "1", "--nope", "2"]) as session:
        ginkgo.param("a", type=int, default=0)
        assert session.unconsumed_extras() == ["--nope", "2"]


def test_extract_flag_values_only_claims_its_own_tokens():
    decl = ParamDecl(name="a", default="")
    values, consumed = extract_flag_values(["--b", "1", "--a", "2", "--c=3"], decl)
    assert values == ["2"]
    assert consumed == {2, 3}


def test_extract_flag_values_ignores_prefix_collisions():
    decl = ParamDecl(name="a", default="")
    values, _ = extract_flag_values(["--ab", "1", "--a-b", "2"], decl)
    assert values == []


def test_resolve_param_require_false_keeps_default_source():
    decl = ParamDecl(name="a")
    resolution = resolve_param(decl, cli_values=(), config_values={}, require=False)
    assert resolution.value is None
    assert resolution.source == "default"


def test_param_context_round_trip():
    context = ParamContext(config={"a": 1}, cli_extras=("--a", "2"))
    assert ParamContext.from_payload(context.to_payload()) == context


def test_param_context_from_empty_payload():
    context = ParamContext.from_payload({})
    assert context.config == {}
    assert context.cli_extras == ()


def test_decl_payload_describes_declaration():
    decl = ParamDecl(
        name="n_reps",
        value_type=int,
        default=12,
        help_text="Replicates",
        choices=(6, 12),
    )
    assert decl.to_payload() == {
        "name": "n_reps",
        "flag": "--n-reps",
        "type": "int",
        "required": False,
        "default": 12,
        "help": "Replicates",
        "choices": [6, 12],
        "multiple": False,
    }


def test_decl_payload_renders_non_json_default_as_string():
    decl = ParamDecl(name="out", value_type=Path, default=Path("a/b.txt"))
    assert decl.to_payload()["default"] == "a/b.txt"


def test_required_decl_payload_has_no_default():
    payload = ParamDecl(name="region").to_payload()
    assert payload["required"] is True
    assert payload["default"] is None


def test_format_param_help_lines():
    lines = format_param_help(
        [
            ParamDecl(name="n_reps", value_type=int, default=12, help_text="Replicates"),
            ParamDecl(name="region", help_text="Genome region"),
            ParamDecl(name="verbose", value_type=bool, default=False),
        ]
    )
    assert lines[0].strip() == "--n-reps INT  Replicates (default: 12)"
    assert lines[1].strip() == "--region STR  Genome region (required)"
    assert lines[2].strip() == "--verbose     (default: False)"


def test_format_param_help_empty():
    assert format_param_help([]) == []


def test_nested_sessions_do_not_leak_declarations():
    with config_session(cli_extras=["--a", "1"]) as outer:
        ginkgo.param("a", type=int, default=0)
        with config_session(cli_extras=["--b", "2"]) as inner:
            ginkgo.param("b", type=int, default=0)
            assert set(inner.declarations) == {"b"}
        assert set(outer.declarations) == {"a"}


def _task_reading_global(param_globals: dict) -> object:
    """Build a function whose body loads ``tag`` as a global of *param_globals*."""
    source = "def reads_tag():\n    return tag\n"
    exec(compile(source, "<generated>", "exec"), param_globals)
    return param_globals["reads_tag"]


def test_find_global_param_reads_flags_a_global_read():
    param_globals: dict = {}
    function = _task_reading_global(param_globals)

    findings = find_global_param_reads(
        declaration_globals={"tag": param_globals},
        tasks=[("write_it", function)],
    )

    assert [(item.task_name, item.param_name) for item in findings] == [("write_it", "tag")]
    assert "Pass it as an argument" in findings[0].message()
    assert "--tag" in findings[0].message()


def test_find_global_param_reads_ignores_argument_use():
    param_globals: dict = {}
    exec(compile("def uses_arg(tag):\n    return tag\n", "<generated>", "exec"), param_globals)

    findings = find_global_param_reads(
        declaration_globals={"tag": param_globals},
        tasks=[("write_it", param_globals["uses_arg"])],
    )

    assert findings == []


def test_find_global_param_reads_ignores_same_name_in_another_module():
    """A global of the same name in a module that did not declare it is not a violation."""
    declaring_globals: dict = {}
    other_globals: dict = {}
    function = _task_reading_global(other_globals)

    findings = find_global_param_reads(
        declaration_globals={"tag": declaring_globals},
        tasks=[("write_it", function)],
    )

    assert findings == []


def test_find_global_param_reads_ignores_attribute_of_the_same_name():
    """Detection reads LOAD_GLOBAL, not co_names, so an attribute access is not flagged."""
    param_globals: dict = {}
    exec(
        compile("def uses_attr(obj):\n    return obj.tag\n", "<generated>", "exec"),
        param_globals,
    )

    findings = find_global_param_reads(
        declaration_globals={"tag": param_globals},
        tasks=[("write_it", param_globals["uses_attr"])],
    )

    assert findings == []


def test_find_global_param_reads_without_declarations():
    param_globals: dict = {}
    function = _task_reading_global(param_globals)
    assert find_global_param_reads(declaration_globals={}, tasks=[("t", function)]) == []


def test_find_global_param_reads_tolerates_non_python_callables():
    assert (
        find_global_param_reads(
            declaration_globals={"tag": {}},
            tasks=[("t", len), ("u", None)],
        )
        == []
    )


def test_declaring_globals_recorded_on_the_session():
    with config_session() as session:
        ginkgo.param("tag", default="a")
        assert session.declaration_globals["tag"] is globals()


def test_params_table_layers_across_sources():
    """An override that sets one parameter must leave the rest of the base table intact."""
    assert params_table([{"params": {"a": 1, "b": 2}}, {"params": {"a": 9}}]) == {"a": 9, "b": 2}


def test_params_table_ignores_sources_without_a_table():
    assert params_table([{"other": 1}, {"params": {"a": 1}}, {}]) == {"a": 1}


def test_params_table_empty_when_no_source_declares_one():
    assert params_table([{"other": 1}, {}]) == {}


def test_params_table_rejects_a_non_mapping_table():
    with pytest.raises(TypeError, match="must be a mapping"):
        params_table([{"params": ["a", "b"]}])
